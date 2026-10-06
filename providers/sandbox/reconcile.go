package sandbox

import (
	"context"
	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

func (r *ExecutionWorkspaceReconciler) reconcileLifecycle(ctx context.Context, workspace *workspacev1alpha1.ExecutionWorkspace) (ctrl.Result, error) {
	pending := false
	var operationErr error
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current := &workspacev1alpha1.ExecutionWorkspace{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
			return client.IgnoreNotFound(err)
		}
		provider := &workspacev1alpha1.ExecutionWorkspaceProvider{}
		if err := r.Get(ctx, types.NamespacedName{Name: current.Spec.ProviderBinding.Name}, provider); err != nil {
			return client.IgnoreNotFound(err)
		}
		if provider.UID != current.Spec.ProviderBinding.UID || provider.Spec.ControllerName != ControllerName {
			return nil
		}
		deleted := !current.DeletionTimestamp.IsZero() || current.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredDeleted
		quarantined := current.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined
		revoking := workspaceNeedsAttachmentRevocation(current)
		admitted := workspaceCurrentlyAdmittedByCore(current)
		maintenance := deleted || quarantined || revoking || current.Spec.Retirement != nil || current.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
		if !maintenance && !admitted {
			pending = true
			return nil
		}
		// Acknowledge the attachment before Pod startup. This signal lets core
		// establish runtime demand; it must not depend on its own bootstrap work.
		before := current.DeepCopy()
		current.Status.AttachedEpoch = 0
		if admitted && provider.Spec.LifecycleState != workspacev1alpha1.ExecutionWorkspaceProviderDisabled && !deleted && !quarantined && current.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredReady && current.Spec.Attachment != nil {
			current.Status.AttachedEpoch = current.Spec.Attachment.Epoch
			current.Status.State = workspacev1alpha1.ExecutionWorkspaceStateAttached
		}
		attached := current.Status.AttachedEpoch > 0
		workspaceprovider.SetCondition(&current.Status.Conditions, metav1.Condition{Type: string(workspacev1alpha1.ConditionWorkspaceAttached), Status: conditionStatus(attached), Reason: conditionReason(attached, string(workspacev1alpha1.ReasonAttachmentRevoked)), Message: chooseMessage(attached, "attachment epoch acknowledged; runtime startup is independent", "no attachment epoch is active"), ObservedGeneration: current.Generation})
		if !reflect.DeepEqual(before.Status, current.Status) {
			if err := r.Status().Patch(ctx, current, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return err
			}
		}
		before = current.DeepCopy()
		driver := New(r.Client)
		key := workspaceprovider.AllocationKey{Namespace: current.Namespace, Name: current.Name, WorkspaceUID: current.UID, ProviderUID: current.Spec.ProviderBinding.UID}
		observed, err := driver.Observe(ctx, key)
		if deleted || quarantined || current.Spec.Retirement != nil {
			// Cleanup is authorized by the durable instance fence, even when an
			// admission webhook changed the Pod so ordinary observation rejects it.
			_, record, readErr := driver.read(ctx, key)
			err = readErr
			if record != nil {
				observed = record.Observation
			}
		}
		missing := err == workspaceprovider.ErrNotFound
		operationErr = nil
		if err != nil && !missing {
			operationErr = err
		}
		if operationErr == nil {
			switch {
			case deleted || quarantined:
				if missing {
					operationErr = driver.proveNoAllocation(ctx, key)
				} else if workspaceprovider.ValidateWorkloadRetirement(current.Spec.Workload, &observed, current.Spec.Retirement, workspacev1alpha1.WorkloadRetirementStop) == nil {
					observed, operationErr = driver.StopInstance(ctx, key, observed.Identity)
					if operationErr == nil && deleted && observed.State == workspaceprovider.AllocationStopped && workspaceprovider.ValidateWorkloadRetirement(current.Spec.Workload, &observed, current.Spec.Retirement, workspacev1alpha1.WorkloadRetirementDelete) == nil {
						observed, operationErr = driver.DeleteAllocation(ctx, key, observed.Identity, current.Spec.Lifecycle.DeletionPolicy)
					}
				}
			case current.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredSuspended || (current.Spec.Retirement != nil && current.Spec.Retirement.Action == workspacev1alpha1.WorkloadRetirementSuspend):
				if !missing && workspaceprovider.ValidateWorkloadRetirement(current.Spec.Workload, &observed, current.Spec.Retirement, workspacev1alpha1.WorkloadRetirementSuspend) == nil {
					observed, operationErr = driver.SuspendInstance(ctx, key, observed.Identity)
				}
			case !missing && current.Spec.Retirement != nil && workspaceprovider.ValidateWorkloadRetirement(current.Spec.Workload, &observed, current.Spec.Retirement, workspacev1alpha1.WorkloadRetirementStop) == nil:
				observed, operationErr = driver.StopInstance(ctx, key, observed.Identity)
			case !revoking && current.Spec.Retirement == nil && current.Spec.Workload != nil:
				observed, operationErr = driver.EnsureAllocation(ctx, *current.Spec.Workload)
				if operationErr == nil {
					missing = false
				}
			}
		}
		current.Status.ObservedGeneration = current.Generation
		current.Status.ProviderBinding = &workspacev1alpha1.ExecutionWorkspaceProviderBindingStatus{ContractVersion: workspacev1alpha1.ContractVersionV1, AdapterVersion: AdapterVersion, BackendAPIVersion: "agents.x-k8s.io/v1beta1"}
		current.Status.Endpoints = nil
		current.Status.ConnectionSecretRef = nil
		current.Status.Disposition = nil
		ready := operationErr == nil && !missing && observed.State == workspaceprovider.AllocationReady
		if operationErr != nil {
			// A read failure is not termination. Preserve the exact last fence while
			// withdrawing its startup claim so core cannot keep admission open.
			if current.Status.Allocation != nil {
				observed = *current.Status.Allocation
				observed.State = workspaceprovider.AllocationPending
				observed.Startup = nil
				current.Status.Allocation = &observed
			}
			current.Status.State = workspacev1alpha1.ExecutionWorkspaceStatePending
			if attached {
				current.Status.State = workspacev1alpha1.ExecutionWorkspaceStateAttached
			}
		} else {
			if !missing {
				current.Status.Allocation = &observed
				current.Status.ExternalID = observed.Identity.AllocationID
			}
			switch {
			case deleted && (missing || observed.State == workspaceprovider.AllocationDeleted):
				current.Status.State = workspacev1alpha1.ExecutionWorkspaceStateDeleted
				current.Status.Disposition = observed.Disposition
				if missing {
					current.Status.Disposition = deletedDisposition(false)
				}
			case deleted:
				current.Status.State = workspacev1alpha1.ExecutionWorkspaceStateDeleting
			case quarantined && (missing || observed.State == workspaceprovider.AllocationStopped || observed.State == workspaceprovider.AllocationDeleted):
				current.Status.State = workspacev1alpha1.ExecutionWorkspaceStateQuarantined
			case current.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredSuspended && observed.State == workspaceprovider.AllocationStopped && observed.RetainedData != nil:
				current.Status.State = workspacev1alpha1.ExecutionWorkspaceStateSuspended
			case current.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredSuspended:
				current.Status.State = workspacev1alpha1.ExecutionWorkspaceStateSuspending
			case attached:
				current.Status.State = workspacev1alpha1.ExecutionWorkspaceStateAttached
			case ready:
				current.Status.State = workspacev1alpha1.ExecutionWorkspaceStateReady
			default:
				current.Status.State = workspacev1alpha1.ExecutionWorkspaceStatePending
			}
		}
		pending = operationErr == nil && !ready && current.Status.State != workspacev1alpha1.ExecutionWorkspaceStateDeleted && current.Status.State != workspacev1alpha1.ExecutionWorkspaceStateSuspended && current.Status.State != workspacev1alpha1.ExecutionWorkspaceStateQuarantined
		workspaceprovider.SetCondition(&current.Status.Conditions, metav1.Condition{Type: string(workspacev1alpha1.ConditionWorkspaceDataPlaneReady), Status: conditionStatus(ready), Reason: conditionReason(ready, string(workspacev1alpha1.ReasonProgressing)), Message: chooseMessage(ready, "allocated instance is available for core bootstrap and verification", "allocation is pending or retired"), ObservedGeneration: current.Generation})
		return r.Status().Patch(ctx, current, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	})
	if err != nil {
		return ctrl.Result{}, err
	}
	if operationErr != nil {
		return ctrl.Result{}, operationErr
	}
	if pending {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	return ctrl.Result{RequeueAfter: providerHeartbeatPeriod}, nil
}

func workspaceHasCoreAdmission(workspace *workspacev1alpha1.ExecutionWorkspace) bool {
	if workspace == nil || workspace.Spec.CoreAdmission == nil {
		return false
	}
	return workspace.Spec.CoreAdmission.ClassBinding == workspace.Spec.ClassBinding && workspace.Spec.CoreAdmission.ProviderBinding == workspace.Spec.ProviderBinding
}

func workspaceCurrentlyAdmittedByCore(workspace *workspacev1alpha1.ExecutionWorkspace) bool {
	if !workspaceHasCoreAdmission(workspace) || workspace.Spec.CoreAdmission.AdmittedGeneration != workspace.Generation {
		return false
	}
	condition := workspaceprovider.FindCondition(workspace.Status.Conditions, string(workspacev1alpha1.ConditionWorkspaceAdmitted))
	return condition != nil && condition.Status == metav1.ConditionTrue && condition.Reason == string(workspacev1alpha1.ReasonReady) && condition.ObservedGeneration == workspace.Generation
}

func workspaceNeedsAttachmentRevocation(workspace *workspacev1alpha1.ExecutionWorkspace) bool {
	return workspace != nil && workspaceHasCoreAdmission(workspace) && workspace.Spec.Attachment == nil &&
		(workspace.Status.AttachedEpoch > 0 || workspaceprovider.ConditionIsTrue(workspace.Status.Conditions, string(workspacev1alpha1.ConditionWorkspaceAttached)))
}

func workspaceHasMaintenanceIntent(workspace *workspacev1alpha1.ExecutionWorkspace) bool {
	return workspace != nil && (workspace.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredDeleted || workspace.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined)
}

func workspaceComputeCapacityReleased(workspace *workspacev1alpha1.ExecutionWorkspace) bool {
	return workspace != nil && (workspace.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredDeleted || !workspace.DeletionTimestamp.IsZero()) &&
		workspace.Status.ObservedGeneration == workspace.Generation && workspace.Status.State == workspacev1alpha1.ExecutionWorkspaceStateDeleted && workspace.Status.Disposition != nil &&
		(workspace.Status.Disposition.Compute == workspacev1alpha1.DispositionDeleted || workspace.Status.Disposition.Compute == workspacev1alpha1.DispositionNotApplicable)
}

func conditionStatus(ok bool) metav1.ConditionStatus {
	if ok {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}
func conditionReason(ok bool, failure string) string {
	if ok {
		return string(workspacev1alpha1.ReasonReady)
	}
	return failure
}
func chooseMessage(ok bool, success, failure string) string {
	if ok {
		return success
	}
	return failure
}
