package fake

import (
	"context"
	"fmt"
	"strings"
	"time"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// FixtureRequest is fixed public simulation input, never a launchable runtime
// request. Shared schema v1 has no workload field; this remains a status fixture.
func FixtureRequest(workspace *workspacev1alpha1.ExecutionWorkspace) workspaceprovider.WorkloadRequest {
	request := workspaceprovider.WorkloadRequest{
		Key:   workspaceprovider.AllocationKey{Namespace: workspace.Namespace, Name: workspace.Name, WorkspaceUID: workspace.UID, ProviderUID: workspace.Spec.ProviderBinding.UID},
		Image: "fixture.invalid/status-only@sha256:" + strings.Repeat("0", 64),
		Args:  []string{string(workspace.Spec.ClassBinding.UID), workspace.Spec.ClassBinding.ProfileHash},
	}
	request.Revision, _ = workspaceprovider.WorkloadRevision(request)
	return request
}

func (r *FakeExecutionWorkspaceReconciler) reconcileLifecycle(ctx context.Context, workspace *workspacev1alpha1.ExecutionWorkspace) (ctrl.Result, error) {
	admissionPending := false
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current := &workspacev1alpha1.ExecutionWorkspace{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
			return client.IgnoreNotFound(err)
		}
		provider := &workspacev1alpha1.ExecutionWorkspaceProvider{}
		if err := r.Get(ctx, types.NamespacedName{Name: current.Spec.ProviderBinding.Name}, provider); err != nil {
			return client.IgnoreNotFound(err)
		}
		if provider.UID != current.Spec.ProviderBinding.UID || provider.Spec.ControllerName != FakeWorkspaceControllerName {
			return nil
		}
		deleted := !current.DeletionTimestamp.IsZero() || current.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredDeleted
		quarantined := current.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined
		revoking := workspaceNeedsAttachmentRevocation(current)
		maintenance := deleted || quarantined || revoking
		if !maintenance && !workspaceCurrentlyAdmittedByCore(current) {
			admissionPending = true
			return nil
		}
		if !maintenance {
			withinCapacity, err := r.workspaceWithinPoolCapacity(ctx, current)
			if err != nil {
				return err
			}
			if !withinCapacity {
				admissionPending = true
				return nil
			}
		}
		before := current.DeepCopy()
		driver := New(r.Client)
		request := FixtureRequest(current)
		observed, err := driver.Observe(ctx, request.Key)
		missing := err == workspaceprovider.ErrNotFound
		if err != nil && !missing {
			return err
		}
		if deleted || quarantined || current.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredSuspended {
			if !missing {
				observed, err = driver.StopInstance(ctx, request.Key, observed.Identity)
				if err != nil {
					return err
				}
				if deleted {
					observed, err = driver.DeleteAllocation(ctx, request.Key, observed.Identity, current.Spec.Lifecycle.DeletionPolicy)
					if err != nil {
						return err
					}
				}
			}
		} else if !revoking {
			observed, err = driver.EnsureAllocation(ctx, request)
			if err != nil {
				return err
			}
		}
		current.Status.ObservedGeneration = current.Generation
		current.Status.ExternalID = observed.Identity.AllocationID
		current.Status.ProviderBinding = &workspacev1alpha1.ExecutionWorkspaceProviderBindingStatus{ContractVersion: workspacev1alpha1.ContractVersionV1, AdapterVersion: fakeWorkspaceAdapterVersion, BackendAPIVersion: "fake.workspace.orka.ai/v1"}
		current.Status.Endpoints = nil
		current.Status.ConnectionSecretRef = nil
		current.Status.AttachedEpoch = 0
		switch {
		case deleted:
			current.Status.State = workspacev1alpha1.ExecutionWorkspaceStateDeleted
			current.Status.Disposition = observed.Disposition
			if missing {
				current.Status.Disposition = fakeDeletedDisposition(current.Spec.Lifecycle.DeletionPolicy)
			}
		case quarantined:
			current.Status.State = workspacev1alpha1.ExecutionWorkspaceStateQuarantined
		case current.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredSuspended:
			current.Status.State = workspacev1alpha1.ExecutionWorkspaceStateSuspended
		case observed.State == workspaceprovider.AllocationReady:
			current.Status.State = workspacev1alpha1.ExecutionWorkspaceStateReady
			if !revoking && current.Spec.Attachment != nil {
				current.Status.State = workspacev1alpha1.ExecutionWorkspaceStateAttached
				current.Status.AttachedEpoch = current.Spec.Attachment.Epoch
			}
		case observed.State == workspaceprovider.AllocationStopped || observed.State == workspaceprovider.AllocationDeleted:
			// The lifecycle deliberately cannot resurrect the same instance. Resume
			// is not an advertised feature of this status-only fixture provider.
			current.Status.State = workspacev1alpha1.ExecutionWorkspaceStateFailed
		default:
			current.Status.State = workspacev1alpha1.ExecutionWorkspaceStatePending
		}
		ready := current.Status.State == workspacev1alpha1.ExecutionWorkspaceStateReady || current.Status.State == workspacev1alpha1.ExecutionWorkspaceStateAttached
		workspaceprovider.SetCondition(&current.Status.Conditions, metav1.Condition{Type: string(workspacev1alpha1.ConditionWorkspaceDataPlaneReady), Status: conditionStatus(ready), Reason: conditionReason(ready, string(workspacev1alpha1.ReasonProgressing)), Message: chooseMessage(ready, "fake fixture is ready; no runtime or data-plane endpoint is provided", "fake fixture is stopped or not allocated; stopped instances cannot resume"), ObservedGeneration: current.Generation})
		attached := current.Status.AttachedEpoch > 0
		workspaceprovider.SetCondition(&current.Status.Conditions, metav1.Condition{Type: string(workspacev1alpha1.ConditionWorkspaceAttached), Status: conditionStatus(attached), Reason: conditionReason(attached, string(workspacev1alpha1.ReasonAttachmentRevoked)), Message: chooseMessage(attached, "fixture attachment epoch is acknowledged", "no fixture attachment epoch is active"), ObservedGeneration: current.Generation})
		return r.Status().Patch(ctx, current, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	})
	if admissionPending {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, err
	}
	return ctrl.Result{}, err
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

// ConformanceFixture creates only the core-owned fixture objects needed for
// Lifecycle conformance. It is used with an isolated fake client, never to grant
// admission to live workspaces.
func ConformanceFixture(ctx context.Context, c client.Client) (workspaceprovider.WorkloadRequest, error) {
	provider := &workspacev1alpha1.ExecutionWorkspaceProvider{ObjectMeta: metav1.ObjectMeta{Name: "fake", UID: "conformance-provider"}, Spec: workspacev1alpha1.ExecutionWorkspaceProviderSpec{ControllerName: FakeWorkspaceControllerName, LifecycleState: workspacev1alpha1.ExecutionWorkspaceProviderActive}}
	workspace := &workspacev1alpha1.ExecutionWorkspace{ObjectMeta: metav1.ObjectMeta{Namespace: "conformance", Name: "fixture", UID: "conformance-workspace", Generation: 1}}
	workspace.Spec.ClassBinding = workspacev1alpha1.ImmutableObjectBinding{Name: "fixture", UID: "conformance-class", Generation: 1, ProfileHash: "fixture"}
	workspace.Spec.ProviderBinding = workspacev1alpha1.ImmutableObjectBinding{Name: provider.Name, UID: provider.UID, Generation: 1}
	workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredReady
	workspace.Spec.CoreAdmission = &workspacev1alpha1.ExecutionWorkspaceCoreAdmission{ClassBinding: workspace.Spec.ClassBinding, ProviderBinding: workspace.Spec.ProviderBinding, AdmittedGeneration: workspace.Generation}
	workspace.Status.Conditions = []metav1.Condition{{Type: string(workspacev1alpha1.ConditionWorkspaceAdmitted), Status: metav1.ConditionTrue, Reason: string(workspacev1alpha1.ReasonReady), ObservedGeneration: workspace.Generation, LastTransitionTime: metav1.Now()}}
	for _, object := range []client.Object{provider, workspace} {
		if err := c.Create(ctx, object); err != nil {
			return workspaceprovider.WorkloadRequest{}, fmt.Errorf("create conformance fixture: %w", err)
		}
	}
	return FixtureRequest(workspace), nil
}
