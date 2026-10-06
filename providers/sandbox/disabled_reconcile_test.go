// Copyright (c) 2026. MIT License - see LICENSE file for details.

package sandbox

import (
	"reflect"
	"testing"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func disableConditionFixture(t *testing.T, f *conditionContractFixture, admission string, staleSignals bool) {
	t.Helper()
	workspace := conditionContractWorkspace(t, *f)
	provider := &api.ExecutionWorkspaceProvider{}
	if err := f.client.Get(t.Context(), client.ObjectKey{Name: workspace.Spec.ProviderBinding.Name}, provider); err != nil {
		t.Fatal(err)
	}
	provider.Spec.LifecycleState = api.ExecutionWorkspaceProviderDisabled
	if err := f.client.Update(t.Context(), provider); err != nil {
		t.Fatal(err)
	}
	switch admission {
	case "missing":
		workspace.Spec.CoreAdmission = nil
	case "stale":
		workspace.Generation++
	case "withdrawn":
		for i := range workspace.Status.Conditions {
			if workspace.Status.Conditions[i].Type == string(api.ConditionWorkspaceAdmitted) {
				workspace.Status.Conditions[i].Status = metav1.ConditionFalse
			}
		}
	}
	if staleSignals {
		workspace.Spec.AttachmentEpoch = 7
		workspace.Spec.Attachment = &api.ExecutionWorkspaceAttachment{Epoch: 7}
		workspace.Status.AttachedEpoch = 7
		workspace.Status.State = api.ExecutionWorkspaceStateReady
		workspace.Status.Endpoints = []api.ExecutionWorkspaceEndpoint{{Name: "data-plane", URL: "http://stale.invalid"}}
		workspace.Status.ConnectionSecretRef = &api.SecretReference{Name: f.credential.Name}
		if workspace.Status.Allocation != nil {
			workspace.Status.Allocation.State = sdk.AllocationReady
			workspace.Status.Allocation.Startup = &sdk.StartupEvidence{ContractVersion: sdk.LifecycleContractV1, Identity: workspace.Status.Allocation.Identity, Endpoint: "http://stale.invalid"}
		}
		for _, typ := range []api.ExecutionWorkspaceConditionType{api.ConditionWorkspaceAttached, api.ConditionWorkspaceDataPlaneReady} {
			sdk.SetCondition(&workspace.Status.Conditions, metav1.Condition{Type: string(typ), Status: metav1.ConditionTrue, Reason: string(api.ReasonReady), ObservedGeneration: workspace.Generation})
		}
	}
	status := workspace.Status.DeepCopy()
	if err := f.client.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	workspace.Status = *status
	if err := f.client.Status().Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	workspace = conditionContractWorkspace(t, *f)
	for i := range f.coreConditions {
		f.coreConditions[i] = *sdk.FindCondition(workspace.Status.Conditions, f.coreConditions[i].Type)
	}
}

func reconcileDisabledFixture(t *testing.T, f conditionContractFixture) (ctrl.Result, error) {
	t.Helper()
	return f.reconciler(f.client).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKey{Namespace: f.request.Key.Namespace, Name: f.request.Key.Name}})
}

func assertDisabledSignals(t *testing.T, f conditionContractFixture, provisioned metav1.ConditionStatus) *api.ExecutionWorkspace {
	t.Helper()
	workspace := assertConditionContract(t, f, provisioned, metav1.ConditionFalse)
	if workspace.Status.AttachedEpoch != 0 || len(workspace.Status.Endpoints) != 0 || workspace.Status.ConnectionSecretRef != nil ||
		workspace.Status.State == api.ExecutionWorkspaceStateReady || workspace.Status.State == api.ExecutionWorkspaceStateAttached {
		t.Fatal("Disabled provider retained continuation signals")
	}
	if workspace.Status.Allocation != nil && (workspace.Status.Allocation.State == sdk.AllocationReady || workspace.Status.Allocation.Startup != nil) {
		t.Fatal("Disabled provider retained startup evidence")
	}
	for _, typ := range []api.ExecutionWorkspaceConditionType{api.ConditionWorkspaceAttached, api.ConditionWorkspaceDataPlaneReady} {
		condition := sdk.FindCondition(workspace.Status.Conditions, string(typ))
		if condition == nil || condition.Status != metav1.ConditionFalse || condition.ObservedGeneration != workspace.Generation {
			t.Fatalf("Disabled provider retained stale %s acknowledgement", typ)
		}
	}
	return workspace
}

func TestDisabledProviderWithdrawsSignalsWithoutProgressingAllocation(t *testing.T) {
	for _, admission := range []string{"current", "missing", "stale", "withdrawn"} {
		t.Run(admission, func(t *testing.T) {
			f := newConditionContractFixture(t)
			reconcileConditionContract(t, f)
			waitForConditionReady(t, f)
			identity := conditionContractWorkspace(t, f).Status.Allocation.Identity
			before := journalProtectionMap(t, f, journalKey(f.request.Key)).DeepCopy()
			disableConditionFixture(t, &f, admission, true)
			result, err := reconcileDisabledFixture(t, f)
			if err != nil || result.RequeueAfter <= time.Second {
				t.Fatalf("Disabled observation spun or failed: %+v %v", result, err)
			}
			workspace := assertDisabledSignals(t, f, metav1.ConditionTrue)
			if workspace.Status.Allocation == nil || workspace.Status.Allocation.Identity != identity || workspace.Status.ExternalID != identity.AllocationID {
				t.Fatal("Disabled observation discarded the exact live fence")
			}
			if !reflect.DeepEqual(before, journalProtectionMap(t, f, journalKey(f.request.Key))) {
				t.Fatal("Disabled observation changed durable allocation intent")
			}
			workspace.Spec.DesiredState = api.ExecutionWorkspaceDesiredDeleted
			workspace.Spec.Retirement = &api.WorkloadRetirement{Sequence: f.request.Sequence, Identity: identity, Action: api.WorkloadRetirementDelete}
			workspace.Generation++
			if err := f.client.Update(t.Context(), workspace); err != nil {
				t.Fatal(err)
			}
			for range 25 {
				if _, err := reconcileDisabledFixture(t, f); err != nil {
					t.Fatal(err)
				}
				f.tick()
				if sdk.ConditionIsTrue(conditionContractWorkspace(t, f).Status.Conditions, string(api.ConditionWorkspaceFinalized)) {
					break
				}
			}
			workspace = assertConditionContract(t, f, metav1.ConditionFalse, metav1.ConditionTrue)
			if workspace.Status.Allocation.Identity != identity || workspace.Status.State != api.ExecutionWorkspaceStateDeleted ||
				sdk.ValidateDeletedDisposition(workspace.Status.Disposition, workspace.Spec.Lifecycle.DeletionPolicy) != nil {
				t.Fatal("Disabled provider did not finish exact policy-compliant retirement")
			}
		})
	}
}

func TestDisabledProviderNeverAllocatesAndUsesHeartbeat(t *testing.T) {
	f := newConditionContractFixture(t)
	before := &corev1.ConfigMapList{}
	if err := f.client.List(t.Context(), before); err != nil {
		t.Fatal(err)
	}
	disableConditionFixture(t, &f, "missing", false)
	result, err := reconcileDisabledFixture(t, f)
	if err != nil || result.RequeueAfter <= time.Second {
		t.Fatalf("Disabled no-allocation observation spun or failed: %+v %v", result, err)
	}
	workspace := assertDisabledSignals(t, f, metav1.ConditionFalse)
	if workspace.Status.Allocation != nil || workspace.Status.Disposition != nil {
		t.Fatal("Disabled provider fabricated an allocation or cleanup")
	}
	after := &corev1.ConfigMapList{}
	if err := f.client.List(t.Context(), after); err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("Disabled provider created allocation intent")
	}
}

func TestDisabledProviderLostJournalPreservesUncertaintyAndFence(t *testing.T) {
	f := newConditionContractFixture(t)
	reconcileConditionContract(t, f)
	identity := conditionContractWorkspace(t, f).Status.Allocation.Identity
	cm := journalProtectionMap(t, f, journalKey(f.request.Key))
	cm.Finalizers = nil
	if err := f.client.Update(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	if err := f.client.Delete(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	disableConditionFixture(t, &f, "missing", true)
	if _, err := reconcileDisabledFixture(t, f); err == nil {
		t.Fatal("Disabled provider interpreted lost recovery evidence as absence")
	}
	workspace := assertDisabledSignals(t, f, metav1.ConditionUnknown)
	if workspace.Status.Allocation == nil || workspace.Status.Allocation.Identity != identity || workspace.Status.Allocation.State != sdk.AllocationPending ||
		workspace.Status.Disposition != nil || workspace.Status.State == api.ExecutionWorkspaceStateDeleted {
		t.Fatal("Disabled missing-journal observation discarded its fence or claimed cleanup")
	}
}
