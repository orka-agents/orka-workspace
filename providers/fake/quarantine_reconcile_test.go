// Copyright (c) 2026. MIT License - see LICENSE file for details.

package fake

import (
	"testing"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Core authorizes quarantine retirement as an exact-instance Stop, never as the
// data-preserving Suspend a Suspend-capable class otherwise receives. The
// provider must stop that instance and keep its data and journal for deletion.
func TestQuarantineStopAuthorizationStopsExactInstance(t *testing.T) {
	f := newConditionContractFixture(t)
	reconcileConditionContract(t, f)
	waitForConditionReady(t, f)
	workspace := conditionContractWorkspace(t, f)
	identity := workspace.Status.Allocation.Identity
	workspace.Spec.DesiredState = api.ExecutionWorkspaceDesiredQuarantined
	workspace.Spec.Attachment = nil
	workspace.Spec.Retirement = &api.WorkloadRetirement{Sequence: f.request.Sequence, Identity: identity, Action: api.WorkloadRetirementStop}
	workspace.Generation++
	if err := f.client.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	request := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: f.request.Key.Namespace, Name: f.request.Key.Name}}
	for range 25 {
		if _, err := f.reconciler(f.client).Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		f.tick()
		workspace = conditionContractWorkspace(t, f)
		if workspace.Status.Allocation != nil && workspace.Status.Allocation.State == sdk.AllocationStopped {
			break
		}
	}
	allocation := workspace.Status.Allocation
	if allocation == nil || allocation.State != sdk.AllocationStopped || allocation.Identity != identity || allocation.Startup != nil {
		t.Fatalf("quarantine Stop authorization did not stop the exact instance: %#v", allocation)
	}
	if workspace.Status.ObservedGeneration != workspace.Generation || workspace.Status.State != api.ExecutionWorkspaceStateQuarantined {
		t.Fatalf("quarantined stop reported state %q at generation %d/%d", workspace.Status.State, workspace.Status.ObservedGeneration, workspace.Generation)
	}
	if workspace.Status.Disposition != nil || sdk.ConditionIsTrue(workspace.Status.Conditions, string(api.ConditionWorkspaceFinalized)) {
		t.Fatal("quarantine Stop claimed deletion or finalization without a Delete authorization")
	}
	if !sdk.ConditionIsFalse(workspace.Status.Conditions, string(api.ConditionWorkspaceDataPlaneReady)) {
		t.Fatal("quarantined stopped instance still advertised a ready data plane")
	}
}
