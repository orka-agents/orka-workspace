package fake

import (
	"errors"
	"reflect"
	"testing"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func bindingWorkspace(t *testing.T, c client.Client, key sdk.AllocationKey) *api.ExecutionWorkspace {
	t.Helper()
	current := &api.ExecutionWorkspace{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: key.Namespace, Name: key.Name}, current); err != nil {
		t.Fatal(err)
	}
	return current
}

// The referenced workspace has valid admission of its own; validating only
// inside EnsureAllocation would therefore launch it for the wrong reconciler.
func TestReconcileRejectsAnotherWorkspaceWorkload(t *testing.T) {
	c, request := runtimeFixture(t)
	otherBefore := bindingWorkspace(t, c, request.Key)
	current := otherBefore.DeepCopy()
	current.Name = "wrong-request-owner"
	current.UID = "wrong-request-owner-uid"
	current.ResourceVersion = ""
	current.Annotations = nil
	current.Spec.Workload = &request
	current.Spec.Attachment = &api.ExecutionWorkspaceAttachment{Epoch: 5}
	current.Spec.AttachmentEpoch = 5
	status := current.Status.DeepCopy()
	status.AttachedEpoch = 5
	if err := c.Create(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	current.Status = *status
	if err := c.Status().Update(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	class := &api.ExecutionWorkspaceClass{ObjectMeta: metav1.ObjectMeta{Namespace: current.Namespace, Name: current.Spec.ClassBinding.Name, UID: current.Spec.ClassBinding.UID}}
	if err := c.Create(t.Context(), class); err != nil {
		t.Fatal(err)
	}
	r := &FakeExecutionWorkspaceReconciler{Client: c}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(current)}); !errors.Is(err, sdk.ErrStaleIdentity) {
		t.Fatalf("foreign request accepted: %v", err)
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(current), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Allocation != nil || current.Status.ExternalID != "" || current.Status.AttachedEpoch != 0 || current.Status.State == api.ExecutionWorkspaceStateAttached || sdk.ConditionIsTrue(current.Status.Conditions, string(api.ConditionWorkspaceDataPlaneReady)) {
		t.Fatal("foreign readiness or attachment was published")
	}
	// A stale foreign status claim must also be withdrawn, rather than being
	// copied through the normal preserve-last-fence error path.
	current.Status.Allocation = &sdk.AllocationObservation{Key: request.Key, State: sdk.AllocationReady}
	current.Status.ExternalID = "foreign-allocation"
	if err := c.Status().Update(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(current)}); !errors.Is(err, sdk.ErrStaleIdentity) {
		t.Fatalf("foreign status claim accepted: %v", err)
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(current), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Allocation != nil || current.Status.ExternalID != "" {
		t.Fatal("foreign allocation claim survived rejection")
	}
	if !reflect.DeepEqual(otherBefore, bindingWorkspace(t, c, request.Key)) {
		t.Fatal("referenced workspace was mutated")
	}
	var journals corev1.ConfigMapList
	if err := c.List(t.Context(), &journals); err != nil {
		t.Fatal(err)
	}
	if len(journals.Items) != 0 {
		t.Fatal("foreign request wrote an allocation journal")
	}
	var pods corev1.PodList
	if err := c.List(t.Context(), &pods); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 0 {
		t.Fatal("foreign request created a Pod")
	}
}

func TestReconcileInvalidWorkloadPreservesExactOwnedCleanup(t *testing.T) {
	c, request := runtimeFixture(t)
	allocated, err := New(c).EnsureAllocation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	current := bindingWorkspace(t, c, request.Key)
	other := current.DeepCopy()
	other.Name = "foreign-request"
	other.UID = "foreign-request-uid"
	other.ResourceVersion = ""
	other.Annotations = nil
	other.Status = api.ExecutionWorkspaceStatus{}
	foreign := request
	foreign.Key.Name = other.Name
	foreign.Key.WorkspaceUID = other.UID
	foreign.Revision, _ = sdk.WorkloadRevision(foreign)
	other.Spec.Workload = &foreign
	if err := c.Create(t.Context(), other); err != nil {
		t.Fatal(err)
	}
	otherBefore := bindingWorkspace(t, c, foreign.Key)
	current.Spec.Workload = &foreign
	current.Spec.Attachment = nil
	current.Spec.DesiredState = api.ExecutionWorkspaceDesiredDeleted
	current.Spec.Lifecycle.DeletionPolicy = deletionPolicy()
	current.Spec.Retirement = &api.WorkloadRetirement{Sequence: request.Sequence, Identity: allocated.Identity, Action: api.WorkloadRetirementDelete}
	current.Spec.Retirement.Identity.InstanceID = "unauthorized-instance"
	status := current.Status.DeepCopy()
	status.Allocation = &allocated
	status.AttachedEpoch = 9
	if err := c.Update(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	current.Status = *status
	if err := c.Status().Update(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	r := &FakeExecutionWorkspaceReconciler{Client: c}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(current)}); err != nil {
		t.Fatal(err)
	}
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil || record.Operation != "ensure" || record.Observation.State != sdk.AllocationReady {
		t.Fatalf("invalid retirement authorized cleanup: %v", err)
	}
	current = bindingWorkspace(t, c, request.Key)
	if current.Status.AttachedEpoch != 0 || current.Status.Allocation == nil || current.Status.Allocation.Key != request.Key || current.Status.Allocation.Startup != nil || sdk.ConditionIsTrue(current.Status.Conditions, string(api.ConditionWorkspaceDataPlaneReady)) {
		t.Fatal("invalid intent retained attachment or startup readiness")
	}
	current.Spec.Retirement.Identity = allocated.Identity
	if err := c.Update(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	for range 12 {
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(current)}); err != nil {
			t.Fatal(err)
		}
		current = bindingWorkspace(t, c, request.Key)
		if current.Status.State == api.ExecutionWorkspaceStateDeleted {
			break
		}
	}
	if current.Status.State != api.ExecutionWorkspaceStateDeleted || current.Status.AttachedEpoch != 0 || current.Status.Allocation == nil || current.Status.Allocation.Key != request.Key || current.Status.Allocation.State != sdk.AllocationDeleted || current.Status.Allocation.Startup != nil {
		t.Fatalf("invalid launch intent stranded owned cleanup: %#v", current.Status.Allocation)
	}
	if !reflect.DeepEqual(otherBefore, bindingWorkspace(t, c, foreign.Key)) {
		t.Fatal("cleanup mutated the foreign workspace")
	}
	if _, _, err := New(c).read(t.Context(), foreign.Key); !errors.Is(err, sdk.ErrNotFound) {
		t.Fatalf("cleanup touched foreign journal: %v", err)
	}
}

func TestReconcileDoesNotAcknowledgeMismatchedAttachmentEpoch(t *testing.T) {
	c, request := runtimeFixture(t)
	current := bindingWorkspace(t, c, request.Key)
	current.Spec.Workload = nil
	current.Spec.Attachment = &api.ExecutionWorkspaceAttachment{Epoch: 5}
	current.Spec.AttachmentEpoch = 6
	class := &api.ExecutionWorkspaceClass{ObjectMeta: metav1.ObjectMeta{Namespace: current.Namespace, Name: current.Spec.ClassBinding.Name, UID: current.Spec.ClassBinding.UID}}
	if err := c.Create(t.Context(), class); err != nil {
		t.Fatal(err)
	}

	if err := c.Update(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	r := &FakeExecutionWorkspaceReconciler{Client: c}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(current)}); err != nil {
		t.Fatal(err)
	}
	current = bindingWorkspace(t, c, request.Key)
	if current.Status.AttachedEpoch != 0 || current.Status.State == api.ExecutionWorkspaceStateAttached || sdk.ConditionIsTrue(current.Status.Conditions, string(api.ConditionWorkspaceAttached)) {
		t.Fatal("mismatched epoch was acknowledged")
	}
	current.Spec.DesiredState = api.ExecutionWorkspaceDesiredDeleted
	if err := c.Update(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(current)}); err != nil {
		t.Fatal(err)
	}
	current = bindingWorkspace(t, c, request.Key)
	if current.Status.State != api.ExecutionWorkspaceStateDeleted || current.Status.AttachedEpoch != 0 {
		t.Fatal("mismatched attachment prevented no-allocation deletion")
	}
}
