package sandbox

import (
	"context"
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

type conditionContractReconciler interface {
	Reconcile(context.Context, ctrl.Request) (ctrl.Result, error)
}

type conditionUnavailableClient struct {
	client.Client
	journal client.ObjectKey
}

func (c *conditionUnavailableClient) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if _, ok := object.(*corev1.ConfigMap); ok && key == c.journal {
		return errors.New("allocation observation unavailable")
	}
	return c.Client.Get(ctx, key, object, options...)
}

type conditionContractFixture struct {
	client         client.Client
	request        sdk.WorkloadRequest
	reconciler     func(client.Client) conditionContractReconciler
	lifecycle      func(client.Client) *Lifecycle
	tick           func()
	coreConditions []metav1.Condition
	credential     *corev1.Secret
}

func prepareConditionContractFixture(t *testing.T, f conditionContractFixture) conditionContractFixture {
	t.Helper()
	workspace := conditionContractWorkspace(t, f)
	workspace.Finalizers = []string{"workspace.orka.ai/finalizer"}
	workspace.Status.Conditions = append(workspace.Status.Conditions,
		metav1.Condition{Type: string(api.ConditionWorkspaceQuarantined), Status: metav1.ConditionFalse, Reason: "CoreDecision", ObservedGeneration: workspace.Generation, LastTransitionTime: metav1.Now()},
		metav1.Condition{Type: "CoreHealth", Status: metav1.ConditionTrue, Reason: "CoreOnly", ObservedGeneration: workspace.Generation, LastTransitionTime: metav1.Now()},
	)
	status := workspace.Status.DeepCopy()
	if err := f.client.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	workspace.Status = *status
	if err := f.client.Status().Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	workspace = conditionContractWorkspace(t, f)
	f.coreConditions = append([]metav1.Condition(nil), workspace.Status.Conditions...)
	f.credential = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: workspace.Namespace, Name: "core-credential", UID: "core-credential-uid"}, Data: map[string][]byte{"token": []byte("fixture-only")}}
	if err := f.client.Create(t.Context(), f.credential); err != nil {
		t.Fatal(err)
	}
	f.credential = f.credential.DeepCopy()
	return f
}

func conditionContractWorkspace(t *testing.T, f conditionContractFixture) *api.ExecutionWorkspace {
	t.Helper()
	workspace := &api.ExecutionWorkspace{}
	if err := f.client.Get(t.Context(), client.ObjectKey{Namespace: f.request.Key.Namespace, Name: f.request.Key.Name}, workspace); err != nil {
		t.Fatal(err)
	}
	return workspace
}

func reconcileConditionContract(t *testing.T, f conditionContractFixture) {
	t.Helper()
	if _, err := f.reconciler(f.client).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKey{Namespace: f.request.Key.Namespace, Name: f.request.Key.Name}}); err != nil {
		t.Fatal(err)
	}
	f.tick()
}

func assertConditionContract(t *testing.T, f conditionContractFixture, provisioned, finalized metav1.ConditionStatus) *api.ExecutionWorkspace {
	t.Helper()
	workspace := conditionContractWorkspace(t, f)
	for _, wanted := range []struct {
		typ    api.ExecutionWorkspaceConditionType
		status metav1.ConditionStatus
	}{
		{api.ConditionWorkspaceProvisioned, provisioned}, {api.ConditionWorkspaceFinalized, finalized},
	} {
		condition := sdk.FindCondition(workspace.Status.Conditions, string(wanted.typ))
		if condition == nil || condition.Status != wanted.status || condition.ObservedGeneration != workspace.Generation || condition.LastTransitionTime.IsZero() {
			t.Fatalf("%s waiter cannot observe current milestone: %+v, want %s at generation %d", wanted.typ, condition, wanted.status, workspace.Generation)
		}
	}
	for _, wanted := range f.coreConditions {
		actual := sdk.FindCondition(workspace.Status.Conditions, wanted.Type)
		if actual == nil || !reflect.DeepEqual(*actual, wanted) {
			t.Fatalf("provider changed Core condition %q", wanted.Type)
		}
	}
	if !reflect.DeepEqual(workspace.Finalizers, []string{"workspace.orka.ai/finalizer"}) {
		t.Fatal("provider removed Core's finalizer")
	}
	secret := &corev1.Secret{}
	if err := f.client.Get(t.Context(), client.ObjectKeyFromObject(f.credential), secret); err != nil || !reflect.DeepEqual(secret, f.credential) {
		t.Fatal("provider altered Core-owned credentials")
	}
	return workspace
}

func TestProviderConditionMilestonesAcrossAllocationAndCleanup(t *testing.T) {
	f := newConditionContractFixture(t)
	reconcileConditionContract(t, f)
	workspace := assertConditionContract(t, f, metav1.ConditionTrue, metav1.ConditionFalse)
	if workspace.Status.Allocation == nil || !workspace.Status.Allocation.Identity.Valid() {
		t.Fatal("Provisioned lacks a durable exact allocation")
	}
	identity := workspace.Status.Allocation.Identity
	waitForConditionReady(t, f)
	workspace = conditionContractWorkspace(t, f)
	workspace.Spec.DesiredState = api.ExecutionWorkspaceDesiredDeleted
	workspace.Generation++
	if err := f.client.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	reconcileConditionContract(t, f)
	workspace = assertConditionContract(t, f, metav1.ConditionTrue, metav1.ConditionFalse)
	if sdk.ConditionIsTrue(workspace.Status.Conditions, string(api.ConditionWorkspaceDataPlaneReady)) {
		t.Fatal("retirement intent retained data-plane readiness")
	}
	workspace.Spec.DesiredState = api.ExecutionWorkspaceDesiredReady
	workspace.Spec.Retirement = &api.WorkloadRetirement{Sequence: f.request.Sequence, Identity: identity, Action: api.WorkloadRetirementStop}
	workspace.Generation++
	if err := f.client.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	stopped := false
	for range 20 {
		reconcileConditionContract(t, f)
		workspace = assertConditionContract(t, f, metav1.ConditionTrue, metav1.ConditionFalse)
		if workspace.Status.Allocation.State == sdk.AllocationStopped {
			stopped = true
			break
		}
	}
	if !stopped {
		t.Fatal("exact Stop never reached its durable milestone")
	}
	workspace.Spec.DesiredState = api.ExecutionWorkspaceDesiredDeleted
	workspace.Spec.Retirement.Action = api.WorkloadRetirementDelete
	workspace.Generation++
	if err := f.client.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	for range 25 {
		reconcileConditionContract(t, f)
		workspace = conditionContractWorkspace(t, f)
		if sdk.ConditionIsTrue(workspace.Status.Conditions, string(api.ConditionWorkspaceFinalized)) {
			break
		}
	}
	workspace = assertConditionContract(t, f, metav1.ConditionFalse, metav1.ConditionTrue)
	if workspace.Status.State != api.ExecutionWorkspaceStateDeleted || sdk.ValidateDeletedDisposition(workspace.Status.Disposition, workspace.Spec.Lifecycle.DeletionPolicy) != nil {
		t.Fatal("Finalized lacks complete provider cleanup evidence")
	}
	if workspace.Status.Disposition.AccessCredentials != api.DispositionNotApplicable || workspace.Status.Disposition.EphemeralSecrets != api.DispositionNotApplicable {
		t.Fatal("Finalized claimed Core credential cleanup")
	}
	if sdk.ValidateInteractiveDeletedDisposition(workspace.Status.Disposition, workspace.Spec.Lifecycle.DeletionPolicy) == nil {
		t.Fatal("provider cleanup improperly certifies interactive credential revocation")
	}
}

func TestProviderConditionsForNeverAllocatedDeletion(t *testing.T) {
	f := newConditionContractFixture(t)
	workspace := conditionContractWorkspace(t, f)
	workspace.Spec.DesiredState = api.ExecutionWorkspaceDesiredDeleted
	workspace.Generation++
	if err := f.client.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	reconcileConditionContract(t, f)
	workspace = assertConditionContract(t, f, metav1.ConditionFalse, metav1.ConditionTrue)
	if workspace.Status.Allocation != nil || workspace.Status.State != api.ExecutionWorkspaceStateDeleted {
		t.Fatal("never-allocated deletion synthesized an allocation")
	}
	if sdk.ValidateDeletedDisposition(workspace.Status.Disposition, workspace.Spec.Lifecycle.DeletionPolicy) != nil {
		t.Fatal("never-allocated Finalized lacks cleanup evidence")
	}
}

func TestProviderConditionFinalizedRejectsInvalidCleanupDisposition(t *testing.T) {
	f := newConditionContractFixture(t)
	reconcileConditionContract(t, f)
	workspace := conditionContractWorkspace(t, f)
	workspace.Spec.DesiredState = api.ExecutionWorkspaceDesiredDeleted
	workspace.Spec.Retirement = &api.WorkloadRetirement{Sequence: f.request.Sequence, Identity: workspace.Status.Allocation.Identity, Action: api.WorkloadRetirementDelete}
	workspace.Generation++
	if err := f.client.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	for range 25 {
		reconcileConditionContract(t, f)
		if sdk.ConditionIsTrue(conditionContractWorkspace(t, f).Status.Conditions, string(api.ConditionWorkspaceFinalized)) {
			break
		}
	}
	assertConditionContract(t, f, metav1.ConditionFalse, metav1.ConditionTrue)
	driver := f.lifecycle(f.client)
	cm, record, err := driver.read(t.Context(), f.request.Key)
	if err != nil {
		t.Fatal(err)
	}
	record.Observation.Disposition.Compute = api.DispositionActive
	if err := driver.save(t.Context(), cm, record); err != nil {
		t.Fatal(err)
	}
	// A terminal state label alone cannot establish cleanup or compute absence.
	_, _ = f.reconciler(f.client).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)})
	assertConditionContract(t, f, metav1.ConditionUnknown, metav1.ConditionFalse)
}

func TestProviderConditionErrorsWithdrawMilestonesWithoutClaimingAbsence(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		t.Run(map[bool]string{false: "allocation observation", true: "cleanup observation"}[deleting], func(t *testing.T) {
			f := newConditionContractFixture(t)
			reconcileConditionContract(t, f)
			workspace := assertConditionContract(t, f, metav1.ConditionTrue, metav1.ConditionFalse)
			identity := workspace.Status.Allocation.Identity
			if deleting {
				workspace.Spec.DesiredState = api.ExecutionWorkspaceDesiredDeleted
				workspace.Spec.Retirement = &api.WorkloadRetirement{Sequence: f.request.Sequence, Identity: identity, Action: api.WorkloadRetirementDelete}
				workspace.Generation++
				if err := f.client.Update(t.Context(), workspace); err != nil {
					t.Fatal(err)
				}
			}
			wrapped := &conditionUnavailableClient{Client: f.client, journal: journalKey(f.request.Key)}
			if _, err := f.reconciler(wrapped).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err == nil {
				t.Fatal("backend outage was hidden")
			}
			workspace = assertConditionContract(t, f, metav1.ConditionUnknown, metav1.ConditionFalse)
			if workspace.Status.Allocation == nil || workspace.Status.Allocation.Identity != identity || workspace.Status.Allocation.State != sdk.AllocationPending || workspace.Status.Allocation.Startup != nil {
				t.Fatal("observation failure lost the fence or asserted startup/termination")
			}
			if sdk.ConditionIsTrue(workspace.Status.Conditions, string(api.ConditionWorkspaceDataPlaneReady)) {
				t.Fatal("backend outage retained data-plane readiness")
			}
		})
	}
}

func newConditionContractFixture(t *testing.T) conditionContractFixture {
	t.Helper()
	c, request := fixture(t, false)
	return prepareConditionContractFixture(t, conditionContractFixture{client: c, request: request,
		reconciler: func(c client.Client) conditionContractReconciler { return &ExecutionWorkspaceReconciler{Client: c} },
		lifecycle:  New, tick: func() {
			if err := nativeTick(t.Context(), c); err != nil {
				t.Fatal(err)
			}
		},
	})
}

func waitForConditionReady(t *testing.T, f conditionContractFixture) {
	t.Helper()
	for range 6 {
		reconcileConditionContract(t, f)
		if sdk.ConditionIsTrue(conditionContractWorkspace(t, f).Status.Conditions, string(api.ConditionWorkspaceDataPlaneReady)) {
			return
		}
	}
	t.Fatal("materialized Pod did not publish data-plane readiness")
}
