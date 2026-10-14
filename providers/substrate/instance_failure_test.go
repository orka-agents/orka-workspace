package substrate

import (
	"context"
	"strings"
	"testing"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func reconcileFailureFixture(t *testing.T, c client.Client, native *nativeFixture, request sdk.WorkloadRequest) *api.ExecutionWorkspace {
	t.Helper()
	r := &ExecutionWorkspaceReconciler{Client: c, Control: native, Config: Config{ActorDNSSuffix: "actors.local", DirectEgressEnabled: true}}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: clientKey(request.Key)}); err != nil {
		t.Fatal(err)
	}
	workspace := &api.ExecutionWorkspace{}
	if err := c.Get(t.Context(), clientKey(request.Key), workspace); err != nil {
		t.Fatal(err)
	}
	return workspace
}

func updateFailureWorkspace(t *testing.T, c client.Client, workspace *api.ExecutionWorkspace) {
	t.Helper()
	workspace.Generation++
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
}

func TestLostNativeActorFailsWithoutRestartAndRetiresOnlyByCoreStop(t *testing.T) {
	for _, mode := range []string{"disappeared", "crashed"} {
		t.Run(mode, func(t *testing.T) {
			c, native, request := fixture(t, false)
			identity := ready(t, c, native, request).Identity
			_, record, err := driver(c, native).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			actorKey := record.Atespace + "/" + record.Actor.Name
			if mode == "disappeared" {
				delete(native.actors, actorKey)
			} else {
				native.actors[actorKey].Status.State = pb.ActorState_ACTOR_STATE_CRASHED
			}
			boots := native.boots
			var workspace *api.ExecutionWorkspace
			for range 3 {
				workspace = reconcileFailureFixture(t, c, native, request)
			}
			a := workspace.Status.Allocation
			if workspace.Status.State != api.ExecutionWorkspaceStateFailed || a == nil || a.Identity != identity || a.State != sdk.AllocationPending || a.Startup != nil {
				t.Fatalf("lost instance was not reported as a failed exact fence: %s %+v", workspace.Status.State, a)
			}
			provisioned := sdk.FindCondition(workspace.Status.Conditions, string(api.ConditionWorkspaceProvisioned))
			if provisioned == nil || provisioned.Status != metav1.ConditionFalse || provisioned.Reason != instanceFailedReason || !strings.Contains(provisioned.Message, "uncertain work is never replayed") {
				t.Fatalf("failure cause is not visible in status: %+v", provisioned)
			}
			if sdk.ConditionIsTrue(workspace.Status.Conditions, string(api.ConditionWorkspaceDataPlaneReady)) {
				t.Fatal("failed instance kept data-plane readiness")
			}
			if native.boots != boots || (mode == "disappeared") != (native.actors[actorKey] == nil) {
				t.Fatal("failed instance was restarted, recreated or removed without authorization")
			}
			if len(privateWorkerPools(t, c)) != 1 {
				t.Fatal("private compute was released before Core authorized Stop")
			}

			workspace.Spec.Retirement = &api.WorkloadRetirement{Sequence: request.Sequence, Identity: identity, Action: api.WorkloadRetirementStop}
			updateFailureWorkspace(t, c, workspace)
			for range 20 {
				workspace = reconcileFailureFixture(t, c, native, request)
				if workspace.Status.Allocation.State == sdk.AllocationStopped {
					break
				}
			}
			if workspace.Status.State != api.ExecutionWorkspaceStateFailed || workspace.Status.Allocation.State != sdk.AllocationStopped || workspace.Status.Allocation.RetainedData != nil {
				t.Fatalf("authorized Stop did not settle the failed instance: %s %+v", workspace.Status.State, workspace.Status.Allocation)
			}
			if native.actors[actorKey] != nil || len(privateWorkerPools(t, c)) != 0 {
				t.Fatal("authorized Stop left the Actor or private WorkerPool")
			}
			pod := &corev1.Pod{}
			if err := c.Get(t.Context(), client.ObjectKey{Namespace: record.Worker.Namespace, Name: record.Worker.Pod}, pod); !apierrors.IsNotFound(err) {
				t.Fatalf("authorized Stop left the worker Pod: %v", err)
			}

			workspace.Spec.DesiredState = api.ExecutionWorkspaceDesiredDeleted
			workspace.Spec.Retirement.Action = api.WorkloadRetirementDelete
			updateFailureWorkspace(t, c, workspace)
			for range 25 {
				workspace = reconcileFailureFixture(t, c, native, request)
				if workspace.Status.State == api.ExecutionWorkspaceStateDeleted {
					break
				}
			}
			if workspace.Status.State != api.ExecutionWorkspaceStateDeleted || sdk.ValidateDeletedDisposition(workspace.Status.Disposition, workspace.Spec.Lifecycle.DeletionPolicy) != nil {
				t.Fatalf("failed workspace deletion did not finish: %s", workspace.Status.State)
			}
		})
	}
}

func privateWorkerPools(t *testing.T, c client.Client) []unstructured.Unstructured {
	t.Helper()
	pools := &unstructured.UnstructuredList{}
	pools.SetAPIVersion("ate.dev/v1alpha1")
	pools.SetKind("WorkerPoolList")
	if err := c.List(t.Context(), pools); err != nil {
		t.Fatal(err)
	}
	var private []unstructured.Unstructured
	for _, pool := range pools.Items {
		if pool.GetLabels()[workerInstanceLabel] != "" {
			private = append(private, pool)
		}
	}
	return private
}

func TestNativeBootTimeoutFailsAllocationWithoutProcessEvidence(t *testing.T) {
	c, native, request := fixture(t, false)
	resumes := 0
	native.beforeResume = func(context.Context, *pb.Actor) error {
		resumes++
		return status.Error(codes.Unavailable, "boot outcome unknown")
	}
	d := driver(c, native)
	for range 12 {
		if _, err := d.EnsureAllocation(t.Context(), request); err != nil && status.Code(err) != codes.Unavailable {
			t.Fatal(err)
		}
		if resumes != 0 {
			break
		}
	}
	cm, record, err := d.read(t.Context(), request.Key)
	if err != nil || !record.BootRequested || record.StartedAt.IsZero() {
		t.Fatalf("boot was not issued with a recorded start: %v", err)
	}
	if _, err := d.EnsureAllocation(t.Context(), request); err != nil || resumes != 1 {
		t.Fatalf("an unknown boot outcome was replayed: %d %v", resumes, err)
	}
	// A journal without a recorded start begins the window on its next pass.
	record.StartedAt = metav1.Time{}
	if err := d.save(t.Context(), cm, record); err != nil {
		t.Fatal(err)
	}
	if _, err := d.EnsureAllocation(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if cm, record, err = d.read(t.Context(), request.Key); err != nil || record.StartedAt.IsZero() || record.Failure != "" {
		t.Fatalf("missing boot start was not initialized: %v", err)
	}
	record.StartedAt = metav1.NewTime(time.Now().Add(-defaultNativeBootTimeout - time.Minute))
	if err := d.save(t.Context(), cm, record); err != nil {
		t.Fatal(err)
	}
	observed, err := d.EnsureAllocation(t.Context(), request)
	if err != nil || observed.State != sdk.AllocationPending || observed.Startup != nil {
		t.Fatalf("expired boot did not fail closed: %+v %v", observed, err)
	}
	if _, record, err = d.read(t.Context(), request.Key); err != nil || !strings.Contains(record.Failure, "no process evidence within 6m0s") {
		t.Fatalf("expired boot did not record a failure: %q %v", record.Failure, err)
	}
	if _, err := d.EnsureAllocation(t.Context(), request); err != nil || resumes != 1 {
		t.Fatal("failed allocation kept booting")
	}
	retired(t, c, native, request, observed.Identity, false)
	if len(native.actors) != 0 || len(privateWorkerPools(t, c)) != 0 {
		t.Fatal("authorized Stop left native compute behind")
	}
}

func TestRejectedNativeBootIsRetriedButUnknownOutcomeIsNot(t *testing.T) {
	for _, code := range []codes.Code{codes.ResourceExhausted, codes.Unavailable} {
		t.Run(code.String(), func(t *testing.T) {
			c, native, request := fixture(t, false)
			resumes := 0
			native.beforeResume = func(context.Context, *pb.Actor) error {
				resumes++
				if resumes == 1 {
					return status.Error(code, "first boot request")
				}
				return nil
			}
			d := driver(c, native)
			var observed sdk.AllocationObservation
			for range 12 {
				var err error
				observed, err = d.EnsureAllocation(t.Context(), request)
				if err != nil && status.Code(err) != code {
					t.Fatal(err)
				}
				if observed.State == sdk.AllocationReady {
					break
				}
			}
			if code == codes.ResourceExhausted && (observed.State != sdk.AllocationReady || native.boots != 1 || resumes != 2) {
				t.Fatalf("refused boot was not retried exactly once: ready=%v boots=%d resumes=%d", observed.State == sdk.AllocationReady, native.boots, resumes)
			}
			if code == codes.Unavailable && (observed.State == sdk.AllocationReady || native.boots != 0 || resumes != 1) {
				t.Fatalf("possibly accepted boot was replayed: boots=%d resumes=%d", native.boots, resumes)
			}
		})
	}
}

func TestDataOnlyProfileRunsWithoutSessionReuse(t *testing.T) {
	c, native, request := fixture(t, true)
	workspace := &api.ExecutionWorkspace{}
	if err := c.Get(t.Context(), clientKey(request.Key), workspace); err != nil {
		t.Fatal(err)
	}
	workspace.Spec.SessionRef = nil
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	ready(t, c, native, request)
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil || !record.SuspendEnabled {
		t.Fatalf("session-less DataOnly workspace lost its durable layout: %v", err)
	}
}

func TestLostResumedActorExportsInheritedCheckpointOnlyByExplicitRecovery(t *testing.T) {
	c, native, request := fixture(t, true)
	first := ready(t, c, native, request)
	stopped := retired(t, c, native, request, first.Identity, true)
	next := *request.DeepCopy()
	next.Sequence++
	next.PreviousInstance = &first.Identity
	next.RetainedData = stopped.RetainedData
	next.Runtime.Template.Spec.Containers[0].Env[0].Value = "rotated-public-nonce"
	next.Revision, _ = sdk.WorkloadRevision(next)
	if err := admit(c)(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	ready(t, c, native, next)
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil || record.Checkpoint != nil || record.InheritedCatalog == nil {
		t.Fatalf("resumed successor does not carry only its inherited checkpoint: %v", err)
	}
	inherited := record.InheritedCatalog.Digest
	delete(native.actors, record.Atespace+"/"+record.Actor.Name)
	if workspace := reconcileFailureFixture(t, c, native, next); workspace.Status.State != api.ExecutionWorkspaceStateFailed {
		t.Fatalf("lost resumed instance reported %s", workspace.Status.State)
	}

	plain := &api.ExecutionWorkspaceCheckpoint{ObjectMeta: metav1.ObjectMeta{Namespace: request.Key.Namespace, Name: "unconfirmed", UID: "unconfirmed-uid", Generation: 1},
		Spec: api.ExecutionWorkspaceCheckpointSpec{WorkspaceRef: api.ObjectIdentityReference{Name: request.Key.Name, UID: request.Key.WorkspaceUID}}}
	if err := c.Create(t.Context(), plain); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if _, err := checkpointController(c, native).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(plain)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(plain), plain); err != nil {
		t.Fatal(err)
	}
	if condition := sdk.FindCondition(plain.Status.Conditions, "Ready"); plain.Status.Phase != "Pending" || plain.Status.Digest != "" || condition == nil || condition.Reason != "AwaitingSuspension" {
		t.Fatalf("plain export of a failed source did not wait for explicit recovery: %s %+v", plain.Status.Phase, condition)
	}
	recovered := exportReady(t, c, native, newCheckpoint(t, c, request, true))
	if recovered.Status.Digest != inherited {
		t.Fatalf("recovery exported %s, want the inherited checkpoint %s", recovered.Status.Digest, inherited)
	}
}
