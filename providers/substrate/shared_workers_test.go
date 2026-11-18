package substrate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	profile "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func sharedDriver(c client.Client, native *nativeFixture) *Lifecycle {
	d := New(c, native, credentialConfig())
	d.challengeClient = native
	return d
}

func sharedReplenisher(c client.Client, native *nativeFixture) *SpareWorkerReplenisher {
	return &SpareWorkerReplenisher{Client: c, Control: native, Config: credentialConfig()}
}

// createSharedProfile creates an opted-in profile and returns its binding.
func createSharedProfile(t *testing.T, c client.Client, namespace, name string, uid types.UID, maxActors int32) *api.ImmutableObjectBinding {
	t.Helper()
	parameter := &profile.SubstrateWorkspaceProfile{TypeMeta: metav1.TypeMeta{APIVersion: profile.GroupVersion.String(), Kind: "SubstrateWorkspaceProfile"}, ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: uid, Generation: 1}, Spec: profile.SubstrateWorkspaceProfileSpec{
		TemplateRef: profile.SubstrateTemplateReference{Name: "base", Namespace: "native"}, Suspend: &profile.SubstrateSuspendPolicy{Mode: profile.SubstrateSuspendModeDataOnly},
		Placement: &profile.SubstratePlacementPolicy{SharedWorkers: &profile.SharedWorkersPolicy{MaxActorsPerWorker: maxActors}},
	}}
	if err := parameter.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(parameter)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := sdk.ParametersProfileHash(&unstructured.Unstructured{Object: raw})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Create(t.Context(), parameter); err != nil {
		t.Fatal(err)
	}
	return &api.ImmutableObjectBinding{Name: name, UID: uid, Generation: 1, ProfileHash: hash}
}

// sharedWorkspace creates (or rebinds) a workspace whose workload uses the
// named profile with gateway credential injection, and admits it.
func sharedWorkspace(t *testing.T, c client.Client, base sdk.WorkloadRequest, namespace, name string, uid types.UID, binding *api.ImmutableObjectBinding) sdk.WorkloadRequest {
	t.Helper()
	request := base
	request.Runtime = base.Runtime.DeepCopy()
	request.Key = sdk.AllocationKey{Namespace: namespace, Name: name, WorkspaceUID: uid, ProviderUID: base.Key.ProviderUID}
	request.ParametersRef = &api.TypedObjectReference{Group: profile.GroupVersion.Group, Kind: "SubstrateWorkspaceProfile", Name: binding.Name}
	request.ParametersBinding = binding.DeepCopy()
	request = credentialRequest(t, request)
	template := &api.ExecutionWorkspace{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "tenant", Name: "workspace"}, template); err != nil {
		t.Fatal(err)
	}
	workspace := &api.ExecutionWorkspace{}
	err := c.Get(t.Context(), client.ObjectKey{Namespace: namespace, Name: name}, workspace)
	switch {
	case apierrors.IsNotFound(err):
		workspace = &api.ExecutionWorkspace{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: uid, Generation: 1}, Spec: *template.Spec.DeepCopy()}
		workspace.Spec.Workload = &request
		if err := c.Create(t.Context(), workspace); err != nil {
			t.Fatal(err)
		}
		workspace.Status = *template.Status.DeepCopy()
		if err := c.Status().Update(t.Context(), workspace); err != nil {
			t.Fatal(err)
		}
	case err != nil:
		t.Fatal(err)
	default:
		workspace.Spec.Workload = &request
		if err := c.Update(t.Context(), workspace); err != nil {
			t.Fatal(err)
		}
	}
	return request
}

// sharedFixture returns a data-only fixture whose profile opts in to shared
// workers of maxActors slots, and its first workspace's request.
func sharedFixture(t *testing.T, maxActors int32) (client.Client, *nativeFixture, sdk.WorkloadRequest, *api.ImmutableObjectBinding) {
	t.Helper()
	c, native, request := fixture(t, true)
	binding := createSharedProfile(t, c, "tenant", "shared", "shared-profile-uid", maxActors)
	return c, native, sharedWorkspace(t, c, request, "tenant", "workspace", "workspace-uid", binding), binding
}

func sharedReady(t *testing.T, c client.Client, native *nativeFixture, request sdk.WorkloadRequest) (sdk.AllocationObservation, *journalRecord) {
	t.Helper()
	for range 16 {
		observed, err := sharedDriver(c, native).EnsureAllocation(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == sdk.AllocationReady {
			_, record, err := sharedDriver(c, native).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			return observed, record
		}
	}
	t.Fatal("never ready")
	return sdk.AllocationObservation{}, nil
}

func sharedRetired(t *testing.T, c client.Client, native *nativeFixture, request sdk.WorkloadRequest, identity sdk.InstanceIdentity, suspend bool) (sdk.AllocationObservation, *journalRecord) {
	t.Helper()
	for range 24 {
		var out sdk.AllocationObservation
		var err error
		if suspend {
			out, err = sharedDriver(c, native).SuspendInstance(t.Context(), request.Key, identity)
		} else {
			out, err = sharedDriver(c, native).StopInstance(t.Context(), request.Key, identity)
		}
		if err != nil {
			t.Fatal(err)
		}
		if out.State == sdk.AllocationStopped {
			_, record, err := sharedDriver(c, native).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			return out, record
		}
	}
	t.Fatal("never stopped")
	return sdk.AllocationObservation{}, nil
}

func sharedPools(t *testing.T, c client.Client) []unstructured.Unstructured {
	t.Helper()
	pools := &unstructured.UnstructuredList{}
	pools.SetAPIVersion("ate.dev/v1")
	pools.SetKind("WorkerPoolList")
	if err := c.List(t.Context(), pools, client.HasLabels{sharedPoolLabel}); err != nil {
		t.Fatal(err)
	}
	return pools.Items
}

func poolLedger(t *testing.T, c client.Client, name string) (*unstructured.Unstructured, *sharedLedger) {
	t.Helper()
	pool, found := getPool(t, c, name)
	if !found {
		t.Fatalf("shared pool %s is gone", name)
	}
	ledger, err := readSharedLedger(pool)
	if err != nil {
		t.Fatal(err)
	}
	return pool, ledger
}

func setNow(t *testing.T, at time.Time) {
	t.Helper()
	previous := sharedNow
	sharedNow = func() time.Time { return at }
	t.Cleanup(func() { sharedNow = previous })
}

func podExists(t *testing.T, c client.Client, namespace, name string, uid string) bool {
	t.Helper()
	pod := &corev1.Pod{}
	err := c.Get(t.Context(), client.ObjectKey{Namespace: namespace, Name: name}, pod)
	if apierrors.IsNotFound(err) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(pod.UID) == uid
}

func TestSharedWorkersAbsentKeepsThePrivatePath(t *testing.T) {
	c, native, request := fixture(t, true)
	raw := &unstructured.Unstructured{}
	raw.SetGroupVersionKind(profile.GroupVersion.WithKind("SubstrateWorkspaceProfile"))
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "tenant", Name: "native"}, raw); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(raw.Object, "spec", "placement"); found {
		t.Fatal("an absent placement serialized into the profile, changing its hash")
	}
	ready(t, c, native, request)
	cm := &corev1.ConfigMap{}
	if err := c.Get(t.Context(), journalKey(request.Key), cm); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(cm.Data[journalDataKey], "sharedWorker") {
		t.Fatal("a private allocation journaled shared-worker state")
	}
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.SharedWorker != nil || record.RuntimePoolLabels[workerInstanceLabel] == "" || len(sharedPools(t, c)) != 0 {
		t.Fatalf("absent opt-in left the private single-Actor path: %#v", record.RuntimePoolLabels)
	}
	if cpu, memory := expectedWorkerCapacity(record); cpu != 1000 || memory != 512<<20 {
		t.Fatalf("private worker capacity changed: %d %d", cpu, memory)
	}
}

func TestSharedWorkersRequireGatewayCredentialsAndASafeWorkspace(t *testing.T) {
	c, native, request, binding := sharedFixture(t, 2)
	// Without gateway credential injection the provider token would enter the
	// sandbox, so the allocation stays private.
	plain := request
	plain.Runtime = request.Runtime.DeepCopy()
	plain.Runtime.Egress = fixtureEgress()
	plain.Runtime.RequiredFeatures = nil
	plain.Revision, _ = sdk.WorkloadRevision(plain)
	if err := admit(c)(t.Context(), plain); err != nil {
		t.Fatal(err)
	}
	ready(t, c, native, plain)
	_, record, err := driver(c, native).read(t.Context(), plain.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.SharedWorker != nil {
		t.Fatal("an allocation without gateway credential injection was shared")
	}
	// A tainted workspace never shares.
	tainted := sharedWorkspace(t, c, request, "tenant", "tainted", "tainted-uid", binding)
	workspace := &api.ExecutionWorkspace{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "tenant", Name: "tainted"}, workspace); err != nil {
		t.Fatal(err)
	}
	workspace.Annotations = map[string]string{operatorTaintAnnotation: "{}"}
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	_, record = sharedReady(t, c, native, tainted)
	if record.SharedWorker != nil {
		t.Fatal("a tainted workspace was shared")
	}
}

func TestSharedWorkersHostTwoActorsOfOneNamespaceAndProfileOnly(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	_, a := sharedReady(t, c, native, first)
	_, b := sharedReady(t, c, native, second)
	if a.SharedWorker == nil || b.SharedWorker == nil || a.SharedWorker.Pool != b.SharedWorker.Pool || a.Worker.PodUID != b.Worker.PodUID {
		t.Fatalf("two Actors of one namespace and profile did not share a worker: %#v %#v", a.SharedWorker, b.SharedWorker)
	}
	if cpu, memory := expectedWorkerCapacity(a); cpu != 2000 || memory != 1024<<20 {
		t.Fatalf("shared worker is not sized for two Actors: %d %d", cpu, memory)
	}
	pool, ledger := poolLedger(t, c, a.SharedWorker.Pool)
	if len(pool.GetOwnerReferences()) != 0 || len(ledger.Entries) != 2 || ledger.Entries[0].ActorUID == "" || ledger.Entries[1].ActorUID == "" || ledger.Worker == nil || ledger.Worker.PodUID != a.Worker.PodUID {
		t.Fatalf("ledger does not record both Actors: %#v", ledger)
	}
	limits, _, _ := unstructured.NestedStringMap(pool.Object, "spec", "template", "resources", "limits")
	if limits["cpu"] != "2" || limits["memory"] != "1088Mi" {
		t.Fatalf("shared pool limits = %v, want cpu=2 memory=2x512Mi+64Mi", limits)
	}
	if _, err := sharedDriver(c, native).Observe(t.Context(), first.Key); err != nil {
		t.Fatal(err)
	}

	// A full pool, another profile, and another namespace each get their own pool.
	third := sharedWorkspace(t, c, first, "tenant", "third", "third-uid", binding)
	_, full := sharedReady(t, c, native, third)
	other := createSharedProfile(t, c, "tenant", "shared-b", "shared-b-uid", 2)
	fourth := sharedWorkspace(t, c, first, "tenant", "fourth", "fourth-uid", other)
	_, byProfile := sharedReady(t, c, native, fourth)
	foreign := createSharedProfile(t, c, "elsewhere", "shared", "elsewhere-profile-uid", 2)
	fifth := sharedWorkspace(t, c, first, "elsewhere", "fifth", "fifth-uid", foreign)
	_, byNamespace := sharedReady(t, c, native, fifth)
	pools := map[string]bool{a.SharedWorker.Pool: true}
	for _, record := range []*journalRecord{full, byProfile, byNamespace} {
		if record.SharedWorker == nil || pools[record.SharedWorker.Pool] {
			t.Fatalf("an allocation joined a full or foreign-scope pool: %#v", record.SharedWorker)
		}
		pools[record.SharedWorker.Pool] = true
	}
	if byNamespace.SharedWorker.Scope.Namespace != "elsewhere" || byProfile.SharedWorker.Scope.ProfileUID != "shared-b-uid" {
		t.Fatal("scope does not record the namespace and profile")
	}
}

func TestSharedWorkerForeignAssignmentFailsClosed(t *testing.T) {
	c, native, request, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, request)
	native.foreignWorkers = map[string]bool{record.Worker.Name: true}
	if _, err := sharedDriver(c, native).Observe(t.Context(), request.Key); err == nil || !strings.Contains(err.Error(), "outside its ledger") {
		t.Fatalf("observation accepted a foreign Actor on a shared worker: %v", err)
	}
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("a shared pool hosting a foreign Actor was not quarantined and retired")
	}
}

func TestSharedSuspendProvesInterimEvidenceWithoutTouchingTheNeighbor(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	a, recordA := sharedReady(t, c, native, first)
	_, recordB := sharedReady(t, c, native, second)
	var egressAtSuspend []*pb.EgressRule
	suspended := false
	if len(native.egress[refKey(actorRef(recordA))].GetRules()) == 0 {
		t.Fatal("fixture Actor has no admitted egress to revoke")
	}
	native.beforeSuspend = func(actor *pb.Actor) {
		if actor.Metadata.Name == recordA.Actor.Name {
			policy := native.egress[refKey(actorRef(recordA))]
			suspended = policy != nil
			egressAtSuspend = policy.GetRules()
		}
	}
	logs := &bytes.Buffer{}
	ctx := ctrl.LoggerInto(t.Context(), zap.New(zap.WriteTo(logs), zap.UseDevMode(false)))
	var stopped sdk.AllocationObservation
	for range 24 {
		var err error
		stopped, err = sharedDriver(c, native).SuspendInstance(ctx, first.Key, a.Identity)
		if err != nil {
			t.Fatal(err)
		}
		if stopped.State == sdk.AllocationStopped {
			break
		}
	}
	if stopped.State != sdk.AllocationStopped || stopped.RetainedData == nil {
		t.Fatalf("shared suspension did not report retained data: %#v", stopped)
	}
	if !suspended || len(egressAtSuspend) != 0 {
		t.Fatalf("egress was not revoked before SuspendActor: suspended=%v rules=%v", suspended, egressAtSuspend)
	}
	_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	shared := record.SharedWorker
	if shared.Proof == nil || shared.Strong != "" || !shared.EgressRevoked || !shared.Released || shared.Proof.WorkerUID != recordA.Worker.UID || shared.Proof.Epoch != recordA.Worker.Epoch {
		t.Fatalf("interim evidence was not journaled: %#v", shared)
	}
	if !podExists(t, c, recordB.Worker.Namespace, recordB.Worker.Pod, recordB.Worker.PodUID) {
		t.Fatal("retiring one Actor deleted the shared worker Pod")
	}
	_, ledger := poolLedger(t, c, recordB.SharedWorker.Pool)
	if len(ledger.Entries) != 1 || ledger.Entries[0].ActorUID != recordB.Actor.UID || ledger.State != sharedAdmitting {
		t.Fatalf("retiring one Actor changed its neighbour's slot: %#v", ledger)
	}
	if actor := native.actors[refKey(actorRef(recordB))]; actor == nil || actor.Status.State != pb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatal("retiring one Actor disturbed its neighbour")
	}
	if _, err := sharedDriver(c, native).Observe(t.Context(), second.Key); err != nil {
		t.Fatalf("the neighbour no longer observes Ready: %v", err)
	}
	if !strings.Contains(logs.String(), `"workerSource":"shared"`) || !strings.Contains(logs.String(), `"terminationProof":"interim"`) {
		t.Fatalf("timing log lacks the shared source and interim proof: %s", logs.String())
	}
}

func TestSharedStopSuspendsOnlyForEvidence(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	a, _ := sharedReady(t, c, native, first)
	_, recordB := sharedReady(t, c, native, second)
	_, record := sharedRetired(t, c, native, first, a.Identity, false)
	if record.SharedWorker.Proof == nil || record.SharedWorker.Strong != "" || native.suspends != 1 || len(native.tags) != 0 || record.Observation.RetainedData != nil {
		t.Fatalf("a shared Stop did not prove termination with an evidence-only suspend: %#v suspends=%d tags=%d", record.SharedWorker, native.suspends, len(native.tags))
	}
	if !podExists(t, c, recordB.Worker.Namespace, recordB.Worker.Pod, recordB.Worker.PodUID) {
		t.Fatal("a shared Stop deleted the worker Pod")
	}
}

func TestSharedRetirementDoubtTakesTheStrongPath(t *testing.T) {
	for _, tc := range []struct {
		name  string
		doubt func(native *nativeFixture, record *journalRecord)
	}{
		{"assignment present", func(native *nativeFixture, record *journalRecord) {
			native.afterSuspend = func(actor *pb.Actor) {
				if actor.Metadata.Name == record.Actor.Name {
					native.lingering = map[string]*pb.ActorAssignment{record.Worker.Name + "|" + actor.Metadata.Name: {Actor: &pb.ObjectRef{Atespace: actor.Metadata.Atespace, Name: actor.Metadata.Name}, ActorUid: actor.Metadata.Uid}}
				}
			}
		}},
		{"epoch change mid-proof", func(native *nativeFixture, record *journalRecord) {
			native.afterSuspend = func(actor *pb.Actor) {
				if actor.Metadata.Name == record.Actor.Name {
					worker := native.workers[record.Worker.Name]
					worker.Status.RegisteredEpoch++
					worker.Status.ObservedEpoch++
				}
			}
		}},
		{"upstream error", func(native *nativeFixture, record *journalRecord) {
			native.afterSuspend = func(actor *pb.Actor) {
				if actor.Metadata.Name == record.Actor.Name {
					native.failGetWorker = 1
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, native, first, binding := sharedFixture(t, 2)
			second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
			a, recordA := sharedReady(t, c, native, first)
			b, recordB := sharedReady(t, c, native, second)
			tc.doubt(native, recordA)
			_, record := sharedRetired(t, c, native, first, a.Identity, true)
			if record.SharedWorker.Strong == "" || record.SharedWorker.Proof != nil || !record.WorkloadAbsent {
				t.Fatalf("doubt did not take the strong path: %#v", record.SharedWorker)
			}
			if podExists(t, c, recordA.Worker.Namespace, recordA.Worker.Pod, recordA.Worker.PodUID) {
				t.Fatal("the strong path reported termination while the worker Pod exists")
			}
			if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
				t.Fatal("the strong path left the shared pool admitting")
			}
			native.lingering, native.afterSuspend = nil, nil
			// Core suspends the neighbour through the eviction path; it also
			// proves termination by Pod absence.
			_, neighbour := sharedRetired(t, c, native, second, b.Identity, true)
			if neighbour.SharedWorker.Strong == "" || neighbour.Observation.RetainedData == nil || recordB.Worker.PodUID != recordA.Worker.PodUID {
				t.Fatalf("the neighbour did not retire through the strong path with its data: %#v", neighbour.SharedWorker)
			}
		})
	}
}

func TestSharedWorkerMaxLifetimeDrainsAndRetiresWithPodAbsence(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	a, recordA := sharedReady(t, c, native, first)
	pool, ledger := poolLedger(t, c, recordA.SharedWorker.Pool)
	born := sharedBorn(pool, ledger)
	// Admission closes an hour before the lifetime ends.
	setNow(t, born.Add(23*time.Hour+time.Minute))
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, ledger := poolLedger(t, c, recordA.SharedWorker.Pool); ledger.State != sharedDraining {
		t.Fatalf("a pool past its admission window still admits: %s", ledger.State)
	}
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	_, recordB := sharedReady(t, c, native, second)
	if recordB.SharedWorker.Pool == recordA.SharedWorker.Pool {
		t.Fatal("an allocation joined a draining pool")
	}
	setNow(t, born.Add(24*time.Hour))
	// The lifetime bound holds even while the native control plane fails.
	native.nativeDown = true
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err == nil {
		t.Fatal("fixture control plane did not fail")
	}
	native.nativeDown = false
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
		t.Fatal("a pool past its lifetime was not retired")
	}
	_, record := sharedRetired(t, c, native, first, a.Identity, true)
	if record.SharedWorker.Strong == "" || podExists(t, c, recordA.Worker.Namespace, recordA.Worker.Pod, recordA.Worker.PodUID) {
		t.Fatalf("lifetime retirement did not prove Pod absence: %#v", record.SharedWorker)
	}
}

func TestSharedTaintOrQuarantineRetiresThroughTheStrongPath(t *testing.T) {
	for _, quarantine := range []bool{false, true} {
		t.Run(map[bool]string{false: "taint", true: "quarantine"}[quarantine], func(t *testing.T) {
			c, native, first, binding := sharedFixture(t, 2)
			second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
			a, recordA := sharedReady(t, c, native, first)
			sharedReady(t, c, native, second)
			workspace := &api.ExecutionWorkspace{}
			if err := c.Get(t.Context(), clientKey(first.Key), workspace); err != nil {
				t.Fatal(err)
			}
			if quarantine {
				workspace.Spec.DesiredState = api.ExecutionWorkspaceDesiredQuarantined
			} else {
				workspace.Annotations[operatorTaintAnnotation] = "{}"
			}
			if err := c.Update(t.Context(), workspace); err != nil {
				t.Fatal(err)
			}
			if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, ledger := poolLedger(t, c, recordA.SharedWorker.Pool); ledger.State != sharedDraining {
				t.Fatalf("a pool hosting a suspect workspace still admits: %s", ledger.State)
			}
			_, record := sharedRetired(t, c, native, first, a.Identity, false)
			if record.SharedWorker.Strong == "" || record.SharedWorker.Proof != nil || podExists(t, c, recordA.Worker.Namespace, recordA.Worker.Pod, recordA.Worker.PodUID) {
				t.Fatalf("a suspect workspace on a shared worker did not retire through the strong path: %#v", record.SharedWorker)
			}
		})
	}
}

func TestSharedRetirementRecoversFromTheJournalWithoutReplay(t *testing.T) {
	for _, operation := range []string{"updateEgress", "suspend", "tag", "deleteActor"} {
		t.Run(operation, func(t *testing.T) {
			c, native, first, binding := sharedFixture(t, 2)
			second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
			a, _ := sharedReady(t, c, native, first)
			_, recordB := sharedReady(t, c, native, second)
			native.failAfter = operation
			lost := false
			var stopped sdk.AllocationObservation
			for range 24 {
				// A fresh Lifecycle each pass: only the journal and ledger persist.
				observed, err := sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity)
				if err != nil {
					if lost {
						t.Fatal(err)
					}
					lost = true
					continue
				}
				if observed.State == sdk.AllocationStopped {
					stopped = observed
					break
				}
			}
			_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
			if err != nil {
				t.Fatal(err)
			}
			if !lost || stopped.RetainedData == nil || native.suspends != 1 || native.deletes != 1 || record.SharedWorker.Proof == nil || record.SharedWorker.Strong != "" {
				t.Fatalf("recovery replayed a mutation or lost the proof: lost=%v suspends=%d deletes=%d %#v", lost, native.suspends, native.deletes, record.SharedWorker)
			}
			if _, ledger := poolLedger(t, c, recordB.SharedWorker.Pool); len(ledger.Entries) != 1 {
				t.Fatalf("recovery left the retired slot held: %#v", ledger.Entries)
			}
		})
	}
}

func TestSharedClaimRecoversALostLedgerWrite(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	_, recordA := sharedReady(t, c, native, first)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	lose := true
	lossy := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if err := w.Update(ctx, obj, opts...); err != nil {
			return err
		}
		if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "WorkerPool" && lose {
			lose = false
			return errors.New("claim response lost")
		}
		return nil
	}})
	if _, err := sharedDriver(lossy, native).EnsureAllocation(t.Context(), second); err == nil {
		t.Fatal("expected the lost claim response")
	}
	_, recordB := sharedReady(t, c, native, second)
	if recordB.SharedWorker.Pool != recordA.SharedWorker.Pool {
		t.Fatal("a lost claim response moved the allocation to another pool")
	}
	if _, ledger := poolLedger(t, c, recordA.SharedWorker.Pool); len(ledger.Entries) != 2 {
		t.Fatalf("a lost claim response duplicated or lost the slot: %#v", ledger.Entries)
	}
}

func TestSharedWarmSlotsReplaceSparesAndIdlePoolsAreCollected(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	item := &profile.SubstrateWorkspaceProfile{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "tenant", Name: "shared"}, item); err != nil {
		t.Fatal(err)
	}
	item.Annotations = map[string]string{spareWorkersAnnotation: "1"}
	if err := c.Update(t.Context(), item); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	setNow(t, start)
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(spares(t, c, native)) != 0 {
		t.Fatal("an opted-in profile kept private spares")
	}
	warm := sharedPools(t, c)
	if len(warm) != 1 {
		t.Fatalf("warm shared pools = %d, want 1", len(warm))
	}
	a, record := sharedReady(t, c, native, first)
	if record.SharedWorker.Pool != warm[0].GetName() || workerSource(record) != "shared" {
		t.Fatalf("the allocation did not take a warm slot: %#v", record.SharedWorker)
	}
	sharedRetired(t, c, native, first, a.Identity, true)
	// The empty pool stays while it is the warm capacity.
	setNow(t, start.Add(time.Minute))
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	setNow(t, start.Add(10*time.Minute))
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, found := getPool(t, c, warm[0].GetName()); !found {
		t.Fatal("the warm pool was collected while it was the only warm capacity")
	}
	item.Annotations = nil
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(item), item); err != nil {
		t.Fatal(err)
	}
	item.Annotations = map[string]string{}
	if err := c.Update(t.Context(), item); err != nil {
		t.Fatal(err)
	}
	setNow(t, start.Add(20*time.Minute))
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, found := getPool(t, c, warm[0].GetName()); found {
		t.Fatal("an idle shared pool that is not warm capacity was not collected")
	}
}

func TestSharedSweepCollectsOnlyPositivelyDisownedEntries(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 3)
	_, record := sharedReady(t, c, native, first)
	// A private workspace in the same namespace: its exact journal record
	// holds no shared slot.
	private := sharedWorkspace(t, c, first, "tenant", "private", "private-uid", first.ParametersBinding)
	private.Runtime.Egress = fixtureEgress()
	private.Runtime.RequiredFeatures = nil
	private.Revision, _ = sdk.WorkloadRevision(private)
	if err := admit(c)(t.Context(), private); err != nil {
		t.Fatal(err)
	}
	sharedReady(t, c, native, private)
	own, other := &corev1.ConfigMap{}, &corev1.ConfigMap{}
	if err := c.Get(t.Context(), journalKey(first.Key), own); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), journalKey(private.Key), other); err != nil {
		t.Fatal(err)
	}
	pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
	disowned := other.Namespace + "/" + other.Name + "/" + string(other.UID) + "/1"
	// A sequence the journal has not reached is not proof of disownership.
	future := own.Namespace + "/" + own.Name + "/" + string(own.UID) + "/7"
	ledger.Entries = append(ledger.Entries, sharedLedgerEntry{Journal: disowned, Actor: "native/orphan"}, sharedLedgerEntry{Journal: future, Actor: "native/future"})
	if err := setSharedLedger(pool, ledger); err != nil {
		t.Fatal(err)
	}
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, ledger = poolLedger(t, c, record.SharedWorker.Pool)
	journals := []string{}
	for _, entry := range ledger.Entries {
		journals = append(journals, entry.Journal)
	}
	if len(journals) != 2 || journals[0] != claimMarker(own, record) || journals[1] != future {
		t.Fatalf("sweep did not collect exactly the disowned entry: %v", journals)
	}
}

func TestSharedProfileValidation(t *testing.T) {
	base := profile.SubstrateWorkspaceProfile{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant"}, Spec: profile.SubstrateWorkspaceProfileSpec{TemplateRef: profile.SubstrateTemplateReference{Name: "base"}, Suspend: &profile.SubstrateSuspendPolicy{Mode: profile.SubstrateSuspendModeDataOnly}}}
	for _, tc := range []struct {
		name     string
		actors   int32
		lifetime *metav1.Duration
		suspend  bool
		valid    bool
	}{
		{"defaults", 2, nil, true, true},
		{"sixteen", 16, &metav1.Duration{Duration: 24 * time.Hour}, true, true},
		{"one", 1, nil, true, false},
		{"seventeen", 17, nil, true, false},
		{"short lifetime", 4, &metav1.Duration{Duration: 30 * time.Minute}, true, false},
		{"long lifetime", 4, &metav1.Duration{Duration: 25 * time.Hour}, true, false},
		{"not data-only", 4, nil, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			item := base.DeepCopy()
			item.Spec.Placement = &profile.SubstratePlacementPolicy{SharedWorkers: &profile.SharedWorkersPolicy{MaxActorsPerWorker: tc.actors, MaxWorkerLifetime: tc.lifetime}}
			if !tc.suspend {
				item.Spec.Suspend = nil
			}
			if err := item.Validate(); (err == nil) != tc.valid {
				t.Fatalf("Validate() = %v, want valid=%v", err, tc.valid)
			}
		})
	}
	if lifetime := (&profile.SharedWorkersPolicy{MaxActorsPerWorker: 2}).WorkerLifetime(); lifetime != 24*time.Hour {
		t.Fatalf("default lifetime = %s", lifetime)
	}
}

func TestSharedLedgerRejectsMalformedState(t *testing.T) {
	scope := sharedScope{Namespace: "tenant", Atespace: "native", ProfileUID: "p", ProfileHash: "h", SourceUID: "s", Shape: "x", CPUMillicores: 1, MemoryBytes: 1, MaxActors: 2, Lifetime: metav1.Duration{Duration: time.Hour}}
	for _, ledger := range []sharedLedger{
		{Version: "v0", Scope: scope, BornAt: metav1.Now(), State: sharedAdmitting},
		{Version: sharedLedgerVersion, Scope: scope, BornAt: metav1.Now(), State: "Open"},
		{Version: sharedLedgerVersion, Scope: scope, BornAt: metav1.Now(), State: sharedAdmitting, Entries: []sharedLedgerEntry{{Journal: "a", Actor: "x"}, {Journal: "b", Actor: "y"}, {Journal: "c", Actor: "z"}}},
		{Version: sharedLedgerVersion, Scope: scope, BornAt: metav1.Now(), State: sharedAdmitting, Entries: []sharedLedgerEntry{{Journal: "a", Actor: "x"}, {Journal: "b", Actor: "x"}}},
	} {
		pool := poolObject()
		data, _ := json.Marshal(ledger)
		pool.SetAnnotations(map[string]string{sharedLedgerAnnotation: string(data)})
		if _, err := readSharedLedger(pool); err == nil {
			t.Fatalf("accepted malformed ledger %#v", ledger)
		}
	}
}

func TestSharedProofRetriesAChangingWorkerThenTakesTheStrongPath(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	a, recordA := sharedReady(t, c, native, first)
	native.afterSuspend = func(actor *pb.Actor) {
		if actor.Metadata.Name == recordA.Actor.Name {
			native.churnWorker = recordA.Worker.Name
		}
	}
	_, record := sharedRetired(t, c, native, first, a.Identity, true)
	if record.SharedWorker.ProofAttempts != sharedMaxProofAttempts-1 || record.SharedWorker.Strong == "" || record.SharedWorker.Proof != nil {
		t.Fatalf("a changing worker did not exhaust bounded retries before the strong path: %#v", record.SharedWorker)
	}
}

func TestSharedWorkerWaitsForASettledEpochAndRetiresARestartedWorker(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	d := sharedDriver(c, native)
	var record *journalRecord
	for range 16 {
		if _, err := native.ListWorkers(t.Context(), &pb.ListWorkersRequest{}); err != nil {
			t.Fatal(err)
		}
		for _, worker := range native.workers {
			if worker.Labels[sharedPoolLabel] != "" && worker.Status.ObservedEpoch == 1 && worker.Status.RegisteredEpoch == 1 {
				worker.Status.ObservedEpoch = 0
			}
		}
		if _, err := d.EnsureAllocation(t.Context(), first); err != nil {
			t.Fatal(err)
		}
		var err error
		if _, record, err = d.read(t.Context(), first.Key); err != nil {
			t.Fatal(err)
		}
	}
	if record.BootRequested || native.boots != 0 {
		t.Fatal("a shared worker booted an Actor before its registered epoch was observed")
	}
	for _, worker := range native.workers {
		worker.Status.ObservedEpoch = worker.Status.RegisteredEpoch
	}
	_, record = sharedReady(t, c, native, first)
	// A restarted worker process lost every sandbox: the pool is retired.
	worker := native.workers[record.Worker.Name]
	worker.Status.RegisteredEpoch, worker.Status.ObservedEpoch = 2, 2
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("a restarted shared worker was not retired")
	}
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	_, next := sharedReady(t, c, native, second)
	if next.SharedWorker.Pool == record.SharedWorker.Pool {
		t.Fatal("an allocation joined a retired shared worker")
	}
}

func TestSharedStaleAssignmentOfARetiredActorIsNotForeign(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	a, recordA := sharedReady(t, c, native, first)
	_, recordB := sharedReady(t, c, native, second)
	sharedRetired(t, c, native, first, a.Identity, true)
	stale := &pb.ActorAssignment{Actor: actorRef(recordA), ActorUid: recordA.Actor.UID}
	native.staleOnce = map[string]*pb.ActorAssignment{recordB.Worker.Name: stale}
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, ledger := poolLedger(t, c, recordB.SharedWorker.Pool); ledger.State != sharedAdmitting {
		t.Fatalf("a stale listing of a released Actor quarantined the pool: %s", ledger.State)
	}
	native.staleOnce = map[string]*pb.ActorAssignment{recordB.Worker.Name: stale}
	if _, err := sharedDriver(c, native).Observe(t.Context(), second.Key); err != nil {
		t.Fatalf("a stale listing of a released Actor failed the neighbour: %v", err)
	}
	// A foreign Actor that stays assigned still fails closed.
	native.foreignWorkers = map[string]bool{recordB.Worker.Name: true}
	if _, err := sharedDriver(c, native).Observe(t.Context(), second.Key); err == nil {
		t.Fatal("a persistent foreign assignment was accepted")
	}
}
