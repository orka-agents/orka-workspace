package substrate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	profile "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func sharedConfig() Config {
	config := credentialConfig()
	config.DedicatedInstall = true
	return config
}

func sharedDriver(c client.Client, native *nativeFixture) *Lifecycle {
	d := New(c, native, sharedConfig())
	d.challengeClient = native
	return d
}

func sharedReplenisher(c client.Client, native *nativeFixture) *SpareWorkerReplenisher {
	return &SpareWorkerReplenisher{Client: c, Control: native, Config: sharedConfig()}
}

// testClock is the shared-worker clock in tests, in Unix nanoseconds; tick
// advances it past the egress settle window that precedes each suspension.
var testClock atomic.Int64

func testNow() time.Time { return time.Unix(0, testClock.Load()) }

func useTestClock(t *testing.T) {
	t.Helper()
	previous := sharedNow
	testClock.Store(time.Now().UnixNano())
	sharedNow = testNow
	t.Cleanup(func() { sharedNow = previous })
}

func tick() { testClock.Add(int64(sharedEgressSettle + time.Second)) }

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
	useTestClock(t)
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
		tick()
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
	testClock.Store(at.UnixNano())
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
	_, record := sharedReady(t, c, native, plain)
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
		tick()
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

func TestSharedWorkerMaxLifetimeIsAHardBound(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	a, recordA := sharedReady(t, c, native, first)
	pool, ledger := poolLedger(t, c, recordA.SharedWorker.Pool)
	born := sharedBorn(pool, ledger)
	// Strong retirement starts one worker Pod grace plus a margin before the
	// 24h bound, and admission closes an hour before that.
	if got := sharedRetireAt(pool, ledger).Sub(born); got != 24*time.Hour-workerPodTerminationGrace-sharedRetireMargin {
		t.Fatalf("strong retirement starts %s after birth", got)
	}
	setNow(t, born.Add(22*time.Hour))
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
	// The worker Pod lingers (a slow kubelet), and the pool waits on it.
	keep := func(key client.ObjectKey, object client.Object) {
		if err := c.Get(t.Context(), key, object); err != nil {
			t.Fatal(err)
		}
		object.SetFinalizers([]string{"test.invalid/hold"})
		if err := c.Update(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	keep(client.ObjectKey{Namespace: recordA.Worker.Namespace, Name: recordA.Worker.Pod}, &corev1.Pod{})
	keep(client.ObjectKey{Namespace: recordA.Placement.Namespace, Name: recordA.SharedWorker.Pool}, poolObject())
	// Retirement starts at its time even while the native control plane is
	// down: it needs Kubernetes alone.
	setNow(t, sharedRetireAt(pool, ledger))
	native.nativeDown = true
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err == nil {
		t.Fatal("fixture control plane did not fail")
	}
	native.nativeDown = false
	if current, _ := getPool(t, c, recordA.SharedWorker.Pool); current == nil || current.GetDeletionTimestamp() == nil {
		t.Fatal("a pool at its strong retirement time was not retired")
	}
	var escalated []int64
	escalating := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Delete: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		if _, ok := obj.(*corev1.Pod); ok {
			options := &client.DeleteOptions{}
			options.ApplyOptions(opts)
			if options.GracePeriodSeconds != nil {
				escalated = append(escalated, *options.GracePeriodSeconds)
			}
		}
		return w.Delete(ctx, obj, opts...)
	}})
	setNow(t, sharedEscalateAt(pool, ledger).Add(-time.Second))
	if err := sharedReplenisher(escalating, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(escalated) != 0 {
		t.Fatal("a worker Pod was escalated before its grace ran out")
	}
	native.nativeDown = true
	setNow(t, sharedEscalateAt(pool, ledger))
	_ = sharedReplenisher(escalating, native).Sweep(t.Context())
	native.nativeDown = false
	if len(escalated) != 1 || escalated[0] != sharedEscalationGrace {
		t.Fatalf("a worker Pod outliving its grace was not re-deleted with a one-second grace: %v", escalated)
	}
	// Model the kubelet confirming the Pod's exit, then the pool's removal.
	for _, object := range []client.Object{&corev1.Pod{}, poolObject()} {
		key := client.ObjectKey{Namespace: recordA.Worker.Namespace, Name: recordA.Worker.Pod}
		if _, ok := object.(*corev1.Pod); !ok {
			key = client.ObjectKey{Namespace: recordA.Placement.Namespace, Name: recordA.SharedWorker.Pool}
		}
		if err := c.Get(t.Context(), key, object); err != nil {
			t.Fatal(err)
		}
		object.SetFinalizers(nil)
		if err := c.Update(t.Context(), object); err != nil {
			t.Fatal(err)
		}
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
			if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
				t.Fatal("a pool hosting a suspect workspace was not strongly retired")
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
			// After the suspension intent, any lost response before the proof
			// is doubt, so it takes the strong path without replay.
			strong := operation != "deleteActor"
			c, native, first, binding := sharedFixture(t, 2)
			second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
			a, _ := sharedReady(t, c, native, first)
			_, recordB := sharedReady(t, c, native, second)
			native.failAfter = operation
			lost := false
			var stopped sdk.AllocationObservation
			for range 24 {
				tick()
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
			if (operation == "updateEgress") == lost || stopped.RetainedData == nil || native.suspends != 1 || native.deletes != 1 || (record.SharedWorker.Proof == nil) != strong || (record.SharedWorker.Strong != "") != strong {
				t.Fatalf("recovery replayed a mutation or lost the proof: lost=%v suspends=%d deletes=%d %#v", lost, native.suspends, native.deletes, record.SharedWorker)
			}
			if strong {
				return
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
		{"short lifetime", 4, &metav1.Duration{Duration: 2 * time.Hour}, true, false},
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
	scope := sharedScope{Namespace: "tenant", Atespace: "native", ProfileUID: "p", ProfileHash: "h", SourceUID: "s", Shape: "x", CPUMillicores: 1, MemoryBytes: 1, MaxActors: 2, Lifetime: metav1.Duration{Duration: profile.MinSharedWorkerLifetime}}
	encode := func(ledger sharedLedger) *unstructured.Unstructured {
		pool := poolObject()
		data, err := json.Marshal(ledger)
		if err != nil {
			t.Fatal(err)
		}
		pool.SetAnnotations(map[string]string{sharedLedgerAnnotation: string(data)})
		return pool
	}
	// A well-formed ledger is accepted, so each case below fails only on the
	// field it changes.
	valid := sharedLedger{Version: sharedLedgerVersion, Scope: scope, BornAt: metav1.Now(), State: sharedAdmitting, Entries: []sharedLedgerEntry{{Journal: "a", Actor: "x"}, {Journal: "b", Actor: "y"}}}
	if _, err := readSharedLedger(encode(valid)); err != nil {
		t.Fatalf("rejected a well-formed ledger: %v", err)
	}
	for _, tc := range []struct {
		want   string
		mutate func(*sharedLedger)
	}{
		{"is invalid", func(l *sharedLedger) { l.Version = "v0" }},
		{"state is invalid", func(l *sharedLedger) { l.State = "Open" }},
		{"exceeds its slots", func(l *sharedLedger) { l.Entries = append(l.Entries, sharedLedgerEntry{Journal: "c", Actor: "z"}) }},
		{"duplicate or empty entry", func(l *sharedLedger) { l.Entries[1].Actor = "x" }},
		{"duplicate or empty entry", func(l *sharedLedger) { l.Entries[1].Journal = "a" }},
		{"duplicate or empty entry", func(l *sharedLedger) { l.Entries[1].Journal = "" }},
	} {
		ledger := valid
		ledger.Entries = slices.Clone(valid.Entries)
		tc.mutate(&ledger)
		if _, err := readSharedLedger(encode(ledger)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("ledger %#v: got %v, want an error containing %q", ledger, err, tc.want)
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

func TestSharedWorkersRequireADedicatedInstall(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	plain := New(c, native, credentialConfig())
	plain.challengeClient = native
	if _, err := plain.EnsureAllocation(t.Context(), first); !errors.Is(err, errSharedWorkersNotDedicated) {
		t.Fatalf("a shared-worker profile was admitted without a dedicated install: %v", err)
	}
	if len(sharedPools(t, c)) != 0 {
		t.Fatal("a refused profile created a shared pool")
	}
	// Withdrawing the assertion retires every existing shared pool.
	_, record := sharedReady(t, c, native, first)
	if err := (&SpareWorkerReplenisher{Client: c, Control: native, Config: credentialConfig()}).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("a shared pool survived the withdrawn dedicated-install assertion")
	}
}

func TestSharedPoolCarriesOnlyProviderOwnedLabels(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	pool, _ := poolLedger(t, c, record.SharedWorker.Pool)
	want := map[string]string{sharedPoolLabel: record.SharedWorker.Label}
	if !maps.Equal(pool.GetLabels(), want) || !maps.Equal(record.TemplateSpec.GetWorkerSelector().GetMatchLabels(), want) || !maps.Equal(native.workers[record.Worker.Name].Labels, want) {
		t.Fatalf("shared pool, template or worker carries labels beyond the provider's random label: %v %v", pool.GetLabels(), record.TemplateSpec.GetWorkerSelector().GetMatchLabels())
	}
	if !maps.Equal(operatorPoolLabels(record), map[string]string{"selected": "native"}) {
		t.Fatalf("the operator pool labels were not kept as provenance: %v", operatorPoolLabels(record))
	}
	// A shared and a private template of the same lineage stay compatible.
	private := proto.Clone(record.TemplateSpec).(*pb.ActorTemplate)
	private.WorkerSelector = &pb.Selector{MatchLabels: map[string]string{"selected": "native", workerAllocationLabel: "a", workerInstanceLabel: "b"}}
	if !compatibleRestore(private, record.TemplateSpec) || !compatibleRestore(record.TemplateSpec, private) {
		t.Fatal("shared and private templates of one lineage are not restore-compatible")
	}
}

func TestSharedForeignTemplateFailsClosed(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	_, record := sharedReady(t, c, native, first)
	// Templates that cannot reach the pool are fine: another selector, or the
	// pool's own Atespace (where the ledger guards placement).
	native.templates["foreign/selective"] = &pb.ActorTemplate{Metadata: &pb.ResourceMetadata{Atespace: "foreign", Name: "selective"}, WorkerSelector: &pb.Selector{MatchLabels: map[string]string{"team": "other"}}}
	native.templates["native/open"] = &pb.ActorTemplate{Metadata: &pb.ResourceMetadata{Atespace: "native", Name: "open"}}
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	_, joined := sharedReady(t, c, native, second)
	if joined.SharedWorker.Pool != record.SharedWorker.Pool {
		t.Fatal("a template that cannot reach the pool closed it")
	}
	// A foreign template with an empty selector could land on any worker.
	native.templates["foreign/open"] = &pb.ActorTemplate{Metadata: &pb.ResourceMetadata{Atespace: "foreign", Name: "open"}}
	third := sharedWorkspace(t, c, first, "tenant", "third", "third-uid", binding)
	if _, err := sharedDriver(c, native).EnsureAllocation(t.Context(), third); !errors.Is(err, errForeignTemplate) {
		t.Fatalf("an admission proceeded while a foreign template could reach the pool: %v", err)
	}
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("a pool reachable by a foreign template was not quarantined and retired")
	}
	// One selecting the pool's exact label is caught by the sweep too.
	delete(native.templates, "foreign/open")
	fourth := sharedWorkspace(t, c, first, "tenant", "fourth", "fourth-uid", binding)
	_, fresh := sharedReady(t, c, native, fourth)
	native.templates["foreign/targeted"] = &pb.ActorTemplate{Metadata: &pb.ResourceMetadata{Atespace: "foreign", Name: "targeted"}, WorkerSelector: &pb.Selector{MatchLabels: map[string]string{sharedPoolLabel: fresh.SharedWorker.Label}}}
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, found := getPool(t, c, fresh.SharedWorker.Pool); found {
		t.Fatal("the sweep did not retire a pool a foreign template targets")
	}
}

// crashWorkerActors models upstream crashing the Actors of a worker whose Pod
// is gone.
func crashWorkerActors(native *nativeFixture, worker string) {
	for _, actor := range native.actors {
		if actor.Status.GetWorkerAssignment().GetWorker().GetName() == worker && actor.Status.State == pb.ActorState_ACTOR_STATE_RUNNING {
			actor.Status.State = pb.ActorState_ACTOR_STATE_CRASHED
			native.releaseWorker(actor)
		}
	}
}

func TestSharedSuspendUncertaintyTakesTheStrongPathWithoutReplay(t *testing.T) {
	for _, tc := range []struct {
		name   string
		before func(*nativeFixture)
		after  func(*nativeFixture)
		lost   bool
	}{
		{name: "rpc rejected before effect", before: func(native *nativeFixture) { native.failSuspendBeforeEffect = true }, lost: true},
		{name: "actor read fails during capture", after: func(native *nativeFixture) { native.failGetActor = 1 }},
		{name: "egress revocation fails", before: func(native *nativeFixture) { native.failEgressUpdate = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, native, first, binding := sharedFixture(t, 2)
			second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
			a, recordA := sharedReady(t, c, native, first)
			sharedReady(t, c, native, second)
			if tc.before != nil {
				tc.before(native)
			}
			var observed sdk.AllocationObservation
			for i := range 24 {
				tick()
				if i == 1 && tc.after != nil {
					tc.after(native)
				}
				var err error
				observed, err = sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity)
				if err != nil {
					continue
				}
				if _, record, _ := sharedDriver(c, native).read(t.Context(), first.Key); (tc.lost && record.Failure != "") || observed.State == sdk.AllocationStopped {
					break
				}
				if !podExists(t, c, recordA.Worker.Namespace, recordA.Worker.Pod, recordA.Worker.PodUID) {
					crashWorkerActors(native, recordA.Worker.Name)
				}
			}
			_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
			if err != nil {
				t.Fatal(err)
			}
			if record.SharedWorker.Strong == "" || record.SharedWorker.Proof != nil {
				t.Fatalf("uncertainty did not take the strong path: %#v", record.SharedWorker)
			}
			if native.suspendCalls != 1 {
				t.Fatalf("SuspendActor was issued %d times; uncertain work must never be replayed", native.suspendCalls)
			}
			if _, found := getPool(t, c, recordA.SharedWorker.Pool); found || podExists(t, c, recordA.Worker.Namespace, recordA.Worker.Pod, recordA.Worker.PodUID) {
				t.Fatal("the strong path did not retire the worker and prove its Pod absent")
			}
			if tc.lost {
				if record.Failure == "" {
					t.Fatal("a suspension that never took effect did not fail the instance")
				}
				if _, stopped := sharedRetired(t, c, native, first, a.Identity, false); stopped.Observation.RetainedData != nil || !stopped.WorkloadAbsent {
					t.Fatal("the lost instance did not retire through Stop with Pod absence")
				}
				return
			}
			if observed.State != sdk.AllocationStopped || observed.RetainedData == nil {
				t.Fatalf("the strong path lost the captured data: %#v", observed)
			}
		})
	}
}

func TestSharedUnavailableWorkerTakesTheStrongPath(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	a, recordA := sharedReady(t, c, native, first)
	native.afterSuspend = func(actor *pb.Actor) {
		if actor.Metadata.Name == recordA.Actor.Name {
			// MarkWorkerUnavailable keeps identity and epochs.
			native.workers[recordA.Worker.Name].Status.State = pb.WorkerState_WORKER_STATE_UNAVAILABLE
		}
	}
	_, record := sharedRetired(t, c, native, first, a.Identity, true)
	if record.SharedWorker.Strong == "" || record.SharedWorker.Proof != nil || !strings.Contains(record.SharedWorker.Strong, "UNAVAILABLE") {
		t.Fatalf("an unavailable worker did not take the strong path: %#v", record.SharedWorker)
	}
}

func TestSharedEgressSettlesBeforeSuspend(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	a, recordA := sharedReady(t, c, native, first)
	var suspendedAt time.Time
	native.beforeSuspend = func(actor *pb.Actor) {
		if actor.Metadata.Name == recordA.Actor.Name {
			suspendedAt = testNow()
		}
	}
	for range 3 {
		if _, err := sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity); err != nil {
			t.Fatal(err)
		}
	}
	if native.suspendCalls != 0 {
		t.Fatal("SuspendActor was issued before the revoked egress settled")
	}
	_, record := sharedRetired(t, c, native, first, a.Identity, true)
	if record.SharedWorker.EgressRevokedAt == nil || suspendedAt.Sub(record.SharedWorker.EgressRevokedAt.Time) < sharedEgressSettle {
		t.Fatalf("SuspendActor came %s after revocation, before the %s settle window", suspendedAt.Sub(record.SharedWorker.EgressRevokedAt.Time), sharedEgressSettle)
	}
}

func TestSharedSuspectNeighbourStopsAnUnbootedClaim(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	_, recordA := sharedReady(t, c, native, first)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	// B claims its slot, creates its template and Actor, and pauses before
	// Resume while its Actor UID enters the ledger.
	if _, err := sharedDriver(c, native).EnsureAllocation(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	_, recordB, err := sharedDriver(c, native).read(t.Context(), second.Key)
	if err != nil {
		t.Fatal(err)
	}
	if recordB.SharedWorker.Pool != recordA.SharedWorker.Pool || !recordB.Template.CreateIssued || recordB.BootRequested {
		t.Fatalf("fixture did not leave B with an unbooted claim: %#v", recordB.SharedWorker)
	}
	workspace := &api.ExecutionWorkspace{}
	if err := c.Get(t.Context(), clientKey(first.Key), workspace); err != nil {
		t.Fatal(err)
	}
	workspace.Annotations[operatorTaintAnnotation] = "{}"
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	boots := native.boots
	if _, err := sharedDriver(c, native).EnsureAllocation(t.Context(), second); err == nil {
		t.Fatal("an unbooted claim on a pool hosting a tainted workspace was resumed")
	}
	if native.boots != boots {
		t.Fatal("an Actor booted beside a tainted workspace")
	}
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
		t.Fatal("a pool hosting a tainted workspace was not strongly retired")
	}
}

func TestSharedClaimMovesToAPrivateWorkerWhenTheWorkspaceBecomesSuspect(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	fail := true
	flaky := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		options := &client.ListOptions{}
		options.ApplyOptions(opts)
		if fail && options.LabelSelector != nil && strings.Contains(options.LabelSelector.String(), sharedPoolLabel) {
			fail = false
			return errors.New("shared pool listing unavailable")
		}
		return w.List(ctx, list, opts...)
	}})
	if _, err := sharedDriver(flaky, native).EnsureAllocation(t.Context(), first); err == nil {
		t.Fatal("expected the listing failure")
	}
	workspace := &api.ExecutionWorkspace{}
	if err := c.Get(t.Context(), clientKey(first.Key), workspace); err != nil {
		t.Fatal(err)
	}
	workspace.Annotations[operatorTaintAnnotation] = "{}"
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	_, record := sharedReady(t, c, native, first)
	if record.SharedWorker != nil || record.RuntimePoolLabels[workerInstanceLabel] == "" || len(sharedPools(t, c)) != 0 {
		t.Fatalf("a workspace tainted before its claim did not move to a private worker: %#v", record.SharedWorker)
	}
}

func TestSharedBoundsRunIndependentlyOfAStalledNativeSweep(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
	native.block = true
	// A native sweep that stalls on every call is bounded by its deadline.
	stalled := sharedReplenisher(c, native)
	stalled.SweepDeadline = 50 * time.Millisecond
	started := time.Now()
	_ = stalled.Sweep(t.Context())
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("a stalled native sweep ran %s past its deadline", elapsed)
	}
	if _, found := getPool(t, c, record.SharedWorker.Pool); !found {
		t.Fatal("a pool inside its lifetime was retired")
	}
	// A long native sweep is blocked; the bounds loop still retires the pool
	// once its strong retirement time arrives.
	ctx, cancel := context.WithCancel(t.Context())
	var running sync.WaitGroup
	defer func() {
		cancel()
		running.Wait()
	}()
	blocked := sharedReplenisher(c, native)
	blocked.SweepDeadline = time.Hour
	running.Add(2)
	go func() {
		defer running.Done()
		_ = blocked.Sweep(ctx)
	}()
	go func() {
		defer running.Done()
		(&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).RunSharedBounds(ctx, 10*time.Millisecond)
	}()
	time.Sleep(50 * time.Millisecond)
	setNow(t, sharedRetireAt(pool, ledger))
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, found := getPool(t, c, record.SharedWorker.Pool); !found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the bounds loop was blocked behind a stalled native sweep")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSharedPostIntentSuspendFailuresTakeTheStrongPathWithoutReplay(t *testing.T) {
	for _, tc := range []struct {
		name   string
		inject func(*nativeFixture)
	}{
		{"template read fails", func(native *nativeFixture) { native.failGetTemplate = 3 }},
		{"invalid snapshot evidence", func(native *nativeFixture) {
			native.suspendFidelity = pb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
		}},
		{"tag create fails", func(native *nativeFixture) { native.failCreateTag = 1 }},
		{"tag read fails", func(native *nativeFixture) { native.failGetTag = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, native, first, binding := sharedFixture(t, 2)
			second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
			a, recordA := sharedReady(t, c, native, first)
			sharedReady(t, c, native, second)
			// First pass persists the suspension intent and revokes egress.
			if _, err := sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity); err != nil {
				t.Fatal(err)
			}
			if tc.name != "invalid snapshot evidence" && tc.name != "template read fails" {
				// Let the suspend take effect before the Tag-capture failure.
				native.afterSuspend = func(*pb.Actor) { tc.inject(native) }
			} else {
				tc.inject(native)
			}
			for range 8 {
				tick()
				_, _ = sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity)
			}
			_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
			if err != nil {
				t.Fatal(err)
			}
			if record.SharedWorker.Strong == "" {
				t.Fatalf("a post-intent failure did not select the strong path: %#v", record.SharedWorker)
			}
			if native.suspendCalls > 1 {
				t.Fatalf("SuspendActor was replayed: %d calls", native.suspendCalls)
			}
			if _, found := getPool(t, c, recordA.SharedWorker.Pool); found || podExists(t, c, recordA.Worker.Namespace, recordA.Worker.Pod, recordA.Worker.PodUID) {
				t.Fatal("the strong path did not delete the pool and prove its Pod absent")
			}
		})
	}
}

func TestSharedPersistedStrongMarkerFinishesDeletionAfterACrash(t *testing.T) {
	for _, driverPath := range []string{"bounds", "retirement"} {
		t.Run(driverPath, func(t *testing.T) {
			c, native, first, _ := sharedFixture(t, 2)
			a, recordA := sharedReady(t, c, native, first)
			// The suspend took effect; then the provider persisted Strong and
			// crashed before deleting the pool. Tag capture keeps failing.
			if _, err := sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity); err != nil {
				t.Fatal(err)
			}
			cm, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
			if err != nil {
				t.Fatal(err)
			}
			record.SharedWorker.Strong = "crashed after persisting the strong path"
			if err := sharedDriver(c, native).save(t.Context(), cm, record); err != nil {
				t.Fatal(err)
			}
			native.failCreateTag = 100
			if driverPath == "bounds" {
				if err := (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context()); err != nil {
					t.Fatal(err)
				}
			} else {
				for range 4 {
					tick()
					_, _ = sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity)
				}
			}
			if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
				t.Fatal("a persisted strong marker did not drive the pool's deletion")
			}
			if native.suspendCalls > 1 {
				t.Fatalf("SuspendActor was replayed: %d calls", native.suspendCalls)
			}
		})
	}
}

func TestSharedUnreadableLifetimeEvidenceRetiresAtOnce(t *testing.T) {
	for _, corrupt := range []string{"ledger", "deadline", "extended"} {
		t.Run(corrupt, func(t *testing.T) {
			c, native, first, _ := sharedFixture(t, 2)
			_, record := sharedReady(t, c, native, first)
			pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
			annotations := maps.Clone(pool.GetAnnotations())
			switch corrupt {
			case "ledger":
				annotations[sharedLedgerAnnotation] = "{"
			case "deadline":
				delete(annotations, sharedDeadlineAnnotation)
			case "extended":
				// A rewritten ledger cannot extend the creation-time deadline.
				ledger.BornAt = metav1.NewTime(ledger.BornAt.Add(12 * time.Hour))
				if err := setSharedLedger(pool, ledger); err != nil {
					t.Fatal(err)
				}
				maps.Copy(annotations, pool.GetAnnotations())
			}
			pool.SetAnnotations(annotations)
			if err := c.Update(t.Context(), pool); err != nil {
				t.Fatal(err)
			}
			if err := (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, found := getPool(t, c, record.SharedWorker.Pool); found {
				t.Fatal("unreadable or inconsistent lifetime evidence did not retire the pool at once")
			}
		})
	}
}

func TestSharedWarmCreationRequiresACleanTemplateInventory(t *testing.T) {
	c, native, _, _ := sharedFixture(t, 2)
	item := &profile.SubstrateWorkspaceProfile{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "tenant", Name: "shared"}, item); err != nil {
		t.Fatal(err)
	}
	item.Annotations = map[string]string{spareWorkersAnnotation: "1"}
	if err := c.Update(t.Context(), item); err != nil {
		t.Fatal(err)
	}
	native.templates["foreign/open"] = &pb.ActorTemplate{Metadata: &pb.ResourceMetadata{Atespace: "foreign", Name: "open"}}
	_ = sharedReplenisher(c, native).Sweep(t.Context())
	if len(sharedPools(t, c)) != 0 {
		t.Fatal("a warm shared pool was created beside a foreign empty-selector template")
	}
	delete(native.templates, "foreign/open")
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(sharedPools(t, c)) != 1 {
		t.Fatal("a clean inventory did not allow the warm pool")
	}
}

func TestSharedEgressSettleFollowsTheDeclaredGatewayTTL(t *testing.T) {
	if got := (Config{}).egressSettle(); got != sharedEgressSettle {
		t.Fatalf("default settle = %s, want %s", got, sharedEgressSettle)
	}
	if got := (Config{EgressPolicyCacheTTL: time.Minute}).egressSettle(); got != time.Minute+sharedEgressSettleMargin {
		t.Fatalf("settle for a 1m gateway cache = %s", got)
	}
	if got := (Config{EgressPolicyCacheTTL: time.Second}).egressSettle(); got != dnsRelayPolicyTTL+sharedEgressSettleMargin {
		t.Fatalf("settle below the DNS relay cache = %s", got)
	}
	c, native, first, _ := sharedFixture(t, 2)
	a, _ := sharedReady(t, c, native, first)
	config := sharedConfig()
	config.EgressPolicyCacheTTL = time.Minute
	d := New(c, native, config)
	d.challengeClient = native
	for range 4 {
		tick()
		if _, err := d.SuspendInstance(t.Context(), first.Key, a.Identity); err != nil {
			t.Fatal(err)
		}
	}
	if native.suspendCalls != 0 {
		t.Fatal("SuspendActor was issued before a 1m gateway cache expired")
	}
	for range 4 {
		testClock.Add(int64(time.Minute))
		if _, err := d.SuspendInstance(t.Context(), first.Key, a.Identity); err != nil {
			t.Fatal(err)
		}
	}
	if native.suspendCalls != 1 {
		t.Fatalf("SuspendActor calls after the settle window = %d", native.suspendCalls)
	}
}

func TestSharedPersistedRetirementStateIsDeletionIntentForTheBoundsLoop(t *testing.T) {
	for _, state := range []string{sharedQuarantined, sharedRetiring} {
		t.Run(state, func(t *testing.T) {
			c, native, first, _ := sharedFixture(t, 2)
			_, record := sharedReady(t, c, native, first)
			// A sweep saved the state, then crashed (or ran out of time) before
			// deleting the pool; no journal carries a strong marker.
			pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
			ledger.State, ledger.Reason = state, "the shared worker hosts an Actor outside its ledger"
			if err := setSharedLedger(pool, ledger); err != nil {
				t.Fatal(err)
			}
			if err := c.Update(t.Context(), pool); err != nil {
				t.Fatal(err)
			}
			native.nativeDown = true
			if err := (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, found := getPool(t, c, record.SharedWorker.Pool); found {
				t.Fatalf("a persisted %s state did not drive the pool's deletion", state)
			}
		})
	}
}

func TestSharedRejectedEvidenceSuspendTakesTheStrongPathAtOnce(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	a, recordA := sharedReady(t, c, native, first)
	native.failSuspendBeforeEffect = true
	for range 4 {
		tick()
		if _, err := sharedDriver(c, native).StopInstance(t.Context(), first.Key, a.Identity); err != nil {
			t.Fatal(err)
		}
		if native.suspendCalls == 1 {
			break
		}
	}
	_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if native.suspendCalls != 1 || record.SharedWorker.Strong == "" || record.SharedWorker.Proof != nil {
		t.Fatalf("a rejected evidence suspend did not persist the strong path: calls=%d %#v", native.suspendCalls, record.SharedWorker)
	}
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
		t.Fatal("the pool's retirement did not start before the failed Stop pass returned")
	}
	_, stopped := sharedRetired(t, c, native, first, a.Identity, false)
	if native.suspendCalls != 1 || !stopped.WorkloadAbsent || stopped.SharedWorker.Proof != nil {
		t.Fatalf("the evidence suspend was replayed or proven by interim evidence: calls=%d", native.suspendCalls)
	}
}

func TestSharedJournalFailureIsSuspectBeforeTheWorkspaceReportsFailed(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	_, recordA := sharedReady(t, c, native, first)
	cm, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	record.Failure = nativeActorStopped
	if err := sharedDriver(c, native).save(t.Context(), cm, record); err != nil {
		t.Fatal(err)
	}
	// Admission never places another Actor beside the failed instance.
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	_, recordB := sharedReady(t, c, native, second)
	if recordB.SharedWorker.Pool == recordA.SharedWorker.Pool {
		t.Fatal("an Actor was placed beside an instance whose journal recorded a failure")
	}
	// The Kubernetes-only bounds loop and the sweep both retire the pool.
	if err := (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
		t.Fatal("a pool hosting a failed instance kept running")
	}
}

func TestSharedStrongMarkerWinsOverAJournaledInterimProof(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	a, recordA := sharedReady(t, c, native, first)
	sharedReady(t, c, native, second)
	// Drive the Stop until its interim proof is journaled.
	for range 12 {
		tick()
		if _, err := sharedDriver(c, native).StopInstance(t.Context(), first.Key, a.Identity); err != nil {
			t.Fatal(err)
		}
		if _, record, _ := sharedDriver(c, native).read(t.Context(), first.Key); record.SharedWorker.Proof != nil {
			break
		}
	}
	// Model a crash after the proof, before WorkloadAbsent, then a later Actor
	// read failure that persisted Strong, and a retirement that failed before
	// closing the pool's ledger.
	cm, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.SharedWorker.Proof == nil {
		t.Fatal("fixture did not journal an interim proof")
	}
	record.WorkloadAbsent = false
	record.Observation.State = sdk.AllocationPending
	record.SharedWorker.Strong = "the Actor could not be read during retirement"
	if err := sharedDriver(c, native).save(t.Context(), cm, record); err != nil {
		t.Fatal(err)
	}
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); !found {
		t.Fatal("fixture lost the pool before the strong path ran")
	}
	// A direct release cannot drop the entry while the strong path is open:
	// it drives the pool's deletion instead and refuses until the Pod is gone.
	if err := sharedDriver(c, native).releaseSharedSlot(t.Context(), cm, record); err == nil {
		t.Fatal("a slot with an unfinished strong marker was released")
	}
	if _, current, _ := sharedDriver(c, native).read(t.Context(), first.Key); current.SharedWorker.Released {
		t.Fatal("the journal released a slot whose strong retirement had not finished")
	}
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
		t.Fatal("the refused release did not drive the pool's deletion")
	}
	_, stopped := sharedRetired(t, c, native, first, a.Identity, false)
	if stopped.SharedWorker.Strong == "" || !stopped.SharedWorker.Released {
		t.Fatalf("the strong retirement did not finish: %#v", stopped.SharedWorker)
	}
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); found || podExists(t, c, recordA.Worker.Namespace, recordA.Worker.Pod, recordA.Worker.PodUID) {
		t.Fatal("the interim proof shortcut skipped pool deletion and the absence proof")
	}
}

func TestSharedStrongTriggersPersistAfterTheRequestContextExpires(t *testing.T) {
	for _, tc := range []struct {
		name  string
		block func(*nativeFixture)
	}{
		{"suspend outlives the request", func(native *nativeFixture) { native.blockSuspend = true }},
		{"actor read outlives the request", func(native *nativeFixture) { native.blockGetActor = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, native, first, binding := sharedFixture(t, 2)
			second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
			a, recordA := sharedReady(t, c, native, first)
			sharedReady(t, c, native, second)
			// Persist the suspension intent; the suspend is issued on a later
			// pass, once the revoked egress has settled.
			if _, err := sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity); err != nil {
				t.Fatal(err)
			}
			if native.suspendCalls != 0 {
				t.Fatal("fixture issued the suspend before egress settled")
			}
			tc.block(native)
			for range 3 {
				tick()
				ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
				_, err := sharedDriver(contextAwareClient(c), native).SuspendInstance(ctx, first.Key, a.Identity)
				cancel()
				if err == nil {
					continue
				}
				if _, record, _ := sharedDriver(c, native).read(t.Context(), first.Key); record.SharedWorker.Strong != "" {
					break
				}
			}
			_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
			if err != nil {
				t.Fatal(err)
			}
			if record.SharedWorker.Strong == "" {
				t.Fatal("a strong trigger after the request deadline was not persisted")
			}
			if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
				t.Fatal("the pool's retirement did not start after the request deadline")
			}
			if native.suspendCalls > 1 {
				t.Fatalf("SuspendActor was replayed: %d calls", native.suspendCalls)
			}
		})
	}
}

func TestSharedSuspectWorkspaceRetiresThePoolWhileNativeIsDown(t *testing.T) {
	for _, change := range []string{"taint", "quarantine", "failed"} {
		t.Run(change, func(t *testing.T) {
			c, native, first, binding := sharedFixture(t, 2)
			second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
			_, recordA := sharedReady(t, c, native, first)
			sharedReady(t, c, native, second)
			workspace := &api.ExecutionWorkspace{}
			if err := c.Get(t.Context(), clientKey(second.Key), workspace); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "taint":
				workspace.Annotations[operatorTaintAnnotation] = "{}"
			case "quarantine":
				workspace.Spec.DesiredState = api.ExecutionWorkspaceDesiredQuarantined
			}
			if err := c.Update(t.Context(), workspace); err != nil {
				t.Fatal(err)
			}
			if change == "failed" {
				workspace.Status.State = api.ExecutionWorkspaceStateFailed
				if err := c.Status().Update(t.Context(), workspace); err != nil {
					t.Fatal(err)
				}
			}
			// Every native call stalls; the sweep's leading pass and the
			// independent bounds loop act on Kubernetes alone.
			native.block = true
			swept := sharedReplenisher(c, native)
			swept.SweepDeadline = 50 * time.Millisecond
			_ = swept.Sweep(t.Context())
			if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
				t.Fatal("a pool hosting a suspect workspace kept running while the native API was down")
			}
		})
	}
}

// contextAwareClient fails Kubernetes calls whose context has ended, as a
// real API client does; the fake client ignores contexts.
func contextAwareClient(c client.Client) client.Client {
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, w client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return w.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return w.List(ctx, list, opts...)
		},
		Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return w.Update(ctx, obj, opts...)
		},
		Delete: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return w.Delete(ctx, obj, opts...)
		},
	})
}

func TestSharedPoolCreateRetryRechecksTheTemplateInventory(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	createFailed := false
	failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Create: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if _, shared := obj.GetLabels()[sharedPoolLabel]; shared && !createFailed {
			createFailed = true
			return errors.New("injected pool create failure")
		}
		return inner.Create(ctx, obj, opts...)
	}})
	for range 16 {
		if _, err := sharedDriver(failing, native).EnsureAllocation(t.Context(), first); err != nil || createFailed {
			break
		}
	}
	if !createFailed {
		t.Fatal("the shared pool Create was never attempted")
	}
	_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.SharedWorker == nil || !record.SharedWorker.CreateIssued || len(sharedPools(t, c)) != 0 {
		t.Fatal("the failed Create did not leave an issued creation with no pool")
	}
	// A foreign empty-selector template appears before the retry.
	native.templates["foreign/open"] = &pb.ActorTemplate{Metadata: &pb.ResourceMetadata{Atespace: "foreign", Name: "open"}}
	if _, err := sharedDriver(c, native).EnsureAllocation(t.Context(), first); !errors.Is(err, errForeignTemplate) {
		t.Fatalf("a creation retry skipped the template inventory: %v", err)
	}
	if len(sharedPools(t, c)) != 0 {
		t.Fatal("a creation retry created a pool a foreign template could reach")
	}
	delete(native.templates, "foreign/open")
	sharedReady(t, c, native, first)
	if len(sharedPools(t, c)) != 1 {
		t.Fatal("a clean inventory did not allow the retried creation")
	}
}

func TestSharedUndecodableHostedJournalIsDoubt(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	_, recordA := sharedReady(t, c, native, first)
	cm := &corev1.ConfigMap{}
	if err := c.Get(t.Context(), journalKey(first.Key), cm); err != nil {
		t.Fatal(err)
	}
	cm.Data = map[string]string{journalDataKey: "{"}
	if err := c.Update(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	// Admission never places an Actor beside an allocation whose Strong and
	// Failure markers cannot be read.
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	_, recordB := sharedReady(t, c, native, second)
	if recordB.SharedWorker.Pool == recordA.SharedWorker.Pool {
		t.Fatal("an Actor joined a pool hosting an undecodable journal")
	}
	if err := (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
		t.Fatal("a pool hosting an undecodable journal was not strongly retired")
	}
}

func TestSharedDeadlineKeepsSubSecondLifetimePrecision(t *testing.T) {
	scope := sharedScope{Namespace: "tenant", Atespace: "native", ProfileUID: "p", ProfileHash: "h", SourceUID: "s", Shape: "x", CPUMillicores: 1, MemoryBytes: 1, MaxActors: 2, Lifetime: metav1.Duration{Duration: 3*time.Hour + 500*time.Millisecond}}
	ledger := &sharedLedger{Version: sharedLedgerVersion, Scope: scope, BornAt: metav1.NewTime(time.Now().UTC().Truncate(time.Second)), State: sharedAdmitting, Entries: []sharedLedgerEntry{}}
	pool := poolObject()
	pool.SetCreationTimestamp(ledger.BornAt)
	if err := setSharedLedger(pool, ledger); err != nil {
		t.Fatal(err)
	}
	setSharedDeadline(pool, ledger)
	if _, _, evidence := sharedBoundsOf(pool); !evidence {
		t.Fatalf("a fractional lifetime's deadline annotation %q is inconsistent with its ledger", pool.GetAnnotations()[sharedDeadlineAnnotation])
	}
	// Validation rejects sub-second lifetimes outright.
	item := &profile.SubstrateWorkspaceProfile{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant"}, Spec: profile.SubstrateWorkspaceProfileSpec{TemplateRef: profile.SubstrateTemplateReference{Name: "base"}, Suspend: &profile.SubstrateSuspendPolicy{Mode: profile.SubstrateSuspendModeDataOnly}, Placement: &profile.SubstratePlacementPolicy{SharedWorkers: &profile.SharedWorkersPolicy{MaxActorsPerWorker: 2, MaxWorkerLifetime: &metav1.Duration{Duration: 3*time.Hour + 500*time.Millisecond}}}}}
	if err := item.Validate(); err == nil {
		t.Fatal("a sub-second maxWorkerLifetime was accepted")
	}
}

func TestSharedSelectorMatchingRequiresKeyPresence(t *testing.T) {
	worker := map[string]string{sharedPoolLabel: "abc"}
	for _, tc := range []struct {
		selector map[string]string
		match    bool
	}{
		{nil, true},
		{map[string]string{}, true},
		{map[string]string{sharedPoolLabel: "abc"}, true},
		{map[string]string{sharedPoolLabel: "other"}, false},
		{map[string]string{"dedicated": ""}, false},
		{map[string]string{sharedPoolLabel: "abc", "dedicated": ""}, false},
	} {
		if got := selectorMatches(tc.selector, worker); got != tc.match {
			t.Fatalf("selectorMatches(%v) = %v, want %v", tc.selector, got, tc.match)
		}
	}
	if !selectorMatches(map[string]string{"dedicated": ""}, map[string]string{"dedicated": ""}) {
		t.Fatal("a present empty-valued label did not match")
	}
	// A foreign template selecting an absent key with an empty value does not
	// quarantine a shared pool.
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	native.templates["foreign/dedicated"] = &pb.ActorTemplate{Metadata: &pb.ResourceMetadata{Atespace: "foreign", Name: "dedicated"}, WorkerSelector: &pb.Selector{MatchLabels: map[string]string{"dedicated": ""}}}
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, ledger := poolLedger(t, c, record.SharedWorker.Pool); ledger.State != sharedAdmitting {
		t.Fatalf("a template that cannot reach the pool changed its state to %s", ledger.State)
	}
}

func TestSharedPoolSizedByARuntimeOverrideStaysAdmitting(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	override := func(request sdk.WorkloadRequest) sdk.WorkloadRequest {
		request.Runtime = request.Runtime.DeepCopy()
		limits := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("256Mi")}
		request.Runtime.Template.Spec.Containers[0].Resources.Limits = limits
		request.Resources = corev1.ResourceRequirements{Limits: limits}
		request.Revision, _ = sdk.WorkloadRevision(request)
		if err := admit(c)(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		return request
	}
	first = override(first)
	_, a := sharedReady(t, c, native, first)
	if a.SharedWorker.Scope.CPUMillicores != 500 || a.SharedWorker.Scope.MemoryBytes != 256<<20 {
		t.Fatalf("the runtime override did not size the shared scope: %#v", a.SharedWorker.Scope)
	}
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, ledger := poolLedger(t, c, a.SharedWorker.Pool); ledger.State != sharedAdmitting {
		t.Fatalf("a valid runtime-sized shared pool was closed: %s %s", ledger.State, ledger.Reason)
	}
	second := override(sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding))
	_, b := sharedReady(t, c, native, second)
	if b.SharedWorker.Pool != a.SharedWorker.Pool {
		t.Fatal("a runtime-sized shared pool was not reused")
	}
	// A pool whose recorded size no longer matches the current operator pool
	// still closes.
	pool, ledger := poolLedger(t, c, a.SharedWorker.Pool)
	ledger.Scope.Shape = "sha256:stale"
	if err := setSharedLedger(pool, ledger); err != nil {
		t.Fatal(err)
	}
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, ledger := poolLedger(t, c, a.SharedWorker.Pool); ledger.State == sharedAdmitting {
		t.Fatal("a pool with a stale shape kept admitting")
	}
}

func TestSharedEvidenceStopJournalFailureTakesTheStrongPath(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	a, recordA := sharedReady(t, c, native, first)
	sharedReady(t, c, native, second)
	// The journal write that records the suspended snapshot fails after the
	// evidence suspend took effect.
	failed := false
	lossy := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if cm, ok := obj.(*corev1.ConfigMap); ok && !failed && strings.Contains(cm.Data[journalDataKey], `"snapshotDigest"`) {
			failed = true
			return errors.New("journal write unavailable")
		}
		return w.Update(ctx, obj, opts...)
	}})
	for range 6 {
		tick()
		if _, err := sharedDriver(lossy, native).StopInstance(t.Context(), first.Key, a.Identity); err != nil {
			t.Fatal(err)
		}
		if failed {
			break
		}
	}
	if !failed {
		t.Fatal("fixture did not fail the snapshot journal write")
	}
	_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.SharedWorker.Strong == "" || record.SharedWorker.Proof != nil {
		t.Fatalf("a failed journal write after the evidence suspend did not persist the strong path: %#v", record.SharedWorker)
	}
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
		t.Fatal("the pool's retirement did not start on the failing pass")
	}
	_, stopped := sharedRetired(t, c, native, first, a.Identity, false)
	if native.suspendCalls != 1 || stopped.SharedWorker.Proof != nil || !stopped.WorkloadAbsent {
		t.Fatalf("the Stop replayed the suspend or fell back to interim evidence: calls=%d", native.suspendCalls)
	}
}

func TestSharedUnverifiableStopTakesTheStrongPath(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	a, recordA := sharedReady(t, c, native, first)
	// The shared pool cannot be read during the Stop's verification.
	failures := 1
	flaky := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, w client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "WorkerPool" && key.Name == recordA.SharedWorker.Pool && failures > 0 {
			failures--
			return apierrors.NewInternalError(errors.New("pool read unavailable"))
		}
		return w.Get(ctx, key, obj, opts...)
	}})
	tick()
	_, _ = sharedDriver(flaky, native).StopInstance(t.Context(), first.Key, a.Identity)
	_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if failures != 0 || record.SharedWorker.Strong == "" {
		t.Fatalf("an unverifiable Stop did not persist the strong path: %#v", record.SharedWorker)
	}
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
		t.Fatal("an unverifiable Stop did not start the pool's deletion")
	}
	if native.suspendCalls != 0 {
		t.Fatal("an unverifiable Stop issued the evidence suspend")
	}
}

func TestSharedLedgerEntryDisownedByItsJournalIsDoubt(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	_, recordA := sharedReady(t, c, native, first)
	// A healthy private allocation whose journal holds no shared slot.
	private := sharedWorkspace(t, c, first, "tenant", "private", "private-uid", binding)
	private.Runtime.Egress = fixtureEgress()
	private.Runtime.RequiredFeatures = nil
	private.Revision, _ = sdk.WorkloadRevision(private)
	if err := admit(c)(t.Context(), private); err != nil {
		t.Fatal(err)
	}
	sharedReady(t, c, native, private)
	other := &corev1.ConfigMap{}
	if err := c.Get(t.Context(), journalKey(private.Key), other); err != nil {
		t.Fatal(err)
	}
	// Rewrite A's entry to name the private journal, keeping A's Actor.
	pool, ledger := poolLedger(t, c, recordA.SharedWorker.Pool)
	ledger.Entries[0].Journal = other.Namespace + "/" + other.Name + "/" + string(other.UID) + "/1"
	if err := setSharedLedger(pool, ledger); err != nil {
		t.Fatal(err)
	}
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	if suspect, err := sharedDriver(c, native).sharedPoolHostsSuspect(t.Context(), pool, ledger); err != nil || !suspect {
		t.Fatalf("admission accepted a booted entry its journal disowns: %v %v", suspect, err)
	}
	if err := (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
		t.Fatal("a pool hiding an Actor behind another allocation's journal was not retired")
	}
}

func TestSharedInvalidSnapshotAfterStrongAbsenceBecomesInstanceLoss(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	a, recordA := sharedReady(t, c, native, first)
	sharedReady(t, c, native, second)
	if _, err := sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity); err != nil {
		t.Fatal(err)
	}
	native.suspendFidelity = pb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	for range 8 {
		tick()
		_, _ = sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity)
	}
	_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.SharedWorker.Strong == "" || record.Failure == "" || record.Checkpoint != nil {
		t.Fatalf("invalid evidence after proven absence did not become an honest instance loss: failure=%q checkpoint=%v", record.Failure, record.Checkpoint)
	}
	if podExists(t, c, recordA.Worker.Namespace, recordA.Worker.Pod, recordA.Worker.PodUID) {
		t.Fatal("the instance was reported lost before its worker Pod was proven absent")
	}
	_, stopped := sharedRetired(t, c, native, first, a.Identity, false)
	if stopped.Observation.RetainedData != nil || native.suspendCalls != 1 {
		t.Fatalf("the lost instance's Stop invented retained data or replayed the suspend: calls=%d", native.suspendCalls)
	}
}

func TestSharedWarmCreationIgnoresATemplateForAnEmptyPoolLabel(t *testing.T) {
	c, native, _, _ := sharedFixture(t, 2)
	item := &profile.SubstrateWorkspaceProfile{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "tenant", Name: "shared"}, item); err != nil {
		t.Fatal(err)
	}
	item.Annotations = map[string]string{spareWorkersAnnotation: "1"}
	if err := c.Update(t.Context(), item); err != nil {
		t.Fatal(err)
	}
	// A generated shared label is never empty, so this cannot reach a pool.
	native.templates["foreign/empty-value"] = &pb.ActorTemplate{Metadata: &pb.ResourceMetadata{Atespace: "foreign", Name: "empty-value"}, WorkerSelector: &pb.Selector{MatchLabels: map[string]string{sharedPoolLabel: ""}}}
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(sharedPools(t, c)) != 1 {
		t.Fatal("a template that can never reach a shared pool blocked warm provisioning")
	}
}
