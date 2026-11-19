package substrate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
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
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	sigsyaml "sigs.k8s.io/yaml"
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
	sharedProvenanceHealth.reset()
	t.Cleanup(func() { sharedNow = previous; sharedProvenanceHealth.reset() })
}

func tick() { testClock.Add(int64(sharedEgressSettle + time.Second)) }

// createSharedProfile creates an opted-in profile and returns its binding.
func createSharedProfile(t *testing.T, c client.Client, namespace, name string, uid types.UID, maxActors int32, lifetime ...time.Duration) *api.ImmutableObjectBinding {
	t.Helper()
	parameter := &profile.SubstrateWorkspaceProfile{TypeMeta: metav1.TypeMeta{APIVersion: profile.GroupVersion.String(), Kind: "SubstrateWorkspaceProfile"}, ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: uid, Generation: 1}, Spec: profile.SubstrateWorkspaceProfileSpec{
		TemplateRef: profile.SubstrateTemplateReference{Name: "base", Namespace: "native"}, Suspend: &profile.SubstrateSuspendPolicy{Mode: profile.SubstrateSuspendModeDataOnly},
		Placement: &profile.SubstratePlacementPolicy{SharedWorkers: &profile.SharedWorkersPolicy{MaxActorsPerWorker: maxActors}},
	}}
	if len(lifetime) == 1 {
		parameter.Spec.Placement.SharedWorkers.MaxWorkerLifetime = &metav1.Duration{Duration: lifetime[0]}
	}
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
	// A template that cannot reach the pool is fine.
	native.templates["foreign/selective"] = &pb.ActorTemplate{Metadata: &pb.ResourceMetadata{Atespace: "foreign", Name: "selective"}, WorkerSelector: &pb.Selector{MatchLabels: map[string]string{"team": "other"}}}
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
	if _, _, evidence := sharedBoundsOf(pool, sharedProvenance{}, true); !evidence {
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
	// A ledger whose scope no longer matches its journals' frozen scope is
	// doubt, so the bounds loop retires the pool outright.
	if pool, found := getPool(t, c, a.SharedWorker.Pool); found {
		if ledger, err := readSharedLedger(pool); err == nil && ledger.State == sharedAdmitting {
			t.Fatal("a pool with a stale shape kept admitting")
		}
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

func TestSharedClosingPoolNeverUncountedFromTheWarmReserve(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
	item := &profile.SubstrateWorkspaceProfile{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "tenant", Name: "shared"}, item); err != nil {
		t.Fatal(err)
	}
	item.Annotations = map[string]string{spareWorkersAnnotation: "2"}
	if err := c.Update(t.Context(), item); err != nil {
		t.Fatal(err)
	}
	// The hosting pool's admission window has ended: it never counted toward
	// the reserve, so closing it must not take its free slot away again.
	setNow(t, sharedAdmissionEnds(pool, ledger).Add(time.Minute))
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, closed := poolLedger(t, c, record.SharedWorker.Pool); closed.State != sharedDraining {
		t.Fatalf("the expired pool is %s, want Draining", closed.State)
	}
	// Two free slots are wanted and one new 2-slot pool provides them.
	if pools := sharedPools(t, c); len(pools) != 2 {
		t.Fatalf("the sweep left %d shared pools, want the draining pool and one new warm pool", len(pools))
	}
}

func TestSharedConcurrentSlotReleaseIsNotDoubt(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	_, recordA := sharedReady(t, c, native, first)
	cmA := &corev1.ConfigMap{}
	if err := c.Get(t.Context(), journalKey(first.Key), cmA); err != nil {
		t.Fatal(err)
	}
	// A releases its slot exactly while another allocation's admission reads
	// A's journal: the release removes the ledger entry, then marks the
	// journal Released, as releaseSharedSlot does, after the reader's pool
	// snapshot was taken.
	released := false
	racing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, w client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if _, ok := obj.(*corev1.ConfigMap); ok && key.Name == cmA.Name && !released {
			released = true
			d := sharedDriver(c, native)
			cm, record, err := d.read(ctx, first.Key)
			if err != nil {
				return err
			}
			// A release follows its Actor's deletion.
			key := refKey(actorRef(record))
			native.releaseWorker(native.actors[key])
			delete(native.actors, key)
			if err := d.releaseSharedSlot(ctx, cm, record); err != nil {
				return err
			}
		}
		return w.Get(ctx, key, obj, opts...)
	}})
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	var recordB *journalRecord
	for range 16 {
		observed, err := sharedDriver(racing, native).EnsureAllocation(t.Context(), second)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == sdk.AllocationReady {
			_, recordB, _ = sharedDriver(c, native).read(t.Context(), second.Key)
			break
		}
	}
	if !released {
		t.Fatal("fixture did not overlap admission with the slot release")
	}
	if recordB == nil || recordB.SharedWorker.Pool != recordA.SharedWorker.Pool {
		t.Fatal("the admitting allocation did not join the healthy pool")
	}
	if _, ledger := poolLedger(t, c, recordA.SharedWorker.Pool); ledger.State != sharedAdmitting {
		t.Fatalf("a concurrent slot release retired a healthy pool: %s %s", ledger.State, ledger.Reason)
	}
}

func TestSharedPoolWithoutItsLabelIsStillDiscoveredAndRetired(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	pool, _ := poolLedger(t, c, record.SharedWorker.Pool)
	pool.SetLabels(nil)
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	native.nativeDown = true
	if err := (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("a shared pool whose label was removed escaped the bounds loop")
	}
}

func TestSharedHardRetirementCannotBeBlockedByALedgerWrite(t *testing.T) {
	t.Run("oversized scope", func(t *testing.T) {
		c, native, first, _ := sharedFixture(t, 2)
		_, record := sharedReady(t, c, native, first)
		pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
		// A scope field padded toward the annotation limit is no longer a
		// valid ledger, so it can never leave too little room to retire.
		ledger.Scope.Shape = strings.Repeat("x", 30<<10)
		data, err := json.Marshal(ledger)
		if err != nil {
			t.Fatal(err)
		}
		annotations := maps.Clone(pool.GetAnnotations())
		annotations[sharedLedgerAnnotation] = string(data)
		pool.SetAnnotations(annotations)
		if err := c.Update(t.Context(), pool); err != nil {
			t.Fatal(err)
		}
		if _, err := readSharedLedger(pool); err == nil {
			t.Fatal("a ledger with an oversized scope field was accepted")
		}
		if err := (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context()); err != nil {
			t.Fatal(err)
		}
		if _, found := getPool(t, c, record.SharedWorker.Pool); found {
			t.Fatal("an oversized ledger blocked hard retirement")
		}
	})
	t.Run("ledger write fails", func(t *testing.T) {
		c, native, first, _ := sharedFixture(t, 2)
		_, record := sharedReady(t, c, native, first)
		pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
		setNow(t, sharedRetireAt(pool, ledger))
		failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "WorkerPool" {
				return apierrors.NewConflict(corev1.Resource("workerpools"), u.GetName(), errors.New("ledger write refused"))
			}
			return w.Update(ctx, obj, opts...)
		}})
		_ = (&SpareWorkerReplenisher{Client: failing, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
		if _, found := getPool(t, c, record.SharedWorker.Pool); found {
			t.Fatal("a failed ledger write blocked the lifetime retirement's Delete")
		}
	})
	t.Run("escalation without a started deletion", func(t *testing.T) {
		c, native, first, _ := sharedFixture(t, 2)
		_, record := sharedReady(t, c, native, first)
		pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
		setNow(t, sharedEscalateAt(pool, ledger))
		var escalated []int64
		failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Delete: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "WorkerPool" {
				return apierrors.NewInternalError(errors.New("pool delete refused"))
			}
			if _, ok := obj.(*corev1.Pod); ok {
				options := &client.DeleteOptions{}
				options.ApplyOptions(opts)
				if options.GracePeriodSeconds != nil {
					escalated = append(escalated, *options.GracePeriodSeconds)
				}
			}
			return w.Delete(ctx, obj, opts...)
		}})
		_ = (&SpareWorkerReplenisher{Client: failing, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
		if len(escalated) != 1 || escalated[0] != sharedEscalationGrace {
			t.Fatalf("a failed pool deletion postponed the worker Pod's escalation: %v", escalated)
		}
	})
}

func TestSharedRewrittenLifetimeCannotExtendTheBound(t *testing.T) {
	c, native, request := fixture(t, true)
	useTestClock(t)
	binding := createSharedProfile(t, c, "tenant", "short", "short-profile-uid", 2, 3*time.Hour)
	first := sharedWorkspace(t, c, request, "tenant", "workspace", "workspace-uid", binding)
	_, record := sharedReady(t, c, native, first)
	pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
	created := pool.GetCreationTimestamp().Time
	// A privileged writer rewrites the ledger lifetime and the deadline
	// annotation consistently, to 24h.
	ledger.Scope.Lifetime = metav1.Duration{Duration: 24 * time.Hour}
	if err := setSharedLedger(pool, ledger); err != nil {
		t.Fatal(err)
	}
	annotations := maps.Clone(pool.GetAnnotations())
	annotations[sharedDeadlineAnnotation] = ledger.BornAt.Add(24 * time.Hour).UTC().Format(time.RFC3339Nano)
	pool.SetAnnotations(annotations)
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	native.nativeDown = true
	// The journal froze 3h: the bound and the disagreement both hold.
	_, escalateAt, evidence := sharedBoundsOf(pool, sharedProvenance{found: true, lifetime: 3 * time.Hour, scopes: []sharedScope{record.SharedWorker.Scope}}, true)
	if evidence || escalateAt.After(created.Add(3*time.Hour)) {
		t.Fatalf("a rewritten lifetime extended the bound: evidence=%v escalateAt=%s", evidence, escalateAt.Sub(created))
	}
	if err := (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("a pool whose lifetime was rewritten past its journals' was not retired")
	}
}

func TestSharedSameAtespaceTemplateNotCreatedByTheProviderIsForeign(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	_, record := sharedReady(t, c, native, first)
	// The provider's own derived templates in the Atespace select the pool
	// and are not foreign; a second allocation joins.
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	if _, joined := sharedReady(t, c, native, second); joined.SharedWorker.Pool != record.SharedWorker.Pool {
		t.Fatal("the provider's own derived templates were treated as foreign")
	}
	// A template in the same Atespace that this provider did not record,
	// selecting the pool's label, quarantines the pool before placement.
	native.templates["native/intruder"] = &pb.ActorTemplate{Metadata: &pb.ResourceMetadata{Atespace: "native", Name: "intruder", Uid: "intruder-uid"}, WorkerSelector: &pb.Selector{MatchLabels: map[string]string{sharedPoolLabel: record.SharedWorker.Label}}}
	third := sharedWorkspace(t, c, first, "tenant", "third", "third-uid", binding)
	boots := native.boots
	if _, err := sharedDriver(c, native).EnsureAllocation(t.Context(), third); !errors.Is(err, errForeignTemplate) {
		t.Fatalf("a same-Atespace foreign template did not close admission: %v", err)
	}
	if native.boots != boots {
		t.Fatal("an Actor was placed beside a same-Atespace foreign template")
	}
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("a pool reachable by a same-Atespace foreign template was not quarantined")
	}
}

func TestSharedOperatorPoolWithTheSharedPrefixIsNeverTouched(t *testing.T) {
	c, native, _, _ := sharedFixture(t, 2)
	operator := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "ate.dev/v1", "kind": "WorkerPool", "metadata": map[string]any{"namespace": "native-workers", "name": "ate-shared-prod", "labels": map[string]any{"team": "prod"}}, "spec": map[string]any{"replicas": int64(1), "workerImage": "fixture.invalid/native-worker:pin"}}}
	if err := c.Create(t.Context(), operator); err != nil {
		t.Fatal(err)
	}
	for _, dedicated := range []bool{true, false} {
		config := sharedConfig()
		config.DedicatedInstall = dedicated
		if err := (&SpareWorkerReplenisher{Client: c, Config: config}).EnforceSharedBounds(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := (&SpareWorkerReplenisher{Client: c, Control: native, Config: config}).Sweep(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if current, found := getPool(t, c, "ate-shared-prod"); !found || current.GetDeletionTimestamp() != nil {
		t.Fatal("an operator pool with only the shared name prefix was deleted")
	}
}

func TestSharedUnavailableProvenanceBoundsByTheMinimumLifetimePastTheOutageBound(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
	created := pool.GetCreationTimestamp().Time
	// Both mutable annotations are rewritten to the 24h maximum.
	ledger.Scope.Lifetime = metav1.Duration{Duration: 24 * time.Hour}
	if err := setSharedLedger(pool, ledger); err != nil {
		t.Fatal(err)
	}
	annotations := maps.Clone(pool.GetAnnotations())
	annotations[sharedDeadlineAnnotation] = ledger.BornAt.Add(24 * time.Hour).UTC().Format(time.RFC3339Nano)
	pool.SetAnnotations(annotations)
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	// The journal inventory cannot be listed.
	failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if _, ok := list.(*corev1.ConfigMapList); ok {
			return apierrors.NewInternalError(errors.New("configmap list unavailable"))
		}
		return w.List(ctx, list, opts...)
	}})
	bounds := &SpareWorkerReplenisher{Client: failing, Config: sharedConfig()}
	setNow(t, created.Add(profile.MinSharedWorkerLifetime-workerPodTerminationGrace-sharedRetireMargin))
	// Inside the outage window the annotations' bound applies, and
	// admission is closed: the pool is not retired yet.
	_ = bounds.EnforceSharedBounds(t.Context())
	if current, found := getPool(t, c, record.SharedWorker.Pool); !found || current.GetDeletionTimestamp() != nil {
		t.Fatal("a failed journal List applied the minimum lifetime inside the outage window")
	}
	// Past the outage bound, creation plus the minimum applies.
	setNow(t, sharedNow().Add(sharedProvenanceOutageBound))
	_ = bounds.EnforceSharedBounds(t.Context())
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("unavailable provenance past its bound let rewritten annotations extend the bound past creation plus the minimum lifetime")
	}
}

func TestSharedRecordedStrongMarkerSurvivesAnEntryRemoval(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	cm, journal, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	journal.SharedWorker.Strong = "persisted before a crash"
	if err := sharedDriver(c, native).save(t.Context(), cm, journal); err != nil {
		t.Fatal(err)
	}
	// The entry naming that journal is removed from the mutable ledger.
	pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
	ledger.Entries = nil
	if err := setSharedLedger(pool, ledger); err != nil {
		t.Fatal(err)
	}
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	if err := (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("removing the ledger entry hid a journaled strong marker from the bounds loop")
	}
}

func TestSharedWorkerIdentityIsBoundedAndStrongDeletesSurviveLedgerWrites(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
	ledger.Worker.Name = strings.Repeat("w", 30<<10)
	data, err := json.Marshal(ledger)
	if err != nil {
		t.Fatal(err)
	}
	padded := pool.DeepCopy()
	annotations := maps.Clone(padded.GetAnnotations())
	annotations[sharedLedgerAnnotation] = string(data)
	padded.SetAnnotations(annotations)
	if _, err := readSharedLedger(padded); err == nil {
		t.Fatal("a ledger with an oversized worker identity was accepted")
	}
	// retireSharedPool on a strong path deletes even when the ledger write
	// fails for a reason other than a conflict.
	failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "WorkerPool" {
			return apierrors.NewRequestEntityTooLargeError("ledger write too large")
		}
		return w.Update(ctx, obj, opts...)
	}})
	if err := New(failing, native, sharedConfig()).retireSharedPool(t.Context(), pool, sharedQuarantined, "foreign Actor"); err != nil {
		t.Fatal(err)
	}
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("a failed ledger write blocked a strong retirement's Delete")
	}
}

func TestSharedEscapedCharacterLedgerCannotBlockRetirement(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
	// Fabricated booted entries within the per-field limits, made of
	// characters json.Marshal would escape six-fold.
	ledger.Scope.MaxActors = 16
	ledger.Entries = nil
	for i := range 16 {
		ledger.Entries = append(ledger.Entries, sharedLedgerEntry{Journal: fmt.Sprintf("%03d", i) + strings.Repeat("<", sharedLedgerJournalMax-3), Actor: fmt.Sprintf("%03d", i) + strings.Repeat("&", sharedLedgerFieldMax-3), ActorUID: strings.Repeat(">", sharedLedgerFieldMax)})
	}
	raw, err := encodeSharedLedger(ledger)
	if err != nil {
		t.Fatal(err)
	}
	if escaped, _ := json.Marshal(ledger); len(escaped) <= sharedLedgerMaxBytes || len(raw) >= sharedLedgerMaxBytes {
		t.Fatalf("fixture does not model six-fold escaping: raw=%d escaped=%d", len(raw), len(escaped))
	}
	annotations := maps.Clone(pool.GetAnnotations())
	annotations[sharedLedgerAnnotation] = string(raw)
	pool.SetAnnotations(annotations)
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	// Written without HTML escaping, the accepted ledger re-encodes to the
	// same size, so its retirement write always fits.
	if accepted, err := readSharedLedger(pool); err == nil {
		reencoded, err := encodeSharedLedger(accepted)
		if err != nil || len(reencoded) > sharedLedgerMaxBytes-sharedLedgerRetirementHeadroom {
			t.Fatalf("an accepted ledger re-encodes past its headroom: %d %v", len(reencoded), err)
		}
	}
	// A suspicion-driven retirement of exactly this pool deletes it.
	if err := sharedDriver(c, native).retireSharedPool(t.Context(), pool, sharedRetiring, "fabricated booted entries"); err != nil {
		t.Fatal(err)
	}
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("an escaped-character ledger blocked the strong retirement")
	}
}

func TestSharedBlockedLedgerWriteCannotDelayTheDelete(t *testing.T) {
	for _, path := range []string{"retire", "hard", "bounds"} {
		t.Run(path, func(t *testing.T) {
			c, native, first, _ := sharedFixture(t, 2)
			_, record := sharedReady(t, c, native, first)
			pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
			// An update-only admission webhook that times out: every pool
			// Update blocks until its context expires.
			blocking := contextAwareClient(interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "WorkerPool" {
					<-ctx.Done()
					return ctx.Err()
				}
				return w.Update(ctx, obj, opts...)
			}}))
			// Keep the pool observable after its Delete, as foreground
			// propagation does, so the later state write really runs.
			pool.SetFinalizers([]string{"test.invalid/hold"})
			if err := c.Update(t.Context(), pool); err != nil {
				t.Fatal(err)
			}
			pool, _ = poolLedger(t, c, record.SharedWorker.Pool)
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			switch path {
			case "retire":
				_ = New(blocking, native, sharedConfig()).retireSharedPool(ctx, pool, sharedQuarantined, "foreign Actor")
			case "hard":
				_ = New(blocking, native, sharedConfig()).hardRetireSharedPool(ctx, pool, sharedRetiring, "lifetime")
			case "bounds":
				setNow(t, sharedRetireAt(pool, ledger))
				_ = (&SpareWorkerReplenisher{Client: blocking, Config: sharedConfig()}).EnforceSharedBounds(ctx)
			}
			current, found := getPool(t, c, record.SharedWorker.Pool)
			if !found || current.GetDeletionTimestamp() == nil {
				t.Fatal("a blocked ledger write delayed or prevented the mandatory Delete")
			}
		})
	}
}

func TestSharedRejectedStrongMarkerStillDeletesThePool(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	a, recordA := sharedReady(t, c, native, first)
	// The Actor read fails during Stop, and the journal write that would
	// persist Strong is rejected with a non-conflict error.
	native.failGetActor = 1
	rejecting := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if cm, ok := obj.(*corev1.ConfigMap); ok && strings.Contains(cm.Data[journalDataKey], `"strong":`) {
			return apierrors.NewForbidden(corev1.Resource("configmaps"), cm.Name, errors.New("journal write rejected"))
		}
		return w.Update(ctx, obj, opts...)
	}})
	tick()
	if _, err := sharedDriver(rejecting, native).StopInstance(t.Context(), first.Key, a.Identity); err == nil {
		t.Fatal("expected the persistence error to be returned")
	}
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
		t.Fatal("a rejected strong-marker write gated the pool's deletion")
	}
	// A later reconcile never takes the interim path: the deleting pool and
	// the absent Pod are themselves strong evidence.
	_, stopped := sharedRetired(t, c, native, first, a.Identity, false)
	if stopped.SharedWorker.Strong == "" || stopped.SharedWorker.Proof != nil || native.suspendCalls != 0 {
		t.Fatalf("a later Stop forgot the doubt: %#v suspends=%d", stopped.SharedWorker, native.suspendCalls)
	}
}

func TestSharedBoundsPassDoesMandatoryWorkBeforeBookkeeping(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	// Many pools, all due at once.
	var pools []string
	_, record := sharedReady(t, c, native, first)
	pools = append(pools, record.SharedWorker.Pool)
	for i := range 11 {
		item := sharedWorkspace(t, c, first, "tenant", fmt.Sprintf("w%02d", i), types.UID(fmt.Sprintf("w%02d-uid", i)), binding)
		// A fresh pool per workspace: fill the previous one first.
		_, r := sharedReady(t, c, native, item)
		if !slices.Contains(pools, r.SharedWorker.Pool) {
			pools = append(pools, r.SharedWorker.Pool)
		}
	}
	if len(pools) < 5 {
		t.Fatalf("fixture created only %d pools", len(pools))
	}
	// Every pool's worker Pod lingers and every pool stays observable while
	// deleting, and every ledger write blocks until its context ends.
	for _, name := range pools {
		pool, _ := poolLedger(t, c, name)
		pool.SetFinalizers([]string{"test.invalid/hold"})
		if err := c.Update(t.Context(), pool); err != nil {
			t.Fatal(err)
		}
	}
	pool, ledger := poolLedger(t, c, pools[0])
	setNow(t, sharedEscalateAt(pool, ledger).Add(time.Minute))
	var deleted, escalated atomic.Int64
	blocking := contextAwareClient(interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "WorkerPool" {
				<-ctx.Done()
				return ctx.Err()
			}
			return w.Update(ctx, obj, opts...)
		},
		Delete: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			switch o := obj.(type) {
			case *unstructured.Unstructured:
				if o.GetKind() == "WorkerPool" {
					deleted.Add(1)
				}
			case *corev1.Pod:
				escalated.Add(1)
			}
			return w.Delete(ctx, obj, opts...)
		},
	}))
	done := make(chan struct{})
	started := time.Now()
	go func() {
		defer close(done)
		_ = (&SpareWorkerReplenisher{Client: blocking, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
	}()
	// Every Delete and escalation lands within one pass, long before the
	// blocked bookkeeping finishes.
	deadline := time.Now().Add(10 * time.Second)
	for deleted.Load() < int64(len(pools)) || escalated.Load() < int64(len(pools)) {
		if time.Now().After(deadline) {
			t.Fatalf("after %s only %d/%d deletes and %d/%d escalations happened", time.Since(started), deleted.Load(), len(pools), escalated.Load(), len(pools))
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-done:
	case <-time.After(sharedBookkeepingBudget + 10*time.Second):
		t.Fatal("bookkeeping exceeded its total budget")
	}
}

// manySharedPools readies workspaces until at least n shared pools exist.
func manySharedPools(t *testing.T, c client.Client, native *nativeFixture, first sdk.WorkloadRequest, binding *api.ImmutableObjectBinding, n int) []string {
	t.Helper()
	_, record := sharedReady(t, c, native, first)
	pools := []string{record.SharedWorker.Pool}
	for i := 0; len(pools) < n; i++ {
		item := sharedWorkspace(t, c, first, "tenant", fmt.Sprintf("m%02d", i), types.UID(fmt.Sprintf("m%02d-uid", i)), binding)
		_, r := sharedReady(t, c, native, item)
		if !slices.Contains(pools, r.SharedWorker.Pool) {
			pools = append(pools, r.SharedWorker.Pool)
		}
	}
	return pools
}

func TestSharedEscalationIsIndependentOfSlowPoolDeletes(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	pools := manySharedPools(t, c, native, first, binding, 11)
	pool, ledger := poolLedger(t, c, pools[0])
	setNow(t, sharedEscalateAt(pool, ledger).Add(time.Minute))
	var escalated atomic.Int64
	// Every WorkerPool DELETE blocks until its context ends; Pod DELETEs work.
	slow := contextAwareClient(interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Delete: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		switch o := obj.(type) {
		case *unstructured.Unstructured:
			if o.GetKind() == "WorkerPool" {
				<-ctx.Done()
				return ctx.Err()
			}
		case *corev1.Pod:
			escalated.Add(1)
		}
		return w.Delete(ctx, obj, opts...)
	}}))
	t.Run("bounds pass", func(t *testing.T) {
		escalated.Store(0)
		done := make(chan struct{})
		started := time.Now()
		go func() {
			defer close(done)
			_ = (&SpareWorkerReplenisher{Client: slow, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
		}()
		deadline := time.Now().Add(3 * time.Second)
		for escalated.Load() < int64(len(pools)) {
			if time.Now().After(deadline) {
				t.Fatalf("only %d/%d escalations within 3s of a pass whose pool deletes block", escalated.Load(), len(pools))
			}
			time.Sleep(10 * time.Millisecond)
		}
		// 11 blocked deletes, 8 at a time, each bounded by its own timeout.
		bound := 2*sharedCleanupTimeout + 15*time.Second
		select {
		case <-done:
		case <-time.After(bound):
			t.Fatalf("the bounds pass did not finish within %s", bound)
		}
		if elapsed := time.Since(started); elapsed > bound {
			t.Fatalf("the bounds pass took %s", elapsed)
		}
	})
	t.Run("escalation loop", func(t *testing.T) {
		escalated.Store(0)
		// Pods lost to the first subtest are recreated as the workers' Pods.
		for _, name := range pools {
			podName := name + "-worker"
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "native-workers", Name: podName, UID: types.UID(podName + "-uid"), Labels: map[string]string{workerPoolLabel: name}}}
			if err := c.Create(t.Context(), pod); err != nil && !apierrors.IsAlreadyExists(err) {
				t.Fatal(err)
			}
		}
		started := time.Now()
		if err := (&SpareWorkerReplenisher{Client: slow, Config: sharedConfig()}).EnforceSharedEscalation(t.Context()); err != nil {
			t.Fatal(err)
		}
		if escalated.Load() < int64(len(pools)) || time.Since(started) > 3*time.Second {
			t.Fatalf("the escalation-only pass escalated %d/%d in %s", escalated.Load(), len(pools), time.Since(started))
		}
	})
}

func TestSharedFinalizeFailureKeepsTheDataThroughTheStrongPath(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	a, recordA := sharedReady(t, c, native, first)
	sharedReady(t, c, native, second)
	// A transient control-plane database timeout in FinalizeSuspended, after
	// the checkpoint was taken.
	native.failFinalizeOnce = true
	for range 24 {
		tick()
		observed, err := sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity)
		if err == nil && observed.State == sdk.AllocationStopped {
			break
		}
	}
	_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.Observation.RetainedData == nil || record.Checkpoint == nil {
		t.Fatal("a transient FinalizeSuspended failure lost the workspace data")
	}
	if native.tags[record.Checkpoint.Atespace+"/"+record.Checkpoint.Tag.Name] == nil {
		t.Fatal("the continued suspension's Tag was not captured")
	}
	if record.SharedWorker.Strong == "" || record.Failure != "" {
		t.Fatalf("the failure did not take the strong path, or was reported as a loss: %#v failure=%q", record.SharedWorker, record.Failure)
	}
	if native.suspendCalls != 2 || record.Pending != nil {
		t.Fatalf("SuspendActor calls = %d, want the original and one re-entry of the same workflow", native.suspendCalls)
	}
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); found || podExists(t, c, recordA.Worker.Namespace, recordA.Worker.Pod, recordA.Worker.PodUID) {
		t.Fatal("the strong deletion was skipped")
	}
}

func TestSharedFailedDeleteKeepsTheRetirementDecision(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	_, record := sharedReady(t, c, native, first)
	pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
	setNow(t, sharedRetireAt(pool, ledger))
	// The mandatory Delete fails, and the client holds only the shipped
	// RBAC verbs on WorkerPools.
	failing := interceptor.NewClient(shippedRBACClient(t, c).(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "WorkerPool" {
				return apierrors.NewInternalError(errors.New("pool delete refused"))
			}
			return w.Delete(ctx, obj, opts...)
		},
	})
	if err := (&SpareWorkerReplenisher{Client: failing, Config: sharedConfig()}).EnforceSharedBounds(t.Context()); err == nil {
		t.Fatal("expected the failed Delete to be reported")
	}
	current, found := getPool(t, c, record.SharedWorker.Pool)
	if !found || !sharedRetireIntended(current) {
		t.Fatal("a failed Delete left no persisted retirement decision with the shipped RBAC verbs")
	}
	if state, _ := sharedPersistedRetirement(current); !sharedRetirementPersisted(current) || state != sharedRetiring {
		t.Fatal("a failed Delete did not record the retirement state with its intent")
	}
	// A new claim arrives and is refused that pool.
	setNow(t, ledger.BornAt.Time.Add(time.Minute))
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	if _, joined := sharedReady(t, c, native, second); joined.SharedWorker.Pool == record.SharedWorker.Pool {
		t.Fatal("a pool whose retirement was decided accepted a new claim")
	}
	// The next pass retries and deletes.
	if err := (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("the next pass did not retry the failed Delete")
	}
}

func TestSharedRecordedTemplateCheckHitsTheCacheAndRefreshesOnAMiss(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 4)
	_, record := sharedReady(t, c, native, first)
	var configMapLists, configMapGets atomic.Int64
	counting := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.ConfigMapList); ok {
				configMapLists.Add(1)
			}
			return w.List(ctx, list, opts...)
		},
		Get: func(ctx context.Context, w client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if cm, ok := obj.(*corev1.ConfigMap); ok && strings.HasPrefix(key.Name, "substrate-checkpoint") {
				_ = cm
				configMapGets.Add(1)
			}
			return w.Get(ctx, key, obj, opts...)
		},
	})
	// Hit: the provider's own derived template is in the in-process record,
	// so a claim and its Resume list no ConfigMaps for the template check.
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	for range 16 {
		observed, err := sharedDriver(counting, native).EnsureAllocation(t.Context(), second)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == sdk.AllocationReady {
			break
		}
	}
	if configMapLists.Load() != 0 {
		t.Fatalf("a cache hit listed ConfigMaps %d times", configMapLists.Load())
	}
	// Miss: an unrecorded template in the same Atespace reaching the pool
	// refreshes from Kubernetes (two Lists, no per-catalog Gets) and then
	// fails closed.
	native.templates["native/intruder"] = &pb.ActorTemplate{Metadata: &pb.ResourceMetadata{Atespace: "native", Name: "intruder", Uid: "intruder-uid"}, WorkerSelector: &pb.Selector{MatchLabels: map[string]string{sharedPoolLabel: record.SharedWorker.Label}}}
	configMapLists.Store(0)
	configMapGets.Store(0)
	third := sharedWorkspace(t, c, first, "tenant", "third", "third-uid", binding)
	if _, err := sharedDriver(counting, native).EnsureAllocation(t.Context(), third); !errors.Is(err, errForeignTemplate) {
		t.Fatalf("a miss that stayed unrecorded after refresh was not foreign: %v", err)
	}
	if configMapLists.Load() != 2 || configMapGets.Load() != 0 {
		t.Fatalf("a miss refreshed with %d ConfigMap lists and %d catalog gets, want 2 and 0", configMapLists.Load(), configMapGets.Load())
	}
}

func TestSharedEscalationsAreIssuedConcurrentlyWhenPodDeletesStall(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	pools := manySharedPools(t, c, native, first, binding, 11)
	pool, ledger := poolLedger(t, c, pools[0])
	setNow(t, sharedEscalateAt(pool, ledger).Add(time.Minute))
	var issued atomic.Int64
	// Every escalating Pod DELETE stalls until its context ends.
	stalling := contextAwareClient(interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Delete: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		if _, ok := obj.(*corev1.Pod); ok {
			issued.Add(1)
			<-ctx.Done()
			return ctx.Err()
		}
		return w.Delete(ctx, obj, opts...)
	}}))
	done := make(chan struct{})
	started := time.Now()
	go func() {
		defer close(done)
		_ = (&SpareWorkerReplenisher{Client: stalling, Config: sharedConfig()}).EnforceSharedEscalation(t.Context())
	}()
	deadline := time.Now().Add(3 * time.Second)
	for issued.Load() < int64(len(pools)) {
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d escalations were issued within 3s while Pod deletes stall", issued.Load(), len(pools))
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-done:
	case <-time.After(sharedRetireMargin):
		t.Fatal("escalation exceeded the lifetime margin")
	}
	if elapsed := time.Since(started); elapsed > 2*sharedEscalationTimeout+5*time.Second {
		t.Fatalf("stalled escalations took %s; they were not issued concurrently", elapsed)
	}
}

func TestSharedRetireIntentNeverMarksAReplacementPool(t *testing.T) {
	for _, tc := range []struct {
		name string
		// deleteErr is the Delete's result against the replacement.
		deleteErr error
		// replaceAfterRead replaces the pool between the intent's read and
		// its Update.
		replaceAfterRead bool
	}{
		{name: "uid precondition refused", deleteErr: apierrors.NewConflict(schema.GroupResource{Group: "ate.dev", Resource: "workerpools"}, "pool", errors.New("Precondition failed: UID in precondition"))},
		{name: "delete failed otherwise", deleteErr: apierrors.NewInternalError(errors.New("pool delete refused"))},
		{name: "replaced after the read", deleteErr: apierrors.NewInternalError(errors.New("pool delete refused")), replaceAfterRead: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, native, first, _ := sharedFixture(t, 3)
			_, record := sharedReady(t, c, native, first)
			decided, _ := poolLedger(t, c, record.SharedWorker.Pool)
			replace := func(ctx context.Context, w client.WithWatch) {
				live := poolObject()
				if err := w.Get(ctx, client.ObjectKeyFromObject(decided), live); err != nil {
					t.Fatal(err)
				}
				if err := w.Delete(ctx, live); err != nil {
					t.Fatal(err)
				}
				replacement := live.DeepCopy()
				replacement.SetResourceVersion("")
				replacement.SetUID(types.UID("replacement-" + string(live.GetUID())))
				if err := w.Create(ctx, replacement); err != nil {
					t.Fatal(err)
				}
			}
			if !tc.replaceAfterRead {
				replace(t.Context(), c.(client.WithWatch))
			}
			var replaced bool
			guarded := interceptor.NewClient(shippedRBACClient(t, c).(client.WithWatch), interceptor.Funcs{
				// The API server refuses the Delete by its UID precondition;
				// the fake client does not enforce preconditions.
				Delete: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					return tc.deleteErr
				},
				Get: func(ctx context.Context, w client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					err := w.Get(ctx, key, obj, opts...)
					if tc.replaceAfterRead && !replaced && err == nil && obj.GetUID() == decided.GetUID() {
						replaced = true
						replace(ctx, w)
					}
					return err
				},
			})
			_ = driver(guarded, native).hardRetireSharedPool(t.Context(), decided, sharedRetiring, "stale decision")
			after, found := getPool(t, c, record.SharedWorker.Pool)
			if !found || after.GetUID() == decided.GetUID() || sharedRetireIntended(after) || sharedRetirementPersisted(after) {
				t.Fatal("a stale retirement decision marked the replacement pool")
			}
			if tc.replaceAfterRead && !replaced {
				t.Fatal("the race was not exercised")
			}
		})
	}
}

// shippedRBACClient refuses every WorkerPool request whose verb the shipped
// config/rbac.yaml does not grant on ate.dev workerpools.
func shippedRBACClient(t *testing.T, c client.Client) client.Client {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("config", "rbac.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	granted := map[string]bool{}
	for _, document := range strings.Split(string(data), "\n---\n") {
		var role rbacv1.ClusterRole
		if err := sigsyaml.Unmarshal([]byte(document), &role); err != nil {
			t.Fatal(err)
		}
		if role.Kind != "ClusterRole" && role.Kind != "Role" {
			continue
		}
		for _, rule := range role.Rules {
			if slices.Contains(rule.APIGroups, "ate.dev") && slices.Contains(rule.Resources, "workerpools") && len(rule.ResourceNames) == 0 {
				for _, verb := range rule.Verbs {
					granted[verb] = true
				}
			}
		}
	}
	if len(granted) == 0 {
		t.Fatal("the shipped RBAC grants nothing on workerpools")
	}
	pool := func(obj runtime.Object) bool {
		switch typed := obj.(type) {
		case *unstructured.Unstructured:
			return typed.GetKind() == "WorkerPool"
		case *unstructured.UnstructuredList:
			return typed.GetKind() == "WorkerPoolList"
		}
		return false
	}
	check := func(verb string, obj runtime.Object) error {
		if pool(obj) && !granted[verb] {
			return apierrors.NewForbidden(schema.GroupResource{Group: "ate.dev", Resource: "workerpools"}, "", fmt.Errorf("verb %q is not granted", verb))
		}
		return nil
	}
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, w client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := check("get", obj); err != nil {
				return err
			}
			return w.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if err := check("list", list); err != nil {
				return err
			}
			return w.List(ctx, list, opts...)
		},
		Create: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if err := check("create", obj); err != nil {
				return err
			}
			return w.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if err := check("update", obj); err != nil {
				return err
			}
			return w.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, w client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if err := check("patch", obj); err != nil {
				return err
			}
			return w.Patch(ctx, obj, patch, opts...)
		},
		Delete: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if err := check("delete", obj); err != nil {
				return err
			}
			return w.Delete(ctx, obj, opts...)
		},
	})
}

func TestSharedStopRecoveringAnUnresolvedSuspendingActorTakesTheStrongPath(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	a, recordA := sharedReady(t, c, native, first)
	// The evidence suspend takes effect upstream but its workflow fails in
	// FinalizeSuspended, leaving the Actor SUSPENDING, and the provider crashes
	// before it handles the result: nothing after the RPC is saved.
	native.failFinalizeOnce = true
	crashing := &suspensionReviewControl{nativeFixture: native, submit: func(request *pb.SuspendActorRequest) (*pb.SuspendActorResponse, error) {
		_, _ = native.SuspendActor(t.Context(), request)
		panic("provider crashed after the evidence suspend")
	}}
	for range 4 {
		tick()
		crashed := func() (crashed bool) {
			defer func() { crashed = recover() != nil }()
			d := sharedDriver(c, native)
			d.control = crashing
			if _, err := d.StopInstance(t.Context(), first.Key, a.Identity); err != nil {
				t.Fatal(err)
			}
			return false
		}()
		if crashed {
			break
		}
	}
	if crashing.calls != 1 || native.actors[refKey(actorRef(recordA))].Status.State != pb.ActorState_ACTOR_STATE_SUSPENDING {
		t.Fatalf("the crash was not modelled: calls=%d", crashing.calls)
	}
	// Recovery: one Stop pass on a healthy worker and a matching ledger.
	tick()
	d := sharedDriver(c, native)
	d.control = crashing
	if _, err := d.StopInstance(t.Context(), first.Key, a.Identity); err != nil {
		t.Fatal(err)
	}
	_, record, err := d.read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.SharedWorker.Strong == "" || record.SharedWorker.Proof != nil {
		t.Fatalf("an unresolved SUSPENDING Actor left the Stop waiting: %#v", record.SharedWorker)
	}
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
		t.Fatal("the pool's retirement did not start on recovery")
	}
	if crashing.calls != 1 {
		t.Fatalf("the evidence suspend was reissued or continued: calls=%d", crashing.calls)
	}
}

func TestSharedUnavailableProvenanceClosesAdmissionThenRetiresPastItsBound(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	_, record := sharedReady(t, c, native, first)
	var listing atomic.Bool
	listing.Store(true)
	// The journal inventory List fails while every pool's annotations stay
	// consistent.
	failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.ConfigMapList); ok && !listing.Load() {
				return apierrors.NewServiceUnavailable("journal inventory unavailable")
			}
			return w.List(ctx, list, opts...)
		},
	})
	bounds := &SpareWorkerReplenisher{Client: failing, Config: sharedConfig()}
	listing.Store(false)
	if err := bounds.EnforceSharedBounds(t.Context()); err == nil {
		t.Fatal("expected the unavailable provenance to be reported")
	}
	if pool, found := getPool(t, c, record.SharedWorker.Pool); !found || pool.GetDeletionTimestamp() != nil || sharedRetireIntended(pool) || sharedRetirementPersisted(pool) {
		t.Fatal("one failed inventory List retired a healthy shared pool")
	}
	// Admission is closed at once: no new claim onto the pool...
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	if _, err := sharedDriver(failing, native).EnsureAllocation(t.Context(), second); err == nil || !strings.Contains(err.Error(), "provenance is unavailable") {
		t.Fatalf("a claim was admitted without provenance: %v", err)
	}
	if _, ledger := poolLedger(t, c, record.SharedWorker.Pool); len(ledger.Entries) != 1 {
		t.Fatal("a claim landed on the pool without provenance")
	}
	// ...and no Resume onto it.
	cm, current, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sharedDriver(failing, native).prepareSharedWorker(t.Context(), cm, current); err == nil || !strings.Contains(err.Error(), "provenance is unavailable") {
		t.Fatalf("a Resume onto the pool was admitted without provenance: %v", err)
	}
	// Still unavailable past the bound: every pool is strongly retired.
	setNow(t, sharedNow().Add(sharedProvenanceOutageBound))
	_ = bounds.EnforceSharedBounds(t.Context())
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("provenance unavailable past its bound did not retire the pool")
	}
}

func TestSharedProvenanceRecoveryWithinItsBoundRetiresNothing(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	_, record := sharedReady(t, c, native, first)
	var listing atomic.Bool
	failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.ConfigMapList); ok && !listing.Load() {
				return apierrors.NewServiceUnavailable("journal inventory unavailable")
			}
			return w.List(ctx, list, opts...)
		},
	})
	bounds := &SpareWorkerReplenisher{Client: failing, Config: sharedConfig()}
	_ = bounds.EnforceSharedBounds(t.Context())
	setNow(t, sharedNow().Add(sharedProvenanceOutageBound-time.Second))
	listing.Store(true)
	if err := bounds.EnforceSharedBounds(t.Context()); err != nil {
		t.Fatal(err)
	}
	// A later failure restarts the bound from its own first failure.
	listing.Store(false)
	setNow(t, sharedNow().Add(2*time.Second))
	_ = bounds.EnforceSharedBounds(t.Context())
	if _, found := getPool(t, c, record.SharedWorker.Pool); !found {
		t.Fatal("an outage that recovered within its bound retired the pool")
	}
	listing.Store(true)
	// The recovering read starts after the last failure; a read that starts
	// at the same instant cannot be ordered after it, and does not count.
	setNow(t, sharedNow().Add(time.Second))
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	if _, recordB := sharedReady(t, c, native, second); recordB.SharedWorker.Pool != record.SharedWorker.Pool {
		t.Fatal("admission did not reopen once provenance was read again")
	}
}

func TestSharedEscalationNeverDeletesAReplacementPoolsPods(t *testing.T) {
	for _, recordedBy := range []string{"journal", "ledger"} {
		t.Run(recordedBy, func(t *testing.T) {
			c, native, first, _ := sharedFixture(t, 2)
			_, record := sharedReady(t, c, native, first)
			retired, ledger := poolLedger(t, c, record.SharedWorker.Pool)
			if ledger.Worker == nil || ledger.Worker.PodUID != record.Worker.PodUID {
				t.Fatal("the fixture's ledger does not record the fenced worker Pod")
			}
			if recordedBy == "ledger" {
				// Without journals, the ledger's fenced worker Pod alone
				// ties the old Pod to the retired pool.
				cms := &corev1.ConfigMapList{}
				if err := c.List(t.Context(), cms, client.HasLabels{ownershipLabel, providerLabel}); err != nil {
					t.Fatal(err)
				}
				for i := range cms.Items {
					if err := c.Delete(t.Context(), &cms.Items[i]); err != nil {
						t.Fatal(err)
					}
				}
			}
			setNow(t, sharedEscalateAt(retired, ledger).Add(time.Minute))
			replacementPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: record.Worker.Namespace, Name: "replacement-worker", UID: "replacement-worker-uid", Labels: map[string]string{workerPoolLabel: retired.GetName()}}}
			var replaced bool
			// The pool is replaced under the same name after the pass listed
			// it and before its Pods are listed; the old fenced Pod lingers.
			racing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
				List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if _, ok := list.(*corev1.PodList); ok && !replaced {
						replaced = true
						live := poolObject()
						if err := w.Get(ctx, client.ObjectKeyFromObject(retired), live); err != nil {
							return err
						}
						if err := w.Delete(ctx, live); err != nil {
							return err
						}
						replacement := live.DeepCopy()
						replacement.SetResourceVersion("")
						replacement.SetUID("replacement-pool-uid")
						replacement.SetCreationTimestamp(metav1.NewTime(sharedNow()))
						// A healthy replacement: born now, with its own deadline.
						fresh := *ledger
						fresh.BornAt = metav1.NewTime(sharedNow())
						if err := setSharedLedger(replacement, &fresh); err != nil {
							return err
						}
						setSharedDeadline(replacement, &fresh)
						if err := w.Create(ctx, replacement); err != nil {
							return err
						}
						if err := w.Create(ctx, replacementPod.DeepCopy()); err != nil {
							return err
						}
					}
					return w.List(ctx, list, opts...)
				},
			})
			_ = (&SpareWorkerReplenisher{Client: racing, Config: sharedConfig()}).EnforceSharedEscalation(t.Context())
			if !replaced {
				t.Fatal("the replacement race was not exercised")
			}
			if !podExists(t, c, replacementPod.Namespace, replacementPod.Name, string(replacementPod.UID)) {
				t.Fatal("escalation deleted a replacement pool's worker Pod")
			}
			if podExists(t, c, record.Worker.Namespace, record.Worker.Pod, record.Worker.PodUID) {
				t.Fatal("escalation skipped the retired pool's recorded worker Pod")
			}
		})
	}
}

func TestSharedSuspensionWhoseContinuationsNeverReturnTakesTheStrongPath(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	a, recordA := sharedReady(t, c, native, first)
	// The original suspend leaves the Actor SUSPENDING, and the provider
	// crashes on it and on every continuation before handling a result.
	native.failFinalizeOnce = true
	crashing := &suspensionReviewControl{nativeFixture: native, submit: func(request *pb.SuspendActorRequest) (*pb.SuspendActorResponse, error) {
		if native.actors[refKey(request.Actor)].Status.State == pb.ActorState_ACTOR_STATE_RUNNING {
			_, _ = native.SuspendActor(t.Context(), request)
		}
		panic("provider crashed during SuspendActor")
	}}
	pass := func() (crashed bool) {
		defer func() { crashed = recover() != nil }()
		tick()
		d := sharedDriver(c, native)
		d.control = crashing
		_, _ = d.SuspendInstance(t.Context(), first.Key, a.Identity)
		return false
	}
	for range 4*maxSuspendContinuations + 8 {
		pass()
		if _, record, err := sharedDriver(c, native).read(t.Context(), first.Key); err == nil && record.SharedWorker.Strong != "" {
			break
		}
	}
	_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.SharedWorker.Strong == "" {
		t.Fatalf("a suspension whose continuations never return waited instead of taking the strong path: calls=%d", crashing.calls)
	}
	if crashing.calls != 1+maxSuspendContinuations {
		t.Fatalf("SuspendActor calls = %d, want the original and %d continuations", crashing.calls, maxSuspendContinuations)
	}
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
		t.Fatal("the strong deletion did not start")
	}
}

func TestSharedRemovedEntryCannotHideASuspectWorkspaceFromTheBoundsLoop(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	_, recordA := sharedReady(t, c, native, first)
	sharedReady(t, c, native, second)
	// A's entry is removed from an otherwise valid ledger.
	pool, ledger := poolLedger(t, c, recordA.SharedWorker.Pool)
	ledger.Entries = slices.DeleteFunc(ledger.Entries, func(entry sharedLedgerEntry) bool { return entry.ActorUID == recordA.Actor.UID })
	if len(ledger.Entries) != 1 {
		t.Fatal("the fixture did not remove exactly A's entry")
	}
	if err := setSharedLedger(pool, ledger); err != nil {
		t.Fatal(err)
	}
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	// Then A's workspace is tainted, with native control down.
	workspace := &api.ExecutionWorkspace{}
	if err := c.Get(t.Context(), clientKey(first.Key), workspace); err != nil {
		t.Fatal(err)
	}
	annotations := workspace.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[operatorTaintAnnotation] = "true"
	workspace.SetAnnotations(annotations)
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	native.nativeDown = true
	var workspaceLists atomic.Int64
	counting := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*api.ExecutionWorkspaceList); ok {
				workspaceLists.Add(1)
			}
			return w.List(ctx, list, opts...)
		},
	})
	_ = (&SpareWorkerReplenisher{Client: counting, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
		t.Fatal("a removed ledger entry hid its suspect workspace from the bounds loop")
	}
	if workspaceLists.Load() != 1 {
		t.Fatalf("the recorded workspaces were read with %d Lists, want one per namespace", workspaceLists.Load())
	}
}

// sharedTemplateFixture returns a ready shared allocation whose derived
// template is then replaced natively by a foreign template of the same name.
func sharedTemplateFixture(t *testing.T) (client.Client, *nativeFixture, *journalRecord, map[string]string) {
	t.Helper()
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	template := native.templates[refKey(&pb.ObjectRef{Atespace: record.Atespace, Name: record.Template.Name})]
	if template == nil || template.Metadata.Uid != record.Template.UID {
		t.Fatal("the fixture's derived template is missing")
	}
	template.Metadata.Uid = "foreign-template-uid"
	return c, native, record, sharedLabels(record.SharedWorker.Label)
}

func TestSharedTemplateRefreshNeverRestoresAnUnknownUID(t *testing.T) {
	c, native, record, poolLabels := sharedTemplateFixture(t)
	// The journal still holds the template before its UID was written, and
	// the process has no record yet.
	cm, current, err := sharedDriver(c, native).read(t.Context(), record.Request.Key)
	if err != nil {
		t.Fatal(err)
	}
	known := current.Template
	current.Template.UID, current.CreateTemplate.UID = "", ""
	data, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	cm.Data[journalDataKey] = string(data)
	if err := c.Update(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	recordedTemplates.replace(nil, recordedTemplates.begin())
	// A refresh lists that journal; before it completes, save() records the
	// real UID.
	var interleaved bool
	racing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			err := w.List(ctx, list, opts...)
			if _, ok := list.(*corev1.ConfigMapList); ok && !interleaved {
				interleaved = true
				recordedTemplates.add(record.Atespace, known)
			}
			return err
		},
	})
	foreign, err := sharedDriver(racing, native).foreignTemplateMatches(t.Context(), record.Atespace, poolLabels)
	if err != nil {
		t.Fatal(err)
	}
	if !interleaved {
		t.Fatal("the refresh was not interleaved with the save")
	}
	if !foreign {
		t.Fatal("a refresh restored an unknown UID, so a foreign same-name template passed")
	}
	if uid := recordedTemplates.snapshot()[record.Atespace+"/"+known.Name]; uid != known.UID {
		t.Fatalf("the refresh replaced the recorded UID with %q", uid)
	}
}

func TestSharedCachedUnknownTemplateUIDIsRereadBeforeItMatches(t *testing.T) {
	c, native, record, poolLabels := sharedTemplateFixture(t)
	// The cache holds the template with its UID still unknown, while the
	// journal has since persisted the real UID.
	entries := recordedTemplates.snapshot()
	for key := range entries {
		entries[key] = ""
	}
	recordedTemplates.replace(entries, recordedTemplates.begin())
	foreign, err := sharedDriver(c, native).foreignTemplateMatches(t.Context(), record.Atespace, poolLabels)
	if err != nil {
		t.Fatal(err)
	}
	if !foreign {
		t.Fatal("a cached unknown UID accepted a foreign same-name template without a reread")
	}
}

// warmSharedPool creates an unused warm pool of a profile with this
// lifetime, and lists its worker so the pool has a worker Pod.
func warmSharedPool(t *testing.T, lifetime time.Duration) (client.Client, *nativeFixture, *profile.SubstrateWorkspaceProfile, *unstructured.Unstructured, *sharedLedger) {
	t.Helper()
	c, native, _ := fixture(t, true)
	useTestClock(t)
	createSharedProfile(t, c, "tenant", "warm", "warm-profile-uid", 2, lifetime)
	item := &profile.SubstrateWorkspaceProfile{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "tenant", Name: "warm"}, item); err != nil {
		t.Fatal(err)
	}
	item.Annotations = map[string]string{spareWorkersAnnotation: "1"}
	if err := c.Update(t.Context(), item); err != nil {
		t.Fatal(err)
	}
	if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	warm := sharedPools(t, c)
	if len(warm) != 1 {
		t.Fatalf("warm shared pools = %d, want 1", len(warm))
	}
	pool, ledger := poolLedger(t, c, warm[0].GetName())
	if len(ledger.Entries) != 0 {
		t.Fatal("the warm pool is not unused")
	}
	if _, bound := sharedNameBinding(pool, ledger.Scope); !bound {
		t.Fatal("a new shared pool's name does not commit to its scope")
	}
	if _, err := native.ListWorkers(t.Context(), &pb.ListWorkersRequest{}); err != nil {
		t.Fatal(err)
	}
	native.nativeDown = true
	return c, native, item, pool, ledger
}

// rewriteSharedLifetime rewrites a pool's ledger scope and deadline
// annotation consistently.
func rewriteSharedLifetime(t *testing.T, c client.Client, pool *unstructured.Unstructured, ledger *sharedLedger, scope sharedScope) {
	t.Helper()
	ledger.Scope = scope
	if err := setSharedLedger(pool, ledger); err != nil {
		t.Fatal(err)
	}
	annotations := maps.Clone(pool.GetAnnotations())
	annotations[sharedDeadlineAnnotation] = ledger.BornAt.Add(scope.Lifetime.Duration).UTC().Format(time.RFC3339Nano)
	pool.SetAnnotations(annotations)
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
}

func workerPodCount(t *testing.T, c client.Client, pool string) int {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(t.Context(), pods, client.MatchingLabels{workerPoolLabel: pool}); err != nil {
		t.Fatal(err)
	}
	return len(pods.Items)
}

func TestSharedUnusedWarmPoolIsBoundedByItsProfile(t *testing.T) {
	for _, tc := range []struct {
		name            string
		profileLifetime time.Duration
		rewrite         bool
		deleteProfile   bool
		// bound is the lifetime that must govern escalation.
		bound time.Duration
	}{
		{name: "profile 3h, rewritten to 24h", profileLifetime: 3 * time.Hour, rewrite: true, bound: 3 * time.Hour},
		{name: "profile 6h, rewritten to 24h", profileLifetime: 6 * time.Hour, rewrite: true, bound: profile.MinSharedWorkerLifetime},
		{name: "profile 6h", profileLifetime: 6 * time.Hour, bound: 6 * time.Hour},
		{name: "profile 6h, gone", profileLifetime: 6 * time.Hour, deleteProfile: true, bound: profile.MinSharedWorkerLifetime},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _, item, pool, ledger := warmSharedPool(t, tc.profileLifetime)
			created := pool.GetCreationTimestamp().Time
			if tc.rewrite {
				scope := ledger.Scope
				scope.Lifetime = metav1.Duration{Duration: 24 * time.Hour}
				rewriteSharedLifetime(t, c, pool, ledger, scope)
			}
			if tc.deleteProfile {
				if err := c.Delete(t.Context(), item); err != nil {
					t.Fatal(err)
				}
			}
			if workerPodCount(t, c, pool.GetName()) == 0 {
				t.Fatal("the warm pool has no worker Pod")
			}
			escalation := &SpareWorkerReplenisher{Client: c, Config: sharedConfig()}
			setNow(t, created.Add(tc.bound-sharedRetireMargin-time.Minute))
			if err := escalation.EnforceSharedEscalation(t.Context()); err != nil {
				t.Fatal(err)
			}
			if workerPodCount(t, c, pool.GetName()) == 0 {
				t.Fatal("the warm pool was escalated before its bound")
			}
			setNow(t, created.Add(tc.bound-sharedRetireMargin+time.Minute))
			if err := escalation.EnforceSharedEscalation(t.Context()); err != nil {
				t.Fatal(err)
			}
			if workerPodCount(t, c, pool.GetName()) != 0 {
				t.Fatalf("an unused warm pool outlived its %s bound", tc.bound)
			}
		})
	}
}

func TestSharedWarmPoolRewrittenToAnotherProfileIsDoubt(t *testing.T) {
	c, _, _, pool, ledger := warmSharedPool(t, 3*time.Hour)
	// Another valid opted-in profile with a 24h lifetime.
	long := createSharedProfile(t, c, "tenant", "long", "long-profile-uid", 2, 24*time.Hour)
	scope := ledger.Scope
	scope.ProfileUID, scope.ProfileHash, scope.Lifetime = string(long.UID), long.ProfileHash, metav1.Duration{Duration: 24 * time.Hour}
	rewriteSharedLifetime(t, c, pool, ledger, scope)
	// The rewritten scope passes the profile check on its own...
	provenance := map[types.UID]sharedProvenance{}
	current, _ := getPool(t, c, pool.GetName())
	if err := sharedDriver(c, nil).sharedProfileProvenance(t.Context(), []unstructured.Unstructured{*current}, provenance); err != nil {
		t.Fatal(err)
	}
	if provenance[current.GetUID()].profileLifetime != 0 {
		t.Fatal("a ledger rewritten to another profile took that profile's lifetime")
	}
	// ...but the name does not commit to it: doubt, retired at once.
	if _, _, evidence := sharedBoundsOf(current, provenance[current.GetUID()], true); evidence {
		t.Fatal("a scope the pool's name does not commit to counted as evidence")
	}
	_ = (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
	if _, found := getPool(t, c, pool.GetName()); found {
		t.Fatal("a pool rewritten to another 24h profile was not retired")
	}
}

func TestSharedLegacyNamedPoolWithoutJournalsIsBoundedByTheMinimum(t *testing.T) {
	c, _, _, pool, _ := warmSharedPool(t, 24*time.Hour)
	// A pool created before names committed to their scope.
	legacy := pool.DeepCopy()
	legacy.SetName(sharedPoolPrefix + strings.Repeat("ab", 16))
	legacy.SetUID("legacy-pool-uid")
	created := legacy.GetCreationTimestamp().Time
	provenance := map[types.UID]sharedProvenance{}
	if err := sharedDriver(c, nil).sharedProfileProvenance(t.Context(), []unstructured.Unstructured{*legacy}, provenance); err != nil {
		t.Fatal(err)
	}
	retireAt, escalateAt, evidence := sharedBoundsOf(legacy, provenance[legacy.GetUID()], true)
	if !evidence || escalateAt.After(created.Add(profile.MinSharedWorkerLifetime-sharedRetireMargin)) || retireAt.After(created.Add(profile.MinSharedWorkerLifetime)) {
		t.Fatalf("a legacy-named pool without journals outlived the minimum: escalateAt=%s", escalateAt.Sub(created))
	}
}

func TestSharedOverlappingTemplateRefreshesKeepASavedUID(t *testing.T) {
	cache := &recordedTemplateCache{}
	known := nativeReference{Name: "derived", UID: "known-uid"}
	key := "native/" + known.Name
	// Refresh A begins and reads the journal before the UID was written.
	a := cache.begin()
	// The save records the UID.
	cache.add("native", known)
	// Refresh B begins after the save, reads the UID, and finishes first.
	b := cache.begin()
	if merged := cache.replace(map[string]string{key: known.UID}, b); merged[key] != known.UID {
		t.Fatalf("refresh B lost the saved UID: %q", merged[key])
	}
	// The older refresh A finishes last with its stale unknown UID.
	if merged := cache.replace(map[string]string{key: ""}, a); merged[key] != known.UID {
		t.Fatalf("an older refresh restored an unknown UID over a saved one: %q", merged[key])
	}
	if uid := cache.snapshot()[key]; uid != known.UID {
		t.Fatalf("cached UID = %q, want the saved one", uid)
	}
	// A refresh that began after everything still drops a deleted entry.
	if merged := cache.replace(map[string]string{}, cache.begin()); len(merged) != 0 {
		t.Fatalf("a later complete refresh kept a deleted journal's entry: %v", merged)
	}
}

func TestSharedEscalationNeverWaitsOnProvenanceDiscovery(t *testing.T) {
	for _, loop := range []string{"escalation", "bounds"} {
		t.Run(loop, func(t *testing.T) {
			c, _, _, pool, ledger := warmSharedPool(t, 24*time.Hour)
			setNow(t, sharedEscalateAt(pool, ledger).Add(time.Minute))
			var discovering atomic.Int64
			// Every provenance List stalls until its context ends.
			stalling := contextAwareClient(interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
				List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					switch list.(type) {
					case *corev1.ConfigMapList, *api.ExecutionWorkspaceList:
						discovering.Add(1)
						<-ctx.Done()
						return ctx.Err()
					case *unstructured.UnstructuredList:
						if list.(*unstructured.UnstructuredList).GetKind() == "SubstrateWorkspaceProfileList" {
							discovering.Add(1)
							<-ctx.Done()
							return ctx.Err()
						}
					}
					return w.List(ctx, list, opts...)
				},
			}))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				r := &SpareWorkerReplenisher{Client: stalling, Config: sharedConfig()}
				if loop == "escalation" {
					_ = r.EnforceSharedEscalation(ctx)
				} else {
					_ = r.EnforceSharedBounds(ctx)
				}
			}()
			deadline := time.Now().Add(3 * time.Second)
			for workerPodCount(t, c, pool.GetName()) != 0 {
				if time.Now().After(deadline) {
					t.Fatal("a due escalation waited on provenance discovery")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if discovering.Load() == 0 {
				t.Fatal("provenance discovery was not exercised")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("the pass did not end after its context")
			}
		})
	}
}

func TestSharedProvenanceDiscoveryIsBounded(t *testing.T) {
	c, _, _, _, _ := warmSharedPool(t, 24*time.Hour)
	stalling := contextAwareClient(interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if u, ok := list.(*unstructured.UnstructuredList); ok && u.GetKind() == "SubstrateWorkspaceProfileList" {
				<-ctx.Done()
				return ctx.Err()
			}
			return w.List(ctx, list, opts...)
		},
	}))
	previous := sharedDiscoveryTimeout
	sharedDiscoveryTimeout = 500 * time.Millisecond
	t.Cleanup(func() { sharedDiscoveryTimeout = previous })
	started := time.Now()
	found := sharedDriver(stalling, nil).withSharedPass().discoverShared(t.Context(), true)
	if elapsed := time.Since(started); elapsed > sharedDiscoveryTimeout+2*time.Second {
		t.Fatalf("discovery took %s past its deadline", elapsed)
	}
	if len(found.errs) == 0 {
		t.Fatal("a stalled profile List was not reported")
	}
}

func TestSharedSlowProvenanceSuccessNeverClearsANewerFailure(t *testing.T) {
	c, native, _, _ := sharedFixture(t, 2)
	release := make(chan struct{})
	entered := make(chan struct{})
	var calls atomic.Int64
	interleaved := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.ConfigMapList); !ok {
				return w.List(ctx, list, opts...)
			}
			switch calls.Add(1) {
			case 1:
				// Read A is slow, and succeeds.
				close(entered)
				<-release
			case 2:
				// Read B starts later and fails.
				return apierrors.NewServiceUnavailable("journal inventory unavailable")
			}
			return w.List(ctx, list, opts...)
		},
	})
	d := sharedDriver(interleaved, native)
	start := sharedNow()
	slow := make(chan error, 1)
	go func() {
		_, err := d.sharedJournalProvenance(t.Context())
		slow <- err
	}()
	<-entered
	setNow(t, start.Add(time.Second))
	if _, err := d.sharedJournalProvenance(t.Context()); err == nil {
		t.Fatal("read B did not fail")
	}
	close(release)
	if err := <-slow; err != nil {
		t.Fatal(err)
	}
	now := start.Add(2 * time.Second)
	setNow(t, now)
	if sharedProvenanceHealth.fresh(now) {
		t.Fatal("a slow success that started before a newer failure reopened admission")
	}
	if !sharedProvenanceHealth.outlasted(start.Add(time.Second + sharedProvenanceOutageBound)) {
		t.Fatal("the slow success cleared the newer failure's outage")
	}
	// A success that starts after the failure reopens admission.
	setNow(t, now.Add(time.Second))
	if _, err := d.sharedJournalProvenance(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !sharedProvenanceHealth.fresh(sharedNow()) {
		t.Fatal("a success that started after the failure did not reopen admission")
	}
}

type countingControl struct {
	pb.ControlClient
	mu    sync.Mutex
	calls map[string]int
}

func (c *countingControl) count(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[name]++
}

func (c *countingControl) ListWorkerActorAssignments(ctx context.Context, req *pb.ListWorkerActorAssignmentsRequest, opts ...grpc.CallOption) (*pb.ListWorkerActorAssignmentsResponse, error) {
	c.count("native ListWorkerActorAssignments")
	return c.ControlClient.ListWorkerActorAssignments(ctx, req, opts...)
}

func (c *countingControl) ListWorkers(ctx context.Context, req *pb.ListWorkersRequest, opts ...grpc.CallOption) (*pb.ListWorkersResponse, error) {
	c.count("native ListWorkers")
	return c.ControlClient.ListWorkers(ctx, req, opts...)
}

func (c *countingControl) ListActorTemplates(ctx context.Context, req *pb.ListActorTemplatesRequest, opts ...grpc.CallOption) (*pb.ListActorTemplatesResponse, error) {
	c.count("native ListActorTemplates")
	return c.ControlClient.ListActorTemplates(ctx, req, opts...)
}

func countingKubernetes(c client.Client, calls map[string]int, mu *sync.Mutex) client.Client {
	kind := func(obj runtime.Object) string {
		switch typed := obj.(type) {
		case *unstructured.Unstructured:
			return typed.GetKind()
		case *unstructured.UnstructuredList:
			return typed.GetKind()
		}
		return strings.TrimPrefix(fmt.Sprintf("%T", obj), "*")
	}
	note := func(verb string, obj runtime.Object) {
		mu.Lock()
		defer mu.Unlock()
		calls[verb+" "+kind(obj)]++
	}
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, w client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			note("Get", obj)
			return w.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			note("List", list)
			return w.List(ctx, list, opts...)
		},
	})
}

func TestSharedPassReadCounts(t *testing.T) {
	for _, shape := range []struct{ namespaces, workspaces int }{{1, 4}, {1, 8}, {2, 8}} {
		t.Run(fmt.Sprintf("M=%d,workspaces=%d", shape.namespaces, shape.workspaces), func(t *testing.T) {
			c, native, request := fixture(t, true)
			useTestClock(t)
			for m := range shape.namespaces {
				namespace := fmt.Sprintf("tenant%d", m)
				binding := createSharedProfile(t, c, namespace, "shared", types.UID(namespace+"-profile-uid"), 2)
				for k := range shape.workspaces {
					item := sharedWorkspace(t, c, request, namespace, fmt.Sprintf("w%02d", k), types.UID(fmt.Sprintf("%s-w%02d-uid", namespace, k)), binding)
					sharedReady(t, c, native, item)
				}
			}
			pools := len(sharedPools(t, c))
			measure := func(run func(client.Client, pb.ControlClient)) (map[string]int, map[string]int) {
				var mu sync.Mutex
				kube := map[string]int{}
				control := &countingControl{ControlClient: native, calls: map[string]int{}}
				run(countingKubernetes(c, kube, &mu), control)
				return kube, control.calls
			}
			bounds, _ := measure(func(k client.Client, _ pb.ControlClient) {
				_ = (&SpareWorkerReplenisher{Client: k, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
			})
			sweepKube, sweepNative := measure(func(k client.Client, n pb.ControlClient) {
				_ = (&SpareWorkerReplenisher{Client: k, Control: n, Config: sharedConfig()}).Sweep(t.Context())
			})
			t.Logf("N=%d pools, M=%d namespaces, %d Actors", pools, shape.namespaces, shape.namespaces*shape.workspaces)
			t.Logf("bounds pass: %v", bounds)
			t.Logf("sweep pass: kube %v native %v", sweepKube, sweepNative)
			if bounds["Get v1.ConfigMap"] != 0 || bounds["Get v1alpha1.ExecutionWorkspace"] != 0 || bounds["List v1alpha1.ExecutionWorkspaceList"] > shape.namespaces {
				t.Fatalf("the bounds pass still reads per pool: %v", bounds)
			}
			if sweepNative["native ListWorkerActorAssignments"] > pools || sweepKube["Get v1.ConfigMap"] != 0 || sweepKube["Get v1alpha1.ExecutionWorkspace"] != 0 {
				t.Fatalf("the sweep pass still reads per pool: kube %v native %v", sweepKube, sweepNative)
			}
		})
	}
}

func TestSharedJournalTimeoutStillAppliesTheUnavailableBound(t *testing.T) {
	c, _, _, pool, ledger := warmSharedPool(t, 24*time.Hour)
	previous := sharedDiscoveryTimeout
	sharedDiscoveryTimeout = 300 * time.Millisecond
	t.Cleanup(func() { sharedDiscoveryTimeout = previous })
	// The journal List uses up the whole discovery deadline.
	stalling := contextAwareClient(interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.ConfigMapList); ok {
				<-ctx.Done()
				return ctx.Err()
			}
			return w.List(ctx, list, opts...)
		},
	}))
	found := sharedDriver(stalling, nil).withSharedPass().discoverShared(t.Context(), true)
	if found.listErr != nil || len(found.pools) != 1 || found.provenanceErr == nil {
		t.Fatalf("a journal timeout lost the pool inventory: listErr=%v pools=%d provenanceErr=%v", found.listErr, len(found.pools), found.provenanceErr)
	}
	// With provenance unavailable since that first read, well past the outage
	// bound, a 24h pool past creation plus the 3h minimum's escalation time is
	// escalated, not left to its annotations.
	created := pool.GetCreationTimestamp().Time
	setNow(t, created.Add(profile.MinSharedWorkerLifetime-sharedRetireMargin+time.Minute))
	if !sharedNow().Before(sharedEscalateAt(pool, ledger)) {
		t.Fatal("the fixture pool is already due by its annotations")
	}
	_ = (&SpareWorkerReplenisher{Client: stalling, Config: sharedConfig()}).EnforceSharedEscalation(t.Context())
	if workerPodCount(t, c, pool.GetName()) != 0 {
		t.Fatal("a journal timeout skipped the unavailable-provenance bound")
	}
}

func TestSharedOutageRetirementFiresOnTimeWhenTheJournalReadUsesTheBudget(t *testing.T) {
	c, _, _, pool, _ := warmSharedPool(t, 24*time.Hour)
	previous := sharedDiscoveryTimeout
	sharedDiscoveryTimeout = 300 * time.Millisecond
	t.Cleanup(func() { sharedDiscoveryTimeout = previous })
	stalling := contextAwareClient(interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.ConfigMapList); ok {
				<-ctx.Done()
				return ctx.Err()
			}
			return w.List(ctx, list, opts...)
		},
	}))
	bounds := &SpareWorkerReplenisher{Client: stalling, Config: sharedConfig()}
	start := sharedNow()
	_ = bounds.EnforceSharedBounds(t.Context())
	if current, found := getPool(t, c, pool.GetName()); !found || current.GetDeletionTimestamp() != nil {
		t.Fatal("a young pool was retired before the outage bound")
	}
	if sharedProvenanceHealth.fresh(sharedNow()) {
		t.Fatal("admission stayed open while the journal read timed out")
	}
	setNow(t, start.Add(sharedProvenanceOutageBound))
	_ = bounds.EnforceSharedBounds(t.Context())
	if _, found := getPool(t, c, pool.GetName()); found {
		t.Fatal("a journal read that used the whole discovery budget kept the outage retirement from firing")
	}
}

func TestSharedAdmissionRequiresTheReadItselfToEstablishFreshness(t *testing.T) {
	t.Run("older success after a newer failure", func(t *testing.T) {
		c, native, _, _ := sharedFixture(t, 2)
		release := make(chan struct{})
		entered := make(chan struct{})
		var calls atomic.Int64
		interleaved := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
			List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*corev1.ConfigMapList); !ok {
					return w.List(ctx, list, opts...)
				}
				switch calls.Add(1) {
				case 1:
					// Admission's own read is slow, and succeeds.
					close(entered)
					<-release
				case 2:
					// The bounds loop's newer read fails meanwhile.
					return apierrors.NewServiceUnavailable("journal inventory unavailable")
				}
				return w.List(ctx, list, opts...)
			},
		})
		d := sharedDriver(interleaved, native)
		start := sharedNow()
		admitted := make(chan error, 1)
		go func() { admitted <- d.requireSharedProvenance(t.Context()) }()
		<-entered
		setNow(t, start.Add(time.Second))
		if _, err := d.sharedJournalProvenance(t.Context()); err == nil {
			t.Fatal("the newer read did not fail")
		}
		close(release)
		if err := <-admitted; err == nil {
			t.Fatal("an admission read that started before a newer failure admitted")
		}
	})
	t.Run("slow read", func(t *testing.T) {
		c, native, _, _ := sharedFixture(t, 2)
		start := sharedNow()
		// The read takes longer than the freshness window.
		slow := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
			List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*corev1.ConfigMapList); ok {
					setNow(t, start.Add(sharedProvenanceFreshness+time.Second))
				}
				return w.List(ctx, list, opts...)
			},
		})
		if err := sharedDriver(slow, native).requireSharedProvenance(t.Context()); err == nil {
			t.Fatal("a read already stale when it finished admitted")
		}
	})
}

func TestSharedStaleJournalSnapshotNeverRetiresAHealthyPool(t *testing.T) {
	for _, loop := range []string{"bounds", "sweep"} {
		t.Run(loop, func(t *testing.T) {
			c, native, first, binding := sharedFixture(t, 3)
			second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
			_, recordA := sharedReady(t, c, native, first)
			sharedReady(t, c, native, second)
			journal := journalKey(recordA.Request.Key)
			// The pass's inventory List was taken just before A saved its
			// Actor UID: it lists A's journal without it, while the pool,
			// listed afterwards, already holds A's booted entry.
			stale := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
				List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if err := w.List(ctx, list, opts...); err != nil {
						return err
					}
					cms, ok := list.(*corev1.ConfigMapList)
					if !ok {
						return nil
					}
					for i := range cms.Items {
						if client.ObjectKeyFromObject(&cms.Items[i]) != journal {
							continue
						}
						record, ok := decodeClaimRecord(&cms.Items[i])
						if !ok {
							t.Fatal("the fixture journal does not decode")
						}
						record.Actor.UID = ""
						data, err := json.Marshal(record)
						if err != nil {
							t.Fatal(err)
						}
						cms.Items[i].Data[journalDataKey] = string(data)
					}
					return nil
				},
			})
			if loop == "bounds" {
				_ = (&SpareWorkerReplenisher{Client: stale, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
			} else {
				_ = sharedReplenisher(stale, native).Sweep(t.Context())
			}
			pool, found := getPool(t, c, recordA.SharedWorker.Pool)
			if !found || pool.GetDeletionTimestamp() != nil || sharedRetireIntended(pool) || sharedRetirementPersisted(pool) {
				t.Fatal("a journal snapshot older than the pool List retired a healthy pool")
			}
			if _, ledger := poolLedger(t, c, recordA.SharedWorker.Pool); len(ledger.Entries) != 2 {
				t.Fatalf("a stale snapshot collected a valid entry: %d entries", len(ledger.Entries))
			}
		})
	}
}

func TestSharedOneFailedJournalListNeverRetiresOrEscalatesAHealthyPool(t *testing.T) {
	for _, age := range []time.Duration{2 * time.Hour, profile.MinSharedWorkerLifetime - sharedRetireMargin + time.Minute} {
		t.Run(age.String(), func(t *testing.T) {
			c, native, first, _ := sharedFixture(t, 3)
			_, record := sharedReady(t, c, native, first)
			pool, _ := poolLedger(t, c, record.SharedWorker.Pool)
			created := pool.GetCreationTimestamp().Time
			setNow(t, created.Add(age))
			var failing atomic.Bool
			failing.Store(true)
			flaky := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
				List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if _, ok := list.(*corev1.ConfigMapList); ok && failing.Load() {
						return apierrors.NewServiceUnavailable("journal inventory unavailable")
					}
					return w.List(ctx, list, opts...)
				},
			})
			r := &SpareWorkerReplenisher{Client: flaky, Config: sharedConfig()}
			_ = r.EnforceSharedBounds(t.Context())
			_ = r.EnforceSharedEscalation(t.Context())
			current, found := getPool(t, c, record.SharedWorker.Pool)
			if !found || current.GetDeletionTimestamp() != nil || sharedRetireIntended(current) || sharedRetirementPersisted(current) {
				t.Fatalf("one failed journal List retired a healthy %s-old pool", age)
			}
			if !podExists(t, c, record.Worker.Namespace, record.Worker.Pod, record.Worker.PodUID) {
				t.Fatalf("one failed journal List escalated a healthy %s-old pool", age)
			}
			if err := sharedDriver(flaky, native).requireSharedProvenance(t.Context()); err == nil {
				t.Fatal("admission stayed open while the journal inventory was unavailable")
			}
			// A sustained outage past its bound still retires the pool.
			setNow(t, sharedNow().Add(sharedProvenanceOutageBound))
			_ = r.EnforceSharedBounds(t.Context())
			if _, found := getPool(t, c, record.SharedWorker.Pool); found {
				t.Fatal("an outage past its bound did not retire the pool")
			}
		})
	}
}

func TestSharedEnforcementReadsAreBounded(t *testing.T) {
	previousDiscovery, previousList := sharedDiscoveryTimeout, sharedPoolListTimeout
	sharedDiscoveryTimeout, sharedPoolListTimeout = 300*time.Millisecond, 300*time.Millisecond
	t.Cleanup(func() { sharedDiscoveryTimeout, sharedPoolListTimeout = previousDiscovery, previousList })
	t.Run("stalled hosted reads never hold up a due retirement", func(t *testing.T) {
		c, native, first, _ := sharedFixture(t, 2)
		_, record := sharedReady(t, c, native, first)
		pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
		setNow(t, sharedRetireAt(pool, ledger))
		// Every workspace and journal read in the decision phase stalls.
		stalling := contextAwareClient(interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
			Get: func(ctx context.Context, w client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				switch obj.(type) {
				case *api.ExecutionWorkspace, *corev1.ConfigMap:
					<-ctx.Done()
					return ctx.Err()
				}
				return w.Get(ctx, key, obj, opts...)
			},
			List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*api.ExecutionWorkspaceList); ok {
					<-ctx.Done()
					return ctx.Err()
				}
				return w.List(ctx, list, opts...)
			},
		}))
		started := time.Now()
		_ = (&SpareWorkerReplenisher{Client: stalling, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
		if elapsed := time.Since(started); elapsed > 3*time.Second {
			t.Fatalf("the bounds pass took %s with stalled hosted reads", elapsed)
		}
		if _, found := getPool(t, c, record.SharedWorker.Pool); found {
			t.Fatal("a due pool waited on stalled hosted reads")
		}
	})
	t.Run("a stalled pool List ends the escalation pass", func(t *testing.T) {
		c, _, _, _, _ := warmSharedPool(t, 24*time.Hour)
		stalling := contextAwareClient(interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
			List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if u, ok := list.(*unstructured.UnstructuredList); ok && u.GetKind() == "WorkerPoolList" {
					<-ctx.Done()
					return ctx.Err()
				}
				return w.List(ctx, list, opts...)
			},
		}))
		started := time.Now()
		_ = (&SpareWorkerReplenisher{Client: stalling, Config: sharedConfig()}).EnforceSharedEscalation(t.Context())
		if elapsed := time.Since(started); elapsed > 3*time.Second {
			t.Fatalf("the escalation pass took %s on a stalled pool List", elapsed)
		}
	})
}

func TestSharedContinuationLosingThePodRaceReportsTheInstanceLost(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	a, _ := sharedReady(t, c, native, first)
	// A first suspension leaves a verified checkpoint, and the workspace
	// resumes from it.
	_, suspended := sharedRetired(t, c, native, first, a.Identity, true)
	if suspended.Observation.RetainedData == nil {
		t.Fatal("the first suspension retained no data")
	}
	retained := *suspended.Observation.RetainedData
	next := *first.DeepCopy()
	next.Sequence++
	next.PreviousInstance = &a.Identity
	next.RetainedData = &retained
	next.Runtime.Template.Spec.Containers[0].Env[0].Value = "rotated-public-nonce"
	next.Revision, _ = sdk.WorkloadRevision(next)
	if err := admit(c)(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	b, recordB := sharedReady(t, c, native, next)
	tags := len(native.tags)
	calls := native.suspendCalls
	// The second suspension's FinalizeSuspended fails, leaving it
	// SUSPENDING; the strong path starts.
	native.failFinalizeOnce = true
	tick()
	_, _ = sharedDriver(c, native).SuspendInstance(t.Context(), next.Key, b.Identity)
	// Upstream's worker-delete crashes the SUSPENDING Actor before any
	// continuation is sent.
	actor := native.actors[refKey(actorRef(recordB))]
	if actor.Status.State != pb.ActorState_ACTOR_STATE_SUSPENDING {
		t.Fatalf("the fixture Actor is %s, not SUSPENDING", actor.Status.State)
	}
	native.finalizePending = ""
	actor.Status.State = pb.ActorState_ACTOR_STATE_CRASHED
	actor.Metadata.Version++
	native.releaseWorker(actor)
	// The suspension's data can no longer be captured: the instance is
	// reported lost, never suspended.
	for range 8 {
		tick()
		if observed, _ := sharedDriver(c, native).SuspendInstance(t.Context(), next.Key, b.Identity); observed.State == sdk.AllocationStopped {
			t.Fatal("a suspension whose data was lost reported success")
		}
		if _, current, err := sharedDriver(c, native).read(t.Context(), next.Key); err == nil && current.Failure != "" {
			break
		}
	}
	if _, current, err := sharedDriver(c, native).read(t.Context(), next.Key); err != nil || current.Failure == "" {
		t.Fatalf("the lost race was not reported as a lost instance: %v", err)
	}
	// It retires through Stop.
	observed, record := sharedRetired(t, c, native, next, b.Identity, false)
	if observed.State != sdk.AllocationStopped {
		t.Fatalf("the lost instance did not retire through Stop: %s", observed.State)
	}
	// The last verified checkpoint, the one it resumed from, is kept: its Tag
	// still exists, so the workspace's earlier retained data stays restorable.
	source := recordB.InheritedCheckpoint
	if source == nil {
		t.Fatal("the resumed instance recorded no restore checkpoint")
	}
	if native.tags[source.Atespace+"/"+source.Tag.Name] == nil {
		t.Fatal("the lost instance's last verified checkpoint was not kept")
	}
	if observed.RetainedData != nil {
		t.Fatal("the lost instance reported new retained data")
	}
	if len(native.tags) != tags {
		t.Fatal("a Tag was invented for the lost suspension")
	}
	if native.suspendCalls != calls+1 {
		t.Fatalf("SuspendActor calls = %d, want only the failed original", native.suspendCalls-calls)
	}
	if record.SharedWorker.Strong == "" || !record.WorkloadAbsent {
		t.Fatal("the lost instance did not finish the strong path")
	}
}
