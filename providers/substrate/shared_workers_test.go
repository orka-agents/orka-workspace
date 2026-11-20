package substrate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	profile "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	sharedRecordsLabelled.Store(false)
	sharedTemplateInventoryHealth.reset()
	sharedNativeTemplateHealth.reset()
	sharedNativeCredentials.Store(nil)
	sharedUnverifiedPools.reset()
	sharedNativeDegraded.Store(nil)
	sharedProfileInventoryHealth.reset()
	sharedDecidedStamps.reset()
	sharedUndurableRetirements.reset()
	t.Cleanup(func() {
		sharedUndurableRetirements.reset()
		sharedDecidedStamps.reset()
		sharedUnverifiedPools.reset()
		sharedNativeDegraded.Store(nil)
		sharedProfileInventoryHealth.reset()
		sharedNativeTemplateHealth.reset()
		sharedNativeCredentials.Store(nil)
		sharedNow = previous
		sharedProvenanceHealth.reset()
		sharedRecordsLabelled.Store(false)
		sharedTemplateInventoryHealth.reset()
	})
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

// A failure after the interim proof is doubt too: a DeleteActor that keeps
// failing persists the strong marker and starts the pool's retirement before
// the pass returns, so the bounds loop has a marker and the worker stops
// admitting.
func TestSharedFailureAfterTheInterimProofTakesTheStrongPath(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	a, recordA := sharedReady(t, c, native, first)
	sharedReady(t, c, native, second)
	native.failDelete = true
	var record *journalRecord
	for range 24 {
		tick()
		_, _ = sharedDriver(c, native).StopInstance(t.Context(), first.Key, a.Identity)
		_, current, err := sharedDriver(c, native).read(t.Context(), first.Key)
		if err != nil {
			t.Fatal(err)
		}
		record = current
		if record.DeleteIssued {
			break
		}
	}
	if record == nil || !record.DeleteIssued || record.SharedWorker.Proof == nil {
		t.Fatal("the fixture did not reach the Actor's deletion after an interim proof")
	}
	if record.SharedWorker.Strong == "" {
		t.Fatal("a failing DeleteActor after the interim proof left no strong marker")
	}
	if pool, found := getPool(t, c, recordA.SharedWorker.Pool); found && pool.GetDeletionTimestamp() == nil && !sharedRetireIntended(pool) {
		t.Fatal("a failing DeleteActor after the interim proof did not start the pool's retirement")
	}
	third := sharedWorkspace(t, c, first, "tenant", "third", "third-uid", binding)
	if _, recordC := sharedReady(t, c, native, third); recordC.SharedWorker.Pool == recordA.SharedWorker.Pool {
		t.Fatal("the worker kept admitting after a failure past its interim proof")
	}
}

// Every step of a shared Stop from its evidence suspend through deletion,
// slot release and journal finalization follows one rule: a persistent
// failure there persists the strong marker and starts the pool's retirement
// before the pass returns. Each case injects a lasting failure at one step.
func TestSharedEveryPostEvidenceFailureTakesTheStrongPath(t *testing.T) {
	// journalTransition fails journal writes that newly set a field.
	journalTransition := func(changed func(before, after *journalRecord) bool) func(*sharedStepFault) {
		return func(f *sharedStepFault) {
			f.journal = changed
		}
	}
	for _, tc := range []struct {
		name   string
		inject func(*sharedStepFault)
	}{
		{"evidence SuspendActor", func(f *sharedStepFault) { f.native.failSuspendBeforeEffect = true }},
		{"GetWorker during the proof", func(f *sharedStepFault) { f.afterSuspend = func(n *nativeFixture) { n.failGetWorker = 1 << 20 } }},
		{"assignment List during the proof", func(f *sharedStepFault) { f.afterSuspend = func(n *nativeFixture) { n.assignmentsRejected = true } }},
		{"suspend observation save", journalTransition(func(before, after *journalRecord) bool {
			return before.SharedWorker.Suspend != nil && before.SharedWorker.Suspend.SnapshotDigest == "" && after.SharedWorker.Suspend != nil && after.SharedWorker.Suspend.SnapshotDigest != ""
		})},
		{"proof save", journalTransition(func(before, after *journalRecord) bool {
			return before.SharedWorker.Proof == nil && after.SharedWorker.Proof != nil
		})},
		{"workload absence save", journalTransition(func(before, after *journalRecord) bool { return !before.WorkloadAbsent && after.WorkloadAbsent })},
		{"Actor deletion save", journalTransition(func(before, after *journalRecord) bool { return !before.DeleteIssued && after.DeleteIssued })},
		{"GetActor after the proof", func(f *sharedStepFault) { f.afterAbsent = func(n *nativeFixture) { n.failGetActor = 1 << 20 } }},
		{"DeleteActor", func(f *sharedStepFault) { f.native.failDelete = true }},
		{"slot release ledger write", func(f *sharedStepFault) { f.failLedger = true }},
		{"slot release save", journalTransition(func(before, after *journalRecord) bool {
			return !before.SharedWorker.Released && after.SharedWorker.Released
		})},
		{"journal finalization save", journalTransition(func(before, after *journalRecord) bool {
			return before.Observation.State != sdk.AllocationStopped && after.Observation.State == sdk.AllocationStopped
		})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, native, first, binding := sharedFixture(t, 3)
			second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
			a, recordA := sharedReady(t, c, native, first)
			sharedReady(t, c, native, second)
			fault := &sharedStepFault{native: native}
			tc.inject(fault)
			native.afterSuspend = func(*pb.Actor) {
				if fault.afterSuspend != nil {
					fault.fired.Store(true)
					fault.afterSuspend(native)
				}
			}
			faulty := fault.client(t, c)
			started := func() bool {
				pool, found := getPool(t, c, recordA.SharedWorker.Pool)
				return !found || pool.GetDeletionTimestamp() != nil || sharedRetireIntended(pool)
			}
			for range 24 {
				tick()
				_, _ = sharedDriver(faulty, native).StopInstance(t.Context(), first.Key, a.Identity)
				if started() {
					break
				}
			}
			if !fault.fired.Load() {
				t.Fatal("the fixture never injected its failure")
			}
			if !started() {
				t.Fatal("a lasting failure at this step never started the pool's strong retirement")
			}
		})
	}
}

// cancellingDeleteControl cancels the request inside DeleteActor and fails
// the call as a real one would.
type cancellingDeleteControl struct {
	*nativeFixture
	cancel context.CancelFunc
}

func (c *cancellingDeleteControl) DeleteActor(ctx context.Context, req *pb.DeleteActorRequest, opts ...grpc.CallOption) (*pb.Actor, error) {
	c.cancel()
	return nil, status.FromContextError(ctx.Err()).Err()
}

// The retry half of the same rule: a failure after the proof that comes only
// from the request's own context ending never starts the strong path, and a
// live pass then finishes the Stop on its interim proof.
func TestSharedCancelledPostProofFailureIsRetried(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	a, recordA := sharedReady(t, c, native, first)
	sharedReady(t, c, native, second)
	cancelled := false
	for range 24 {
		tick()
		_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
		if err != nil {
			t.Fatal(err)
		}
		d := sharedDriver(c, native)
		ctx, cancel := context.WithCancel(t.Context())
		if record.WorkloadAbsent || record.SharedWorker.Proof != nil {
			d.control = &cancellingDeleteControl{nativeFixture: native, cancel: cancel}
			cancelled = true
		}
		_, _ = d.StopInstance(ctx, first.Key, a.Identity)
		cancel()
		if cancelled {
			break
		}
	}
	_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if !cancelled || !record.DeleteIssued {
		t.Fatal("the fixture never cancelled the Actor's deletion")
	}
	if pool, found := getPool(t, c, recordA.SharedWorker.Pool); record.SharedWorker.Strong != "" || !found || pool.GetDeletionTimestamp() != nil || sharedRetireIntended(pool) {
		t.Fatalf("a cancelled request started the strong path: %q", record.SharedWorker.Strong)
	}
	observed, record := sharedRetired(t, c, native, first, a.Identity, false)
	if observed.State != sdk.AllocationStopped || record.SharedWorker.Strong != "" {
		t.Fatalf("the retried Stop did not finish on its interim proof: %s %q", observed.State, record.SharedWorker.Strong)
	}
}

// deadlineControl fails chosen native calls with a cancellation or deadline
// while the request itself is still live.
type deadlineControl struct {
	*nativeFixture
	getWorker, getActor, deleteActor, getEgress func() error
}

func (c *deadlineControl) GetActorEgressPolicy(ctx context.Context, req *pb.GetActorEgressPolicyRequest, opts ...grpc.CallOption) (*pb.EgressPolicy, error) {
	if c.getEgress != nil {
		if err := c.getEgress(); err != nil {
			return nil, err
		}
	}
	return c.nativeFixture.GetActorEgressPolicy(ctx, req, opts...)
}

func (c *deadlineControl) GetWorker(ctx context.Context, req *pb.GetWorkerRequest, opts ...grpc.CallOption) (*pb.Worker, error) {
	if c.getWorker != nil {
		if err := c.getWorker(); err != nil {
			return nil, err
		}
	}
	return c.nativeFixture.GetWorker(ctx, req, opts...)
}

func (c *deadlineControl) GetActor(ctx context.Context, req *pb.GetActorRequest, opts ...grpc.CallOption) (*pb.Actor, error) {
	if c.getActor != nil {
		if err := c.getActor(); err != nil {
			return nil, err
		}
	}
	return c.nativeFixture.GetActor(ctx, req, opts...)
}

func (c *deadlineControl) DeleteActor(ctx context.Context, req *pb.DeleteActorRequest, opts ...grpc.CallOption) (*pb.Actor, error) {
	if c.deleteActor != nil {
		if err := c.deleteActor(); err != nil {
			return nil, err
		}
	}
	return c.nativeFixture.DeleteActor(ctx, req, opts...)
}

// The other half of the rule, at every kind of post-evidence step: a
// cancellation or a deadline is always a retry, never doubt, also when it
// comes from a child context that timed out while the request was live, or
// as a gRPC status, or wrapped. A live pass then finishes the Stop on its
// interim proof, and no pool is retired.
func TestSharedPostEvidenceCancellationsAndDeadlinesAreRetried(t *testing.T) {
	childTimeout := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		defer cancel()
		<-ctx.Done()
		return fmt.Errorf("child budget: %w", ctx.Err())
	}
	grpcDeadline := func() error {
		return fmt.Errorf("wrapped: %w", status.Error(codes.DeadlineExceeded, "upstream deadline"))
	}
	grpcCanceled := func() error {
		return status.Error(codes.Canceled, "workflow failed at step FinalizeSuspended: context canceled")
	}
	for _, tc := range []struct {
		name string
		// stage selects where the failure is injected.
		stage string
		err   func() error
	}{
		{"GetWorker during the proof, gRPC deadline", "proof", grpcDeadline},
		{"proof attempt save, child timeout", "proofAttemptSave", childTimeout},
		{"egress read during the proof, gRPC deadline", "egressRead", grpcDeadline},
		{"final Actor read of the proof, child timeout", "proofActorRead", childTimeout},
		{"GetActor after the proof, gRPC canceled", "getActor", grpcCanceled},
		{"DeleteActor, gRPC deadline", "delete", grpcDeadline},
		{"DeleteActor, child timeout", "delete", childTimeout},
		{"DeleteActor, a deadline joined behind another status", "delete", func() error {
			return errors.Join(status.Error(codes.Unavailable, "attempt failed"), status.Error(codes.DeadlineExceeded, "retry budget expired"))
		}},
		{"workload absence save, child timeout", "absentSave", childTimeout},
		{"slot release ledger write, child timeout", "ledger", childTimeout},
		{"journal finalization save, child timeout", "finishSave", childTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, native, first, binding := sharedFixture(t, 3)
			second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
			a, recordA := sharedReady(t, c, native, first)
			sharedReady(t, c, native, second)
			var remaining atomic.Int64
			remaining.Store(3)
			var suspended, absent atomic.Bool
			fail := func(active bool) error {
				if !active || remaining.Load() <= 0 {
					return nil
				}
				remaining.Add(-1)
				return tc.err()
			}
			native.afterSuspend = func(*pb.Actor) {
				suspended.Store(true)
				if tc.stage == "proofAttemptSave" {
					// The worker's version keeps moving, so the proof records
					// another attempt, until the injected failures run out.
					native.churnWorker = recordA.Worker.Name
				}
			}
			var passActorReads atomic.Int64
			control := &deadlineControl{nativeFixture: native,
				getWorker: func() error { return fail(tc.stage == "proof" && suspended.Load()) },
				getActor: func() error {
					reads := passActorReads.Add(1)
					return fail(tc.stage == "getActor" && absent.Load() || tc.stage == "proofActorRead" && suspended.Load() && !absent.Load() && reads == 2)
				},
				deleteActor: func() error { return fail(tc.stage == "delete") },
				getEgress:   func() error { return fail(tc.stage == "egressRead" && suspended.Load()) },
			}
			faulty := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				switch o := obj.(type) {
				case *corev1.ConfigMap:
					after, ok := decodeClaimRecord(o)
					stored := &corev1.ConfigMap{}
					if ok && w.Get(ctx, client.ObjectKeyFromObject(o), stored) == nil {
						if before, ok := decodeClaimRecord(stored); ok && before.SharedWorker != nil {
							if !before.WorkloadAbsent && after.WorkloadAbsent {
								if err := fail(tc.stage == "absentSave"); err != nil {
									return err
								}
								absent.Store(true)
							}
							if before.Observation.State != sdk.AllocationStopped && after.Observation.State == sdk.AllocationStopped {
								if err := fail(tc.stage == "finishSave"); err != nil {
									return err
								}
							}
							if after.SharedWorker.ProofAttempts > before.SharedWorker.ProofAttempts && tc.stage == "proofAttemptSave" {
								err := fail(true)
								if remaining.Load() <= 0 {
									native.churnWorker = ""
								}
								if err != nil {
									return err
								}
							}
						}
					}
				case *unstructured.Unstructured:
					if o.GetKind() == "WorkerPool" && absent.Load() {
						if err := fail(tc.stage == "ledger"); err != nil {
							return err
						}
					}
				}
				return w.Update(ctx, obj, opts...)
			}})
			var observed sdk.AllocationObservation
			for range 24 {
				tick()
				passActorReads.Store(0)
				d := sharedDriver(faulty, native)
				d.control = control
				observed, _ = d.StopInstance(t.Context(), first.Key, a.Identity)
				if observed.State == sdk.AllocationStopped {
					break
				}
			}
			_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
			if err != nil {
				t.Fatal(err)
			}
			if remaining.Load() > 0 {
				t.Fatal("the fixture never injected its failure")
			}
			if record.SharedWorker.Strong != "" {
				t.Fatalf("a cancellation or deadline started the strong path: %q", record.SharedWorker.Strong)
			}
			if pool, found := getPool(t, c, recordA.SharedWorker.Pool); !found || pool.GetDeletionTimestamp() != nil || sharedRetireIntended(pool) {
				t.Fatal("a cancellation or deadline retired the pool and its neighbours")
			}
			if observed.State != sdk.AllocationStopped {
				t.Fatalf("the retried Stop did not finish: %s", observed.State)
			}
		})
	}
}

// A one-shot finalization failure takes the strong path on the journal as
// last persisted: finishStop's provisional Stopped state is never made
// durable by the marker's write, and Stopped is reported only once the exact
// worker Pod is proven absent, although the slot was already released.
func TestSharedFinalizationFailureNeverPersistsAProvisionalStop(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	a, recordA := sharedReady(t, c, native, first)
	sharedReady(t, c, native, second)
	podKey := client.ObjectKey{Namespace: recordA.Worker.Namespace, Name: recordA.Worker.Pod}
	pod := &corev1.Pod{}
	if err := c.Get(t.Context(), podKey, pod); err != nil {
		t.Fatal(err)
	}
	pod.SetFinalizers([]string{"test.invalid/hold"})
	if err := c.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	var failed atomic.Bool
	faulty := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if o, ok := obj.(*corev1.ConfigMap); ok && !failed.Load() {
			if after, ok := decodeClaimRecord(o); ok && after.SharedWorker != nil && after.Observation.State == sdk.AllocationStopped && after.SharedWorker.Strong == "" {
				failed.Store(true)
				return apierrors.NewInternalError(errors.New("journal write refused once"))
			}
		}
		return w.Update(ctx, obj, opts...)
	}})
	for range 24 {
		tick()
		_, _ = sharedDriver(faulty, native).StopInstance(t.Context(), first.Key, a.Identity)
		if failed.Load() {
			break
		}
	}
	if !failed.Load() {
		t.Fatal("the fixture never failed finalization")
	}
	for range 4 {
		tick()
		observed, _ := sharedDriver(c, native).StopInstance(t.Context(), first.Key, a.Identity)
		if observed.State == sdk.AllocationStopped {
			t.Fatal("Stopped was reported before the strongly retired worker Pod was proven absent")
		}
	}
	_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.SharedWorker.Strong == "" || record.Observation.State == sdk.AllocationStopped || record.Timing.StoppedAt != nil {
		t.Fatalf("the provisional finalization was persisted: strong=%q state=%s", record.SharedWorker.Strong, record.Observation.State)
	}
	if err := c.Get(t.Context(), podKey, pod); err != nil {
		t.Fatal(err)
	}
	pod.SetFinalizers(nil)
	if err := c.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	if observed, _ := sharedRetired(t, c, native, first, a.Identity, false); observed.State != sdk.AllocationStopped {
		t.Fatalf("the Stop did not finish once the Pod was absent: %s", observed.State)
	}
}

// A ledger that becomes unreadable after a booted Actor's interim proof and
// deletion is doubt during the slot release: the pool is retired strongly,
// and Stopped needs exact Pod absence.
func TestSharedUnreadableLedgerAtReleaseTakesTheStrongPath(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	a, recordA := sharedReady(t, c, native, first)
	sharedReady(t, c, native, second)
	for range 24 {
		tick()
		_, _ = sharedDriver(c, native).StopInstance(t.Context(), first.Key, a.Identity)
		if _, record, err := sharedDriver(c, native).read(t.Context(), first.Key); err == nil && record.DeleteIssued {
			break
		}
	}
	pool, _ := poolLedger(t, c, recordA.SharedWorker.Pool)
	annotations := maps.Clone(pool.GetAnnotations())
	annotations[sharedLedgerAnnotation] = "{"
	pool.SetAnnotations(annotations)
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	var observed sdk.AllocationObservation
	for range 24 {
		tick()
		if observed, _ = sharedDriver(c, native).StopInstance(t.Context(), first.Key, a.Identity); observed.State == sdk.AllocationStopped {
			break
		}
	}
	_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if observed.State != sdk.AllocationStopped || record.SharedWorker.Strong == "" {
		t.Fatalf("an unreadable ledger at release did not take the strong path: %s %q", observed.State, record.SharedWorker.Strong)
	}
	if podExists(t, c, recordA.Worker.Namespace, recordA.Worker.Pod, recordA.Worker.PodUID) {
		t.Fatal("Stopped was reported before exact Pod absence")
	}
}

// A strong marker persisted after the slot release (a finalization that
// failed, then a crash before the pool's deletion) still drives the pool's
// retirement from the bounds loop, although no ledger entry carries it.
func TestSharedPostReleaseStrongMarkerStillRetiresThePool(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	_, recordA := sharedReady(t, c, native, first)
	sharedReady(t, c, native, second)
	d := sharedDriver(c, native)
	cm, record, err := d.read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	pool, ledger := poolLedger(t, c, recordA.SharedWorker.Pool)
	marker := claimMarker(cm, record)
	ledger.Entries = slices.DeleteFunc(ledger.Entries, func(entry sharedLedgerEntry) bool { return entry.Journal == marker })
	if err := setSharedLedger(pool, ledger); err != nil {
		t.Fatal(err)
	}
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	record.SharedWorker.Released = true
	record.SharedWorker.Strong = "the shared retirement could not complete: finalization failed"
	if err := d.save(t.Context(), cm, record); err != nil {
		t.Fatal(err)
	}
	if err := (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context()); err != nil {
		t.Fatal(err)
	}
	if current, found := getPool(t, c, recordA.SharedWorker.Pool); found && current.GetDeletionTimestamp() == nil && !sharedRetireIntended(current) {
		t.Fatal("a post-release strong marker left the pool admitting")
	}
}

// A strong path decided after the slot release starts the pool's deletion
// before its marker is written, so a crash in between cannot leave the pool
// admitting with no ledger entry to recover the decision from.
func TestSharedPostReleaseDoubtStartsTheDeletionBeforeTheMarker(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	a, recordA := sharedReady(t, c, native, first)
	sharedReady(t, c, native, second)
	var finalized, crashed atomic.Bool
	crashing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if o, ok := obj.(*corev1.ConfigMap); ok {
			if after, ok := decodeClaimRecord(o); ok && after.SharedWorker != nil && after.SharedWorker.Released {
				if after.Observation.State == sdk.AllocationStopped && !finalized.Load() {
					finalized.Store(true)
					return apierrors.NewInternalError(errors.New("journal write refused once"))
				}
				if after.SharedWorker.Strong != "" {
					crashed.Store(true)
					panic("provider crashed while saving the post-release marker")
				}
			}
		}
		return w.Update(ctx, obj, opts...)
	}})
	for range 24 {
		tick()
		func() {
			defer func() { _ = recover() }()
			_, _ = sharedDriver(crashing, native).StopInstance(t.Context(), first.Key, a.Identity)
		}()
		if crashed.Load() {
			break
		}
	}
	if !crashed.Load() {
		t.Fatal("the fixture never crashed on the post-release marker")
	}
	if current, found := getPool(t, c, recordA.SharedWorker.Pool); found && current.GetDeletionTimestamp() == nil && !sharedRetireIntended(current) {
		t.Fatal("a crash before the post-release marker left the pool admitting")
	}
}

// A slot claimed before the pool's isolation checks failed never boots while
// the pool is unverified.
func TestSharedUnverifiedPoolNeverResumesAClaimedSlot(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	_, recordA := sharedReady(t, c, native, first)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	var stamped atomic.Bool
	stamping := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if err := w.Update(ctx, obj, opts...); err != nil {
			return err
		}
		if o, ok := obj.(*unstructured.Unstructured); ok && o.GetKind() == "WorkerPool" && o.GetName() == recordA.SharedWorker.Pool && !stamped.Load() {
			if ledger, err := readSharedLedger(o); err == nil && len(ledger.Entries) == 2 {
				// The claim landed; an isolation check then fails.
				stamped.Store(true)
				pool := poolObject()
				if err := w.Get(ctx, client.ObjectKeyFromObject(o), pool); err != nil {
					return err
				}
				annotations := maps.Clone(pool.GetAnnotations())
				annotations[sharedUnverifiedAnnotation] = sharedNow().UTC().Format(time.RFC3339Nano)
				pool.SetAnnotations(annotations)
				return w.Update(ctx, pool)
			}
		}
		return nil
	}})
	boots := native.boots
	for range 6 {
		tick()
		if observed, _ := sharedDriver(stamping, native).EnsureAllocation(t.Context(), second); observed.State == sdk.AllocationReady {
			break
		}
	}
	if !stamped.Load() {
		t.Fatal("the second allocation never claimed the pool")
	}
	if native.boots != boots {
		t.Fatal("a slot claimed before an isolation-check failure booted while the pool was unverified")
	}
}

// A replacement-worker marker whose save fails for good still starts the
// pool's retirement: only a durably persisted marker means the strong path
// is under way.
func TestSharedFailedReplacementMarkerSaveStillRetires(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	a, recordA := sharedReady(t, c, native, first)
	sharedReady(t, c, native, second)
	actor := native.actors[refKey(actorRef(recordA))]
	assignment := proto.Clone(actor.Status.WorkerAssignment).(*pb.WorkerAssignment)
	assignment.Worker = &pb.ObjectRef{Name: "replacement-worker"}
	assignment.WorkerPod, assignment.WorkerPodUid = "replacement-pod", "replacement-pod-uid"
	actor.Status.WorkerAssignment = assignment
	refusing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if o, ok := obj.(*corev1.ConfigMap); ok {
			if after, ok := decodeClaimRecord(o); ok && len(after.RetirementWorkers) > 0 {
				return apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, o.GetName(), errors.New("update refused"))
			}
		}
		return w.Update(ctx, obj, opts...)
	}})
	tick()
	_, _ = sharedDriver(refusing, native).StopInstance(t.Context(), first.Key, a.Identity)
	if current, found := getPool(t, c, recordA.SharedWorker.Pool); found && current.GetDeletionTimestamp() == nil && !sharedRetireIntended(current) {
		t.Fatal("a failed replacement-marker save skipped the pool's retirement")
	}
}

// After the suspension intent, every SuspendInstance pass rechecks the
// retained-data reservation. A definite failure of that read, before or after
// the capture, is doubt like any other: the pool's strong retirement starts.
// A conflict or a cancellation is retried.
func TestSharedSuspensionReservationFailureTakesTheStrongPath(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    func() error
		strong bool
	}{
		{"definite failure", func() error { return apierrors.NewInternalError(errors.New("reservation read refused")) }, true},
		{"deadline", func() error { return fmt.Errorf("reservation read: %w", context.DeadlineExceeded) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, native, first, binding := sharedFixture(t, 3)
			second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
			a, recordA := sharedReady(t, c, native, first)
			sharedReady(t, c, native, second)
			d := sharedDriver(c, native)
			cm, record, err := d.read(t.Context(), first.Key)
			if err != nil {
				t.Fatal(err)
			}
			limit := int32(5)
			record.MaxSuspended = &limit
			if err := d.save(t.Context(), cm, record); err != nil {
				t.Fatal(err)
			}
			// The intent is persisted, and the reservation made.
			if _, err := sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity); err != nil {
				t.Fatal(err)
			}
			if _, current, err := sharedDriver(c, native).read(t.Context(), first.Key); err != nil || current.Pending == nil {
				t.Fatalf("the suspension intent was not persisted: %v", err)
			}
			retention := retentionKey(record)
			failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, w client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok && key == retention {
					return tc.err()
				}
				return w.Get(ctx, key, obj, opts...)
			}})
			for range 3 {
				tick()
				_, _ = sharedDriver(failing, native).SuspendInstance(t.Context(), first.Key, a.Identity)
			}
			pool, found := getPool(t, c, recordA.SharedWorker.Pool)
			retiring := !found || pool.GetDeletionTimestamp() != nil || sharedRetireIntended(pool)
			if retiring != tc.strong {
				t.Fatalf("pool retirement = %v, want %v", retiring, tc.strong)
			}
		})
	}
}

// Every exit of a shared data suspension after its capture follows the
// post-evidence rule: a lasting definite failure at any step starts the
// pool's strong retirement before the pass returns.
func TestSharedEveryPostCaptureFailureTakesTheStrongPath(t *testing.T) {
	transition := func(changed func(before, after *journalRecord) bool) func(*sharedStepFault) {
		return func(f *sharedStepFault) { f.journal = changed }
	}
	for _, tc := range []struct {
		name   string
		inject func(*sharedStepFault)
	}{
		{"source snapshot digest save", transition(func(before, after *journalRecord) bool {
			return before.Pending != nil && after.Pending != nil && before.Pending.SourceSnapshotDigest == "" && after.Pending.SourceSnapshotDigest != ""
		})},
		{"Tag creation intent save", transition(func(before, after *journalRecord) bool {
			return before.Pending != nil && after.Pending != nil && !before.Pending.Tag.CreateIssued && after.Pending.Tag.CreateIssued
		})},
		{"captured checkpoint save", transition(func(before, after *journalRecord) bool { return before.Checkpoint == nil && after.Checkpoint != nil })},
		{"CreateTag", func(f *sharedStepFault) { f.native.failCreateTag = 1 << 20 }},
		{"GetTag", func(f *sharedStepFault) { f.afterSuspend = func(n *nativeFixture) { n.failGetTag = 1 << 20 } }},
		{"workload absence save", transition(func(before, after *journalRecord) bool { return !before.WorkloadAbsent && after.WorkloadAbsent })},
		{"slot release save", transition(func(before, after *journalRecord) bool {
			return !before.SharedWorker.Released && after.SharedWorker.Released
		})},
		{"journal finalization save", transition(func(before, after *journalRecord) bool {
			return before.Observation.State != sdk.AllocationStopped && after.Observation.State == sdk.AllocationStopped
		})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, native, first, binding := sharedFixture(t, 3)
			second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
			a, recordA := sharedReady(t, c, native, first)
			sharedReady(t, c, native, second)
			fault := &sharedStepFault{native: native}
			tc.inject(fault)
			if native.failCreateTag > 0 {
				fault.fired.Store(true)
			}
			native.afterSuspend = func(*pb.Actor) {
				if fault.afterSuspend != nil {
					fault.fired.Store(true)
					fault.afterSuspend(native)
				}
			}
			faulty := fault.client(t, c)
			started := func() bool {
				pool, found := getPool(t, c, recordA.SharedWorker.Pool)
				return !found || pool.GetDeletionTimestamp() != nil || sharedRetireDecided(pool)
			}
			for range 24 {
				tick()
				_, _ = sharedDriver(faulty, native).SuspendInstance(t.Context(), first.Key, a.Identity)
				if started() {
					break
				}
			}
			if !fault.fired.Load() {
				t.Fatal("the fixture never injected its failure")
			}
			if !started() {
				t.Fatal("a lasting failure after the capture never started the pool's strong retirement")
			}
		})
	}
}

// sharedStepFault injects one lasting failure into a shared Stop.
type sharedStepFault struct {
	native       *nativeFixture
	afterSuspend func(*nativeFixture)
	afterAbsent  func(*nativeFixture)
	journal      func(before, after *journalRecord) bool
	failLedger   bool
	fired        atomic.Bool
}

func (f *sharedStepFault) client(t *testing.T, c client.Client) client.Client {
	t.Helper()
	absent := false
	if f.native.failSuspendBeforeEffect || f.native.failDelete {
		f.fired.Store(true)
	}
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		switch o := obj.(type) {
		case *corev1.ConfigMap:
			after, ok := decodeClaimRecord(o)
			stored := &corev1.ConfigMap{}
			if !ok || w.Get(ctx, client.ObjectKeyFromObject(o), stored) != nil {
				break
			}
			before, ok := decodeClaimRecord(stored)
			if !ok || before.SharedWorker == nil || after.SharedWorker == nil {
				break
			}
			if f.journal != nil && f.journal(before, after) {
				f.fired.Store(true)
				return apierrors.NewInternalError(errors.New("journal write refused"))
			}
			if !before.WorkloadAbsent && after.WorkloadAbsent {
				absent = true
				if f.afterAbsent != nil {
					defer func() {
						f.fired.Store(true)
						f.afterAbsent(f.native)
					}()
				}
			}
		case *unstructured.Unstructured:
			if o.GetKind() == "WorkerPool" && f.failLedger && absent {
				f.fired.Store(true)
				return apierrors.NewInternalError(errors.New("ledger write refused"))
			}
		}
		return w.Update(ctx, obj, opts...)
	}})
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
			// After the suspension intent, any lost response is doubt, so it
			// takes the strong path without replay. Before the proof none is
			// journaled; a lost DeleteActor response comes after it, and is
			// doubt too.
			proofJournaled := operation == "deleteActor"
			c, native, first, binding := sharedFixture(t, 2)
			second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
			a, _ := sharedReady(t, c, native, first)
			sharedReady(t, c, native, second)
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
			if (operation == "updateEgress") == lost || stopped.RetainedData == nil || native.suspends != 1 || native.deletes != 1 || (record.SharedWorker.Proof != nil) != proofJournaled || record.SharedWorker.Strong == "" {
				t.Fatalf("recovery replayed a mutation or lost the proof: lost=%v suspends=%d deletes=%d %#v", lost, native.suspends, native.deletes, record.SharedWorker)
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

// Inconsistent lifetime evidence is lifetime doubt, not identity doubt: the
// pool retires strongly, but its exact neighbours stay observable and are
// suspended through Orka core's eviction drain, as on the taint path, instead
// of being rejected as stale and lost.
func TestSharedInconsistentLifetimeEvidenceKeepsNeighboursObservableForEviction(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	a, recordA := sharedReady(t, c, native, first)
	b, recordB := sharedReady(t, c, native, second)
	if recordB.SharedWorker.Pool != recordA.SharedWorker.Pool {
		t.Fatal("the neighbours do not share a pool")
	}
	// The worker Pod drains and the pool waits on it, as on a live cluster.
	hold := func(key client.ObjectKey, object client.Object, finalizers []string) {
		t.Helper()
		if err := c.Get(t.Context(), key, object); err != nil {
			t.Fatal(err)
		}
		object.SetFinalizers(finalizers)
		if err := c.Update(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	podKey := client.ObjectKey{Namespace: recordA.Worker.Namespace, Name: recordA.Worker.Pod}
	poolKey := client.ObjectKey{Namespace: recordA.Placement.Namespace, Name: recordA.SharedWorker.Pool}
	hold(podKey, &corev1.Pod{}, []string{"test.invalid/hold"})
	hold(poolKey, poolObject(), []string{"test.invalid/hold"})
	// The deadline annotation moves earlier than the ledger's, with grace left.
	pool, _ := poolLedger(t, c, recordA.SharedWorker.Pool)
	annotations := maps.Clone(pool.GetAnnotations())
	annotations[sharedDeadlineAnnotation] = sharedNow().Add(2 * time.Hour).UTC().Format(time.RFC3339Nano)
	pool.SetAnnotations(annotations)
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	observe := func(when string) {
		t.Helper()
		for _, request := range []sdk.WorkloadRequest{first, second} {
			if _, err := sharedDriver(c, native).Observe(t.Context(), request.Key); err != nil {
				t.Fatalf("%s: a neighbour of a pool with inconsistent lifetime evidence was rejected: %v", when, err)
			}
		}
	}
	observe("before retirement")
	if err := (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context()); err != nil {
		t.Fatal(err)
	}
	if current, found := getPool(t, c, recordA.SharedWorker.Pool); !found || current.GetDeletionTimestamp() == nil {
		t.Fatal("a pool with inconsistent lifetime evidence was not strongly retired")
	}
	observe("while the pool retires")
	// Admission and boots still require consistent evidence.
	third := sharedWorkspace(t, c, first, "tenant", "third", "third-uid", binding)
	if _, recordC := sharedReady(t, c, native, third); recordC.SharedWorker.Pool == recordA.SharedWorker.Pool {
		t.Fatal("an allocation joined a pool with inconsistent lifetime evidence")
	}
	// Core's eviction drain suspends each neighbour through the strong path.
	tick()
	if _, err := sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity); err != nil {
		t.Fatalf("the eviction suspend of a neighbour was rejected: %v", err)
	}
	hold(podKey, &corev1.Pod{}, nil)
	hold(poolKey, poolObject(), nil)
	for _, retired := range []struct {
		request  sdk.WorkloadRequest
		identity sdk.InstanceIdentity
	}{{first, a.Identity}, {second, b.Identity}} {
		out, record := sharedRetired(t, c, native, retired.request, retired.identity, true)
		if record.SharedWorker.Strong == "" || out.RetainedData == nil || record.Failure != "" {
			t.Fatalf("a neighbour was not suspended through the strong path: strong=%q retained=%v failure=%q", record.SharedWorker.Strong, out.RetainedData, record.Failure)
		}
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

// A request deadline is never doubt by itself: it is retried. A suspend
// that outlived the request and never took effect is still doubt on the next
// pass, from the evidence (the Actor is still RUNNING after the issued
// suspend, which is never replayed), and its marker persists although the
// request is gone. An Actor read that outlives the request is only retried,
// and the suspension finishes once native answers again.
func TestSharedRequestDeadlinesAreRetriedAndEvidenceStillRetires(t *testing.T) {
	for _, tc := range []struct {
		name   string
		block  func(*nativeFixture)
		strong bool
	}{
		{"suspend outlives the request", func(native *nativeFixture) { native.blockSuspend = true }, true},
		{"actor read outlives the request", func(native *nativeFixture) { native.blockGetActor = true }, false},
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
				_, _ = sharedDriver(contextAwareClient(c), native).SuspendInstance(ctx, first.Key, a.Identity)
				cancel()
			}
			native.blockSuspend, native.blockGetActor = false, false
			tick()
			_, _ = sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity)
			_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
			if err != nil {
				t.Fatal(err)
			}
			if native.suspendCalls > 1 {
				t.Fatalf("SuspendActor was replayed: %d calls", native.suspendCalls)
			}
			if !tc.strong {
				if record.SharedWorker.Strong != "" {
					t.Fatalf("a request deadline started the strong path: %q", record.SharedWorker.Strong)
				}
				observed, _ := sharedRetired(t, c, native, first, a.Identity, true)
				if observed.RetainedData == nil {
					t.Fatal("the retried suspension retained no data")
				}
				return
			}
			if record.SharedWorker.Strong == "" || strings.Contains(record.SharedWorker.Strong, "deadline") || strings.Contains(record.SharedWorker.Strong, "Canceled") {
				t.Fatalf("an unresolved suspend did not take the strong path from its evidence: %q", record.SharedWorker.Strong)
			}
			if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
				t.Fatal("the pool's retirement did not start")
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
	shortSharedBudgets(t)
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
	shortSharedBudgets(t)
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
	shortSharedBudgets(t)
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

// deleteGoneWorker models upstream's worker-delete workflow, which runs once
// a worker Pod is gone: every Actor still bound to that worker, SUSPENDING
// included, is crashed and its node state reclaimed.
func deleteGoneWorker(t *testing.T, c client.Client, native *nativeFixture, record *journalRecord) {
	t.Helper()
	if podExists(t, c, record.Worker.Namespace, record.Worker.Pod, record.Worker.PodUID) {
		return
	}
	for key, actor := range native.actors {
		state := actor.Status.State
		if actor.Status.GetWorkerAssignment().GetWorker().GetName() != record.Worker.Name || (state != pb.ActorState_ACTOR_STATE_RUNNING && state != pb.ActorState_ACTOR_STATE_SUSPENDING) {
			continue
		}
		if native.finalizePending == key {
			native.finalizePending = ""
		}
		actor.Status.State = pb.ActorState_ACTOR_STATE_CRASHED
		actor.Metadata.Version++
		native.releaseWorker(actor)
	}
}

// A worker Pod hosting no running sandbox exits within seconds of its delete,
// whatever its grace, and upstream's worker-delete then crashes a SUSPENDING
// Actor. A suspension that failed in FinalizeSuspended therefore holds the
// strong path's deletion until its continuation finishes, then retires the
// worker strongly.
func TestSharedSuspensionDoubtHoldsTheDeletionUntilTheContinuationFinishes(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	a, recordA := sharedReady(t, c, native, first)
	native.failFinalizeOnce = true
	for range 24 {
		tick()
		observed, err := sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity)
		deleteGoneWorker(t, c, native, recordA)
		if err == nil && observed.State == sdk.AllocationStopped {
			break
		}
		if _, current, err := sharedDriver(c, native).read(t.Context(), first.Key); err == nil && current.Failure != "" {
			break
		}
	}
	_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.Failure != "" || record.Checkpoint == nil || native.tags[record.Checkpoint.Atespace+"/"+record.Checkpoint.Tag.Name] == nil || record.Observation.RetainedData == nil {
		t.Fatalf("the strong path's deletion let upstream crash the suspension before its continuation: failure=%q", record.Failure)
	}
	if record.SharedWorker.Strong == "" {
		t.Fatal("the held doubt never took the strong path")
	}
	if native.suspendCalls != 2 {
		t.Fatalf("SuspendActor calls = %d, want the original and one continuation", native.suspendCalls)
	}
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); found || podExists(t, c, recordA.Worker.Namespace, recordA.Worker.Pod, recordA.Worker.PodUID) {
		t.Fatal("the strong deletion did not follow the continuation")
	}
}

// The window ends only the hold. Once the strong path has started, the
// remaining continuations still run while the worker exists.
func TestSharedContinuationsOutliveTheHoldWindow(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	a, recordA := sharedReady(t, c, native, first)
	// The worker Pod lingers while it terminates.
	pod := &corev1.Pod{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: recordA.Worker.Namespace, Name: recordA.Worker.Pod}, pod); err != nil {
		t.Fatal(err)
	}
	pod.SetFinalizers([]string{"test.invalid/hold"})
	if err := c.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	native.failFinalizeOnce = true
	var issued time.Time
	for range 8 {
		tick()
		_, _ = sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity)
		if _, record, err := sharedDriver(c, native).read(t.Context(), first.Key); err == nil && record.Pending != nil && record.Pending.Doubt != "" {
			issued = record.Timing.SuspendIssuedAt.Time
			break
		}
	}
	if issued.IsZero() {
		t.Fatal("the failed suspend recorded no doubt")
	}
	// An Actor-read outage outlasts the window without any continuation.
	native.failGetActor = 1 << 20
	setNow(t, issued.Add(suspendContinuationWindow))
	_, _ = sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity)
	if _, record, err := sharedDriver(c, native).read(t.Context(), first.Key); err != nil || record.SharedWorker.Strong == "" {
		t.Fatalf("the window did not start the strong path: %v", err)
	}
	native.failGetActor = 0
	for range 8 {
		tick()
		_, _ = sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity)
		if _, record, err := sharedDriver(c, native).read(t.Context(), first.Key); err == nil && record.Checkpoint != nil {
			break
		}
	}
	_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if native.suspendCalls != 2 || record.Checkpoint == nil || native.tags[record.Checkpoint.Atespace+"/"+record.Checkpoint.Tag.Name] == nil {
		t.Fatalf("the remaining continuations stopped with the hold: calls=%d", native.suspendCalls)
	}
}

// A doubt write that fails only because the request's own context ended (a
// cancelled reconcile, a lost lease, a shutdown) is retried. It never starts
// the strong path, whose detached deletion would crash the SUSPENDING Actor
// mid-continuation.
func TestSharedCancelledDoubtWriteNeverStartsTheStrongPath(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	a, recordA := sharedReady(t, c, native, first)
	native.failFinalizeOnce = true
	var cancel context.CancelFunc
	cancelling := &suspensionReviewControl{nativeFixture: native, submit: func(request *pb.SuspendActorRequest) (*pb.SuspendActorResponse, error) {
		// The request is cancelled as the failed call returns.
		defer cancel()
		return native.SuspendActor(t.Context(), request)
	}}
	for range 8 {
		tick()
		ctx, cancelPass := context.WithCancel(t.Context())
		cancel = cancelPass
		d := sharedDriver(contextAwareClient(c), native)
		d.control = cancelling
		_, _ = d.SuspendInstance(ctx, first.Key, a.Identity)
		cancelPass()
		if cancelling.calls > 0 {
			break
		}
	}
	if cancelling.calls != 1 {
		t.Fatalf("the suspend was issued %d times", cancelling.calls)
	}
	_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.SharedWorker.Strong != "" || !podExists(t, c, recordA.Worker.Namespace, recordA.Worker.Pod, recordA.Worker.PodUID) {
		t.Fatalf("a cancelled doubt write started the strong path: %q", record.SharedWorker.Strong)
	}
	// Live passes continue the suspension, then retire strongly.
	for range 24 {
		tick()
		observed, err := sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity)
		deleteGoneWorker(t, c, native, recordA)
		if err == nil && observed.State == sdk.AllocationStopped {
			break
		}
	}
	if _, record, err = sharedDriver(c, native).read(t.Context(), first.Key); err != nil {
		t.Fatal(err)
	}
	if record.Failure != "" || record.Checkpoint == nil || native.tags[record.Checkpoint.Atespace+"/"+record.Checkpoint.Tag.Name] == nil {
		t.Fatalf("the suspension's data was lost: failure=%q", record.Failure)
	}
}

// cancellingTemplateControl cancels the request inside GetActorTemplate and
// fails the call as a real one would.
type cancellingTemplateControl struct {
	*nativeFixture
	cancel context.CancelFunc
}

func (c *cancellingTemplateControl) GetActorTemplate(ctx context.Context, req *pb.GetActorTemplateRequest, opts ...grpc.CallOption) (*pb.ActorTemplate, error) {
	c.cancel()
	return nil, status.FromContextError(ctx.Err()).Err()
}

// The rest of the hold path follows the same rule: once the request's own
// context has ended, a later read failing on it (here the template check of
// a held continuation pass) is retried, not doubt.
func TestSharedCancelledHoldPassNeverStartsTheStrongPath(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	a, recordA := sharedReady(t, c, native, first)
	native.failFinalizeOnce = true
	for range 8 {
		tick()
		_, _ = sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity)
		if _, record, err := sharedDriver(c, native).read(t.Context(), first.Key); err == nil && record.Pending != nil && record.Pending.Doubt != "" {
			break
		}
	}
	tick()
	ctx, cancel := context.WithCancel(t.Context())
	d := sharedDriver(c, native)
	d.control = &cancellingTemplateControl{nativeFixture: native, cancel: cancel}
	_, _ = d.SuspendInstance(ctx, first.Key, a.Identity)
	cancel()
	_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.Pending == nil || record.Pending.Doubt == "" {
		t.Fatal("the fixture recorded no held doubt")
	}
	if record.SharedWorker.Strong != "" || !podExists(t, c, recordA.Worker.Namespace, recordA.Worker.Pod, recordA.Worker.PodUID) {
		t.Fatalf("a cancelled hold pass started the strong path: %q", record.SharedWorker.Strong)
	}
}

// The held deletion is bounded: an Actor that cannot be read after a failed
// suspend takes the strong path once the continuation window ends, and not
// before.
func TestSharedHeldSuspensionDoubtIsBoundedByTheContinuationWindow(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	a, recordA := sharedReady(t, c, native, first)
	native.failFinalizeOnce = true
	var issued time.Time
	for range 8 {
		tick()
		_, _ = sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity)
		if _, record, err := sharedDriver(c, native).read(t.Context(), first.Key); err == nil && record.Pending != nil && record.Pending.Doubt != "" {
			issued = record.Timing.SuspendIssuedAt.Time
			break
		}
	}
	if issued.IsZero() {
		t.Fatal("the failed suspend recorded no doubt")
	}
	native.failGetActor = 1 << 20
	setNow(t, issued.Add(suspendContinuationWindow-time.Second))
	_, _ = sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity)
	if _, record, err := sharedDriver(c, native).read(t.Context(), first.Key); err != nil || record.SharedWorker.Strong != "" {
		t.Fatalf("the strong path started inside the continuation window: %v", err)
	}
	if !podExists(t, c, recordA.Worker.Namespace, recordA.Worker.Pod, recordA.Worker.PodUID) {
		t.Fatal("the worker Pod was deleted inside the continuation window")
	}
	setNow(t, issued.Add(suspendContinuationWindow))
	_, _ = sharedDriver(c, native).SuspendInstance(t.Context(), first.Key, a.Identity)
	if _, record, err := sharedDriver(c, native).read(t.Context(), first.Key); err != nil || record.SharedWorker.Strong == "" {
		t.Fatalf("the held doubt outlived its continuation window: %v", err)
	}
	if podExists(t, c, recordA.Worker.Namespace, recordA.Worker.Pod, recordA.Worker.PodUID) {
		t.Fatal("the strong deletion did not start when the window ended")
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
	shortSharedBudgets(t)
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

func TestSharedKnownStrongTriggersActBeforeAFailingHostedRead(t *testing.T) {
	for _, trigger := range []string{"foreign template", "worker restarted"} {
		t.Run(trigger, func(t *testing.T) {
			c, native, record, _ := sharedTemplateFixture(t)
			if trigger == "worker restarted" {
				// Undo the fixture's foreign template; restart the worker.
				native.templates[refKey(&pb.ObjectRef{Atespace: record.Atespace, Name: record.Template.Name})].Metadata.Uid = record.Template.UID
				native.workers[record.Worker.Name].Status.RegisteredEpoch++
			}
			// Every hosted workspace read fails.
			failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
				Get: func(ctx context.Context, w client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*api.ExecutionWorkspace); ok {
						return apierrors.NewServiceUnavailable("workspace reads unavailable")
					}
					return w.Get(ctx, key, obj, opts...)
				},
				List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if _, ok := list.(*api.ExecutionWorkspaceList); ok {
						return apierrors.NewServiceUnavailable("workspace reads unavailable")
					}
					return w.List(ctx, list, opts...)
				},
			})
			_ = sharedReplenisher(failing, native).Sweep(t.Context())
			if _, found := getPool(t, c, record.SharedWorker.Pool); found {
				t.Fatalf("a failing hosted read suppressed the strong retirement for a %s", trigger)
			}
		})
	}
}

func TestSharedRecordedTemplateCacheIsBounded(t *testing.T) {
	t.Run("private allocations add nothing", func(t *testing.T) {
		recordedTemplates.replace(nil, recordedTemplates.begin())
		c, native, request := fixture(t, true)
		ready(t, c, native, request)
		if entries := recordedTemplates.snapshot(); len(entries) != 0 {
			t.Fatalf("a private allocation grew the shared template record: %v", entries)
		}
	})
	t.Run("a sweep with nothing shared prunes it", func(t *testing.T) {
		c, native, _ := fixture(t, true)
		for i := range 8 {
			recordedTemplates.add("native", nativeReference{Name: fmt.Sprintf("stale-%d", i), UID: fmt.Sprintf("stale-%d-uid", i)})
		}
		if err := (&SpareWorkerReplenisher{Client: c, Control: native, Config: sharedConfig()}).Sweep(t.Context()); err != nil {
			t.Fatal(err)
		}
		if entries := recordedTemplates.snapshot(); len(entries) != 0 {
			t.Fatalf("a sweep with no shared profile or pool kept %d template entries", len(entries))
		}
	})
	t.Run("capped", func(t *testing.T) {
		cache := &recordedTemplateCache{}
		for i := range recordedTemplateCacheMax + 10 {
			cache.add("native", nativeReference{Name: fmt.Sprintf("t-%d", i), UID: fmt.Sprintf("t-%d-uid", i)})
		}
		if size := len(cache.snapshot()); size > recordedTemplateCacheMax {
			t.Fatalf("the template record holds %d entries, over its cap of %d", size, recordedTemplateCacheMax)
		}
		if cache.snapshot()["native/t-"+strconv.Itoa(recordedTemplateCacheMax+9)] == "" {
			t.Fatal("the newest entry was dropped")
		}
	})
}

func TestSharedTemplateRefreshOverTheCapCachesNothing(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	_, record := sharedReady(t, c, native, first)
	sharedReady(t, c, native, second)
	previous := recordedTemplateCacheMax
	recordedTemplateCacheMax = 1
	t.Cleanup(func() { recordedTemplateCacheMax = previous })
	var configMapLists atomic.Int64
	counting := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.ConfigMapList); ok {
				configMapLists.Add(1)
			}
			return w.List(ctx, list, opts...)
		},
	})
	d := sharedDriver(counting, native)
	poolLabels := sharedLabels(record.SharedWorker.Label)
	// A miss forces a refresh; its two shared templates exceed the cap.
	recordedTemplates.replace(nil, recordedTemplates.begin())
	foreign, err := d.foreignTemplateMatches(t.Context(), record.Atespace, poolLabels)
	if err != nil || foreign {
		t.Fatalf("the over-cap refresh changed the decision: foreign=%v err=%v", foreign, err)
	}
	if lists := configMapLists.Load(); lists == 0 {
		t.Fatal("the first check did not refresh")
	}
	if entries := recordedTemplates.snapshot(); len(entries) != 0 {
		t.Fatalf("a refresh over the cap was cached: %v", entries)
	}
	// The next check is never a hit: it rereads.
	configMapLists.Store(0)
	if foreign, err := d.foreignTemplateMatches(t.Context(), record.Atespace, poolLabels); err != nil || foreign {
		t.Fatalf("the reread changed the decision: foreign=%v err=%v", foreign, err)
	}
	if configMapLists.Load() == 0 {
		t.Fatal("a refresh that skipped caching was treated as a hit")
	}
}

func TestSharedTemplateRefreshCachesOnlySharedRelevantTemplates(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	all, shared, err := sharedDriver(c, native).providerTemplates(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	key := record.Atespace + "/" + record.Template.Name
	if shared[key] != record.Template.UID || all[key] != record.Template.UID {
		t.Fatal("the shared allocation's template is missing from the inventory")
	}
	// A private allocation's template is inventoried but never cached.
	privateC, privateNative, privateRequest := fixture(t, true)
	ready(t, privateC, privateNative, privateRequest)
	_, privateRecord, err := driver(privateC, privateNative).read(t.Context(), privateRequest.Key)
	if err != nil {
		t.Fatal(err)
	}
	privateAll, privateShared, err := driver(privateC, privateNative).providerTemplates(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	privateKey := privateRecord.Atespace + "/" + privateRecord.Template.Name
	if privateAll[privateKey] == "" || len(privateShared) != 0 {
		t.Fatalf("a private allocation's template is cached as shared-relevant: all=%v shared=%v", privateAll, privateShared)
	}
}

// deleteRecord is one Delete the provider issued.
type deleteRecord struct {
	kind  string
	uid   string
	grace *int64
}

// recordingDeletes records every WorkerPool and Pod Delete, in order, with
// its grace and UID precondition (the fake client ignores both).
func recordingDeletes(t *testing.T, c client.Client) (client.Client, func() []deleteRecord) {
	t.Helper()
	var mu sync.Mutex
	var records []deleteRecord
	wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			options := &client.DeleteOptions{}
			options.ApplyOptions(opts)
			kind := ""
			switch o := obj.(type) {
			case *corev1.Pod:
				kind = "Pod"
			case *unstructured.Unstructured:
				kind = o.GetKind()
			}
			if kind == "Pod" || kind == "WorkerPool" {
				if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != obj.GetUID() {
					t.Errorf("a %s Delete carried no UID precondition", kind)
				}
				mu.Lock()
				records = append(records, deleteRecord{kind: kind, uid: string(obj.GetUID()), grace: options.GracePeriodSeconds})
				mu.Unlock()
			}
			return w.Delete(ctx, obj, opts...)
		},
	})
	return wrapped, func() []deleteRecord {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(records)
	}
}

// assertWorkerPodFirst checks the worker Pod was deleted directly, before
// its pool, with the expected grace.
func assertWorkerPodFirst(t *testing.T, records []deleteRecord, podUID string, grace int64) {
	t.Helper()
	pod, pool := -1, -1
	for i, record := range records {
		switch {
		case record.kind == "Pod" && record.uid == podUID && pod < 0:
			pod = i
			if record.grace == nil || *record.grace != grace {
				got := "none"
				if record.grace != nil {
					got = strconv.FormatInt(*record.grace, 10)
				}
				t.Fatalf("the worker Pod's first delete carried grace %s, want %d", got, grace)
			}
		case record.kind == "WorkerPool" && pool < 0:
			pool = i
		}
	}
	if pod < 0 || pool < 0 || pod > pool {
		t.Fatalf("the worker Pod was not deleted directly before its pool: %+v", records)
	}
}

func TestSharedRetirementDeletesTheWorkerPodFirstWithABoundedGrace(t *testing.T) {
	full := int64(workerPodTerminationGrace / time.Second)
	t.Run("lifetime at its retirement time", func(t *testing.T) {
		c, native, first, _ := sharedFixture(t, 2)
		_, record := sharedReady(t, c, native, first)
		pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
		setNow(t, sharedRetireAt(pool, ledger))
		recording, deletes := recordingDeletes(t, c)
		_ = (&SpareWorkerReplenisher{Client: recording, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
		assertWorkerPodFirst(t, deletes(), record.Worker.PodUID, full)
	})
	t.Run("lifetime retired late", func(t *testing.T) {
		c, native, first, _ := sharedFixture(t, 2)
		_, record := sharedReady(t, c, native, first)
		pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
		// The provider was down past the retirement time: 20 minutes remain.
		setNow(t, sharedEscalateAt(pool, ledger).Add(-20*time.Minute))
		recording, deletes := recordingDeletes(t, c)
		_ = (&SpareWorkerReplenisher{Client: recording, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
		assertWorkerPodFirst(t, deletes(), record.Worker.PodUID, int64((20*time.Minute)/time.Second))
	})
	t.Run("outage past its bound", func(t *testing.T) {
		c, native, first, _ := sharedFixture(t, 2)
		_, record := sharedReady(t, c, native, first)
		pool, _ := poolLedger(t, c, record.SharedWorker.Pool)
		created := pool.GetCreationTimestamp().Time
		failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.ConfigMapList); ok {
				return apierrors.NewServiceUnavailable("journal inventory unavailable")
			}
			return w.List(ctx, list, opts...)
		}})
		recording, deletes := recordingDeletes(t, failing)
		bounds := &SpareWorkerReplenisher{Client: recording, Config: sharedConfig()}
		// The outage starts 10 minutes before creation plus the 3h minimum's
		// escalation time and outlasts its bound.
		start := created.Add(profile.MinSharedWorkerLifetime - sharedRetireMargin - 10*time.Minute)
		setNow(t, start)
		_ = bounds.EnforceSharedBounds(t.Context())
		setNow(t, start.Add(sharedProvenanceOutageBound))
		_ = bounds.EnforceSharedBounds(t.Context())
		assertWorkerPodFirst(t, deletes(), record.Worker.PodUID, int64((10*time.Minute-sharedProvenanceOutageBound)/time.Second))
	})
	t.Run("strong path", func(t *testing.T) {
		c, native, first, _ := sharedFixture(t, 2)
		a, record := sharedReady(t, c, native, first)
		native.failSuspendBeforeEffect = true
		recording, deletes := recordingDeletes(t, c)
		for range 4 {
			tick()
			if _, err := sharedDriver(recording, native).StopInstance(t.Context(), first.Key, a.Identity); err != nil {
				t.Fatal(err)
			}
			if native.suspendCalls == 1 {
				break
			}
		}
		assertWorkerPodFirst(t, deletes(), record.Worker.PodUID, full)
	})
	t.Run("quarantine", func(t *testing.T) {
		c, native, record, _ := sharedTemplateFixture(t)
		recording, deletes := recordingDeletes(t, c)
		_ = sharedReplenisher(recording, native).Sweep(t.Context())
		assertWorkerPodFirst(t, deletes(), record.Worker.PodUID, full)
	})
}

func TestSharedRetiredIsLoggedOncePerTransition(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	// The pool is kept deleting (as a foreground cascade would) so later
	// passes see it in that state.
	pool, _ := poolLedger(t, c, record.SharedWorker.Pool)
	pool.SetFinalizers([]string{"foregroundDeletion"})
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	ctx := ctrl.LoggerInto(t.Context(), zap.New(zap.WriteTo(logs), zap.UseDevMode(false)))
	d := sharedDriver(c, native)
	for range 5 {
		current, found := getPool(t, c, record.SharedWorker.Pool)
		if !found {
			t.Fatal("the pool vanished")
		}
		if err := d.hardRetireSharedPool(ctx, current, sharedRetiring, "repeated"); err != nil {
			t.Fatal(err)
		}
	}
	if count := strings.Count(logs.String(), "Substrate shared worker retired"); count != 1 {
		t.Fatalf("the retirement was logged %d times, want once", count)
	}
}

func TestSharedFailingPoolDeleteNeverStopsTheSuspensionContinuation(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	a, recordA := sharedReady(t, c, native, first)
	sharedReady(t, c, native, second)
	// Pod deletes succeed; every WorkerPool delete fails.
	var podDeletes atomic.Int64
	failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			switch o := obj.(type) {
			case *unstructured.Unstructured:
				if o.GetKind() == "WorkerPool" {
					return apierrors.NewInternalError(errors.New("pool delete refused"))
				}
			case *corev1.Pod:
				podDeletes.Add(1)
				// The Pod is terminating under its bounded grace, not gone.
				return nil
			}
			return w.Delete(ctx, obj, opts...)
		},
	})
	// FinalizeSuspended fails, leaving the Actor SUSPENDING; the strong path
	// starts.
	native.failFinalizeOnce = true
	for range 24 {
		tick()
		if _, record, err := sharedDriver(c, native).read(t.Context(), first.Key); err == nil && record.Checkpoint != nil {
			break
		}
		_, _ = sharedDriver(failing, native).SuspendInstance(t.Context(), first.Key, a.Identity)
	}
	_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.SharedWorker.Strong == "" || podDeletes.Load() == 0 {
		t.Fatal("the strong path did not delete the worker Pod first")
	}
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); !found {
		t.Fatal("the fixture's pool delete did not fail")
	}
	if record.Checkpoint == nil || native.tags[record.Checkpoint.Atespace+"/"+record.Checkpoint.Tag.Name] == nil {
		t.Fatal("a failing pool delete stopped the suspension continuation from capturing the Tag")
	}
	if native.suspendCalls != 2 {
		t.Fatalf("SuspendActor calls = %d, want the original and one continuation", native.suspendCalls)
	}
	if pool, _ := getPool(t, c, recordA.SharedWorker.Pool); !sharedRetireIntended(pool) {
		t.Fatal("the failed pool delete left no retirement intent to retry")
	}
}

// countingJournalLists counts provider journal Lists, the items they return
// and the journal bytes those items carry.
func countingJournalLists(c client.Client) (client.Client, func() (int64, int64, int64)) {
	var lists, items, bytes atomic.Int64
	wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if err := w.List(ctx, list, opts...); err != nil {
				return err
			}
			if cms, ok := list.(*corev1.ConfigMapList); ok {
				lists.Add(1)
				for i := range cms.Items {
					items.Add(1)
					bytes.Add(int64(len(cms.Items[i].Data[journalDataKey])))
				}
			}
			return nil
		},
	})
	return wrapped, func() (int64, int64, int64) {
		defer lists.Store(0)
		defer items.Store(0)
		defer bytes.Store(0)
		return lists.Load(), items.Load(), bytes.Load()
	}
}

func TestSharedInventoryListsOnlyLabelledRecordsOnAPrivateInstall(t *testing.T) {
	const journals = 20
	c, native, request := fixture(t, true)
	useTestClock(t)
	ready(t, c, native, request)
	// N private journals and archives.
	cm, _, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	for i := range journals - 1 {
		copied := cm.DeepCopy()
		copied.Name = fmt.Sprintf("substrate-history-private-%02d", i)
		copied.ResourceVersion, copied.UID = "", ""
		if err := c.Create(t.Context(), copied); err != nil {
			t.Fatal(err)
		}
	}
	counting, counts := countingJournalLists(c)
	bounds := &SpareWorkerReplenisher{Client: counting, Config: sharedConfig()}
	_ = bounds.EnforceSharedBounds(t.Context())
	firstLists, firstItems, firstBytes := counts()
	_ = bounds.EnforceSharedBounds(t.Context())
	lists, items, bytes := counts()
	t.Logf("private-only, N=%d journals: first pass %d List(s), %d records, %d bytes decoded; later passes %d List(s), %d records, %d bytes", journals, firstLists, firstItems, firstBytes, lists, items, bytes)
	// The confirming scan sees every labelled provider record (the journals
	// plus the allocation's native anchor).
	if firstItems < journals {
		t.Fatalf("the confirming scan saw %d records, want at least %d", firstItems, journals)
	}
	if lists != 1 || items != 0 || bytes != 0 {
		t.Fatalf("a private-only install still decodes its history every pass: %d records, %d bytes", items, bytes)
	}
}

func TestSharedUnlabelledLegacyRecordIsLabelledAndStillFound(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	cm, _, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if cm.Labels[sharedRecordLabel] != "true" {
		t.Fatal("a shared placement's save did not label its journal")
	}
	// A record written before the label existed.
	delete(cm.Labels, sharedRecordLabel)
	if err := c.Update(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	sharedRecordsLabelled.Store(false)
	d := sharedDriver(c, native).withSharedPass()
	// Discovery counts it as shared, with the full scan, and never labels it.
	provenance, err := d.sharedJournalProvenance(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !provenance[types.UID(record.SharedWorker.UID)].found || sharedRecordsLabelled.Load() {
		t.Fatal("discovery lost the unlabelled legacy record or stopped the full scan")
	}
	// The separate backfill labels it; discovery then lists labelled records.
	if err := (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedLabelBackfill(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !sharedRecordsLabelled.Load() {
		t.Fatal("the backfill did not confirm the labels")
	}
	if provenance, err := d.sharedJournalProvenance(t.Context()); err != nil || !provenance[types.UID(record.SharedWorker.UID)].found {
		t.Fatalf("the labelled record was lost: %v", err)
	}
	labelled, _, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if labelled.Labels[sharedRecordLabel] != "true" {
		t.Fatal("the legacy record was not labelled")
	}
}

func TestSharedStalledRelabelNeverHoldsUpEnforcement(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	cm, _, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	// Many legacy shared records, every label write stalling.
	const legacy = 100
	delete(cm.Labels, sharedRecordLabel)
	if err := c.Update(t.Context(), cm.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	for i := range legacy {
		copied := cm.DeepCopy()
		copied.Name = fmt.Sprintf("substrate-history-legacy-%03d", i)
		copied.ResourceVersion, copied.UID = "", ""
		if err := c.Create(t.Context(), copied); err != nil {
			t.Fatal(err)
		}
	}
	var writes atomic.Int64
	stalling := contextAwareClient(interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if cm, ok := obj.(*corev1.ConfigMap); ok && cm.Labels[sharedRecordLabel] == "true" {
				writes.Add(1)
				<-ctx.Done()
				return ctx.Err()
			}
			return w.Update(ctx, obj, opts...)
		},
	}))
	sharedRecordsLabelled.Store(false)
	pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
	setNow(t, sharedEscalateAt(pool, ledger).Add(time.Minute))
	r := &SpareWorkerReplenisher{Client: stalling, Config: sharedConfig()}
	started := time.Now()
	_ = r.EnforceSharedEscalation(t.Context())
	_ = r.EnforceSharedBounds(t.Context())
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("discovery and enforcement took %s with stalled relabel writes", elapsed)
	}
	if writes.Load() != 0 {
		t.Fatalf("enforcement issued %d relabel writes", writes.Load())
	}
	if podExists(t, c, record.Worker.Namespace, record.Worker.Pod, record.Worker.PodUID) {
		t.Fatal("escalation did not issue while legacy records were unlabelled")
	}
	// The backfill pass itself is bounded, and stays incomplete.
	previous := sharedLabelBackfillTimeout
	sharedLabelBackfillTimeout = 300 * time.Millisecond
	t.Cleanup(func() { sharedLabelBackfillTimeout = previous })
	started = time.Now()
	if err := r.EnforceSharedLabelBackfill(t.Context()); err == nil {
		t.Fatal("a stalled backfill reported success")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("the backfill pass took %s past its bound", elapsed)
	}
	if sharedRecordsLabelled.Load() {
		t.Fatal("an incomplete backfill switched discovery to labelled records")
	}
}

func TestSharedFailingPoolDeleteBeforeTheSuspendStillKeepsTheData(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	a, recordA := sharedReady(t, c, native, first)
	sharedReady(t, c, native, second)
	// Pod deletes succeed (the Pod terminates); every WorkerPool delete fails.
	var podDeletes atomic.Int64
	failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			switch o := obj.(type) {
			case *unstructured.Unstructured:
				if o.GetKind() == "WorkerPool" {
					return apierrors.NewInternalError(errors.New("pool delete refused"))
				}
			case *corev1.Pod:
				podDeletes.Add(1)
				return nil
			}
			return w.Delete(ctx, obj, opts...)
		},
	})
	// Doubt before the suspend: egress revocation fails.
	native.failEgressUpdate = 1
	for range 24 {
		tick()
		if _, record, err := sharedDriver(c, native).read(t.Context(), first.Key); err == nil && record.Checkpoint != nil {
			break
		}
		_, _ = sharedDriver(failing, native).SuspendInstance(t.Context(), first.Key, a.Identity)
	}
	_, record, err := sharedDriver(c, native).read(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.SharedWorker.Strong == "" || podDeletes.Load() == 0 {
		t.Fatal("the pre-suspend doubt did not take the strong path and delete the worker Pod")
	}
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); !found {
		t.Fatal("the fixture's pool delete did not fail")
	}
	if native.suspendCalls == 0 {
		t.Fatal("a failing pool delete before the suspend kept the data-keeping suspend from being issued")
	}
	if record.Checkpoint == nil || native.tags[record.Checkpoint.Atespace+"/"+record.Checkpoint.Tag.Name] == nil {
		t.Fatal("a failing pool delete before the suspend lost the Actor's Tag")
	}
}

// failingCatalogLists fails every checkpoint-catalog List, so the provider's
// recorded-template inventory is unavailable while journal Lists still work.
func failingCatalogLists(c client.Client) client.Client {
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.ConfigMapList); ok {
				options := &client.ListOptions{}
				options.ApplyOptions(opts)
				if options.LabelSelector != nil && strings.Contains(options.LabelSelector.String(), catalogLabel) {
					return apierrors.NewServiceUnavailable("checkpoint catalogs unavailable")
				}
			}
			return w.List(ctx, list, opts...)
		},
	})
}

func TestSharedUnclassifiableTemplateClosesAdmissionThenRetiresPastTheOutageBound(t *testing.T) {
	c, native, record, poolLabels := sharedTemplateFixture(t)
	// The derived template now carries an unrecorded UID, and the in-process
	// record cannot vouch for it.
	recordedTemplates.replace(nil, recordedTemplates.begin())
	failing := failingCatalogLists(c)
	// Admission closes at once.
	if foreign, err := sharedDriver(failing, native).foreignTemplateMatches(t.Context(), record.Atespace, poolLabels); err == nil && !foreign {
		t.Fatal("admission accepted an unclassifiable template while the inventory was unavailable")
	}
	start := sharedNow()
	_ = sharedReplenisher(failing, native).Sweep(t.Context())
	pool, found := getPool(t, c, record.SharedWorker.Pool)
	if !found || pool.GetDeletionTimestamp() != nil {
		t.Fatal("an unclassifiable template retired the pool before the outage bound")
	}
	if _, ledger := poolLedger(t, c, record.SharedWorker.Pool); ledger.State != sharedDraining {
		t.Fatalf("the pool kept admitting with an unclassifiable template: %s", ledger.State)
	}
	// Still unavailable past the bound: strongly retired.
	setNow(t, start.Add(sharedProvenanceOutageBound))
	_ = sharedReplenisher(failing, native).Sweep(t.Context())
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("an unclassifiable template outlasting the inventory outage bound did not retire the pool")
	}
}

func TestSharedRecordedTemplateStaysFineWhileTheInventoryIsUnavailable(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	failing := failingCatalogLists(c)
	start := sharedNow()
	for _, at := range []time.Duration{0, sharedProvenanceOutageBound} {
		setNow(t, start.Add(at))
		_ = sharedReplenisher(failing, native).Sweep(t.Context())
	}
	pool, found := getPool(t, c, record.SharedWorker.Pool)
	if !found || pool.GetDeletionTimestamp() != nil {
		t.Fatal("an inventory outage retired a pool whose templates the in-process record knows")
	}
	if _, ledger := poolLedger(t, c, record.SharedWorker.Pool); ledger.State != sharedAdmitting {
		t.Fatalf("an inventory outage closed a pool whose templates are known: %s", ledger.State)
	}
}

func TestSharedForeignTemplateOutsideTheAtespaceIsQuarantinedWithoutTheInventory(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	// A template in another Atespace with an empty selector reaches the pool.
	native.templates["other/foreign"] = &pb.ActorTemplate{Metadata: &pb.ResourceMetadata{Atespace: "other", Name: "foreign", Uid: "foreign-uid", Version: 1}}
	_ = sharedReplenisher(failingCatalogLists(c), native).Sweep(t.Context())
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("a foreign template outside the Atespace was not quarantined while the inventory was unavailable")
	}
}

func TestSharedNativeTemplateInventoryOutageRetiresPastItsBound(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(*nativeFixture)
		// bound is when the pool must be retired, and not before.
		bound time.Duration
	}{
		{name: "template listing fails while the control plane places", break_: func(f *nativeFixture) { f.failTemplateList = true }, bound: sharedProvenanceOutageBound},
		{name: "template listing stalls past the sweep deadline", break_: func(f *nativeFixture) { f.blockTemplateList = true }, bound: sharedProvenanceOutageBound},
		{name: "the provider's own credentials are rejected", break_: func(f *nativeFixture) { f.credentialsRejected = true }, bound: sharedProvenanceOutageBound},
		{name: "the control plane looks down from the provider", break_: func(f *nativeFixture) { f.nativeDown = true }, bound: sharedControlPlaneDownGrace},
		// A rejection of the provider's own credentials never takes the
		// control-plane-down grace, whatever the worker probe returns.
		{name: "template listing rejects credentials while the worker probe looks down", break_: func(f *nativeFixture) { f.rejectTemplateList, f.nativeDown = true, true }, bound: sharedProvenanceOutageBound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, native, first, _ := sharedFixture(t, 2)
			_, record := sharedReady(t, c, native, first)
			tc.break_(native)
			sweep := sharedReplenisher(c, native)
			sweep.SweepDeadline = 300 * time.Millisecond
			// Admission closes at once (under its request's own deadline).
			admitCtx, cancelAdmit := context.WithTimeout(t.Context(), 300*time.Millisecond)
			err := sharedDriver(c, native).refuseForeignTemplates(admitCtx, record.Atespace, sharedLabels(record.SharedWorker.Label), nil)
			cancelAdmit()
			if err == nil {
				t.Fatal("admission stayed open without a native template inventory")
			}
			start := sharedNow()
			for _, at := range []time.Duration{0, sharedProvenanceOutageBound, tc.bound - time.Second} {
				if at >= tc.bound {
					continue
				}
				setNow(t, start.Add(at))
				_ = sweep.Sweep(t.Context())
				if pool, found := getPool(t, c, record.SharedWorker.Pool); !found || pool.GetDeletionTimestamp() != nil {
					t.Fatalf("the pool was retired at %s, before its %s bound", at, tc.bound)
				}
			}
			setNow(t, start.Add(tc.bound))
			_ = sweep.Sweep(t.Context())
			if _, found := getPool(t, c, record.SharedWorker.Pool); found {
				t.Fatalf("a native template inventory outage did not retire the pool at %s", tc.bound)
			}
		})
	}
}

// racingTemplateControl runs during inside each ActorTemplate List.
type racingTemplateControl struct {
	*nativeFixture
	during func()
}

func (c *racingTemplateControl) ListActorTemplates(ctx context.Context, req *pb.ListActorTemplatesRequest, opts ...grpc.CallOption) (*pb.ListActorTemplatesResponse, error) {
	c.during()
	return c.nativeFixture.ListActorTemplates(ctx, req, opts...)
}

// A sweep's inventory that succeeds after a newer one failed does not clear
// the pool's durable outage stamp: the tracked outage is still on.
func TestSharedStaleInventorySuccessKeepsTheNativeOutageStamp(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	pool, _ := poolLedger(t, c, record.SharedWorker.Pool)
	stamped := sharedNow().Add(-30 * time.Second).UTC().Format(time.RFC3339Nano)
	annotations := maps.Clone(pool.GetAnnotations())
	annotations[sharedNativeOutageAnnotation] = stamped
	pool.SetAnnotations(annotations)
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	sharedNativeTemplateHealth.failed(sharedNow().Add(-30 * time.Second))
	racing := &racingTemplateControl{nativeFixture: native, during: func() {
		// An admission List starts after this one and fails first.
		testClock.Add(int64(time.Second))
		sharedNativeTemplateHealth.failed(sharedNow())
	}}
	_ = (&SpareWorkerReplenisher{Client: c, Control: racing, Config: sharedConfig()}).Sweep(t.Context())
	if !sharedNativeTemplateHealth.failing() {
		t.Fatal("the fixture's newer failure did not keep the tracked outage")
	}
	if current, found := getPool(t, c, record.SharedWorker.Pool); !found || current.GetAnnotations()[sharedNativeOutageAnnotation] != stamped {
		t.Fatal("a stale inventory success cleared the native outage stamp")
	}
	// A success that starts after the newer failure clears the tracked
	// outage, and with it the stamp.
	testClock.Add(int64(time.Second))
	_ = sharedReplenisher(c, native).Sweep(t.Context())
	if current, found := getPool(t, c, record.SharedWorker.Pool); !found || current.GetAnnotations()[sharedNativeOutageAnnotation] != "" {
		t.Fatal("a success that cleared the outage left its stamp")
	}
}

// An inventory a newer failed read superseded never authorizes placement:
// admission and Resume refuse it, as if the newer read's failure had been
// theirs.
func TestSharedSupersededTemplateInventoryNeverAuthorizesPlacement(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	racing := &racingTemplateControl{nativeFixture: native, during: func() {
		// A newer List (for example the sweep's, after the provider's
		// template viewer permission was revoked) starts and fails first.
		testClock.Add(int64(time.Second))
		sharedNativeTemplateHealth.failed(sharedNow())
	}}
	d := sharedDriver(c, native)
	d.control = racing
	if err := d.refuseForeignTemplates(t.Context(), record.Atespace, sharedLabels(record.SharedWorker.Label), nil); err == nil {
		t.Fatal("a superseded template inventory authorized placement")
	}
	// A later inventory that clears the outage authorizes it again.
	testClock.Add(int64(time.Second))
	if err := sharedDriver(c, native).refuseForeignTemplates(t.Context(), record.Atespace, sharedLabels(record.SharedWorker.Label), nil); err != nil {
		t.Fatalf("a fresh inventory did not authorize placement: %v", err)
	}
}

// The native inventory outage's classification is stamped with it, so the
// Kubernetes-only bounds loop enforces the matching bound on its own, however
// long a sweep pass takes: 60 s unless the control plane looked down from the
// provider, and never the grace for a credential rejection.
func TestSharedBoundsLoopEnforcesTheClassifiedNativeOutageBound(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(*nativeFixture)
		bound  time.Duration
		class  string
	}{
		{"template listing fails while worker reads succeed", func(f *nativeFixture) { f.failTemplateList = true }, sharedProvenanceOutageBound, sharedUnverifiedGraceNone},
		{"template listing rejects credentials while the worker probe looks down", func(f *nativeFixture) { f.rejectTemplateList, f.nativeDown = true, true }, sharedProvenanceOutageBound, sharedUnverifiedGraceNone},
		{"the control plane looks down from the provider", func(f *nativeFixture) { f.nativeDown = true }, sharedControlPlaneDownGrace, sharedUnverifiedGraceDown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, native, first, _ := sharedFixture(t, 2)
			_, record := sharedReady(t, c, native, first)
			tc.break_(native)
			start := sharedNow()
			// One sweep pass stamps and classifies the outage; later passes
			// are stuck, so only the bounds loop runs.
			_ = sharedReplenisher(c, native).Sweep(t.Context())
			pool, found := getPool(t, c, record.SharedWorker.Pool)
			if !found || pool.GetAnnotations()[sharedNativeOutageAnnotation] == "" || pool.GetAnnotations()[sharedNativeOutageGraceAnnotation] != tc.class {
				t.Fatalf("the outage was not stamped and classified %q: %v", tc.class, pool.GetAnnotations())
			}
			bounds := &SpareWorkerReplenisher{Client: c, Config: sharedConfig()}
			setNow(t, start.Add(tc.bound-time.Second))
			_ = bounds.EnforceSharedBounds(t.Context())
			if pool, found := getPool(t, c, record.SharedWorker.Pool); !found || pool.GetDeletionTimestamp() != nil {
				t.Fatalf("the bounds loop retired the pool before its %s bound", tc.bound)
			}
			setNow(t, start.Add(tc.bound))
			_ = bounds.EnforceSharedBounds(t.Context())
			if _, found := getPool(t, c, record.SharedWorker.Pool); found {
				t.Fatalf("the bounds loop alone did not retire the pool at its %s bound", tc.bound)
			}
		})
	}
}

// The in-process per-pool trackers are pruned only from a complete,
// successful List of all WorkerPools: a deleted pool's entries are dropped, a
// failed List drops nothing, and a live pool's decision is kept, so its
// in-flight writes are still outranked.
func TestSharedPoolTrackersArePrunedOnlyFromACompleteList(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	pool, _ := poolLedger(t, c, record.SharedWorker.Pool)
	live, gone := pool.GetUID(), types.UID("deleted-pool-uid")
	for _, uid := range []types.UID{live, gone} {
		sharedDecidedStamps.record(uid, sharedUnverifiedAnnotation, decidedClear)
		sharedUnverifiedPools.failed(uid, sharedNow())
	}
	// A failed List prunes nothing.
	failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if u, ok := list.(*unstructured.UnstructuredList); ok && u.GetKind() == "WorkerPoolList" {
			return apierrors.NewInternalError(errors.New("list refused"))
		}
		return w.List(ctx, list, opts...)
	}})
	_ = (&SpareWorkerReplenisher{Client: failing, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
	if sharedDecidedStamps.last(gone, sharedUnverifiedAnnotation) == "" || !sharedUnverifiedPools.pending(gone) {
		t.Fatal("a failed WorkerPool List pruned a pool's trackers")
	}
	// A complete List drops the deleted pool's entries and keeps the live
	// pool's.
	_ = (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
	if sharedDecidedStamps.last(gone, sharedUnverifiedAnnotation) != "" || sharedUnverifiedPools.pending(gone) {
		t.Fatal("a deleted pool's trackers outlived a complete List")
	}
	if sharedDecidedStamps.last(live, sharedUnverifiedAnnotation) != decidedClear || !sharedUnverifiedPools.pending(live) {
		t.Fatal("a complete List pruned a live pool's trackers")
	}
	// A decision recorded while the List runs, for a pool created after its
	// snapshot, is kept.
	created := types.UID("created-during-list-uid")
	racing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		err := w.List(ctx, list, opts...)
		if u, ok := list.(*unstructured.UnstructuredList); ok && u.GetKind() == "WorkerPoolList" && sharedDecidedStamps.last(created, sharedUnverifiedAnnotation) == "" {
			// A pool is created after this List's snapshot, and a decision
			// is recorded for it before the prune runs; later Lists see it.
			pool := poolObject()
			pool.SetNamespace(record.Placement.Namespace)
			pool.SetName("created-during-list")
			pool.SetUID(created)
			if err := w.Create(ctx, pool); err != nil {
				return err
			}
			sharedDecidedStamps.record(created, sharedUnverifiedAnnotation, decidedSet)
			sharedUnverifiedPools.failed(created, sharedNow())
		}
		return err
	}})
	_ = (&SpareWorkerReplenisher{Client: racing, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
	if sharedDecidedStamps.last(created, sharedUnverifiedAnnotation) != decidedSet || !sharedUnverifiedPools.pending(created) {
		t.Fatal("a prune dropped a decision recorded while its List ran")
	}
	// The live pool's newer decision still outranks a stale write.
	d := sharedDriver(c, native)
	decided := sharedNow()
	d.writeOutageMarks(t.Context(), []outageMark{{namespace: pool.GetNamespace(), name: pool.GetName(), uid: live, key: sharedNativeOutageAnnotation, value: decided.UTC().Format(time.RFC3339Nano), decidedAt: decided}})
	_ = (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
	d.writeOutageMarks(t.Context(), []outageMark{{namespace: pool.GetNamespace(), name: pool.GetName(), uid: live, key: sharedNativeOutageAnnotation, decidedAt: decided.Add(-time.Second)}})
	if current, found := getPool(t, c, record.SharedWorker.Pool); !found || current.GetAnnotations()[sharedNativeOutageAnnotation] == "" {
		t.Fatal("a stale write undid a live pool's newer decision after a prune")
	}
}

func TestSharedNativeOutageClockSurvivesProviderRestarts(t *testing.T) {
	for _, down := range []bool{false, true} {
		t.Run(fmt.Sprintf("controlPlaneDown=%t", down), func(t *testing.T) {
			c, native, first, _ := sharedFixture(t, 2)
			_, record := sharedReady(t, c, native, first)
			if down {
				native.nativeDown = true
			} else {
				native.failTemplateList = true
			}
			bound := sharedProvenanceOutageBound
			if down {
				bound = sharedControlPlaneDownGrace
			}
			start := sharedNow()
			// A provider that restarts before every sweep keeps no memory.
			for at := time.Duration(0); at < bound; at += 20 * time.Second {
				setNow(t, start.Add(at))
				sharedNativeTemplateHealth.reset()
				_ = sharedReplenisher(c, native).Sweep(t.Context())
			}
			setNow(t, start.Add(bound))
			sharedNativeTemplateHealth.reset()
			_ = sharedReplenisher(c, native).Sweep(t.Context())
			if _, found := getPool(t, c, record.SharedWorker.Pool); found {
				t.Fatal("restarts reset the native outage clock")
			}
		})
	}
}

func TestSharedStalledSpareMaintenanceNeverStarvesSharedSafety(t *testing.T) {
	c, native, record, _ := sharedTemplateFixture(t)
	// The private spare pool lookup stalls until its context ends.
	stalling := contextAwareClient(interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			options := &client.ListOptions{}
			options.ApplyOptions(opts)
			if u, ok := list.(*unstructured.UnstructuredList); ok && u.GetKind() == "WorkerPoolList" && options.LabelSelector != nil && strings.Contains(options.LabelSelector.String(), spareProfileLabel) {
				<-ctx.Done()
				return ctx.Err()
			}
			return w.List(ctx, list, opts...)
		},
	}))
	sweep := sharedReplenisher(stalling, native)
	sweep.SweepDeadline = 300 * time.Millisecond
	_ = sweep.Sweep(t.Context())
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("stalled spare maintenance kept the foreign-template quarantine from running")
	}
}

func TestSharedOutsideAtespaceTemplateIsForeignBeforeAnyReread(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	// A foreign template in another Atespace reaches the pool, and the
	// record does not know the pool's own same-Atespace template, so a
	// reread would be needed for it; the reread fails.
	native.templates["other/foreign"] = &pb.ActorTemplate{Metadata: &pb.ResourceMetadata{Atespace: "other", Name: "foreign", Uid: "foreign-uid", Version: 1}}
	recordedTemplates.replace(nil, recordedTemplates.begin())
	pool, _ := poolLedger(t, c, record.SharedWorker.Pool)
	err := sharedDriver(failingCatalogLists(c), native).refuseForeignTemplates(t.Context(), record.Atespace, sharedLabels(record.SharedWorker.Label), pool)
	if !errors.Is(err, errForeignTemplate) {
		t.Fatalf("the outside-Atespace foreign template waited on a failing reread: %v", err)
	}
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("the pool reached by a foreign template was not retired at once")
	}
}

func TestSharedRejectedNativeCredentialsAreSurfacedOnTheProvider(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	sharedReady(t, c, native, first)
	native.credentialsRejected = true
	_ = sharedReplenisher(c, native).Sweep(t.Context())
	if rejected := NativeCredentialsRejected(); !strings.Contains(rejected, "credentials rejected") || !strings.Contains(rejected, "PermissionDenied") {
		t.Fatalf("a credential rejection was not recorded: %q", rejected)
	}
	provider := &api.ExecutionWorkspaceProvider{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: "substrate"}, provider); err != nil {
		t.Fatal(err)
	}
	r := &ProviderReconciler{Client: c}
	for range 2 {
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
			t.Fatal(err)
		}
	}
	events := &corev1.EventList{}
	if err := c.List(t.Context(), events); err != nil {
		t.Fatal(err)
	}
	found := slices.ContainsFunc(events.Items, func(event corev1.Event) bool {
		return event.Reason == nativeCredentialsEventReason && event.Type == corev1.EventTypeWarning && event.InvolvedObject.UID == provider.UID && strings.Contains(event.Message, "native credentials rejected")
	})
	if !found {
		t.Fatalf("the provider did not surface the rejected native credentials: %+v", events.Items)
	}
	// A successful native read clears it.
	native.credentialsRejected = false
	_ = sharedReplenisher(c, native).Sweep(t.Context())
	if rejected := NativeCredentialsRejected(); rejected != "" {
		t.Fatalf("a successful native read kept the rejection: %q", rejected)
	}
}

// A provider recreated under the same name gets its own credential Warning
// Event; the deleted provider's Event is never patched in its place.
func TestSharedCredentialWarningFollowsARecreatedProvider(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	sharedReady(t, c, native, first)
	native.credentialsRejected = true
	_ = sharedReplenisher(c, native).Sweep(t.Context())
	r := &ProviderReconciler{Client: c}
	reconcile := func() *api.ExecutionWorkspaceProvider {
		t.Helper()
		provider := &api.ExecutionWorkspaceProvider{}
		if err := c.Get(t.Context(), types.NamespacedName{Name: "substrate"}, provider); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
			t.Fatal(err)
		}
		return provider
	}
	old := reconcile()
	recreated := old.DeepCopy()
	if err := c.Delete(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	recreated.ResourceVersion, recreated.UID = "", "recreated-provider-uid"
	recreated.Status = api.ExecutionWorkspaceProviderStatus{}
	if err := c.Create(t.Context(), recreated); err != nil {
		t.Fatal(err)
	}
	current := reconcile()
	events := &corev1.EventList{}
	if err := c.List(t.Context(), events); err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(events.Items, func(event corev1.Event) bool {
		return event.Reason == nativeCredentialsEventReason && event.InvolvedObject.UID == current.UID
	}) {
		t.Fatalf("the recreated provider's credential warning was attributed to the deleted one: %+v", events.Items)
	}
	// The Event name does not grow with the provider's name, so it stays
	// within Kubernetes' 253-character limit for any valid provider name.
	for _, event := range events.Items {
		if event.Reason == nativeCredentialsEventReason && event.InvolvedObject.UID == current.UID && strings.Contains(event.Name, current.Name) {
			t.Fatalf("the credential Event name grows with the provider's name: %q", event.Name)
		}
	}
}

// A strong retirement whose Delete and retirement intent both fail (every
// Kubernetes write failing) still closes admission in this process, and the
// bounds loop keeps retrying it until a write lands.
func TestSharedUndurableRetirementStillClosesAdmission(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 3)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	a, recordA := sharedReady(t, c, native, first)
	sharedReady(t, c, native, second)
	native.failDelete = true
	var broken atomic.Bool
	refused := apierrors.NewInternalError(errors.New("kubernetes write refused"))
	failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if broken.Load() {
				return refused
			}
			if err := w.Update(ctx, obj, opts...); err != nil {
				return err
			}
			if o, ok := obj.(*corev1.ConfigMap); ok {
				if after, ok := decodeClaimRecord(o); ok && after.DeleteIssued {
					// From here on every Kubernetes write fails.
					broken.Store(true)
				}
			}
			return nil
		},
		Delete: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if broken.Load() {
				return refused
			}
			return w.Delete(ctx, obj, opts...)
		},
	})
	for range 24 {
		tick()
		_, _ = sharedDriver(failing, native).StopInstance(t.Context(), first.Key, a.Identity)
		if broken.Load() {
			break
		}
	}
	if !broken.Load() {
		t.Fatal("the fixture never reached the Actor's deletion")
	}
	pool, found := getPool(t, c, recordA.SharedWorker.Pool)
	if !found || pool.GetDeletionTimestamp() != nil || sharedRetireIntended(pool) {
		t.Fatal("the fixture let a Kubernetes write land")
	}
	third := sharedWorkspace(t, c, first, "tenant", "third", "third-uid", binding)
	if _, recordC := sharedReady(t, c, native, third); recordC.SharedWorker.Pool == recordA.SharedWorker.Pool {
		t.Fatal("a pool whose strong retirement could not be written kept admitting")
	}
	// Once writes land again, the bounds loop retires the pool.
	if err := (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context()); err != nil {
		t.Fatal(err)
	}
	if current, found := getPool(t, c, recordA.SharedWorker.Pool); found && current.GetDeletionTimestamp() == nil {
		t.Fatal("the bounds loop did not retry the undurable retirement")
	}
}

func TestSharedRejectedAssignmentReadsFailClosedPastTheBound(t *testing.T) {
	for _, tc := range []struct {
		name    string
		break_  func(*nativeFixture)
		retired bool
	}{
		{name: "assignment reads rejected for the provider's credentials", break_: func(f *nativeFixture) { f.assignmentsRejected = true }, retired: true},
		// A check that never completes is an outage of that check: with the
		// control plane otherwise answering, it retires at the same bound.
		{name: "assignment reads unavailable", break_: func(f *nativeFixture) { f.assignmentsUnavailable = true }, retired: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, native, first, _ := sharedFixture(t, 2)
			_, record := sharedReady(t, c, native, first)
			tc.break_(native)
			start := sharedNow()
			_ = sharedReplenisher(c, native).Sweep(t.Context())
			if pool, found := getPool(t, c, record.SharedWorker.Pool); !found || pool.GetDeletionTimestamp() != nil {
				t.Fatal("a failed assignment read retired the pool before the bound")
			}
			setNow(t, start.Add(sharedProvenanceOutageBound))
			_ = sharedReplenisher(c, native).Sweep(t.Context())
			_, found := getPool(t, c, record.SharedWorker.Pool)
			if tc.retired && found {
				t.Fatal("a credential rejection of the assignment checks past its bound did not fail closed")
			}
			if !tc.retired && !found {
				t.Fatal("an unavailable assignment read retired the pool")
			}
		})
	}
}

func TestSharedPartialAssignmentSuccessNeverResetsTheRejectionBound(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	// An assignment the ledger does not hold forces the foreign-assignment
	// confirmation; the first List succeeds and every confirmation is
	// rejected for the provider's credentials.
	native.lingering = map[string]*pb.ActorAssignment{record.Worker.Name + "|stranger": {Actor: &pb.ObjectRef{Atespace: record.Atespace, Name: "stranger"}, ActorUid: "stranger-uid"}}
	native.rejectEverySecondAssignmentList = true
	start := sharedNow()
	_ = sharedReplenisher(c, native).Sweep(t.Context())
	if pool, found := getPool(t, c, record.SharedWorker.Pool); !found || pool.GetDeletionTimestamp() != nil {
		t.Fatal("a rejected confirmation retired the pool before the bound")
	}
	setNow(t, start.Add(sharedProvenanceOutageBound))
	_ = sharedReplenisher(c, native).Sweep(t.Context())
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("a successful first assignment List reset the rejection bound of its confirmation")
	}
}

func TestSharedOnePoolsRejectedAssignmentsAreNotResetByOtherPools(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	_, recordA := sharedReady(t, c, native, first)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	sharedReady(t, c, native, second)
	third := sharedWorkspace(t, c, first, "tenant", "third", "third-uid", binding)
	_, recordC := sharedReady(t, c, native, third)
	if recordC.SharedWorker.Pool == recordA.SharedWorker.Pool {
		t.Fatal("the fixture did not create a second occupied pool")
	}
	// A warm pool joins the pass.
	item := &profile.SubstrateWorkspaceProfile{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "tenant", Name: "shared"}, item); err != nil {
		t.Fatal(err)
	}
	item.Annotations = map[string]string{spareWorkersAnnotation: "3"}
	if err := c.Update(t.Context(), item); err != nil {
		t.Fatal(err)
	}
	_ = sharedReplenisher(c, native).Sweep(t.Context())
	if len(sharedPools(t, c)) < 3 {
		t.Fatal("the fixture did not create a warm pool")
	}
	// Only the first pool's assignment reads are rejected.
	native.rejectAssignmentsFor = map[string]bool{recordA.Worker.Name: true}
	start := sharedNow()
	for _, at := range []time.Duration{0, 20 * time.Second, 40 * time.Second} {
		setNow(t, start.Add(at))
		_ = sharedReplenisher(c, native).Sweep(t.Context())
	}
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); !found {
		t.Fatal("the rejected pool was retired before its bound")
	}
	setNow(t, start.Add(sharedProvenanceOutageBound))
	_ = sharedReplenisher(c, native).Sweep(t.Context())
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); found {
		t.Fatal("other pools' successful checks reset one pool's assignment rejection")
	}
	if _, found := getPool(t, c, recordC.SharedWorker.Pool); !found {
		t.Fatal("a healthy pool was retired with the rejected one")
	}
}

func TestSharedDegradedStartRetiresExistingPoolsAtTheBound(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	// The provider starts with native control unavailable: no native sweep
	// runs; only the bounds loop does.
	start := sharedNow()
	SetNativeDegraded("native control could not be reached: cannot read native control CA file")
	bounds := &SpareWorkerReplenisher{Client: c, Config: sharedConfig()}
	_ = bounds.EnforceSharedBounds(t.Context())
	pool, found := getPool(t, c, record.SharedWorker.Pool)
	if !found || pool.GetDeletionTimestamp() != nil {
		t.Fatal("a degraded start retired the pool before its bound")
	}
	if pool.GetAnnotations()[sharedNativeOutageAnnotation] == "" {
		t.Fatal("a degraded start did not stamp the outage durably")
	}
	// The provider restarts, still degraded, 50 s later: the stamp keeps the
	// original start.
	setNow(t, start.Add(50*time.Second))
	SetNativeDegraded("native control could not be reached: cannot read native control CA file")
	_ = bounds.EnforceSharedBounds(t.Context())
	if _, found := getPool(t, c, record.SharedWorker.Pool); !found {
		t.Fatal("a restarted degraded start retired the pool before its bound")
	}
	setNow(t, start.Add(sharedProvenanceOutageBound))
	_ = bounds.EnforceSharedBounds(t.Context())
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("a degraded start did not retire the existing shared pool at its bound")
	}
}

func TestSharedProfileListFailureStillQuarantinesForeignTemplates(t *testing.T) {
	c, native, record, _ := sharedTemplateFixture(t)
	failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*profile.SubstrateWorkspaceProfileList); ok {
				return apierrors.NewServiceUnavailable("profiles unavailable")
			}
			return w.List(ctx, list, opts...)
		},
	})
	_ = sharedReplenisher(failing, native).Sweep(t.Context())
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("a failing profile List kept the foreign-template quarantine from running")
	}
	if err := sharedDriver(c, native).requireSharedProvenance(t.Context()); err == nil {
		t.Fatal("admission stayed open while the profiles could not be listed")
	}
}

func TestSharedStalledOutageStampsNeverDelayAQuarantine(t *testing.T) {
	previous := sharedOutageMarkBudget
	sharedOutageMarkBudget = time.Second
	t.Cleanup(func() { sharedOutageMarkBudget = previous })
	c, native, first, binding := sharedFixture(t, 2)
	names := manySharedPools(t, c, native, first, binding, 8)
	slices.Sort(names)
	// The last pool by name is reached by a foreign template; every pool's
	// assignment reads are rejected, so earlier pools queue outage stamps.
	journals := &corev1.ConfigMapList{}
	if err := c.List(t.Context(), journals, client.HasLabels{ownershipLabel, providerLabel}); err != nil {
		t.Fatal(err)
	}
	var target *journalRecord
	for i := range journals.Items {
		if record, ok := decodeClaimRecord(&journals.Items[i]); ok && record.SharedWorker != nil && record.SharedWorker.Pool == names[len(names)-1] && record.Template.Name != "" {
			target = record
		}
	}
	if target == nil {
		t.Fatal("no record holds the last pool")
	}
	native.templates[refKey(&pb.ObjectRef{Atespace: target.Atespace, Name: target.Template.Name})].Metadata.Uid = "foreign-template-uid"
	native.assignmentsRejected = true
	// Every outage stamp write stalls.
	stalling := contextAwareClient(interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GetAnnotations()[sharedUnverifiedAnnotation] != "" {
				<-ctx.Done()
				return ctx.Err()
			}
			return w.Update(ctx, obj, opts...)
		},
	}))
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sharedReplenisher(stalling, native).Sweep(t.Context())
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, found := getPool(t, c, target.SharedWorker.Pool); !found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stalled outage stamp writes delayed the quarantine of a foreign-reachable pool")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the stamp writes outlasted their budget")
	}
}

func TestSharedStalledHostedListStillRunsTheAssignmentChecks(t *testing.T) {
	previous := sharedDiscoveryTimeout
	sharedDiscoveryTimeout = 500 * time.Millisecond
	t.Cleanup(func() { sharedDiscoveryTimeout = previous })
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	// The worker hosts an Actor outside the ledger, and every hosted
	// workspace read stalls.
	native.foreignWorkers = map[string]bool{record.Worker.Name: true}
	stalling := contextAwareClient(interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, w client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*api.ExecutionWorkspace); ok {
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
	sweep := sharedReplenisher(stalling, native)
	sweep.SweepDeadline = 500 * time.Millisecond
	_ = sweep.Sweep(t.Context())
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("a stalled hosted-workspace read kept the assignment checks from catching a foreign Actor")
	}
}

func TestSharedStalledBoundsDeletesNeverDelayTheIsolationChecks(t *testing.T) {
	shortSharedBudgets(t)
	previous := sharedCleanupTimeout
	sharedCleanupTimeout = 2 * time.Second
	t.Cleanup(func() { sharedCleanupTimeout = previous })
	c, native, first, binding := sharedFixture(t, 2)
	due := manySharedPools(t, c, native, first, binding, 12)
	pool, ledger := poolLedger(t, c, due[0])
	setNow(t, sharedRetireAt(pool, ledger).Add(time.Minute))
	// A young pool, reached by a foreign template.
	young := sharedWorkspace(t, c, first, "tenant", "young", "young-uid", binding)
	_, record := sharedReady(t, c, native, young)
	if slices.Contains(due, record.SharedWorker.Pool) {
		t.Fatal("the young allocation joined a due pool")
	}
	native.templates[refKey(&pb.ObjectRef{Atespace: record.Atespace, Name: record.Template.Name})].Metadata.Uid = "foreign-template-uid"
	// Every WorkerPool delete stalls until its context ends; Pod deletes are
	// recorded with their time.
	var mu sync.Mutex
	var youngPodDeleted time.Time
	stalling := contextAwareClient(interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			switch o := obj.(type) {
			case *unstructured.Unstructured:
				if o.GetKind() == "WorkerPool" {
					<-ctx.Done()
					return ctx.Err()
				}
			case *corev1.Pod:
				if string(o.GetUID()) == record.Worker.PodUID {
					mu.Lock()
					youngPodDeleted = time.Now()
					mu.Unlock()
				}
			}
			return w.Delete(ctx, obj, opts...)
		},
	}))
	started := time.Now()
	_ = sharedReplenisher(stalling, native).Sweep(t.Context())
	mu.Lock()
	defer mu.Unlock()
	if youngPodDeleted.IsZero() || youngPodDeleted.Sub(started) > 2*time.Second {
		t.Fatalf("stalled bounds deletes delayed the foreign-template quarantine: %v", youngPodDeleted.Sub(started))
	}
}

func TestSharedSlowProfileDiscoveryNeverRetiresAHealthyPool(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	// Every profile-shape lookup stalls.
	stalling := contextAwareClient(interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, w client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "SubstrateWorkspaceProfile" {
				<-ctx.Done()
				return ctx.Err()
			}
			return w.Get(ctx, key, obj, opts...)
		},
	}))
	sweep := sharedReplenisher(stalling, native)
	sweep.SweepDeadline = 300 * time.Millisecond
	start := sharedNow()
	for _, at := range []time.Duration{0, 30 * time.Second, sharedProvenanceOutageBound, 2 * sharedProvenanceOutageBound} {
		setNow(t, start.Add(at))
		_ = sweep.Sweep(t.Context())
	}
	pool, found := getPool(t, c, record.SharedWorker.Pool)
	if !found || pool.GetDeletionTimestamp() != nil || sharedPoolUnverified(pool) {
		t.Fatal("slow profile discovery retired or failed the verification of a healthy pool")
	}
}

func TestSharedAssignmentCheckThatAlwaysTimesOutRetiresAtTheBound(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	native.blockAssignments = true
	sweep := sharedReplenisher(c, native)
	sweep.SweepDeadline = 300 * time.Millisecond
	start := sharedNow()
	_ = sweep.Sweep(t.Context())
	pool, found := getPool(t, c, record.SharedWorker.Pool)
	if !found || pool.GetDeletionTimestamp() != nil {
		t.Fatal("a timed-out assignment check retired the pool before the bound")
	}
	if !sharedPoolUnverified(pool) {
		t.Fatal("a timed-out assignment check left admission open")
	}
	setNow(t, start.Add(sharedProvenanceOutageBound))
	_ = sweep.Sweep(t.Context())
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("an assignment check that always times out did not retire the pool at the bound")
	}
}

func TestSharedKnownQuarantineIsNotDelayedByOtherPoolsStalledChecks(t *testing.T) {
	shortSharedBudgets(t)
	previous := sharedIsolationCheckTimeout
	sharedIsolationCheckTimeout = 2 * time.Second
	t.Cleanup(func() { sharedIsolationCheckTimeout = previous })
	c, native, first, binding := sharedFixture(t, 2)
	names := manySharedPools(t, c, native, first, binding, 40)
	slices.Sort(names)
	journals := &corev1.ConfigMapList{}
	if err := c.List(t.Context(), journals, client.HasLabels{ownershipLabel, providerLabel}); err != nil {
		t.Fatal(err)
	}
	var target *journalRecord
	for i := range journals.Items {
		if record, ok := decodeClaimRecord(&journals.Items[i]); ok && record.SharedWorker != nil && record.SharedWorker.Pool == names[len(names)-1] && record.Template.Name != "" {
			target = record
		}
	}
	if target == nil {
		t.Fatal("no record holds the last pool")
	}
	// The last pool by name is reached by a foreign template; every pool's
	// assignment Lists hang.
	native.templates[refKey(&pb.ObjectRef{Atespace: target.Atespace, Name: target.Template.Name})].Metadata.Uid = "foreign-template-uid"
	native.blockAssignments = true
	var mu sync.Mutex
	var podDeleted time.Time
	recording := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if pod, ok := obj.(*corev1.Pod); ok && string(pod.UID) == target.Worker.PodUID {
				mu.Lock()
				podDeleted = time.Now()
				mu.Unlock()
			}
			return w.Delete(ctx, obj, opts...)
		},
	})
	started := time.Now()
	_ = sharedReplenisher(recording, native).Sweep(t.Context())
	mu.Lock()
	defer mu.Unlock()
	if podDeleted.IsZero() || podDeleted.Sub(started) > 3*time.Second {
		t.Fatalf("other pools' stalled checks delayed a known quarantine: %v", podDeleted.Sub(started))
	}
	// The pass itself is bounded by the per-pool budget times the rounds of
	// concurrent checks, not by the number of pools.
	if elapsed := time.Since(started); elapsed > 4*sharedIsolationCheckTimeout+10*time.Second {
		t.Fatalf("40 stalled pools made the pass take %s", elapsed)
	}
}

func TestSharedMaintenanceBudgetStartsAfterIsolation(t *testing.T) {
	previousCheck, previousMaintain := sharedIsolationCheckTimeout, sharedMaintenanceTimeout
	sharedIsolationCheckTimeout, sharedMaintenanceTimeout = 2*time.Second, time.Second
	t.Cleanup(func() { sharedIsolationCheckTimeout, sharedMaintenanceTimeout = previousCheck, previousMaintain })
	c, native, first, _ := sharedFixture(t, 2)
	sharedReady(t, c, native, first)
	item := &profile.SubstrateWorkspaceProfile{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "tenant", Name: "shared"}, item); err != nil {
		t.Fatal(err)
	}
	item.Annotations = map[string]string{spareWorkersAnnotation: "3"}
	if err := c.Update(t.Context(), item); err != nil {
		t.Fatal(err)
	}
	// The isolation scan outlasts the whole maintenance budget.
	native.blockAssignments = true
	before := len(sharedPools(t, c))
	_ = sharedReplenisher(contextAwareClient(c.(client.WithWatch)), native).Sweep(t.Context())
	if after := len(sharedPools(t, c)); after <= before {
		t.Fatal("a long isolation scan left warm replenishment with an expired budget")
	}
}

func TestSharedFreshBudgetsEndOnCancellationAfterTheSweepDeadline(t *testing.T) {
	root, cancelRoot := context.WithCancel(t.Context())
	defer cancelRoot()
	sweepCtx, cancelSweep := context.WithTimeout(withSweepBudget(root, time.Hour), 10*time.Millisecond)
	defer cancelSweep()
	<-sweepCtx.Done()
	// The sweep's own deadline has expired; a fresh budget still starts...
	fresh, cancel := freshBudget(sweepCtx, time.Hour)
	defer cancel()
	if fresh.Err() != nil {
		t.Fatal("a fresh budget inherited the sweep's spent deadline")
	}
	// ...and still ends when the original context is cancelled.
	cancelRoot()
	select {
	case <-fresh.Done():
	case <-time.After(time.Second):
		t.Fatal("a fresh budget outlived the cancellation of the sweep's original context")
	}
}

// stalledPoolDeletes stalls every WorkerPool delete until its context ends
// and counts them; Pod deletes are recorded by UID with their time.
func stalledPoolDeletes(c client.Client) (client.Client, *atomic.Int64, func(string) time.Time, func() time.Time) {
	var poolDeletes atomic.Int64
	var mu sync.Mutex
	podDeleted := map[string]time.Time{}
	var firstStallEnded time.Time
	wrapped := contextAwareClient(interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			switch o := obj.(type) {
			case *unstructured.Unstructured:
				if o.GetKind() == "WorkerPool" {
					poolDeletes.Add(1)
					<-ctx.Done()
					mu.Lock()
					if firstStallEnded.IsZero() {
						firstStallEnded = time.Now()
					}
					mu.Unlock()
					return ctx.Err()
				}
			case *corev1.Pod:
				mu.Lock()
				if _, seen := podDeleted[string(o.UID)]; !seen {
					podDeleted[string(o.UID)] = time.Now()
				}
				mu.Unlock()
			}
			return w.Delete(ctx, obj, opts...)
		},
	}))
	return wrapped, &poolDeletes, func(uid string) time.Time {
			mu.Lock()
			defer mu.Unlock()
			return podDeleted[uid]
		}, func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			return firstStallEnded
		}
}

func TestSharedShutdownStopsIssuingNewRetirements(t *testing.T) {
	shortSharedBudgets(t)
	previous := sharedCleanupTimeout
	sharedCleanupTimeout = 2 * time.Second
	t.Cleanup(func() { sharedCleanupTimeout = previous })
	c, native, first, binding := sharedFixture(t, 2)
	due := manySharedPools(t, c, native, first, binding, 12)
	pool, ledger := poolLedger(t, c, due[0])
	setNow(t, sharedRetireAt(pool, ledger).Add(time.Minute))
	stalling, poolDeletes, _, _ := stalledPoolDeletes(c)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	started := time.Now()
	go func() {
		defer close(done)
		_ = (&SpareWorkerReplenisher{Client: stalling, Config: sharedConfig()}).EnforceSharedBounds(ctx)
	}()
	for poolDeletes.Load() == 0 {
		if time.Since(started) > 5*time.Second {
			t.Fatal("no pool delete was issued")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// The manager stops while the first round of deletes is in flight.
	cancel()
	select {
	case <-done:
	case <-time.After(2*sharedCleanupTimeout + 2*time.Second):
		t.Fatal("the bounds pass did not stop promptly on shutdown")
	}
	if issued := poolDeletes.Load(); issued > sharedDeleteConcurrency {
		t.Fatalf("%d pool deletes were issued after shutdown began, beyond the %d in flight", issued, sharedDeleteConcurrency)
	}
}

func TestSharedDecidedRetirementsNeverDelayAQuarantine(t *testing.T) {
	shortSharedBudgets(t)
	previous := sharedCleanupTimeout
	sharedCleanupTimeout = 2 * time.Second
	t.Cleanup(func() { sharedCleanupTimeout = previous })
	c, native, first, binding := sharedFixture(t, 2)
	retiring := manySharedPools(t, c, native, first, binding, 12)
	// Every one of these pools has a persisted retirement whose pool delete
	// stalls.
	for _, name := range retiring {
		pool, ledger := poolLedger(t, c, name)
		ledger.State, ledger.Reason = sharedRetiring, "persisted before a crash"
		if err := setSharedLedger(pool, ledger); err != nil {
			t.Fatal(err)
		}
		if err := c.Update(t.Context(), pool); err != nil {
			t.Fatal(err)
		}
	}
	// A further pool is reached by a foreign template.
	young := sharedWorkspace(t, c, first, "tenant", "young", "young-uid", binding)
	_, record := sharedReady(t, c, native, young)
	if slices.Contains(retiring, record.SharedWorker.Pool) {
		t.Fatal("the allocation joined a retiring pool")
	}
	native.templates[refKey(&pb.ObjectRef{Atespace: record.Atespace, Name: record.Template.Name})].Metadata.Uid = "foreign-template-uid"
	stalling, _, podDeleted, stallEnded := stalledPoolDeletes(c)
	_ = sharedReplenisher(stalling, native).Sweep(t.Context())
	// Causal order, not wall time: a quarantine queued behind the stalled
	// retirements could only start once one of their pool deletes gave up
	// at its cleanup timeout, so its worker Pod delete must come before
	// the first stall ended.
	at, ended := podDeleted(record.Worker.PodUID), stallEnded()
	if at.IsZero() || ended.IsZero() || !at.Before(ended) {
		t.Fatalf("decided retirements delayed a required quarantine: quarantine Pod delete at %v, first stalled pool delete ended at %v", at, ended)
	}
}

func TestSharedConfirmedForeignAssignmentIsNeverDroppedByAFailedReread(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	native.foreignWorkers = map[string]bool{record.Worker.Name: true}
	// The pool Get that confirms the foreign assignment against a fresh
	// ledger succeeds; every later pool Get fails.
	var poolGets atomic.Int64
	failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, w client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "WorkerPool" && poolGets.Add(1) > 1 {
				return apierrors.NewInternalError(errors.New("pool read refused"))
			}
			return w.Get(ctx, key, obj, opts...)
		},
	})
	recording, deletes := recordingDeletes(t, failing)
	_ = sharedReplenisher(recording, native).Sweep(t.Context())
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("a failed pool reread dropped a confirmed foreign-assignment quarantine")
	}
	assertWorkerPodFirst(t, deletes(), record.Worker.PodUID, int64(workerPodTerminationGrace/time.Second))
	if poolGets.Load() < 2 {
		t.Fatal("the later pool reads were not exercised")
	}
}

func TestSharedFailedPodCleanupStillDeletesTheQuarantinedPool(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	native.foreignWorkers = map[string]bool{record.Worker.Name: true}
	// Every Pod List fails, so the direct worker Pod cleanup cannot run.
	failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.PodList); ok {
				return apierrors.NewInternalError(errors.New("pod list refused"))
			}
			return w.List(ctx, list, opts...)
		},
	})
	recording, deletes := recordingDeletes(t, failing)
	_ = sharedReplenisher(recording, native).Sweep(t.Context())
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("a failed Pod cleanup kept a quarantined pool's Delete from being issued")
	}
	if !slices.ContainsFunc(deletes(), func(d deleteRecord) bool { return d.kind == "WorkerPool" && d.uid == record.SharedWorker.UID }) {
		t.Fatal("the quarantined pool's UID-preconditioned Delete was not issued")
	}
}

func TestSharedFailedNamespaceInventoryIsListedOncePerPass(t *testing.T) {
	shortSharedBudgets(t)
	c, native, first, binding := sharedFixture(t, 2)
	names := manySharedPools(t, c, native, first, binding, 20)
	var lists, gets atomic.Int64
	failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*api.ExecutionWorkspaceList); ok {
				lists.Add(1)
				return apierrors.NewServiceUnavailable("workspaces unavailable")
			}
			return w.List(ctx, list, opts...)
		},
		Get: func(ctx context.Context, w client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*api.ExecutionWorkspace); ok {
				gets.Add(1)
			}
			return w.Get(ctx, key, obj, opts...)
		},
	})
	r := sharedReplenisher(failing, native)
	profiles := &profile.SubstrateWorkspaceProfileList{}
	if err := c.List(t.Context(), profiles); err != nil {
		t.Fatal(err)
	}
	d := New(failing, native, sharedConfig())
	_ = r.sweepShared(t.Context(), d, profiles.Items, &spareWorkerCache{d: d, byNamespace: map[string]map[string][]*pb.Worker{}}, true)
	if lists.Load() != 1 || gets.Load() != 0 {
		t.Fatalf("a failed namespace inventory was re-requested: %d Lists, %d Gets in one pass", lists.Load(), gets.Load())
	}
	for _, name := range names {
		pool, found := getPool(t, c, name)
		if !found || !sharedPoolUnverified(pool) {
			t.Fatalf("pool %s was not marked unverified", name)
		}
	}
}

func TestCaptureTreatsKubernetesTransportErrorsAsRecoverable(t *testing.T) {
	refused := &url.Error{Op: "Get", URL: "https://kubernetes.default.svc/api/v1/namespaces/tenant/configmaps/journal", Err: &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}}
	timeout := &url.Error{Op: "Get", URL: "https://kubernetes.default.svc", Err: errors.New("net/http: TLS handshake timeout")}
	for name, err := range map[string]error{
		"connection refused":    fmt.Errorf("reading the journal: %w", refused),
		"tls handshake timeout": timeout,
		"connection reset":      fmt.Errorf("listing: %w", syscall.ECONNRESET),
		"unexpected eof":        fmt.Errorf("decoding: %w", io.ErrUnexpectedEOF),
		"kubernetes api":        apierrors.NewServiceUnavailable("etcd timeout"),
		"native rpc":            status.Error(codes.Internal, "postgres timeout"),
		"context":               context.DeadlineExceeded,
	} {
		if captureUnrecoverable(err) {
			t.Errorf("%s was treated as final; a transient error must never mark an instance lost", name)
		}
	}
	if !captureUnrecoverable(errors.New("the suspension exposed no new completed data-only snapshot")) {
		t.Error("the provider's own evidence failure was treated as transient")
	}
}

func TestSharedHostedPoolWithoutAListedWorkerIsUnverified(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	// The pool hosts a booted Actor, but its worker is no longer listed.
	native.hiddenWorkers = map[string]bool{record.Worker.Name: true}
	start := sharedNow()
	_ = sharedReplenisher(c, native).Sweep(t.Context())
	pool, found := getPool(t, c, record.SharedWorker.Pool)
	if !found || !sharedPoolUnverified(pool) {
		t.Fatal("a hosted pool without a listed worker was skipped as having nothing to check")
	}
	setNow(t, start.Add(sharedProvenanceOutageBound))
	_ = sharedReplenisher(c, native).Sweep(t.Context())
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("a hosted pool without a listed worker was not retired at the bound")
	}
}

func TestSharedWarmPoolWithoutAWorkerStaysNeutral(t *testing.T) {
	c, native, _, pool, _ := warmSharedPool(t, 24*time.Hour)
	native.nativeDown = false
	native.hiddenWorkers = map[string]bool{pool.GetName() + "-worker": true}
	start := sharedNow()
	for _, at := range []time.Duration{0, sharedProvenanceOutageBound} {
		setNow(t, start.Add(at))
		_ = sharedReplenisher(c, native).Sweep(t.Context())
	}
	current, found := getPool(t, c, pool.GetName())
	if !found || sharedPoolUnverified(current) {
		t.Fatal("an empty warm pool without a worker was treated as unverified")
	}
}

func TestSharedPoolDeleteHasItsOwnBudgetAfterASlowPodCleanup(t *testing.T) {
	previousCleanup, previousEscalation := sharedCleanupTimeout, sharedEscalationTimeout
	sharedCleanupTimeout, sharedEscalationTimeout = time.Second, time.Second
	t.Cleanup(func() { sharedCleanupTimeout, sharedEscalationTimeout = previousCleanup, previousEscalation })
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
	setNow(t, sharedRetireAt(pool, ledger))
	// The direct Pod cleanup stalls through the whole cleanup budget.
	stalling := contextAwareClient(interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.PodList); ok {
				time.Sleep(sharedCleanupTimeout + 200*time.Millisecond)
				return context.DeadlineExceeded
			}
			return w.List(ctx, list, opts...)
		},
	}))
	_ = (&SpareWorkerReplenisher{Client: stalling, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("a slow Pod cleanup left the pool's Delete an expired budget")
	}
}

func TestSharedOutsideAtespaceQuarantineNeverWaitsOnProviderRecords(t *testing.T) {
	shortSharedBudgets(t)
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	native.templates["other/foreign"] = &pb.ActorTemplate{Metadata: &pb.ResourceMetadata{Atespace: "other", Name: "foreign", Uid: "foreign-uid", Version: 1}}
	// The provider's records (the checkpoint catalogs) stall until their
	// context ends.
	stalling := contextAwareClient(interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.ConfigMapList); ok {
				options := &client.ListOptions{}
				options.ApplyOptions(opts)
				if options.LabelSelector != nil && strings.Contains(options.LabelSelector.String(), catalogLabel) {
					<-ctx.Done()
					return ctx.Err()
				}
			}
			return w.List(ctx, list, opts...)
		},
	}))
	recording, deletes := recordingDeletes(t, stalling)
	var podDeleted time.Time
	started := time.Now()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sharedReplenisher(recording, native).Sweep(t.Context())
	}()
	for podDeleted.IsZero() && time.Since(started) < 5*time.Second {
		if slices.ContainsFunc(deletes(), func(d deleteRecord) bool { return d.kind == "Pod" && d.uid == record.Worker.PodUID }) {
			podDeleted = time.Now()
		}
		time.Sleep(10 * time.Millisecond)
	}
	if podDeleted.IsZero() || podDeleted.Sub(started) > 2*time.Second {
		t.Fatalf("an outside-Atespace quarantine waited on the provider's records: %v", podDeleted.Sub(started))
	}
	<-done
}

func TestSharedUnverifiedPoolRetiresAtItsBoundDuringALongScan(t *testing.T) {
	shortSharedBudgets(t)
	previous := sharedIsolationCheckTimeout
	sharedIsolationCheckTimeout = 2 * time.Second
	t.Cleanup(func() { sharedIsolationCheckTimeout = previous })
	c, native, first, binding := sharedFixture(t, 2)
	names := manySharedPools(t, c, native, first, binding, 16)
	// Every pool's assignment List uses up its whole budget, so the scan
	// takes several rounds.
	native.blockAssignments = true
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sharedReplenisher(c, native).Sweep(t.Context())
	}()
	// The first stamped pools are durable as soon as they are decided, long
	// before the scan ends.
	var stamped string
	deadline := time.Now().Add(5 * time.Second)
	for stamped == "" {
		for _, name := range names {
			if pool, found := getPool(t, c, name); found && pool.GetAnnotations()[sharedUnverifiedAnnotation] != "" {
				stamped = name
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no unverified stamp was written while the scan ran")
		}
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case <-done:
		t.Fatal("the scan finished before the test could interleave")
	default:
	}
	// A restart loses every in-process record; the durable stamp keeps the
	// age, and the independent bounds loop enforces it mid-scan.
	sharedUnverifiedPools.reset()
	setNow(t, sharedNow().Add(sharedProvenanceOutageBound))
	_ = (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
	if _, found := getPool(t, c, stamped); found {
		t.Fatal("an unverified pool outlived its bound while a long scan ran")
	}
	select {
	case <-done:
		t.Fatal("the retirement waited for the scan to end")
	default:
	}
	<-done
}

func TestSharedUnclassifiedUnverifiedStampKeepsTheGrace(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	pool, _ := poolLedger(t, c, record.SharedWorker.Pool)
	// A stamp whose classification was never written.
	start := sharedNow()
	annotations := maps.Clone(pool.GetAnnotations())
	annotations[sharedUnverifiedAnnotation] = start.UTC().Format(time.RFC3339Nano)
	pool.SetAnnotations(annotations)
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	bounds := &SpareWorkerReplenisher{Client: c, Config: sharedConfig()}
	setNow(t, start.Add(sharedProvenanceOutageBound))
	_ = bounds.EnforceSharedBounds(t.Context())
	if _, found := getPool(t, c, record.SharedWorker.Pool); !found {
		t.Fatal("an unclassified stamp was retired at the outage bound instead of keeping the grace")
	}
	setNow(t, start.Add(sharedControlPlaneDownGrace))
	_ = bounds.EnforceSharedBounds(t.Context())
	if _, found := getPool(t, c, record.SharedWorker.Pool); found {
		t.Fatal("an unclassified stamp outlived the control-plane-down grace")
	}
}

func TestSharedUnverifiedStampCarriesItsClassification(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	native.assignmentsUnavailable = true
	_ = sharedReplenisher(c, native).Sweep(t.Context())
	pool, _ := getPool(t, c, record.SharedWorker.Pool)
	if pool.GetAnnotations()[sharedUnverifiedAnnotation] == "" || pool.GetAnnotations()[sharedUnverifiedGraceAnnotation] != sharedUnverifiedGraceNone {
		t.Fatalf("the unverified stamp was not written with its classification: %v", pool.GetAnnotations())
	}
}

func TestSharedDelayedOutageStampWritesNeverUndoANewerDecision(t *testing.T) {
	mark := func(pool *unstructured.Unstructured, value string, decided time.Time) outageMark {
		return outageMark{namespace: pool.GetNamespace(), name: pool.GetName(), uid: pool.GetUID(), key: sharedUnverifiedAnnotation, value: value, decidedAt: decided}
	}
	t.Run("a delayed set after a newer clear", func(t *testing.T) {
		c, native, first, _ := sharedFixture(t, 2)
		_, record := sharedReady(t, c, native, first)
		pool, _ := poolLedger(t, c, record.SharedWorker.Pool)
		d := sharedDriver(c, native).withSharedPass()
		t0 := sharedNow()
		d.writeOutageMarks(t.Context(), []outageMark{mark(pool, t0.UTC().Format(time.RFC3339Nano), t0)})
		// The pool recovered: a newer decision clears the stamp...
		d.writeOutageMarks(t.Context(), []outageMark{mark(pool, "", t0.Add(2*time.Second))})
		// ...and a set decided before that recovery lands late.
		d.writeOutageMarks(t.Context(), []outageMark{mark(pool, t0.Add(time.Second).UTC().Format(time.RFC3339Nano), t0.Add(time.Second))})
		current, _ := getPool(t, c, record.SharedWorker.Pool)
		if current.GetAnnotations()[sharedUnverifiedAnnotation] != "" {
			t.Fatal("a delayed set revived a stamp a newer decision cleared")
		}
		setNow(t, t0.Add(sharedControlPlaneDownGrace+time.Minute))
		_ = (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
		if _, found := getPool(t, c, record.SharedWorker.Pool); !found {
			t.Fatal("a recovered pool was retired on a delayed stamp")
		}
	})
	t.Run("a delayed clear after a newer set", func(t *testing.T) {
		c, native, first, _ := sharedFixture(t, 2)
		_, record := sharedReady(t, c, native, first)
		pool, _ := poolLedger(t, c, record.SharedWorker.Pool)
		d := sharedDriver(c, native).withSharedPass()
		t0 := sharedNow()
		// A newer outage is stamped...
		d.writeOutageMarks(t.Context(), []outageMark{mark(pool, t0.UTC().Format(time.RFC3339Nano), t0.Add(2*time.Second))})
		// ...and a clear decided before it lands late.
		d.writeOutageMarks(t.Context(), []outageMark{mark(pool, "", t0.Add(time.Second))})
		current, _ := getPool(t, c, record.SharedWorker.Pool)
		if current.GetAnnotations()[sharedUnverifiedAnnotation] == "" {
			t.Fatal("a delayed clear wiped out a newer outage")
		}
	})
}

func TestSharedSupersededPassStampWritesStop(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	pool, _ := poolLedger(t, c, record.SharedWorker.Pool)
	old := sharedDriver(c, native).withSharedLoopPass("sweep")
	// A later pass of the same loop starts before the old pass's write runs.
	sharedDriver(c, native).withSharedLoopPass("sweep")
	old.startOutageMark(t.Context(), pool, sharedUnverifiedAnnotation, sharedNow().UTC().Format(time.RFC3339Nano))
	old.flushOutageMarks(t.Context())
	current, _ := getPool(t, c, record.SharedWorker.Pool)
	if current.GetAnnotations()[sharedUnverifiedAnnotation] != "" {
		t.Fatal("a superseded pass's stamp write still landed")
	}
}

// shortSharedBudgets shortens every shared-worker budget for a test whose
// stalls would otherwise wait out production-length bounds. Each test that
// depends on a particular budget still sets it after this.
func shortSharedBudgets(t *testing.T) {
	t.Helper()
	cleanup, escalation, ledger, bookkeeping := sharedCleanupTimeout, sharedEscalationTimeout, sharedLedgerWriteTimeout, sharedBookkeepingBudget
	inventory, discovery, marks, check := sharedInventoryTimeout, sharedDiscoveryTimeout, sharedOutageMarkBudget, sharedIsolationCheckTimeout
	sharedCleanupTimeout, sharedEscalationTimeout, sharedLedgerWriteTimeout, sharedBookkeepingBudget = time.Second, time.Second, 500*time.Millisecond, 2*time.Second
	sharedInventoryTimeout, sharedDiscoveryTimeout, sharedOutageMarkBudget, sharedIsolationCheckTimeout = time.Second, time.Second, time.Second, time.Second
	t.Cleanup(func() {
		sharedCleanupTimeout, sharedEscalationTimeout, sharedLedgerWriteTimeout, sharedBookkeepingBudget = cleanup, escalation, ledger, bookkeeping
		sharedInventoryTimeout, sharedDiscoveryTimeout, sharedOutageMarkBudget, sharedIsolationCheckTimeout = inventory, discovery, marks, check
	})
}

func TestSharedRecoveryRecordsItsClearEvenBeforeTheStampLands(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	pool, _ := poolLedger(t, c, record.SharedWorker.Pool)
	failed := sharedDriver(c, native).withSharedPass()
	start := sharedNow()
	// A failing pass decides the stamp, but its write is still in flight:
	// the pool snapshot the next pass reads has no stamp yet.
	stale := outageMark{namespace: pool.GetNamespace(), name: pool.GetName(), uid: pool.GetUID(), key: sharedUnverifiedAnnotation, value: start.UTC().Format(time.RFC3339Nano), decidedAt: start}
	sharedDecidedStamps.record(pool.GetUID(), sharedUnverifiedAnnotation, decidedSet)
	// The next pass verifies the pool and records its recovery.
	setNow(t, start.Add(time.Second))
	recovered := sharedDriver(c, native).withSharedPass()
	recovered.verificationSucceeded(t.Context(), pool)
	recovered.flushOutageMarks(t.Context())
	// The stale write lands only now.
	failed.writeOutageMarks(t.Context(), []outageMark{stale})
	current, _ := getPool(t, c, record.SharedWorker.Pool)
	if current.GetAnnotations()[sharedUnverifiedAnnotation] != "" {
		t.Fatal("a stale stamp write landed after a recorded recovery")
	}
	setNow(t, start.Add(sharedControlPlaneDownGrace+time.Minute))
	_ = (&SpareWorkerReplenisher{Client: c, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
	if _, found := getPool(t, c, record.SharedWorker.Pool); !found {
		t.Fatal("a recovered pool was retired on a stale stamp")
	}
}

func TestSharedNewFailureOutranksAnInFlightRecoveryClear(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	pool, _ := poolLedger(t, c, record.SharedWorker.Pool)
	start := sharedNow()
	// The pool was unverified, and its stamp is durable.
	d := sharedDriver(c, native).withSharedPass()
	d.writeOutageMarks(t.Context(), []outageMark{
		{namespace: pool.GetNamespace(), name: pool.GetName(), uid: pool.GetUID(), key: sharedUnverifiedAnnotation, value: start.UTC().Format(time.RFC3339Nano), decidedAt: start},
		{namespace: pool.GetNamespace(), name: pool.GetName(), uid: pool.GetUID(), key: sharedUnverifiedGraceAnnotation, value: sharedUnverifiedGraceNone, decidedAt: start},
	})
	pool, _ = getPool(t, c, record.SharedWorker.Pool)
	// A recovery decides a clear, whose write is still in flight.
	setNow(t, start.Add(time.Second))
	staleClear := outageMark{namespace: pool.GetNamespace(), name: pool.GetName(), uid: pool.GetUID(), key: sharedUnverifiedAnnotation, decidedAt: sharedNow()}
	sharedDecidedStamps.record(pool.GetUID(), sharedUnverifiedAnnotation, decidedClear)
	// The pool fails again; its snapshot still shows the stamp.
	setNow(t, start.Add(2*time.Second))
	failing := sharedDriver(c, native).withSharedPass()
	_ = failing.verificationFailed(t.Context(), pool, false, errors.New("hosted check failed"))
	failing.flushOutageMarks(t.Context())
	// The recovery's clear lands only now.
	d.writeOutageMarks(t.Context(), []outageMark{staleClear})
	if current, _ := getPool(t, c, record.SharedWorker.Pool); current.GetAnnotations()[sharedUnverifiedAnnotation] == "" {
		t.Fatal("an in-flight recovery clear erased a newer failure's stamp")
	}
}

func TestSharedDeletingPoolStillGetsItsCappedPodDelete(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	_, record := sharedReady(t, c, native, first)
	pool, ledger := poolLedger(t, c, record.SharedWorker.Pool)
	// The pool stays deleting (as a foreground cascade would).
	pool.SetFinalizers([]string{"foregroundDeletion"})
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	setNow(t, sharedRetireAt(pool, ledger))
	// The first retirement's direct Pod cleanup fails: the pool Delete is
	// still issued, and the Pod error returned.
	failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.PodList); ok {
				return apierrors.NewInternalError(errors.New("pod list refused"))
			}
			return w.List(ctx, list, opts...)
		},
	})
	if err := sharedDriver(failing, native).hardRetireSharedPool(t.Context(), pool, sharedRetiring, "lifetime"); err == nil {
		t.Fatal("a failed Pod cleanup was dropped once the pool Delete succeeded")
	}
	current, _ := getPool(t, c, record.SharedWorker.Pool)
	if current.GetDeletionTimestamp() == nil {
		t.Fatal("the pool Delete was not issued")
	}
	// The next bounds pass, well before the hard bound's escalation, retries
	// the capped Pod delete for the deleting pool.
	recording, deletes := recordingDeletes(t, c)
	_ = (&SpareWorkerReplenisher{Client: recording, Config: sharedConfig()}).EnforceSharedBounds(t.Context())
	if !slices.ContainsFunc(deletes(), func(d deleteRecord) bool {
		return d.kind == "Pod" && d.uid == record.Worker.PodUID && d.grace != nil && *d.grace == int64(workerPodTerminationGrace/time.Second)
	}) {
		t.Fatalf("a deleting pool's worker Pod never got its capped delete retried: %+v", deletes())
	}
}

func TestSharedProofAttemptSaveConflictIsRetriedNotDoubt(t *testing.T) {
	c, native, first, binding := sharedFixture(t, 2)
	second := sharedWorkspace(t, c, first, "tenant", "second", "second-uid", binding)
	a, recordA := sharedReady(t, c, native, first)
	_, recordB := sharedReady(t, c, native, second)
	// A neighbour changes the worker's version during the proof, and the save
	// recording that proof attempt conflicts once.
	native.churnWorker = recordA.Worker.Name
	var conflicted atomic.Bool
	contended := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if cm, ok := obj.(*corev1.ConfigMap); ok && strings.Contains(cm.Data[journalDataKey], `"proofAttempts":1`) && conflicted.CompareAndSwap(false, true) {
				return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, cm.Name, errors.New("the journal changed"))
			}
			return w.Update(ctx, obj, opts...)
		},
	})
	for range 6 {
		tick()
		_, _ = sharedDriver(contended, native).StopInstance(t.Context(), first.Key, a.Identity)
		if conflicted.Load() {
			break
		}
	}
	if !conflicted.Load() {
		t.Fatal("the proof attempt's save was not exercised")
	}
	native.churnWorker = ""
	_, record := sharedRetired(t, c, native, first, a.Identity, false)
	if record.SharedWorker.Strong != "" || record.SharedWorker.Proof == nil {
		t.Fatalf("a journal write conflict during the proof took the strong path: %q", record.SharedWorker.Strong)
	}
	if _, found := getPool(t, c, recordA.SharedWorker.Pool); !found {
		t.Fatal("ordinary journal contention deleted the shared pool")
	}
	if actor := native.actors[refKey(actorRef(recordB))]; actor == nil || actor.Status.State != pb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatal("ordinary journal contention disturbed the neighbour")
	}
}

func TestSharedUnknownUIDTemplateMustMatchItsFrozenDigest(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(*pb.ActorTemplate)
		foreign bool
	}{
		{name: "a foreign same-name template with an empty selector", mutate: func(template *pb.ActorTemplate) {
			template.Metadata.Uid = "foreign-template-uid"
			template.WorkerSelector = nil
		}, foreign: true},
		{name: "the provider's own template, its UID not yet recorded", mutate: func(*pb.ActorTemplate) {}, foreign: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, native, first, _ := sharedFixture(t, 2)
			_, record := sharedReady(t, c, native, first)
			// The journal records the template's name and frozen digest, but
			// not yet its UID.
			cm, current, err := sharedDriver(c, native).read(t.Context(), first.Key)
			if err != nil {
				t.Fatal(err)
			}
			if current.Template.Digest == "" {
				t.Fatal("the fixture journal records no template digest")
			}
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
			tc.mutate(native.templates[refKey(&pb.ObjectRef{Atespace: record.Atespace, Name: record.Template.Name})])
			foreign, err := sharedDriver(c, native).foreignTemplateMatches(t.Context(), record.Atespace, sharedLabels(record.SharedWorker.Label))
			if err != nil {
				t.Fatal(err)
			}
			if foreign != tc.foreign {
				t.Fatalf("foreign = %v, want %v", foreign, tc.foreign)
			}
		})
	}
}
