package substrate

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	profile "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func setSpareAnnotation(t *testing.T, c client.Client, value string) {
	t.Helper()
	item := &profile.SubstrateWorkspaceProfile{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "tenant", Name: "native"}, item); err != nil {
		t.Fatal(err)
	}
	annotations := maps.Clone(item.Annotations)
	if annotations == nil {
		annotations = map[string]string{}
	}
	if value == "" {
		delete(annotations, spareWorkersAnnotation)
	} else {
		annotations[spareWorkersAnnotation] = value
	}
	item.Annotations = annotations
	if err := c.Update(t.Context(), item); err != nil {
		t.Fatal(err)
	}
}

func replenisher(c client.Client, native *nativeFixture) *SpareWorkerReplenisher {
	return &SpareWorkerReplenisher{Client: c, Control: native, Config: Config{RouterEndpoint: fixtureRouter}}
}

func sweep(t *testing.T, c client.Client, native *nativeFixture) {
	t.Helper()
	if err := replenisher(c, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// spares returns the unclaimed spare pools, materializing their workers.
func spares(t *testing.T, c client.Client, native *nativeFixture) []unstructured.Unstructured {
	t.Helper()
	if _, err := native.ListWorkers(t.Context(), &pb.ListWorkersRequest{}); err != nil {
		t.Fatal(err)
	}
	pools := &unstructured.UnstructuredList{}
	pools.SetAPIVersion("ate.dev/v1")
	pools.SetKind("WorkerPoolList")
	if err := c.List(t.Context(), pools, client.HasLabels{spareProfileLabel}); err != nil {
		t.Fatal(err)
	}
	return pools.Items
}

func getPool(t *testing.T, c client.Client, name string) (*unstructured.Unstructured, bool) {
	t.Helper()
	pool := poolObject()
	err := c.Get(t.Context(), client.ObjectKey{Namespace: "native-workers", Name: name}, pool)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return pool, true
}

// poolCalls counts allocation-path WorkerPool creates, updates and spare
// lists through a wrapping client.
type poolCalls struct{ creates, updates, spareLists int }

func countingClient(c client.Client, calls *poolCalls, update func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error) client.Client {
	isPool := func(obj client.Object) bool {
		u, ok := obj.(*unstructured.Unstructured)
		return ok && u.GetKind() == "WorkerPool"
	}
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if isPool(obj) {
				calls.creates++
			}
			return w.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if isPool(obj) {
				calls.updates++
				if update != nil {
					return update(ctx, w, obj, opts...)
				}
			}
			return w.Update(ctx, obj, opts...)
		},
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			options := &client.ListOptions{}
			options.ApplyOptions(opts)
			if options.LabelSelector != nil && strings.Contains(options.LabelSelector.String(), spareProfileLabel) {
				calls.spareLists++
			}
			return w.List(ctx, list, opts...)
		},
	})
}

func requireSpareUsed(t *testing.T, c client.Client, native *nativeFixture, key sdk.AllocationKey, spare string) *journalRecord {
	t.Helper()
	cm, record, err := driver(c, native).read(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	if record.SpareWorker == nil || record.SpareWorker.Name != spare || record.RuntimePool.Name != spare || record.RuntimePool.UID != record.SpareWorker.UID {
		t.Fatalf("allocation did not bind the spare %s: %#v %#v", spare, record.SpareWorker, record.RuntimePool)
	}
	pool, found := getPool(t, c, spare)
	if !found {
		t.Fatal("claimed spare disappeared")
	}
	if !maps.Equal(pool.GetLabels(), record.RuntimePoolLabels) || pool.GetLabels()[spareProfileLabel] != "" || pool.GetAnnotations()[spareClaimAnnotation] != claimMarker(cm, record) || !runtimePoolMatches(pool, record) {
		t.Fatalf("claimed spare is not the record's ordinary private pool: %v %v", pool.GetLabels(), pool.GetAnnotations())
	}
	if record.Worker == nil || record.Worker.Pool != spare {
		t.Fatalf("startup fenced a worker outside the spare: %#v", record.Worker)
	}
	return record
}

func requireFresh(t *testing.T, c client.Client, native *nativeFixture, key sdk.AllocationKey) *journalRecord {
	t.Helper()
	_, record, err := driver(c, native).read(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	if record.SpareWorker != nil || record.RuntimePool.Name != record.Actor.Name+"-workers" || !record.RuntimePool.CreateIssued {
		t.Fatalf("allocation did not fall back to a fresh pool: %#v %#v", record.SpareWorker, record.RuntimePool)
	}
	return record
}

func TestSpareWorkerCountIsStrict(t *testing.T) {
	for value, want := range map[string]int{"0": 0, "1": 1, "8": 8} {
		if got, err := spareWorkerCount(map[string]string{spareWorkersAnnotation: value}); err != nil || got != want {
			t.Fatalf("%q = %d, %v", value, got, err)
		}
	}
	if got, err := spareWorkerCount(nil); err != nil || got != 0 {
		t.Fatalf("absent = %d, %v", got, err)
	}
	for _, value := range []string{"", "9", "-1", "+1", "01", " 1", "1 ", "one", "10", "1.0"} {
		if got, err := spareWorkerCount(map[string]string{spareWorkersAnnotation: value}); err == nil || got != 0 {
			t.Fatalf("%q was accepted as %d", value, got)
		}
	}
}

func TestSpareAnnotationDoesNotChangeProfileHash(t *testing.T) {
	c, native, request := fixture(t, false)
	setSpareAnnotation(t, c, "2")
	// The admitted binding froze the hash before the annotation existed.
	record, err := newRecord(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := driver(c, native).resolveProfile(t.Context(), record); err != nil {
		t.Fatal(err)
	}
}

func TestSpareWorkerColdAllocationClaimsSpareWithoutCreatingPool(t *testing.T) {
	c, native, request := fixture(t, false)
	setSpareAnnotation(t, c, "1")
	sweep(t, c, native)
	before := spares(t, c, native)
	if len(before) != 1 {
		t.Fatalf("spares = %d, want 1", len(before))
	}
	spare := before[0].GetName()
	calls := &poolCalls{}
	counted := countingClient(c, calls, nil)
	logs := &timingLogs{}
	ctx := logs.context(t)
	readyWith(t, ctx, counted, native, request)
	if calls.creates != 0 || calls.updates != 1 {
		t.Fatalf("allocation created %d pools and wrote %d, want a single claim write", calls.creates, calls.updates)
	}
	record := requireSpareUsed(t, c, native, request.Key, spare)
	entry := logs.only(t, "Substrate runtime ready")
	if entry["workerSource"] != "spare" || entry["path"] != "cold" {
		t.Fatalf("ready log = %v", entry)
	}
	requireOnlyTimingFields(t, entry)
	requireNoNativeIdentifiers(t, logs.raw(), record)
	// The replenisher replaces the claimed spare and never touches it.
	sweep(t, c, native)
	if after := spares(t, c, native); len(after) != 1 || after[0].GetName() == spare {
		t.Fatalf("replenished spares = %v", after)
	}
	setSpareAnnotation(t, c, "0")
	sweep(t, c, native)
	if len(spares(t, c, native)) != 0 {
		t.Fatal("lowered annotation kept unclaimed spares")
	}
	if _, found := getPool(t, c, spare); !found {
		t.Fatal("replenisher deleted a claimed spare")
	}
	// Retirement is unchanged: the claimed pool and its worker Pod go away.
	retired(t, c, native, request, record.Observation.Identity, false)
	if _, found := getPool(t, c, spare); found {
		t.Fatal("retirement left the claimed spare pool")
	}
	pod := &corev1.Pod{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "native-workers", Name: spare + "-worker"}, pod); !apierrors.IsNotFound(err) {
		t.Fatalf("retirement left the claimed worker Pod: %v", err)
	}
}

func TestSpareWorkerResumeFromTagClaimsSpare(t *testing.T) {
	c, native, request := fixture(t, true)
	next := suspendedSuccessor(t, c, native, request)
	setSpareAnnotation(t, c, "1")
	sweep(t, c, native)
	spare := spares(t, c, native)[0].GetName()
	calls := &poolCalls{}
	logs := &timingLogs{}
	readyWith(t, logs.context(t), countingClient(c, calls, nil), native, next)
	if calls.creates != 0 {
		t.Fatalf("resume created %d pools", calls.creates)
	}
	record := requireSpareUsed(t, c, native, next.Key, spare)
	if record.InheritedCheckpoint == nil {
		t.Fatal("successor did not resume from its Tag")
	}
	entry := logs.only(t, "Substrate runtime ready")
	if entry["workerSource"] != "spare" || entry["path"] != "resume" {
		t.Fatalf("ready log = %v", entry)
	}
	// The lineage continues across a spare: the next suspension and resume
	// still verify the frozen worker specification.
	stopped := retired(t, c, native, next, record.Observation.Identity, true)
	third := *next.DeepCopy()
	third.Sequence++
	third.PreviousInstance = &record.Observation.Identity
	third.RetainedData = stopped.RetainedData
	third.Revision, _ = sdk.WorkloadRevision(third)
	if err := admit(c)(t.Context(), third); err != nil {
		t.Fatal(err)
	}
	ready(t, c, native, third)
	requireFresh(t, c, native, third.Key)
}

func TestSpareWorkerFallsBackToFreshPool(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(t *testing.T, c client.Client, native *nativeFixture, spare string)
	}{
		{name: "size mismatch", mutate: func(t *testing.T, c client.Client, native *nativeFixture, spare string) {
			// The operator resized the template after the spare was born.
			native.templates["native/base"].Resources.Limits.MemoryBytes = 1 << 30
		}},
		{name: "not active", mutate: func(t *testing.T, c client.Client, native *nativeFixture, spare string) {
			native.workers[spare+"-worker"].Status.State = pb.WorkerState_WORKER_STATE_UNAVAILABLE
		}},
		{name: "allocated", mutate: func(t *testing.T, c client.Client, native *nativeFixture, spare string) {
			native.workers[spare+"-worker"].Status.Allocated = &pb.WorkerResources{CpuMillicores: 1, Actors: 1}
		}},
		{name: "assigned", mutate: func(t *testing.T, c client.Client, native *nativeFixture, spare string) {
			native.foreignWorkers = map[string]bool{spare + "-worker": true}
		}},
		{name: "relabeled", mutate: func(t *testing.T, c client.Client, native *nativeFixture, spare string) {
			// Labels an operator selector matches would expose the spare.
			pool, _ := getPool(t, c, spare)
			labels := pool.GetLabels()
			labels["selected"] = "native"
			pool.SetLabels(labels)
			if err := c.Update(t.Context(), pool); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "claimed elsewhere", mutate: func(t *testing.T, c client.Client, native *nativeFixture, spare string) {
			pool, _ := getPool(t, c, spare)
			pool.SetAnnotations(map[string]string{spareClaimAnnotation: "tenant/other/uid/1"})
			if err := c.Update(t.Context(), pool); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, native, request := fixture(t, false)
			setSpareAnnotation(t, c, "1")
			sweep(t, c, native)
			spare := spares(t, c, native)[0].GetName()
			test.mutate(t, c, native, spare)
			ready(t, c, native, request)
			requireFresh(t, c, native, request.Key)
			if pool, found := getPool(t, c, spare); !found || pool.GetAnnotations()[spareClaimAnnotation] != "" && test.name != "claimed elsewhere" {
				t.Fatal("fallback touched the spare")
			}
		})
	}
}

func TestSpareWorkerConcurrentAllocationsNeverShareSpare(t *testing.T) {
	c, native, request := fixture(t, false)
	other := restoreRequest(t, c, request, nil)
	setSpareAnnotation(t, c, "1")
	sweep(t, c, native)
	spare := spares(t, c, native)[0].GetName()
	raced := false
	calls := &poolCalls{}
	// Both allocations select the only spare; the second claim lands between
	// the first one's selection and its claim write.
	first := countingClient(c, calls, func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if !raced {
			raced = true
			ready(t, c, native, other)
		}
		return w.Update(ctx, obj, opts...)
	})
	if _, err := driver(first, native).EnsureAllocation(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	ready(t, c, native, request)
	if !raced {
		t.Fatal("the first allocation never attempted its claim")
	}
	requireSpareUsed(t, c, native, other.Key, spare)
	fresh := requireFresh(t, c, native, request.Key)
	if fresh.Worker.Pool == spare {
		t.Fatal("two allocations share one spare")
	}
}

func TestSpareWorkerWaitsForClaimedLabelsToReachWorker(t *testing.T) {
	c, native, request := fixture(t, false)
	setSpareAnnotation(t, c, "1")
	sweep(t, c, native)
	spare := spares(t, c, native)[0].GetName()
	native.staleWorkerLabels = true
	for range 6 {
		observed, err := driver(c, native).EnsureAllocation(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == sdk.AllocationReady || native.boots != 0 {
			t.Fatal("booted on a worker whose labels do not select it")
		}
	}
	native.staleWorkerLabels = false
	ready(t, c, native, request)
	requireSpareUsed(t, c, native, request.Key, spare)
}

func TestSpareWorkerAbsentAnnotationLeavesAllocationUnchanged(t *testing.T) {
	for _, value := range []string{"", "0", "invalid"} {
		t.Run(value, func(t *testing.T) {
			c, native, request := fixture(t, false)
			setSpareAnnotation(t, c, value)
			sweep(t, c, native)
			if len(spares(t, c, native)) != 0 {
				t.Fatal("spares exist without a positive annotation")
			}
			calls := &poolCalls{}
			ready(t, countingClient(c, calls, nil), native, request)
			if calls.spareLists != 0 || calls.updates != 0 || calls.creates != 1 {
				t.Fatalf("allocation calls = %+v, want only the fresh pool create", calls)
			}
			cm, record, err := driver(c, native).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(cm.Data[journalDataKey], "spareWorker") || record.RuntimePool.Name != record.Actor.Name+"-workers" {
				t.Fatal("journal changed without spares")
			}
			pool, _ := getPool(t, c, record.RuntimePool.Name)
			if len(pool.GetAnnotations()) != 0 {
				t.Fatalf("fresh pool carries spare metadata: %v", pool.GetAnnotations())
			}
		})
	}
}

func TestSpareWorkerReplenisherKeepsCountAndCollectsStaleSpares(t *testing.T) {
	c, native, request := fixture(t, false)
	setSpareAnnotation(t, c, "2")
	sweep(t, c, native)
	current := spares(t, c, native)
	if len(current) != 2 {
		t.Fatalf("spares = %d, want 2", len(current))
	}
	// Each spare is exactly the pool an allocation would create, but with
	// spare birth labels and nothing that selects it.
	record, err := newRecord(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := driver(c, native).resolveProfile(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	for _, pool := range current {
		expected, err := withBirthLabels(record.RuntimePoolSpec, spareBirth(pool.GetName()))
		if err != nil {
			t.Fatal(err)
		}
		if actual, _ := poolSpecDigest(&pool); actual != digest(expected) {
			t.Fatal("spare specification differs from the allocation's private pool")
		}
		if pool.GetLabels()["selected"] != "" || len(pool.GetOwnerReferences()) != 0 || pool.GetNamespace() != record.Placement.Namespace {
			t.Fatalf("spare metadata = %v %v", pool.GetLabels(), pool.GetOwnerReferences())
		}
	}
	// Idempotent.
	sweep(t, c, native)
	if len(spares(t, c, native)) != 2 {
		t.Fatal("a repeated sweep changed the spare count")
	}
	setSpareAnnotation(t, c, "1")
	sweep(t, c, native)
	if len(spares(t, c, native)) != 1 {
		t.Fatal("lowered annotation was not applied")
	}
	// An invalid annotation counts as 0.
	setSpareAnnotation(t, c, "12")
	sweep(t, c, native)
	if len(spares(t, c, native)) != 0 {
		t.Fatal("invalid annotation kept spares")
	}
	// Source pool drift replaces the spare.
	setSpareAnnotation(t, c, "1")
	sweep(t, c, native)
	old := spares(t, c, native)[0].GetName()
	mutateSourcePool(t, c, setSourceWorkerImage(pinnedWorkerImageB))
	sweep(t, c, native)
	if replaced := spares(t, c, native); len(replaced) != 1 || replaced[0].GetName() == old {
		t.Fatal("source drift did not replace the stale spare")
	}
	if image, _, _ := unstructured.NestedString(spares(t, c, native)[0].Object, "spec", "workerImage"); image != pinnedWorkerImageB {
		t.Fatalf("replacement spare image = %s", image)
	}
	// A spare hosting an Actor is never deleted.
	hosting := spares(t, c, native)[0].GetName()
	native.foreignWorkers = map[string]bool{hosting + "-worker": true}
	setSpareAnnotation(t, c, "0")
	if err := replenisher(c, native).Sweep(t.Context()); err == nil {
		t.Fatal("a spare hosting an Actor was deleted silently")
	}
	if _, found := getPool(t, c, hosting); !found {
		t.Fatal("a spare hosting an Actor was deleted")
	}
	native.foreignWorkers = nil
	// Profile deletion collects its spares.
	setSpareAnnotation(t, c, "2")
	sweep(t, c, native)
	item := &profile.SubstrateWorkspaceProfile{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "tenant", Name: "native"}, item); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), item); err != nil {
		t.Fatal(err)
	}
	sweep(t, c, native)
	if left := spares(t, c, native); len(left) != 0 {
		t.Fatalf("deleted profile left %d spares", len(left))
	}
}

func TestSpareWorkerReplenisherDeleteLosesToConcurrentClaim(t *testing.T) {
	c, native, request := fixture(t, false)
	setSpareAnnotation(t, c, "1")
	sweep(t, c, native)
	spare := spares(t, c, native)[0].GetName()
	setSpareAnnotation(t, c, "0")
	claimed := false
	// The claim lands after the replenisher read the spare.
	racing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Delete: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		if obj.GetName() == spare && !claimed {
			claimed = true
			setSpareAnnotation(t, c, "1")
			ready(t, c, native, request)
			setSpareAnnotation(t, c, "0")
		}
		return w.Delete(ctx, obj, opts...)
	}})
	if err := replenisher(racing, native).Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !claimed {
		t.Fatal("the replenisher never deleted the spare")
	}
	requireSpareUsed(t, c, native, request.Key, spare)
}

func TestSpareWorkerClaimCrashRecoveryIsJournalAuthoritative(t *testing.T) {
	t.Run("claim applied, binding save lost", func(t *testing.T) {
		c, native, request := fixture(t, false)
		setSpareAnnotation(t, c, "1")
		sweep(t, c, native)
		spare := spares(t, c, native)[0].GetName()
		claimedAt := 0
		saves := 0
		// Fail the journal save that follows the successful claim write.
		lossy := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "WorkerPool" {
				claimedAt = saves
			} else if _, ok := obj.(*corev1.ConfigMap); ok {
				saves++
				if claimedAt != 0 && saves == claimedAt+1 {
					return errors.New("journal write lost")
				}
			}
			return w.Update(ctx, obj, opts...)
		}})
		if _, err := driver(lossy, native).EnsureAllocation(t.Context(), request); err == nil {
			t.Fatal("expected the lost journal write")
		}
		_, record, err := driver(c, native).read(t.Context(), request.Key)
		if err != nil {
			t.Fatal(err)
		}
		if record.SpareWorker == nil || !record.SpareWorker.ClaimIssued || record.RuntimePool.UID != "" {
			t.Fatalf("journal = %#v %#v", record.SpareWorker, record.RuntimePool)
		}
		ready(t, c, native, request)
		requireSpareUsed(t, c, native, request.Key, spare)
	})
	t.Run("claim write lost, then applied late", func(t *testing.T) {
		c, native, request := fixture(t, false)
		other := restoreRequest(t, c, request, nil)
		setSpareAnnotation(t, c, "1")
		sweep(t, c, native)
		var late *unstructured.Unstructured
		lossy := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "WorkerPool" && late == nil {
				late = u.DeepCopy()
				return errors.New("claim response lost")
			}
			return w.Update(ctx, obj, opts...)
		}})
		for range 12 {
			observed, err := driver(lossy, native).EnsureAllocation(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			if observed.State == sdk.AllocationReady {
				break
			}
		}
		// The unproven claim was abandoned for a fresh pool.
		requireFresh(t, c, native, request.Key)
		// The lost write applies after all, under its original precondition.
		if err := c.Update(t.Context(), late); err != nil {
			t.Fatal(err)
		}
		// The replenisher replaces it, and another workspace claims that.
		sweep(t, c, native)
		if _, found := getPool(t, c, late.GetName()); found {
			t.Fatal("orphaned claim was not collected")
		}
		ready(t, c, native, other)
		_, held, err := driver(c, native).read(t.Context(), other.Key)
		if err != nil || held.SpareWorker == nil {
			t.Fatalf("second allocation did not claim a spare: %v", err)
		}
		sweep(t, c, native)
		if _, found := getPool(t, c, held.SpareWorker.Name); !found {
			t.Fatal("a journal-held claim was collected")
		}
	})
	t.Run("retirement before binding releases the claim", func(t *testing.T) {
		c, native, request := fixture(t, false)
		setSpareAnnotation(t, c, "1")
		sweep(t, c, native)
		spare := spares(t, c, native)[0].GetName()
		var late *unstructured.Unstructured
		unavailable := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
			Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "WorkerPool" {
					late = u.DeepCopy()
					return errors.New("claim response lost")
				}
				return w.Update(ctx, obj, opts...)
			},
			Get: func(ctx context.Context, w client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if u, ok := obj.(*unstructured.Unstructured); ok && late != nil && key.Name == spare && u.GetKind() == "WorkerPool" {
					return apierrors.NewServiceUnavailable("unavailable")
				}
				return w.Get(ctx, key, obj, opts...)
			},
		})
		if _, err := driver(unavailable, native).EnsureAllocation(t.Context(), request); err == nil {
			t.Fatal("expected an unresolved claim")
		}
		_, record, err := driver(c, native).read(t.Context(), request.Key)
		if err != nil || record.SpareWorker == nil || !record.SpareWorker.ClaimIssued {
			t.Fatalf("claim intent was not journaled: %v", err)
		}
		retired(t, c, native, request, record.Observation.Identity, false)
		_, record, err = driver(c, native).read(t.Context(), request.Key)
		if err != nil || !record.SpareWorker.Released || record.RuntimePool.UID != "" {
			t.Fatalf("retirement did not release the unproven claim: %v", err)
		}
		// A late claim cannot make the stopped record own the spare.
		if err := c.Update(t.Context(), late); err != nil {
			t.Fatal(err)
		}
		sweep(t, c, native)
		if _, found := getPool(t, c, spare); found {
			t.Fatal("released late claim was not collected")
		}
	})
	t.Run("missing journal keeps the claimed pool", func(t *testing.T) {
		c, native, request := fixture(t, false)
		setSpareAnnotation(t, c, "1")
		sweep(t, c, native)
		spare := spares(t, c, native)[0].GetName()
		ready(t, c, native, request)
		pool, _ := getPool(t, c, spare)
		annotations := pool.GetAnnotations()
		parts := strings.Split(annotations[spareClaimAnnotation], "/")
		// A replaced journal is not proof the claim is orphaned.
		parts[2] = "replaced-uid"
		annotations[spareClaimAnnotation] = strings.Join(parts, "/")
		pool.SetAnnotations(annotations)
		if err := c.Update(t.Context(), pool); err != nil {
			t.Fatal(err)
		}
		sweep(t, c, native)
		if _, found := getPool(t, c, spare); !found {
			t.Fatal("a claimed pool without a provable journal disowning was collected")
		}
	})
}

func TestSpareWorkerJournalValidation(t *testing.T) {
	c, native, request := fixture(t, false)
	setSpareAnnotation(t, c, "1")
	sweep(t, c, native)
	ready(t, c, native, request)
	cm, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*journalRecord){
		"other pool":     func(r *journalRecord) { r.SpareWorker.Name = "other" },
		"other uid":      func(r *journalRecord) { r.SpareWorker.UID = "other" },
		"released bound": func(r *journalRecord) { r.SpareWorker.Released = true },
		"create issued":  func(r *journalRecord) { r.RuntimePool.CreateIssued = true },
	} {
		copy := *record
		spare := *record.SpareWorker
		copy.SpareWorker = &spare
		mutate(&copy)
		data, err := json.Marshal(&copy)
		if err != nil {
			t.Fatal(err)
		}
		tampered := cm.DeepCopy()
		tampered.Data[journalDataKey] = string(data)
		if err := c.Update(t.Context(), tampered); err != nil {
			t.Fatal(err)
		}
		if _, _, err := driver(c, native).read(t.Context(), request.Key); !errors.Is(err, sdk.ErrStaleIdentity) {
			t.Fatalf("%s: journal accepted: %v", name, err)
		}
		restored := tampered.DeepCopy()
		restored.Data[journalDataKey] = cm.Data[journalDataKey]
		if err := c.Update(t.Context(), restored); err != nil {
			t.Fatal(err)
		}
		cm = restored
	}
}

func TestSpareWorkerEmptyAnnotationLoggedOnce(t *testing.T) {
	var logs []string
	logger := funcr.New(func(prefix, args string) {
		logs = append(logs, prefix+args)
	}, funcr.Options{})
	ctx := ctrl.LoggerInto(t.Context(), logr.Logger(logger))
	r := &SpareWorkerReplenisher{}
	item := &profile.SubstrateWorkspaceProfile{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "tenant",
			Name:        "native",
			UID:         "profile-empty-uid",
			Annotations: map[string]string{spareWorkersAnnotation: ""},
		},
	}
	if got := r.wanted(ctx, item); got != 0 {
		t.Fatalf("wanted = %d, want 0", got)
	}
	if len(logs) != 1 {
		t.Fatalf("first sweep logged %d diagnostics, want 1", len(logs))
	}
	if got := r.wanted(ctx, item); got != 0 {
		t.Fatalf("second wanted = %d, want 0", got)
	}
	if len(logs) != 1 {
		t.Fatalf("second sweep logged %d diagnostics, want 1", len(logs))
	}
}
