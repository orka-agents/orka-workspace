package substrate

import (
	"context"
	"maps"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// fenceOutcome is what one warm-attach pass published.
type fenceOutcome struct {
	failed, ready, acknowledged bool
	state                       api.ExecutionWorkspaceState
}

// fenceRun is one pass's outcome and how many calls it made.
type fenceRun struct {
	outcome fenceOutcome
	calls   int
}

// TestSharedWarmAttachFenceEquivalenceUnderConcurrentRetirement checks the
// steady-Ready fast path against the full EnsureAllocation pass, fence by
// fence. Each case changes one input that EnsureAllocation, admission or
// Resume reads: before the pass, and (for the inputs a concurrent retirement
// can change) right after the pass's first read of the pool, while its
// parallel observation reads are in flight. The fast path must publish
// exactly what the full pass publishes. Dropping a fence from the fast path
// makes the two differ.
func TestSharedWarmAttachFenceEquivalenceUnderConcurrentRetirement(t *testing.T) {
	type env struct {
		c      client.Client
		native *nativeFixture
		record *journalRecord
		pool   *unstructured.Unstructured
	}
	annotate := func(key, value string) func(*testing.T, env) {
		return func(t *testing.T, e env) {
			pool := poolObject()
			if err := e.c.Get(t.Context(), client.ObjectKeyFromObject(e.pool), pool); err != nil {
				t.Fatal(err)
			}
			annotations := maps.Clone(pool.GetAnnotations())
			annotations[key] = value
			pool.SetAnnotations(annotations)
			if err := e.c.Update(t.Context(), pool); err != nil {
				t.Fatal(err)
			}
		}
	}
	journal := func(change func(*journalRecord)) func(*testing.T, env) {
		return func(t *testing.T, e env) {
			d := driver(e.c, e.native)
			cm, record, err := d.read(t.Context(), e.record.Request.Key)
			if err != nil {
				t.Fatal(err)
			}
			change(record)
			if err := d.save(t.Context(), cm, record); err != nil {
				t.Fatal(err)
			}
		}
	}
	now := func() string { return sharedNow().UTC().Format(time.RFC3339Nano) }
	for _, tc := range []struct {
		name string
		// concurrent: a retirement can change this input while a pass runs.
		concurrent bool
		inject     func(*testing.T, env)
		want       fenceOutcome
	}{
		// Admission and Resume fences: a booted Actor keeps serving, in both
		// paths, while its pool retires; Core drains it on the worker's
		// termination.
		{"unverified stamp", true, func(t *testing.T, e env) { annotate(sharedUnverifiedAnnotation, now())(t, e) }, fenceOutcome{ready: true, acknowledged: true}},
		{"unverified in process", true, func(_ *testing.T, e env) { sharedUnverifiedPools.failed(e.pool.GetUID(), sharedNow()) }, fenceOutcome{ready: true, acknowledged: true}},
		{"retirement intent", true, func(t *testing.T, e env) { annotate(sharedRetireIntentAnnotation, "decided")(t, e) }, fenceOutcome{ready: true, acknowledged: true}},
		{"undurable retirement in process", true, func(_ *testing.T, e env) { sharedUndurableRetirements.add(e.pool.GetUID()) }, fenceOutcome{ready: true, acknowledged: true}},
		{"pool deleting", true, func(t *testing.T, e env) {
			pool := poolObject()
			if err := e.c.Get(t.Context(), client.ObjectKeyFromObject(e.pool), pool); err != nil {
				t.Fatal(err)
			}
			// Foreground deletion keeps the pool until its Pods are gone.
			pool.SetFinalizers([]string{"foregroundDeletion"})
			if err := e.c.Update(t.Context(), pool); err != nil {
				t.Fatal(err)
			}
			if err := e.c.Delete(t.Context(), pool); err != nil {
				t.Fatal(err)
			}
		}, fenceOutcome{ready: true, acknowledged: true}},
		{"native outage stamp and classification", true, func(t *testing.T, e env) {
			annotate(sharedNativeOutageAnnotation, now())(t, e)
			annotate(sharedNativeOutageGraceAnnotation, sharedUnverifiedGraceNone)(t, e)
		}, fenceOutcome{ready: true, acknowledged: true}},
		{"held suspension doubt", true, func(t *testing.T, e env) { annotate(sharedHoldAnnotation, now())(t, e) }, fenceOutcome{ready: true, acknowledged: true}},
		{"superseded template inventory", true, func(_ *testing.T, _ env) { sharedNativeTemplateHealth.failed(sharedNow()) }, fenceOutcome{ready: true, acknowledged: true}},
		{"inconsistent lifetime evidence", true, func(t *testing.T, e env) {
			annotate(sharedDeadlineAnnotation, sharedNow().Add(2*time.Hour).UTC().Format(time.RFC3339Nano))(t, e)
		}, fenceOutcome{ready: true, acknowledged: true}},
		// Journal markers: only this workspace's own serialized retirement
		// writes them, so they never change while its pass runs.
		{"journal failure marker", false, journal(func(r *journalRecord) { r.Failure = "an injected failure" }), fenceOutcome{failed: true, state: api.ExecutionWorkspaceStateFailed}},
		{"journal strong marker", false, journal(func(r *journalRecord) { r.SharedWorker.Strong = "an injected strong decision" }), fenceOutcome{ready: true, acknowledged: true}},
		{"released slot", false, journal(func(r *journalRecord) { r.SharedWorker.Released = true }), fenceOutcome{failed: true}},
		// Identity: a retirement never changes it; only tampering or this
		// workspace's own release could.
		{"ledger entry removed", false, func(t *testing.T, e env) {
			d := driver(e.c, e.native)
			cm, record, err := d.read(t.Context(), e.record.Request.Key)
			if err != nil {
				t.Fatal(err)
			}
			pool, ledger := poolLedger(t, e.c, e.pool.GetName())
			marker := claimMarker(cm, record)
			ledger.Entries = slices.DeleteFunc(ledger.Entries, func(entry sharedLedgerEntry) bool { return entry.Journal == marker })
			if err := setSharedLedger(pool, ledger); err != nil {
				t.Fatal(err)
			}
			if err := e.c.Update(t.Context(), pool); err != nil {
				t.Fatal(err)
			}
		}, fenceOutcome{failed: true}},
		{"foreign assignment on the worker", false, func(_ *testing.T, e env) {
			e.native.foreignWorkers = map[string]bool{e.record.Worker.Name: true}
		}, fenceOutcome{failed: true}},
		{"pool identity labels", false, func(t *testing.T, e env) {
			pool := poolObject()
			if err := e.c.Get(t.Context(), client.ObjectKeyFromObject(e.pool), pool); err != nil {
				t.Fatal(err)
			}
			labels := maps.Clone(pool.GetLabels())
			labels["foreign"] = "label"
			pool.SetLabels(labels)
			if err := e.c.Update(t.Context(), pool); err != nil {
				t.Fatal(err)
			}
		}, fenceOutcome{failed: true}},
	} {
		modes := []string{"before"}
		if tc.concurrent {
			modes = append(modes, "during")
		}
		for _, mode := range modes {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				run := func(fullPass bool) fenceRun {
					r, c, native, request, ledger := warmAttach(t, "shared")
					_, record, err := driver(c, native).read(t.Context(), request.Key)
					if err != nil {
						t.Fatal(err)
					}
					pool, _ := poolLedger(t, c, record.SharedWorker.Pool)
					e := env{c: c, native: native, record: record, pool: pool}
					r.fullPass = fullPass
					if mode == "before" {
						tc.inject(t, e)
					} else {
						// The retirement lands right after the pass first
						// reads the pool, while its other reads run.
						var injected atomic.Bool
						r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, w client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
							err := w.Get(ctx, key, obj, opts...)
							if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "WorkerPool" && key.Name == pool.GetName() && injected.CompareAndSwap(false, true) {
								tc.inject(t, e)
							}
							return err
						}})
						defer func() {
							if !injected.Load() {
								t.Fatal("the pass never read the pool")
							}
						}()
					}
					ledger.start()
					_, err = r.Reconcile(t.Context(), ctrl.Request{NamespacedName: clientKey(request.Key)})
					calls := 0
					for _, n := range ledger.stop() {
						calls += n
					}
					workspace := bindingWorkspace(t, c, request.Key)
					out := fenceOutcome{
						failed:       err != nil || workspace.Status.State == api.ExecutionWorkspaceStateFailed,
						ready:        workspace.Status.Allocation != nil && workspace.Status.Allocation.State == sdk.AllocationReady && sdk.ConditionIsTrue(workspace.Status.Conditions, string(api.ConditionWorkspaceDataPlaneReady)),
						acknowledged: workspace.Status.ObservedGeneration == workspace.Generation && workspace.Status.AttachedEpoch == workspace.Spec.AttachmentEpoch,
					}
					if workspace.Status.State == api.ExecutionWorkspaceStateFailed {
						out.state = api.ExecutionWorkspaceStateFailed
					}
					return fenceRun{outcome: out, calls: calls}
				}
				fastRun, fullRun := run(false), run(true)
				fast, full := fastRun.outcome, fullRun.outcome
				if fast != full {
					t.Fatalf("the fast path published %+v, the full pass %+v", fast, full)
				}
				if !tc.want.failed && fastRun.calls >= fullRun.calls {
					// The comparison is only meaningful if the fast path ran.
					t.Fatalf("the fast path was not taken: %d calls, the full pass %d", fastRun.calls, fullRun.calls)
				}
				switch {
				case tc.want.failed && (!fast.failed || tc.want.state != "" && fast.state != tc.want.state):
					t.Fatalf("the pass published %+v, want a failure %+v", fast, tc.want)
				case !tc.want.failed && (fast.failed || !fast.ready || !fast.acknowledged):
					t.Fatalf("the pass published %+v, want Ready and acknowledged", fast)
				}
			})
		}
	}
}
