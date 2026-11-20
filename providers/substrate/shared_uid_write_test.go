package substrate

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"

	sdk "github.com/orka-agents/orka-workspace/sdk"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// A neighbour or the sweeper blocks the pool between this allocation's
// validation and its Actor UID write, which then lands on the newer pool.
// The pass that wrote the UID never resumes the Actor: it rechecks a fresh
// pool read against every fence admission uses.
func TestSharedPoolBlockedBeforeTheActorUIDWriteIsNeverResumed(t *testing.T) {
	stamp := func(key string) func(*unstructured.Unstructured) {
		return func(pool *unstructured.Unstructured) {
			annotations := maps.Clone(pool.GetAnnotations())
			if annotations == nil {
				annotations = map[string]string{}
			}
			annotations[key] = sharedNow().UTC().Format(time.RFC3339Nano)
			pool.SetAnnotations(annotations)
		}
	}
	for _, tc := range []struct {
		name string
		// inWrite changes the pool in the same write as the Actor UID;
		// afterWrite changes in-process state or the pool once it lands.
		inWrite    func(*unstructured.Unstructured)
		afterWrite func(*testing.T, client.WithWatch, *unstructured.Unstructured)
		// stale reports that later passes must keep refusing the pool too.
		stale bool
	}{
		{name: "retiring ledger", inWrite: func(pool *unstructured.Unstructured) {
			ledger, _ := readSharedLedger(pool)
			ledger.State = sharedRetiring
			_ = setSharedLedger(pool, ledger)
		}, stale: true},
		{name: "unverified stamp", inWrite: stamp(sharedUnverifiedAnnotation)},
		{name: "unverified in process", afterWrite: func(_ *testing.T, _ client.WithWatch, pool *unstructured.Unstructured) {
			sharedUnverifiedPools.failed(pool.GetUID(), sharedNow())
		}},
		{name: "held suspension doubt", inWrite: stamp(sharedHoldAnnotation)},
		{name: "retirement intent", inWrite: stamp(sharedRetireIntentAnnotation), stale: true},
		{name: "undurable retirement in process", afterWrite: func(_ *testing.T, _ client.WithWatch, pool *unstructured.Unstructured) {
			sharedUndurableRetirements.add(pool.GetUID())
		}, stale: true},
		{name: "native outage stamp", inWrite: stamp(sharedNativeOutageAnnotation)},
		{name: "superseded template inventory", afterWrite: func(_ *testing.T, _ client.WithWatch, _ *unstructured.Unstructured) {
			sharedNativeTemplateHealth.failed(sharedNow())
		}},
		{name: "deleting pool", afterWrite: func(t *testing.T, w client.WithWatch, pool *unstructured.Unstructured) {
			current := poolObject()
			if err := w.Get(t.Context(), client.ObjectKeyFromObject(pool), current); err != nil {
				t.Fatal(err)
			}
			current.SetFinalizers([]string{"foregroundDeletion"})
			if err := w.Update(t.Context(), current); err != nil {
				t.Fatal(err)
			}
			if err := w.Delete(t.Context(), current); err != nil {
				t.Fatal(err)
			}
		}, stale: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, native, first, _ := sharedFixture(t, 2)
			raced := false
			racing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				pool, ok := obj.(*unstructured.Unstructured)
				if _, shared := obj.GetLabels()[sharedPoolLabel]; !ok || !shared || raced {
					return inner.Update(ctx, obj, opts...)
				}
				ledger, err := readSharedLedger(pool)
				if err != nil || ledger.State != sharedAdmitting || len(ledger.Entries) != 1 || ledger.Entries[0].ActorUID == "" {
					return inner.Update(ctx, obj, opts...)
				}
				raced = true
				if tc.inWrite != nil {
					tc.inWrite(pool)
				}
				if err := inner.Update(ctx, obj, opts...); err != nil {
					return err
				}
				if tc.afterWrite != nil {
					tc.afterWrite(t, inner, pool)
				}
				return nil
			}})
			var lastErr error
			for range 16 {
				_, lastErr = sharedDriver(racing, native).EnsureAllocation(t.Context(), first)
				if raced {
					break
				}
			}
			if !raced {
				t.Fatalf("the Actor UID write was never reached: %v", lastErr)
			}
			if lastErr == nil || native.boots != 0 {
				t.Fatalf("the pass that wrote the Actor UID to a blocked pool resumed it: err=%v boots=%d", lastErr, native.boots)
			}
			if !tc.stale {
				return
			}
			if !errors.Is(lastErr, sdk.ErrStaleIdentity) && tc.name == "retiring ledger" {
				t.Fatalf("a retiring pool was not reported stale: %v", lastErr)
			}
			for range 4 {
				_, _ = sharedDriver(c, native).EnsureAllocation(t.Context(), first)
			}
			if native.boots != 0 {
				t.Fatalf("an Actor was resumed on a pool retired before its Actor UID write (boots=%d)", native.boots)
			}
		})
	}
}
