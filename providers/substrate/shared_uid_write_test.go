package substrate

import (
	"context"
	"errors"
	"testing"

	sdk "github.com/orka-agents/orka-workspace/sdk"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestSharedPoolRetiredBeforeTheActorUIDWriteIsNeverResumed(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	retired := false
	// A neighbour or the sweeper retires the pool between this allocation's
	// validation and its Actor UID write, which then lands on the newer,
	// Retiring ledger.
	racing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		pool, ok := obj.(*unstructured.Unstructured)
		if _, shared := obj.GetLabels()[sharedPoolLabel]; ok && shared && !retired {
			if ledger, err := readSharedLedger(pool); err == nil && ledger.State == sharedAdmitting && len(ledger.Entries) == 1 && ledger.Entries[0].ActorUID != "" {
				ledger.State = sharedRetiring
				if err := setSharedLedger(pool, ledger); err != nil {
					return err
				}
				retired = true
			}
		}
		return inner.Update(ctx, obj, opts...)
	}})
	var lastErr error
	for range 16 {
		_, lastErr = sharedDriver(racing, native).EnsureAllocation(t.Context(), first)
		if retired {
			break
		}
	}
	if !retired {
		t.Fatalf("the Actor UID write was never reached: %v", lastErr)
	}
	if !errors.Is(lastErr, sdk.ErrStaleIdentity) {
		t.Fatalf("the pass that wrote the Actor UID to a retiring pool continued: %v", lastErr)
	}
	for range 4 {
		_, _ = sharedDriver(c, native).EnsureAllocation(t.Context(), first)
	}
	if native.boots != 0 {
		t.Fatalf("an Actor was resumed on a pool retired before its Actor UID write (boots=%d)", native.boots)
	}
}
