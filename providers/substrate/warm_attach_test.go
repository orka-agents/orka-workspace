package substrate

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// Simulated round trips for the warm-attach budget: an API server read or
// write, a native control call, and the router's mTLS challenge read, which
// opens a new connection on every call.
const (
	simulatedKubernetesLatency = 10 * time.Millisecond
	simulatedNativeLatency     = 10 * time.Millisecond
	simulatedRouterLatency     = 30 * time.Millisecond
)

// callLedger counts and times every Kubernetes, native and router call.
type callLedger struct {
	mu      sync.Mutex
	enabled bool
	counts  map[string]int
	elapsed map[string]time.Duration
	// published is when the pass's last status write (the one carrying
	// observedGeneration) completed.
	published time.Time
}

func (l *callLedger) start() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.enabled, l.counts, l.elapsed, l.published = true, map[string]int{}, map[string]time.Duration{}, time.Time{}
}

func (l *callLedger) stop() map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.enabled = false
	return maps.Clone(l.counts)
}

// call records one call and sleeps for its simulated latency.
func (l *callLedger) call(name string, latency time.Duration) func() {
	l.mu.Lock()
	enabled := l.enabled
	l.mu.Unlock()
	if !enabled {
		return func() {}
	}
	began := time.Now()
	time.Sleep(latency)
	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.counts[name]++
		l.elapsed[name] += time.Since(began)
		if strings.HasPrefix(name, "k8s status patch") {
			l.published = time.Now()
		}
	}
}

func (l *callLedger) table() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	names := slices.Sorted(maps.Keys(l.counts))
	var out strings.Builder
	total := 0
	for _, name := range names {
		total += l.counts[name]
		fmt.Fprintf(&out, "%-48s %3d %8s\n", name, l.counts[name], l.elapsed[name].Round(time.Millisecond))
	}
	fmt.Fprintf(&out, "%-48s %3d\n", "total", total)
	return out.String()
}

func kindOf(obj any) string {
	switch u := obj.(type) {
	case *unstructured.Unstructured:
		return u.GetKind()
	case *unstructured.UnstructuredList:
		return u.GetKind()
	}
	name := fmt.Sprintf("%T", obj)
	return name[strings.LastIndex(name, ".")+1:]
}

func ledgerClient(c client.Client, ledger *callLedger) client.Client {
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, w client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			defer ledger.call("k8s get "+kindOf(obj), simulatedKubernetesLatency)()
			return w.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, w client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			defer ledger.call("k8s list "+kindOf(list), simulatedKubernetesLatency)()
			return w.List(ctx, list, opts...)
		},
		Create: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			defer ledger.call("k8s create "+kindOf(obj), simulatedKubernetesLatency)()
			return w.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			defer ledger.call("k8s update "+kindOf(obj), simulatedKubernetesLatency)()
			return w.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, w client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			defer ledger.call("k8s patch "+kindOf(obj), simulatedKubernetesLatency)()
			return w.Patch(ctx, obj, patch, opts...)
		},
		SubResourcePatch: func(ctx context.Context, w client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			defer ledger.call("k8s status patch "+kindOf(obj), simulatedKubernetesLatency)()
			return w.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})
}

// warmAttach returns a Ready workspace's reconciler, whose next pass sees
// only Core's new attachment epoch, and the call ledger it reports into.
func warmAttach(t *testing.T, variant string) (*ExecutionWorkspaceReconciler, client.Client, *nativeFixture, sdk.WorkloadRequest, *callLedger) {
	t.Helper()
	var c client.Client
	var inner *nativeFixture
	var request sdk.WorkloadRequest
	config := credentialConfig()
	switch variant {
	case "resumed":
		// A successor resumed from its predecessor's Tag.
		c, inner, request = fixture(t, true)
		request = suspendedSuccessor(t, c, inner, request)
		config = Config{RouterEndpoint: fixtureRouter}
	default:
		c, inner, request = fixture(t, true)
		request = credentialRequest(t, request)
		if err := admit(c)(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}
	ledger := &callLedger{}
	native := &lockedNative{nativeFixture: inner, onCall: func(method string) func() {
		if method == "Get" {
			return ledger.call("router challenge", simulatedRouterLatency)
		}
		return ledger.call("native "+method, simulatedNativeLatency)
	}}
	r := &ExecutionWorkspaceReconciler{Client: ledgerClient(c, ledger), Control: native, Config: config, challengeClient: native}
	key := ctrl.Request{NamespacedName: clientKey(request.Key)}
	for range 16 {
		if _, err := r.Reconcile(t.Context(), key); err != nil {
			t.Fatal(err)
		}
		if workspace := bindingWorkspace(t, c, request.Key); workspace.Status.State == api.ExecutionWorkspaceStateReady {
			break
		}
	}
	workspace := bindingWorkspace(t, c, request.Key)
	if workspace.Status.State != api.ExecutionWorkspaceStateReady {
		t.Fatalf("fixture workspace is %s, want Ready", workspace.Status.State)
	}
	// Core's warm follow-up turn: a new attachment epoch, newly admitted.
	workspace.Generation++
	workspace.Spec.AttachmentEpoch++
	workspace.Spec.Attachment = &api.ExecutionWorkspaceAttachment{TaskRef: api.ObjectIdentityReference{Name: "turn", UID: "turn-uid"}, Epoch: workspace.Spec.AttachmentEpoch}
	workspace.Spec.CoreAdmission.AdmittedGeneration = workspace.Generation
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	workspace = bindingWorkspace(t, c, request.Key)
	sdk.SetCondition(&workspace.Status.Conditions, metav1.Condition{Type: string(api.ConditionWorkspaceAdmitted), Status: metav1.ConditionTrue, Reason: string(api.ReasonReady), ObservedGeneration: workspace.Generation})
	if err := c.Status().Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	if workspace.Status.ObservedGeneration == workspace.Generation {
		t.Fatal("the fixture did not bump the generation")
	}
	return r, c, inner, request, ledger
}

// TestWarmAttachPassBudget measures the pass that acknowledges a warm
// follow-up turn's attachment and publishes observedGeneration. Core's queue
// waits for that publication before the turn starts.
func TestWarmAttachPassBudget(t *testing.T) {
	for _, variant := range []string{"private", "resumed"} {
		t.Run(variant, func(t *testing.T) {
			r, c, _, request, ledger := warmAttach(t, variant)
			ledger.start()
			began := time.Now()
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: clientKey(request.Key)}); err != nil {
				t.Fatal(err)
			}
			counts := ledger.stop()
			published := ledger.published.Sub(began)
			t.Logf("warm attach (%s): observedGeneration published after %s\n%s", variant, published.Round(time.Millisecond), ledger.table())
			requireWarmReady(t, c, request)
			total := 0
			for name, n := range counts {
				total += n
				if strings.HasPrefix(name, "k8s update") || strings.HasPrefix(name, "k8s create") || strings.HasPrefix(name, "k8s patch") {
					t.Errorf("the warm attach wrote beyond its status: %s", name)
				}
			}
			if counts["k8s status patch ExecutionWorkspace"] != 2 {
				t.Errorf("the warm attach made %d status patches, want the acknowledgement and the publication", counts["k8s status patch ExecutionWorkspace"])
			}
			if total > warmAttachCalls || counts["router challenge"] != 1 || counts["native GetActor"] > 2 {
				t.Errorf("the warm attach made %d calls (%d challenge reads, %d Actor reads), budget %d", total, counts["router challenge"], counts["native GetActor"], warmAttachCalls)
			}
			// Wall-clock time is only meaningful without race instrumentation.
			if !raceEnabled && published > warmAttachBudget {
				t.Errorf("warm attach published observedGeneration after %s, budget %s", published.Round(time.Millisecond), warmAttachBudget)
			}
		})
	}
}

func requireWarmReady(t *testing.T, c client.Client, request sdk.WorkloadRequest) *api.ExecutionWorkspace {
	t.Helper()
	workspace := bindingWorkspace(t, c, request.Key)
	if workspace.Status.ObservedGeneration != workspace.Generation || workspace.Status.AttachedEpoch != workspace.Spec.AttachmentEpoch ||
		workspace.Status.Allocation == nil || workspace.Status.Allocation.State != sdk.AllocationReady || workspace.Status.Allocation.Startup == nil ||
		!sdk.ConditionIsTrue(workspace.Status.Conditions, string(api.ConditionWorkspaceDataPlaneReady)) {
		t.Fatalf("the warm attach did not publish a complete Ready observation: %+v", workspace.Status)
	}
	return workspace
}

// TestWarmAttachFallsBackToTheFullPassOnAnyChange checks that the warm pass
// never stands in for EnsureAllocation when that would act or fail.
func TestWarmAttachFallsBackToTheFullPassOnAnyChange(t *testing.T) {
	t.Run("unprotected journal", func(t *testing.T) {
		r, c, _, request, _ := warmAttach(t, "private")
		cm := &corev1.ConfigMap{}
		if err := c.Get(t.Context(), journalKey(request.Key), cm); err != nil {
			t.Fatal(err)
		}
		cm.Finalizers = nil
		if err := c.Update(t.Context(), cm); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: clientKey(request.Key)}); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(t.Context(), journalKey(request.Key), cm); err != nil || !slices.Contains(cm.Finalizers, journalProtectionFinalizer) {
			t.Fatalf("EnsureAllocation did not restore the journal's protection: %v", err)
		}
		requireWarmReady(t, c, request)
	})
	t.Run("moved Actor version", func(t *testing.T) {
		r, c, native, request, _ := warmAttach(t, "private")
		_, record, err := driver(c, native).read(t.Context(), request.Key)
		if err != nil {
			t.Fatal(err)
		}
		actor := native.actors[refKey(actorRef(record))]
		actor.Metadata.Version++
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: clientKey(request.Key)}); err == nil {
			t.Fatal("an Actor whose version moved after its challenge stayed Ready")
		}
		if workspace := bindingWorkspace(t, c, request.Key); workspace.Status.Allocation.State == sdk.AllocationReady || sdk.ConditionIsTrue(workspace.Status.Conditions, string(api.ConditionWorkspaceDataPlaneReady)) {
			t.Fatal("a changed process was published as Ready")
		}
	})
	t.Run("changed egress", func(t *testing.T) {
		r, c, native, request, _ := warmAttach(t, "private")
		_, record, err := driver(c, native).read(t.Context(), request.Key)
		if err != nil {
			t.Fatal(err)
		}
		native.egress[refKey(actorRef(record))].Rules = nil
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: clientKey(request.Key)}); err == nil {
			t.Fatal("a changed egress policy stayed Ready")
		}
	})
	t.Run("withdrawn credentials", func(t *testing.T) {
		r, c, native, request, _ := warmAttach(t, "private")
		r.Config = Config{RouterEndpoint: fixtureRouter}
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: clientKey(request.Key)}); err == nil {
			t.Fatal("withdrawn credential injection stayed Ready")
		}
		_, record, err := driver(c, native).read(t.Context(), request.Key)
		if err != nil {
			t.Fatal(err)
		}
		if rules := native.egress[refKey(actorRef(record))].GetRules(); len(rules) != 0 || !record.EgressDenyAll {
			t.Fatalf("withdrawn credential effects were not revoked: %v", rules)
		}
	})
	t.Run("withdrawn admission", func(t *testing.T) {
		r, c, _, request, _ := warmAttach(t, "private")
		workspace := bindingWorkspace(t, c, request.Key)
		workspace.Spec.CoreAdmission.AdmittedGeneration--
		if err := c.Update(t.Context(), workspace); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: clientKey(request.Key)}); err != nil {
			t.Fatal(err)
		}
		if workspace := bindingWorkspace(t, c, request.Key); workspace.Status.ObservedGeneration == workspace.Generation && workspace.Status.AttachedEpoch != 0 {
			t.Fatal("an unadmitted generation was acknowledged")
		}
	})
	t.Run("provider disabled during observation", func(t *testing.T) {
		r, c, native, request, _ := warmAttach(t, "private")
		disabled := false
		r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
			Get: func(ctx context.Context, w client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := w.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				// Disable the provider once the pass has read the journal during observation.
				if !disabled && key.Name == journalKey(request.Key).Name {
					disabled = true
					p := &api.ExecutionWorkspaceProvider{}
					if err := c.Get(ctx, client.ObjectKey{Name: "substrate"}, p); err != nil {
						return err
					}
					p.Spec.LifecycleState = api.ExecutionWorkspaceProviderDisabled
					if err := c.Update(ctx, p); err != nil {
						return err
					}
				}
				return nil
			},
		})
		_ = native
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: clientKey(request.Key)}); err == nil {
			t.Fatal("a workspace whose provider was disabled during observation must fail admission")
		}
		if workspace := bindingWorkspace(t, c, request.Key); workspace.Status.Allocation.State == sdk.AllocationReady || sdk.ConditionIsTrue(workspace.Status.Conditions, string(api.ConditionWorkspaceDataPlaneReady)) {
			t.Fatal("a workspace whose provider was disabled during observation was published Ready")
		}
	})
}

// warmAttachBudget bounds the simulated warm-attach pass, and
// warmAttachCalls the calls it makes.
const (
	warmAttachBudget = 300 * time.Millisecond
	warmAttachCalls  = 24
)
