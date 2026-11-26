package substrate

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/grpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// lockedNative serializes the in-memory native fixture, as one control plane
// serializes its own state, so workspaces can be driven concurrently.
type lockedNative struct {
	mu sync.Mutex
	*nativeFixture
	// onCall, when set, observes each call before it takes the lock and
	// returns a function run when the call completes.
	onCall func(method string) func()
}

func (l *lockedNative) enter(method string) func() {
	done := func() {}
	if l.onCall != nil {
		done = l.onCall(method)
	}
	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		done()
	}
}

func (l *lockedNative) GetActorTemplate(ctx context.Context, req *pb.GetActorTemplateRequest, opts ...grpc.CallOption) (*pb.ActorTemplate, error) {
	defer l.enter("GetActorTemplate")()
	return l.nativeFixture.GetActorTemplate(ctx, req, opts...)
}
func (l *lockedNative) CreateActorTemplate(ctx context.Context, req *pb.CreateActorTemplateRequest, opts ...grpc.CallOption) (*pb.ActorTemplate, error) {
	defer l.enter("CreateActorTemplate")()
	return l.nativeFixture.CreateActorTemplate(ctx, req, opts...)
}
func (l *lockedNative) DeleteActorTemplate(ctx context.Context, req *pb.DeleteActorTemplateRequest, opts ...grpc.CallOption) (*pb.ActorTemplate, error) {
	defer l.enter("DeleteActorTemplate")()
	return l.nativeFixture.DeleteActorTemplate(ctx, req, opts...)
}
func (l *lockedNative) ListActorTemplates(ctx context.Context, req *pb.ListActorTemplatesRequest, opts ...grpc.CallOption) (*pb.ListActorTemplatesResponse, error) {
	defer l.enter("ListActorTemplates")()
	return l.nativeFixture.ListActorTemplates(ctx, req, opts...)
}
func (l *lockedNative) GetActor(ctx context.Context, req *pb.GetActorRequest, opts ...grpc.CallOption) (*pb.Actor, error) {
	defer l.enter("GetActor")()
	return l.nativeFixture.GetActor(ctx, req, opts...)
}
func (l *lockedNative) CreateActor(ctx context.Context, req *pb.CreateActorRequest, opts ...grpc.CallOption) (*pb.Actor, error) {
	defer l.enter("CreateActor")()
	return l.nativeFixture.CreateActor(ctx, req, opts...)
}
func (l *lockedNative) UpdateActor(ctx context.Context, req *pb.UpdateActorRequest, opts ...grpc.CallOption) (*pb.Actor, error) {
	defer l.enter("UpdateActor")()
	return l.nativeFixture.UpdateActor(ctx, req, opts...)
}
func (l *lockedNative) ResumeActor(ctx context.Context, req *pb.ResumeActorRequest, opts ...grpc.CallOption) (*pb.ResumeActorResponse, error) {
	defer l.enter("ResumeActor")()
	return l.nativeFixture.ResumeActor(ctx, req, opts...)
}
func (l *lockedNative) SuspendActor(ctx context.Context, req *pb.SuspendActorRequest, opts ...grpc.CallOption) (*pb.SuspendActorResponse, error) {
	defer l.enter("SuspendActor")()
	return l.nativeFixture.SuspendActor(ctx, req, opts...)
}
func (l *lockedNative) DeleteActor(ctx context.Context, req *pb.DeleteActorRequest, opts ...grpc.CallOption) (*pb.Actor, error) {
	defer l.enter("DeleteActor")()
	return l.nativeFixture.DeleteActor(ctx, req, opts...)
}
func (l *lockedNative) GetActorEgressPolicy(ctx context.Context, req *pb.GetActorEgressPolicyRequest, opts ...grpc.CallOption) (*pb.EgressPolicy, error) {
	defer l.enter("GetActorEgressPolicy")()
	return l.nativeFixture.GetActorEgressPolicy(ctx, req, opts...)
}
func (l *lockedNative) CreateActorEgressPolicy(ctx context.Context, req *pb.CreateActorEgressPolicyRequest, opts ...grpc.CallOption) (*pb.EgressPolicy, error) {
	defer l.enter("CreateActorEgressPolicy")()
	return l.nativeFixture.CreateActorEgressPolicy(ctx, req, opts...)
}
func (l *lockedNative) UpdateActorEgressPolicy(ctx context.Context, req *pb.UpdateActorEgressPolicyRequest, opts ...grpc.CallOption) (*pb.EgressPolicy, error) {
	defer l.enter("UpdateActorEgressPolicy")()
	return l.nativeFixture.UpdateActorEgressPolicy(ctx, req, opts...)
}
func (l *lockedNative) GetWorker(ctx context.Context, req *pb.GetWorkerRequest, opts ...grpc.CallOption) (*pb.Worker, error) {
	defer l.enter("GetWorker")()
	return l.nativeFixture.GetWorker(ctx, req, opts...)
}
func (l *lockedNative) ListWorkers(ctx context.Context, req *pb.ListWorkersRequest, opts ...grpc.CallOption) (*pb.ListWorkersResponse, error) {
	defer l.enter("ListWorkers")()
	return l.nativeFixture.ListWorkers(ctx, req, opts...)
}
func (l *lockedNative) ListWorkerActorAssignments(ctx context.Context, req *pb.ListWorkerActorAssignmentsRequest, opts ...grpc.CallOption) (*pb.ListWorkerActorAssignmentsResponse, error) {
	defer l.enter("ListWorkerActorAssignments")()
	return l.nativeFixture.ListWorkerActorAssignments(ctx, req, opts...)
}
func (l *lockedNative) CreateTag(ctx context.Context, req *pb.CreateTagRequest, opts ...grpc.CallOption) (*pb.Tag, error) {
	defer l.enter("CreateTag")()
	return l.nativeFixture.CreateTag(ctx, req, opts...)
}
func (l *lockedNative) GetTag(ctx context.Context, req *pb.GetTagRequest, opts ...grpc.CallOption) (*pb.Tag, error) {
	defer l.enter("GetTag")()
	return l.nativeFixture.GetTag(ctx, req, opts...)
}
func (l *lockedNative) DeleteTag(ctx context.Context, req *pb.DeleteTagRequest, opts ...grpc.CallOption) (*pb.Tag, error) {
	defer l.enter("DeleteTag")()
	return l.nativeFixture.DeleteTag(ctx, req, opts...)
}
func (l *lockedNative) Get(ctx context.Context, endpoint, target string) (bootstrapChallenge, error) {
	defer l.enter("Get")()
	return l.nativeFixture.Get(ctx, endpoint, target)
}

func untilState(step func() (sdk.AllocationObservation, error), want sdk.AllocationState) (sdk.AllocationObservation, error) {
	var lastErr error
	for range 200 {
		observed, err := step()
		if err == nil && observed.State == want {
			return observed, nil
		}
		if err != nil {
			lastErr = err
		}
	}
	return sdk.AllocationObservation{}, fmt.Errorf("never reached %s: %w", want, lastErr)
}

func runConcurrently(t *testing.T, requests []sdk.WorkloadRequest, run func(int, sdk.WorkloadRequest) error) {
	t.Helper()
	var wg sync.WaitGroup
	errs := make([]error, len(requests))
	for i, request := range requests {
		wg.Go(func() { errs[i] = run(i, request) })
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("%s: %v", requests[i].Key.Name, err)
		}
	}
}

func clonePrivateWorkspace(t *testing.T, c client.Client, base sdk.WorkloadRequest, name string) sdk.WorkloadRequest {
	t.Helper()
	source := &api.ExecutionWorkspace{}
	if err := c.Get(t.Context(), clientKey(base.Key), source); err != nil {
		t.Fatal(err)
	}
	ws := source.DeepCopy()
	ws.ResourceVersion = ""
	ws.Name = name
	ws.UID = types.UID(name + "-uid")
	req := *base.DeepCopy()
	req.Key.Namespace = ws.Namespace
	req.Key.Name = ws.Name
	req.Key.WorkspaceUID = ws.UID
	req.Revision, _ = sdk.WorkloadRevision(req)
	ws.Spec.Workload = &req
	if err := c.Create(t.Context(), ws); err != nil {
		t.Fatal(err)
	}
	return req
}

// TestConcurrentPrivateWorkspacesClaimSparesSafely drives concurrent private
// workspaces through cold start, spare claim, suspension and resume while the
// spare replenisher sweeps in parallel, verifying -race safety and exclusive
// private pool ownership.
func TestConcurrentPrivateWorkspacesClaimSparesSafely(t *testing.T) {
	c, inner, base := fixture(t, true)
	setSpareAnnotation(t, c, "3")
	native := &lockedNative{nativeFixture: inner}
	repl := &SpareWorkerReplenisher{Client: c, Control: native, Config: Config{RouterEndpoint: fixtureRouter}}
	if err := repl.Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}

	requests := []sdk.WorkloadRequest{base}
	for i := 1; i < 6; i++ {
		requests = append(requests, clonePrivateWorkspace(t, c, base, fmt.Sprintf("ws-%d", i)))
	}

	stopSweep := make(chan struct{})
	var sweepWG sync.WaitGroup
	sweepWG.Go(func() {
		for {
			select {
			case <-stopSweep:
				return
			default:
				_ = repl.Sweep(t.Context())
				time.Sleep(5 * time.Millisecond)
			}
		}
	})
	defer func() {
		close(stopSweep)
		sweepWG.Wait()
	}()

	d := func() *Lifecycle {
		return &Lifecycle{client: c, control: native, config: Config{RouterEndpoint: fixtureRouter}, challengeClient: native}
	}

	identities := make([]sdk.InstanceIdentity, len(requests))
	runConcurrently(t, requests, func(i int, req sdk.WorkloadRequest) error {
		obs, err := untilState(func() (sdk.AllocationObservation, error) {
			return d().EnsureAllocation(t.Context(), req)
		}, sdk.AllocationReady)
		if err != nil {
			return err
		}
		identities[i] = obs.Identity
		return nil
	})

	// Every private workspace must own a distinct WorkerPool.
	pools := map[string]string{}
	for _, req := range requests {
		_, rec, err := d().read(t.Context(), req.Key)
		if err != nil {
			t.Fatal(err)
		}
		if prev, dup := pools[rec.RuntimePool.Name]; dup {
			t.Fatalf("private pool %s claimed by both %s and %s", rec.RuntimePool.Name, prev, req.Key.Name)
		}
		pools[rec.RuntimePool.Name] = req.Key.Name
	}

	// Suspend all concurrently, then resume all concurrently.
	retained := make([]*api.RetainedDataReference, len(requests))
	runConcurrently(t, requests, func(i int, req sdk.WorkloadRequest) error {
		obs, err := untilState(func() (sdk.AllocationObservation, error) {
			return d().SuspendInstance(t.Context(), req.Key, identities[i], nil)
		}, sdk.AllocationStopped)
		if err != nil {
			return err
		}
		retained[i] = obs.RetainedData
		return nil
	})

	resumed := make([]sdk.WorkloadRequest, len(requests))
	for i, req := range requests {
		next := *req.DeepCopy()
		next.Sequence = 2
		prev := identities[i]
		next.PreviousInstance = &prev
		next.RetainedData = retained[i]
		next.Runtime.Template.Spec.Containers[0].Env[0].Value = fmt.Sprintf("nonce-seq2-%d", i)
		next.Revision, _ = sdk.WorkloadRevision(next)
		if err := admit(c)(t.Context(), next); err != nil {
			t.Fatal(err)
		}
		resumed[i] = next
	}

	runConcurrently(t, resumed, func(_ int, req sdk.WorkloadRequest) error {
		_, err := untilState(func() (sdk.AllocationObservation, error) {
			return d().EnsureAllocation(t.Context(), req)
		}, sdk.AllocationReady)
		return err
	})
}

func TestPendingPollsStartShortAndBackOffPerWorkspace(t *testing.T) {
	polls := &pendingPolls{}
	a := types.NamespacedName{Namespace: "tenant", Name: "a"}
	b := types.NamespacedName{Namespace: "tenant", Name: "b"}
	for _, want := range []time.Duration{200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second, time.Second} {
		if got := polls.next(a); got != want {
			t.Fatalf("poll interval = %s, want %s", got, want)
		}
	}
	if got := polls.next(b); got != pendingPollInitial {
		t.Fatalf("another workspace's first poll = %s", got)
	}
	polls.reset(a)
	if got := polls.next(a); got != pendingPollInitial {
		t.Fatalf("a new pending run starts at %s", got)
	}
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			key := types.NamespacedName{Namespace: "tenant", Name: fmt.Sprint(i % 4)}
			for range 8 {
				if got := polls.next(key); got < pendingPollInitial || got > pendingPollMax {
					t.Errorf("poll interval %s is out of bounds", got)
				}
			}
			polls.reset(key)
		})
	}
	wg.Wait()
}

func TestReconcilePollsAPendingWorkspaceWithBackoff(t *testing.T) {
	c, native, request := fixture(t, false)
	other := clonePrivateWorkspace(t, c, request, "other")
	native.registering = true
	r := &ExecutionWorkspaceReconciler{Client: c, Control: native, Config: credentialConfig()}
	key := ctrl.Request{NamespacedName: clientKey(request.Key)}
	for _, want := range []time.Duration{200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second} {
		result, err := r.Reconcile(t.Context(), key)
		if err != nil || result.RequeueAfter != want {
			t.Fatalf("pending workspace requeued after %s (%v), want %s", result.RequeueAfter, err, want)
		}
	}
	native.registering = false
	for _, worker := range native.workers {
		worker.Status.State = pb.WorkerState_WORKER_STATE_ACTIVE
	}
	if _, err := r.Reconcile(t.Context(), key); err == nil {
		t.Fatal("the bootstrap challenge was read without a router")
	}
	r.polls.mu.Lock()
	remaining := len(r.polls.intervals)
	r.polls.mu.Unlock()
	if remaining != 0 {
		t.Fatal("an ended pending run kept its poll state")
	}
	native.registering = true
	otherKey := ctrl.Request{NamespacedName: clientKey(other.Key)}
	if result, err := r.Reconcile(t.Context(), otherKey); err != nil || result.RequeueAfter != pendingPollInitial {
		t.Fatalf("pending workspace requeued after %s (%v)", result.RequeueAfter, err)
	}
	workspace := &api.ExecutionWorkspace{}
	if err := c.Get(t.Context(), otherKey.NamespacedName, workspace); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), otherKey); err != nil {
		t.Fatal(err)
	}
	r.polls.mu.Lock()
	defer r.polls.mu.Unlock()
	if len(r.polls.intervals) != 0 {
		t.Fatal("a deleted workspace kept its poll state")
	}
}

func TestMaxConcurrentReconcilesIsBounded(t *testing.T) {
	for _, n := range []int{1, DefaultMaxConcurrentReconciles, MaxMaxConcurrentReconciles} {
		if err := ValidateMaxConcurrentReconciles(n); err != nil {
			t.Fatalf("%d: %v", n, err)
		}
	}
	for _, n := range []int{-1, 0, MaxMaxConcurrentReconciles + 1} {
		if err := ValidateMaxConcurrentReconciles(n); err == nil || !strings.Contains(err.Error(), "--max-concurrent-reconciles") {
			t.Fatalf("%d was accepted: %v", n, err)
		}
	}
	if got := (&ExecutionWorkspaceReconciler{}).maxConcurrentReconciles(); got != DefaultMaxConcurrentReconciles {
		t.Fatalf("unset concurrency = %d", got)
	}
	if got := (&ExecutionWorkspaceReconciler{MaxConcurrentReconciles: -2}).maxConcurrentReconciles(); got != -2 {
		t.Fatalf("negative concurrency = %d, want -2", got)
	}
}

var _ = metav1.Time{}
