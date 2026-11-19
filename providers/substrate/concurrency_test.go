package substrate

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	profile "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/grpc"
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
	// The observer's simulated round trip runs before the lock, so calls in
	// flight at once overlap as they would against a real control plane.
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

// untilState repeats one lifecycle operation, as reconciliation does, until
// it reports want. Errors are retried like a requeue; the last is returned.
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

// runConcurrently runs one function per request in parallel and joins their
// errors.
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

// TestConcurrentWorkspacesClaimSparesAndSharedSlotsSafely drives different
// workspaces through cold start, spare and shared admission, suspension and
// resume at once, while the leader's sweeps run, as parallel reconciles do.
// Run with -race, it also checks every piece of shared in-memory state.
func TestConcurrentWorkspacesClaimSparesAndSharedSlotsSafely(t *testing.T) {
	c, inner, base := fixture(t, true)
	useTestClock(t)
	const maxActors, sharedCount, privateCount = 3, 7, 3
	binding := createSharedProfile(t, c, "tenant", "shared", "shared-profile-uid", maxActors)
	private := base.ParametersBinding.DeepCopy()
	setSpareAnnotation(t, c, "2")
	native := &lockedNative{nativeFixture: inner}
	sweeper := &SpareWorkerReplenisher{Client: c, Control: native, Config: sharedConfig()}
	if err := sweeper.Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := len(spares(t, c, inner)); got != 2 {
		t.Fatalf("spares = %d, want 2", got)
	}
	var requests []sdk.WorkloadRequest
	for i := range sharedCount {
		name := fmt.Sprintf("shared-%d", i)
		requests = append(requests, sharedWorkspace(t, c, base, "tenant", name, types.UID(name+"-uid"), binding))
	}
	for i := range privateCount {
		name := fmt.Sprintf("private-%d", i)
		requests = append(requests, sharedWorkspace(t, c, base, "tenant", name, types.UID(name+"-uid"), private))
	}
	lifecycle := func() *Lifecycle {
		d := New(c, native, sharedConfig())
		d.challengeClient = native
		return d
	}
	// The leader's sweeps and bounds loop race every phase.
	stop := make(chan struct{})
	var sweeps sync.WaitGroup
	sweeps.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = sweeper.Sweep(t.Context())
			_ = sweeper.EnforceSharedBounds(t.Context())
			time.Sleep(time.Millisecond)
		}
	})
	defer func() {
		close(stop)
		sweeps.Wait()
	}()

	// Cold starts, all at once.
	ready := make([]sdk.AllocationObservation, len(requests))
	runConcurrently(t, requests, func(i int, request sdk.WorkloadRequest) error {
		var err error
		ready[i], err = untilState(func() (sdk.AllocationObservation, error) { return lifecycle().EnsureAllocation(t.Context(), request) }, sdk.AllocationReady)
		return err
	})
	coldPools := requireConcurrentPlacements(t, c, lifecycle(), requests, sharedCount)
	if want := (sharedCount + maxActors - 1) / maxActors; len(coldPools) != want {
		t.Fatalf("concurrent cold starts used %d shared pools, want %d: each burst must fill a pool before creating another", len(coldPools), want)
	}

	// Suspensions, all at once.
	stopped := make([]sdk.AllocationObservation, len(requests))
	runConcurrently(t, requests, func(i int, request sdk.WorkloadRequest) error {
		var err error
		stopped[i], err = untilState(func() (sdk.AllocationObservation, error) {
			tick()
			return lifecycle().SuspendInstance(t.Context(), request.Key, ready[i].Identity)
		}, sdk.AllocationStopped)
		if err == nil && stopped[i].RetainedData == nil {
			err = fmt.Errorf("suspension retained no data")
		}
		return err
	})
	for i, request := range requests[:sharedCount] {
		_, record, err := lifecycle().read(t.Context(), request.Key)
		if err != nil {
			t.Fatal(err)
		}
		if shared := record.SharedWorker; shared == nil || shared.Proof == nil || shared.Strong != "" || !shared.Released {
			t.Fatalf("%s: concurrent suspension did not take the interim path: %#v", requests[i].Key.Name, shared)
		}
	}

	// Resumes from each Tag, all at once.
	successors := make([]sdk.WorkloadRequest, len(requests))
	for i, request := range requests {
		next := *request.DeepCopy()
		next.Sequence++
		next.PreviousInstance = &ready[i].Identity
		next.RetainedData = stopped[i].RetainedData
		next.Runtime.Template.Spec.Containers[0].Env[0].Value = "rotated-public-nonce"
		next.Revision, _ = sdk.WorkloadRevision(next)
		if err := admit(c)(t.Context(), next); err != nil {
			t.Fatal(err)
		}
		successors[i] = next
	}
	runConcurrently(t, successors, func(_ int, request sdk.WorkloadRequest) error {
		_, err := untilState(func() (sdk.AllocationObservation, error) { return lifecycle().EnsureAllocation(t.Context(), request) }, sdk.AllocationReady)
		return err
	})
	requireConcurrentPlacements(t, c, lifecycle(), successors, sharedCount)
	native.mu.Lock()
	boots := inner.boots
	native.mu.Unlock()
	if want := 2 * len(requests); boots != want {
		t.Fatalf("native boots = %d, want %d: a boot was replayed or skipped", boots, want)
	}
}

// requireConcurrentPlacements checks every Ready record: shared records hold
// a ledger entry with their Actor UID in an admitting pool of at most its
// slot count, and no private pool or spare is held by two records. It
// returns the shared pools in use.
func requireConcurrentPlacements(t *testing.T, c client.Client, d *Lifecycle, requests []sdk.WorkloadRequest, sharedCount int) map[string]bool {
	t.Helper()
	sharedPools := map[string]bool{}
	privatePools := map[string]string{}
	for i, request := range requests {
		cm, record, err := d.read(t.Context(), request.Key)
		if err != nil {
			t.Fatal(err)
		}
		if record.Observation.State != sdk.AllocationReady || record.Request.Sequence != request.Sequence {
			t.Fatalf("%s is not Ready at sequence %d", request.Key.Name, request.Sequence)
		}
		if i < sharedCount {
			if record.SharedWorker == nil || record.SharedWorker.UID == "" {
				t.Fatalf("%s did not take a shared slot", request.Key.Name)
			}
			_, ledger := poolLedger(t, c, record.SharedWorker.Pool)
			entry := ledgerEntry(ledger, claimMarker(cm, record))
			if entry == nil || entry.ActorUID != record.Actor.UID || ledger.Worker == nil || ledger.Worker.PodUID != record.Worker.PodUID {
				t.Fatalf("%s is not in its pool's ledger: %#v", request.Key.Name, ledger)
			}
			sharedPools[record.SharedWorker.Pool] = true
			continue
		}
		if record.SharedWorker != nil {
			t.Fatalf("%s of a private profile shares a worker", request.Key.Name)
		}
		if holder, taken := privatePools[record.RuntimePool.Name]; taken {
			t.Fatalf("%s and %s hold one private pool", holder, request.Key.Name)
		}
		privatePools[record.RuntimePool.Name] = request.Key.Name
		if spare := record.SpareWorker; spare != nil {
			pool, found := getPool(t, c, spare.Name)
			if !found || pool.GetAnnotations()[spareClaimAnnotation] != claimMarker(cm, record) {
				t.Fatalf("%s holds a spare its claim marker does not name", request.Key.Name)
			}
		}
	}
	for name := range sharedPools {
		_, ledger := poolLedger(t, c, name)
		if len(ledger.Entries) > int(ledger.Scope.MaxActors) || ledger.State != sharedAdmitting {
			t.Fatalf("shared pool %s is overfilled or closed: %#v", name, ledger)
		}
	}
	return sharedPools
}

func TestScopeLocksSerializeOneScopeAndHonourCancellation(t *testing.T) {
	locks := &scopeLocks{}
	a := sharedScope{Namespace: "a"}
	b := sharedScope{Namespace: "b"}
	unlockA, err := locks.lock(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	// Another scope is independent.
	unlockB, err := locks.lock(t.Context(), b)
	if err != nil {
		t.Fatal(err)
	}
	unlockB()
	// The held scope blocks until its context ends.
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := locks.lock(ctx, a); err == nil {
		t.Fatal("a held scope was acquired twice")
	}
	unlockA()
	var wg sync.WaitGroup
	inside, most := 0, 0
	var mu sync.Mutex
	for range 16 {
		wg.Go(func() {
			unlock, err := locks.lock(t.Context(), a)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			inside++
			most = max(most, inside)
			mu.Unlock()
			time.Sleep(time.Millisecond)
			mu.Lock()
			inside--
			mu.Unlock()
			unlock()
		})
	}
	wg.Wait()
	if most != 1 {
		t.Fatalf("%d holders of one scope at once", most)
	}
	locks.mu.Lock()
	defer locks.mu.Unlock()
	if len(locks.entries) != 0 {
		t.Fatalf("released scopes left %d entries", len(locks.entries))
	}
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
	// Created first: it copies the fixture workspace before any status.
	other := sharedWorkspace(t, c, request, "tenant", "other", "other-uid", request.ParametersBinding)
	native.registering = true
	r := &ExecutionWorkspaceReconciler{Client: c, Control: native, Config: credentialConfig()}
	key := ctrl.Request{NamespacedName: clientKey(request.Key)}
	for _, want := range []time.Duration{200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second} {
		result, err := r.Reconcile(t.Context(), key)
		if err != nil || result.RequeueAfter != want {
			t.Fatalf("pending workspace requeued after %s (%v), want %s", result.RequeueAfter, err, want)
		}
	}
	// The worker registers and the pass moves on to the bootstrap challenge,
	// which this reconciler's router cannot reach: the pending run ends and
	// the error requeues through the controller's rate limiter.
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
	// A deleted workspace drops its poll state too.
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
}

// TestSharedWarmReserveServesAResumeInOnePass keeps an emptied pool as the
// profile's warm reserve well past the idle grace, and resumes onto its
// running worker in a single pass: the template rotation and the ledger's
// Actor UID write each continue the pass instead of waiting for a poll.
func TestSharedWarmReserveServesAResumeInOnePass(t *testing.T) {
	c, native, first, _ := sharedFixture(t, 2)
	item := &profile.SubstrateWorkspaceProfile{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "tenant", Name: "shared"}, item); err != nil {
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
	a, record := sharedReady(t, c, native, first)
	if record.SharedWorker.Pool != warm[0].GetName() {
		t.Fatal("the cold start did not take the warm slot")
	}
	stopped, _ := sharedRetired(t, c, native, first, a.Identity, true)
	// Long idle: the emptied pool is the profile's only reserve, so it stays.
	for range 3 {
		setNow(t, testNow().Add(sharedIdleGrace))
		if err := sharedReplenisher(c, native).Sweep(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if _, ledger := poolLedger(t, c, warm[0].GetName()); ledger.State != sharedAdmitting || len(ledger.Entries) != 0 {
		t.Fatalf("the reserve pool is not an empty admitting pool: %#v", ledger)
	}
	next := *first.DeepCopy()
	next.Sequence++
	next.PreviousInstance = &a.Identity
	next.RetainedData = stopped.RetainedData
	next.Runtime.Template.Spec.Containers[0].Env[0].Value = "rotated-public-nonce"
	next.Revision, _ = sdk.WorkloadRevision(next)
	if err := admit(c)(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	boots := native.boots
	observed, err := sharedDriver(c, native).EnsureAllocation(t.Context(), next)
	if err != nil || observed.State != sdk.AllocationReady {
		t.Fatalf("a resume onto a running shared worker took more than one pass: %s %v", observed.State, err)
	}
	_, resumed, err := sharedDriver(c, native).read(t.Context(), next.Key)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.SharedWorker.Pool != warm[0].GetName() || len(sharedPools(t, c)) != 1 || native.boots != boots+1 {
		t.Fatalf("the resume did not land on the reserve's running worker: %#v", resumed.SharedWorker)
	}
	if resumed.InheritedCheckpoint == nil || resumed.Timing.TemplateUpdateObservedAt == nil || resumed.CreateTemplate.Name == resumed.Template.Name {
		t.Fatal("the resume did not restore its Tag under the rotated bootstrap template")
	}
	if resumed.Worker == nil || resumed.Worker.PodUID != record.Worker.PodUID {
		t.Fatal("the resume did not reuse the fenced reserve worker")
	}
}
