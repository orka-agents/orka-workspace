package substrate

import (
	"context"
	"testing"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// pendingOnWorker reconciles a workspace whose worker is still registering
// until its poll has backed off to pendingPollMax, and returns the pool it
// waits on and its only worker.
func pendingOnWorker(t *testing.T, r *ExecutionWorkspaceReconciler, c client.Client, native *nativeFixture, request sdk.WorkloadRequest) (types.NamespacedName, *pb.Worker) {
	t.Helper()
	native.registering = true
	key := ctrl.Request{NamespacedName: clientKey(request.Key)}
	for _, want := range []time.Duration{200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second} {
		result, err := r.Reconcile(t.Context(), key)
		if err != nil || result.RequeueAfter != want {
			t.Fatalf("pending workspace requeued after %s (%v), want %s", result.RequeueAfter, err, want)
		}
	}
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	pool := types.NamespacedName{Namespace: record.Placement.Namespace, Name: record.RuntimePool.Name}
	if got := r.wakes.waiting(pool); len(got) != 1 || got[0] != key.NamespacedName {
		t.Fatalf("pending pass registered %v on %s, want the workspace", got, pool)
	}
	worker := native.workers[record.RuntimePool.Name+"-worker"]
	if worker == nil {
		t.Fatal("the private pool has no worker")
	}
	return pool, worker
}

func setPodReady(t *testing.T, c client.Client, worker *pb.Worker, ready bool) (old, current *corev1.Pod) {
	t.Helper()
	pod := &corev1.Pod{}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: worker.WorkerNamespace, Name: worker.WorkerPod}, pod); err != nil {
		t.Fatal(err)
	}
	old = pod.DeepCopy()
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}
	if err := c.Status().Update(t.Context(), pod); err != nil {
		if err := c.Update(t.Context(), pod); err != nil {
			t.Fatal(err)
		}
	}
	return old, pod
}

func activate(native *nativeFixture, worker *pb.Worker) {
	native.registering = false
	worker.Status.State = pb.WorkerState_WORKER_STATE_ACTIVE
}

// The private worker turns ready while its workspace's poll has backed off
// to a second. The Pod's Ready event enqueues the workspace at once on a
// queue whose clock never advances, so the wake comes from the event and not
// from the poll, and the pass it starts boots the Actor.
func TestWorkerPodReadyEventWakesTheWaitingWorkspaceBeforeItsPoll(t *testing.T) {
	c, native, request := fixture(t, false)
	r := &ExecutionWorkspaceReconciler{Client: c, Control: native, Config: credentialConfig(), challengeClient: native}
	key := ctrl.Request{NamespacedName: clientKey(request.Key)}
	_, worker := pendingOnWorker(t, r, c, native, request)

	clock := clocktesting.NewFakeClock(time.Now())
	queue := workqueue.NewTypedRateLimitingQueueWithConfig(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](), workqueue.TypedRateLimitingQueueConfig[reconcile.Request]{Clock: clock})
	defer queue.ShutDown()
	// The poll the last pending pass scheduled.
	queue.AddAfter(key, pendingPollMax)
	if queue.Len() != 0 {
		t.Fatal("the poll fired before its interval")
	}

	activate(native, worker)
	old, ready := setPodReady(t, c, worker, true)
	handler.EnqueueRequestsFromMapFunc(r.wakeWorkerWaiters).Update(t.Context(), event.UpdateEvent{ObjectOld: old, ObjectNew: ready}, queue)
	if queue.Len() != 1 {
		t.Fatalf("the worker Pod's Ready event enqueued %d requests before the poll, want 1", queue.Len())
	}
	woken, _ := queue.Get()
	queue.Done(woken)
	if woken != key {
		t.Fatalf("the event woke %v, want %v", woken, key)
	}
	// The event restarted the poll backoff of the run it woke.
	r.polls.mu.Lock()
	_, backedOff := r.polls.intervals[key.NamespacedName]
	r.polls.mu.Unlock()
	if backedOff {
		t.Fatal("the event kept the backed-off poll interval")
	}
	boots := native.boots
	for range 6 {
		result, err := r.Reconcile(t.Context(), woken)
		if err != nil {
			t.Fatal(err)
		}
		if native.boots > boots && result.RequeueAfter == providerHeartbeatPeriod {
			break
		}
	}
	if native.boots != boots+1 {
		t.Fatalf("the woken pass did not boot the Actor exactly once: %d boots", native.boots-boots)
	}
	if got := r.wakes.waiting(types.NamespacedName{Namespace: worker.WorkerNamespace, Name: worker.WorkerPool}); len(got) != 0 {
		t.Fatalf("a ready workspace still waits on its worker pool: %v", got)
	}
}

// Events wake only the workspaces waiting on that exact pool, from that pool's
// labelled worker Pod or its WorkerPool; anything else enqueues nothing.
func TestWorkerEventsWakeOnlyWorkspacesWaitingOnThatPool(t *testing.T) {
	r := &ExecutionWorkspaceReconciler{}
	pool := types.NamespacedName{Namespace: "native-workers", Name: "ate-a-workers"}
	other := types.NamespacedName{Namespace: "native-workers", Name: "ate-b-workers"}
	a, b, c := types.NamespacedName{Namespace: "tenant", Name: "a"}, types.NamespacedName{Namespace: "tenant", Name: "b"}, types.NamespacedName{Namespace: "tenant", Name: "c"}
	r.wakes.await(a, pool)
	r.wakes.await(b, pool)
	r.wakes.await(c, other)
	pod := func(namespace, poolName string) *corev1.Pod {
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "worker"}}
		if poolName != "" {
			p.Labels = map[string]string{workerPoolLabel: poolName}
		}
		return p
	}
	workerPool := func(namespace, name, kind string) *unstructured.Unstructured {
		u := poolObject()
		u.SetKind(kind)
		u.SetNamespace(namespace)
		u.SetName(name)
		return u
	}
	for name, test := range map[string]struct {
		obj  client.Object
		want []types.NamespacedName
	}{
		"pool pod":              {pod(pool.Namespace, pool.Name), []types.NamespacedName{a, b}},
		"workerpool":            {workerPool(pool.Namespace, pool.Name, "WorkerPool"), []types.NamespacedName{a, b}},
		"other pool pod":        {pod(other.Namespace, other.Name), []types.NamespacedName{c}},
		"unlabelled pod":        {pod(pool.Namespace, ""), nil},
		"same name elsewhere":   {pod("elsewhere", pool.Name), nil},
		"workerpool elsewhere":  {workerPool("elsewhere", pool.Name, "WorkerPool"), nil},
		"another kind":          {workerPool(pool.Namespace, pool.Name, "Deployment"), nil},
		"unrelated object type": {&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: pool.Namespace, Name: pool.Name, Labels: map[string]string{workerPoolLabel: pool.Name}}}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			got := r.wakeWorkerWaiters(context.Background(), test.obj)
			if len(got) != len(test.want) {
				t.Fatalf("woke %v, want %v", got, test.want)
			}
			for i, request := range got {
				if request.NamespacedName != test.want[i] {
					t.Fatalf("woke %v, want %v", got, test.want)
				}
			}
		})
	}
	// A workspace waits on one pool at a time, and forgetting it wakes it no
	// more.
	r.wakes.await(a, other)
	if got := r.wakes.waiting(pool); len(got) != 1 || got[0] != b {
		t.Fatalf("a workspace moved to another pool still waits on the first: %v", got)
	}
	r.wakes.forget(a)
	r.wakes.forget(b)
	if got := r.wakes.waiting(pool); len(got) != 0 {
		t.Fatalf("forgotten workspaces are still woken: %v", got)
	}
	if got := r.wakes.waiting(other); len(got) != 1 || got[0] != c {
		t.Fatalf("forgetting one workspace dropped another: %v", got)
	}
}

// A worker whose exact Pod Kubernetes already shows Ready, but whose native
// Worker has not caught up, polls at nativeLagPoll for at most
// nativeLagPolls passes; an unready Pod or a Pod of another UID keeps the
// ordinary backoff, and events cannot extend the fast polls.
func TestNativeLagPollIsShortAndBounded(t *testing.T) {
	c, native, request := fixture(t, false)
	r := &ExecutionWorkspaceReconciler{Client: c, Control: native, Config: credentialConfig(), challengeClient: native}
	key := ctrl.Request{NamespacedName: clientKey(request.Key)}
	pool, worker := pendingOnWorker(t, r, c, native, request)
	podEvent := func(pod *corev1.Pod) {
		r.wakeWorkerWaiters(t.Context(), pod)
	}

	// Not Ready: the ordinary backoff, still at its cap.
	_, unready := setPodReady(t, c, worker, false)
	if result, err := r.Reconcile(t.Context(), key); err != nil || result.RequeueAfter != pendingPollMax {
		t.Fatalf("an unready worker Pod requeued after %s (%v), want %s", result.RequeueAfter, err, pendingPollMax)
	}
	podEvent(unready)

	// Ready in Kubernetes, still registering natively.
	_, ready := setPodReady(t, c, worker, true)
	for i := range nativeLagPolls {
		if i == nativeLagPolls/2 {
			podEvent(ready)
		}
		if result, err := r.Reconcile(t.Context(), key); err != nil || result.RequeueAfter != nativeLagPoll {
			t.Fatalf("native-lag poll %d requeued after %s (%v), want %s", i, result.RequeueAfter, err, nativeLagPoll)
		}
	}
	podEvent(ready)
	if result, err := r.Reconcile(t.Context(), key); err != nil || result.RequeueAfter != pendingPollInitial {
		t.Fatalf("past its native-lag bound the pass requeued after %s (%v), want the ordinary backoff", result.RequeueAfter, err)
	}

	// A Ready Pod of another UID proves nothing about this worker.
	r.polls.reset(key.NamespacedName)
	saved := worker.WorkerPodUid
	worker.WorkerPodUid = "replaced-uid"
	if result, err := r.Reconcile(t.Context(), key); err != nil || result.RequeueAfter != pendingPollInitial {
		t.Fatalf("a replaced worker Pod requeued after %s (%v), want the ordinary backoff", result.RequeueAfter, err)
	}
	worker.WorkerPodUid = saved
	if got := r.wakes.waiting(pool); len(got) != 1 {
		t.Fatalf("a pending pass did not keep waiting on its pool: %v", got)
	}
}

// A wake only starts a pass. Every pre-Resume fence still holds on the pass an
// event starts: a replaced worker Pod, a worker that is not empty, or a
// foreign assignment refuses the Resume.
func TestWorkerEventWakeKeepsEveryPreResumeFence(t *testing.T) {
	for _, change := range []string{"workerPodUID", "allocated", "foreignAssignment", "workerSize", "workerDeleted"} {
		t.Run(change, func(t *testing.T) {
			c, native, request := fixture(t, false)
			r := &ExecutionWorkspaceReconciler{Client: c, Control: native, Config: credentialConfig(), challengeClient: native}
			key := ctrl.Request{NamespacedName: clientKey(request.Key)}
			_, worker := pendingOnWorker(t, r, c, native, request)
			activate(native, worker)
			old, ready := setPodReady(t, c, worker, true)
			switch change {
			case "workerPodUID":
				worker.WorkerPodUid = "replaced-uid"
			case "allocated":
				worker.Status.Allocated.Actors = 1
			case "foreignAssignment":
				native.foreignAssignment = true
			case "workerSize":
				worker.Status.Capacity.CpuMillicores *= 2
			case "workerDeleted":
				if err := c.Delete(t.Context(), ready); err != nil {
					t.Fatal(err)
				}
			}
			queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer queue.ShutDown()
			handler.EnqueueRequestsFromMapFunc(r.wakeWorkerWaiters).Update(t.Context(), event.UpdateEvent{ObjectOld: old, ObjectNew: ready}, queue)
			if queue.Len() != 1 {
				t.Fatal("the event did not wake the workspace")
			}
			for range 3 {
				_, _ = r.Reconcile(t.Context(), key)
			}
			if native.boots != 0 {
				t.Fatalf("a woken pass resumed onto a worker that fails its %s fence", change)
			}
		})
	}
}

// A workspace gone, or bound elsewhere, drops its wake registration.
func TestWorkerWakeRegistrationIsDroppedWithTheWorkspace(t *testing.T) {
	c, native, request := fixture(t, false)
	r := &ExecutionWorkspaceReconciler{Client: c, Control: native, Config: credentialConfig(), challengeClient: native}
	key := ctrl.Request{NamespacedName: clientKey(request.Key)}
	pool, _ := pendingOnWorker(t, r, c, native, request)
	execution := &api.ExecutionWorkspace{}
	if err := c.Get(t.Context(), key.NamespacedName, execution); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), execution); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	if got := r.wakes.waiting(pool); len(got) != 0 {
		t.Fatalf("a deleted workspace is still woken by its pool: %v", got)
	}
}
