package substrate

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8slabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// A pass that cannot Resume yet because its worker is not ready ends
// pending. Waiting only for the next poll leaves up to pendingPollMax of
// slack after the worker turns ready. So the workspace reconciler also
// watches worker Pods and WorkerPools: an event on the pool a pass waits on
// wakes that workspace at once.
//
// The watches only decide when the next pass runs. They never feed a
// decision: every pass still reads the journal, pool, Pod and native Worker
// through the uncached client and repeats every fence. A missed or early
// event costs at most the poll it replaces, which stays as the fallback.
//
// Agent Substrate marks a Worker ACTIVE right after its Pod turns Ready,
// copies WorkerPool labels onto it, and has the worker process report its
// epoch and runtimes. None of those native steps is visible through
// Kubernetes. A pass whose worker Kubernetes already shows ready, but whose
// native Worker has not caught up, therefore polls at nativeLagPoll for at
// most nativeLagPolls passes before the ordinary backoff resumes.
const (
	nativeLagPoll  = 100 * time.Millisecond
	nativeLagPolls = 20
)

// workerWakes indexes, per worker pool, the workspaces whose latest pass
// waits on it. A workspace waits on at most one pool, its record's. Entries
// are dropped when a pass starts, when it ends without pending, and when
// the workspace is gone.
type workerWakes struct {
	mu         sync.Mutex
	pools      map[types.NamespacedName]map[types.NamespacedName]struct{}
	workspaces map[types.NamespacedName]types.NamespacedName
}

func (w *workerWakes) await(workspace, pool types.NamespacedName) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.forgetLocked(workspace)
	if w.pools == nil {
		w.pools = map[types.NamespacedName]map[types.NamespacedName]struct{}{}
		w.workspaces = map[types.NamespacedName]types.NamespacedName{}
	}
	if w.pools[pool] == nil {
		w.pools[pool] = map[types.NamespacedName]struct{}{}
	}
	w.pools[pool][workspace] = struct{}{}
	w.workspaces[workspace] = pool
}

func (w *workerWakes) forget(workspace types.NamespacedName) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.forgetLocked(workspace)
}

func (w *workerWakes) forgetLocked(workspace types.NamespacedName) {
	pool, found := w.workspaces[workspace]
	if !found {
		return
	}
	delete(w.workspaces, workspace)
	delete(w.pools[pool], workspace)
	if len(w.pools[pool]) == 0 {
		delete(w.pools, pool)
	}
}

// waiting returns the workspaces waiting on pool, in a stable order.
func (w *workerWakes) waiting(pool types.NamespacedName) []types.NamespacedName {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]types.NamespacedName, 0, len(w.pools[pool]))
	for workspace := range w.pools[pool] {
		out = append(out, workspace)
	}
	slices.SortFunc(out, func(a, b types.NamespacedName) int { return strings.Compare(a.String(), b.String()) })
	return out
}

// workerWait is what one workspace pass reports about its worker wait.
type workerWait struct {
	register func(pool types.NamespacedName)

	mu        sync.Mutex
	nativeLag bool
}

func (w *workerWait) begin() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.nativeLag = false
}

func (w *workerWait) lagging() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nativeLag
}

// awaitWorker registers, before the pass reads its worker, the pool an event
// should wake it for. Registering first means an event that lands while the
// pass reads is either already visible to it or re-queues the workspace.
func (d *Lifecycle) awaitWorker(record *journalRecord) {
	if d.wait == nil || d.wait.register == nil || record.RuntimePool.Name == "" {
		return
	}
	d.wait.register(types.NamespacedName{Namespace: record.Placement.Namespace, Name: record.RuntimePool.Name})
}

// noteNativeLag marks the pass as waiting only on native state when the
// selected worker's exact Pod is already Ready in Kubernetes. It only tunes
// the next poll: a failed read leaves the ordinary backoff.
func (d *Lifecycle) noteNativeLag(ctx context.Context, worker *pb.Worker) {
	if d.wait == nil {
		return
	}
	if worker != nil {
		pod := &corev1.Pod{}
		if err := d.client.Get(ctx, types.NamespacedName{Namespace: worker.GetWorkerNamespace(), Name: worker.GetWorkerPod()}, pod); err != nil ||
			string(pod.UID) != worker.GetWorkerPodUid() || pod.DeletionTimestamp != nil || !podReady(pod) {
			return
		}
	}
	d.wait.mu.Lock()
	defer d.wait.mu.Unlock()
	d.wait.nativeLag = true
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// workerPoolOf names the worker pool a watched object belongs to: a worker
// Pod's ate.dev/worker-pool label, or a WorkerPool itself.
func workerPoolOf(obj client.Object) (types.NamespacedName, bool) {
	switch typed := obj.(type) {
	case *corev1.Pod:
		name := typed.GetLabels()[workerPoolLabel]
		return types.NamespacedName{Namespace: typed.GetNamespace(), Name: name}, name != ""
	case *unstructured.Unstructured:
		ok := typed.GetAPIVersion() == workerPoolAPIVersion() && typed.GetKind() == "WorkerPool" && typed.GetName() != ""
		return types.NamespacedName{Namespace: typed.GetNamespace(), Name: typed.GetName()}, ok
	}
	return types.NamespacedName{}, false
}

// wakeWorkerWaiters maps a worker Pod or WorkerPool event to the workspaces
// waiting on that pool. The event, not the poll, starts their next pass, so
// their poll backoff restarts too: a native step that trails the event is
// retried soon.
func (r *ExecutionWorkspaceReconciler) wakeWorkerWaiters(_ context.Context, obj client.Object) []reconcile.Request {
	pool, ok := workerPoolOf(obj)
	if !ok {
		return nil
	}
	waiting := r.wakes.waiting(pool)
	requests := make([]reconcile.Request, 0, len(waiting))
	for _, workspace := range waiting {
		r.polls.restart(workspace)
		requests = append(requests, reconcile.Request{NamespacedName: workspace})
	}
	return requests
}

// ManagerCacheOptions narrows the manager's Pod informer to worker Pods, the
// only Pods the workspace reconciler watches, and drops their managed fields.
// Every read the provider decides on bypasses this cache.
func ManagerCacheOptions() (cache.Options, error) {
	workerPods, err := k8slabels.NewRequirement(workerPoolLabel, selection.Exists, nil)
	if err != nil {
		return cache.Options{}, err
	}
	return cache.Options{ByObject: map[client.Object]cache.ByObject{
		&corev1.Pod{}: {Label: k8slabels.NewSelector().Add(*workerPods), Transform: cache.TransformStripManagedFields()},
	}}, nil
}
