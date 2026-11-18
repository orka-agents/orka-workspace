// Copyright (c) 2026. MIT License - see LICENSE file for details.

// Spare (provider ADR 0007) and shared (provider ADR 0008) worker evidence
// for the latency measurement. Everything read here is public identity:
// WorkerPool labels and the shared ledger, Pod UIDs and restart counts,
// native Worker labels and Actor egress rule counts.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	spareProfileLabel      = "substrate.workspace.orka.ai/spare-profile-uid"
	spareClaimAnnotation   = "substrate.workspace.orka.ai/spare-claim"
	sharedPoolLabel        = "substrate.workspace.orka.ai/shared-pool"
	sharedLedgerAnnotation = "substrate.workspace.orka.ai/shared-ledger"
	spareWorkersAnnotation = "substrate.workspace.orka.ai/spare-workers"
)

// workerPool is the public view of one provider WorkerPool and its Pods.
type workerPool struct {
	Name         string            `json:"name"`
	UID          string            `json:"uid"`
	Created      time.Time         `json:"created"`
	SpareProfile string            `json:"spareProfileUID,omitempty"`
	SpareClaim   string            `json:"spareClaim,omitempty"`
	SharedPool   string            `json:"sharedPool,omitempty"`
	Labels       map[string]string `json:"labels"`
	Ledger       *ledger           `json:"ledger,omitempty"`
	Pods         []workerPod       `json:"pods"`
}

type workerPod struct {
	Name     string    `json:"name"`
	UID      string    `json:"uid"`
	Created  time.Time `json:"created"`
	Restarts int32     `json:"restarts"`
	Running  bool      `json:"running"`
	Deleting bool      `json:"deleting"`
}

type ledger struct {
	State string `json:"state"`
	Scope struct {
		Namespace  string `json:"namespace"`
		ProfileUID string `json:"profileUID"`
		MaxActors  int32  `json:"maxActors"`
	} `json:"scope"`
	Worker *struct {
		UID    string `json:"uid"`
		PodUID string `json:"podUID"`
		Epoch  int64  `json:"epoch"`
	} `json:"worker,omitempty"`
	Entries []struct {
		Journal  string `json:"journal"`
		Actor    string `json:"actor"`
		ActorUID string `json:"actorUID,omitempty"`
	} `json:"entries"`
}

func (l *latency) workerPools(ctx context.Context) ([]workerPool, error) {
	pools := &unstructured.UnstructuredList{}
	pools.SetAPIVersion("ate.dev/v1")
	pools.SetKind("WorkerPoolList")
	if err := l.c.List(ctx, pools, client.InNamespace(l.namespace)); err != nil {
		return nil, err
	}
	var pods corev1.PodList
	if err := l.c.List(ctx, &pods, client.InNamespace(l.namespace)); err != nil {
		return nil, err
	}
	out := []workerPool{}
	for i := range pools.Items {
		item := &pools.Items[i]
		if item.GetName() == "native-core" {
			continue
		}
		pool := workerPool{Name: item.GetName(), UID: string(item.GetUID()), Created: item.GetCreationTimestamp().Time, Labels: item.GetLabels(),
			SpareProfile: item.GetLabels()[spareProfileLabel], SpareClaim: item.GetAnnotations()[spareClaimAnnotation], SharedPool: item.GetLabels()[sharedPoolLabel]}
		if raw := item.GetAnnotations()[sharedLedgerAnnotation]; raw != "" {
			pool.Ledger = &ledger{}
			if err := json.Unmarshal([]byte(raw), pool.Ledger); err != nil {
				return nil, fmt.Errorf("shared ledger of %s: %w", pool.Name, err)
			}
		}
		for _, pod := range pods.Items {
			if !strings.HasPrefix(pod.Name, pool.Name+"-") {
				continue
			}
			p := workerPod{Name: pod.Name, UID: string(pod.UID), Created: pod.CreationTimestamp.Time, Deleting: pod.DeletionTimestamp != nil}
			for _, container := range pod.Status.ContainerStatuses {
				p.Restarts += container.RestartCount
				p.Running = p.Running || container.State.Running != nil
			}
			pool.Pods = append(pool.Pods, p)
		}
		out = append(out, pool)
	}
	return out, nil
}

// actorWorker returns the native Actor's current assignment: its worker pool
// name and native Worker labels.
func (l *latency) actorWorker(ctx context.Context, process string) (string, map[string]string, error) {
	actor, err := l.native.GetActor(ctx, &pb.GetActorRequest{Actor: &pb.ObjectRef{Atespace: l.namespace, Name: process}})
	if err != nil {
		return "", nil, err
	}
	assignment := actor.GetStatus().GetWorkerAssignment()
	worker, err := l.native.GetWorker(ctx, &pb.GetWorkerRequest{Worker: assignment.GetWorker()})
	if err != nil {
		return assignment.GetWorkerPool(), nil, err
	}
	return assignment.GetWorkerPool(), worker.GetLabels(), nil
}

func (l *latency) processName(ctx context.Context, workspace string) (string, error) {
	w, err := l.workspace(ctx, workspace)
	if err != nil {
		return "", err
	}
	if w.Status.Allocation == nil || w.Status.Allocation.Startup == nil || w.Status.Allocation.Startup.Process == nil {
		return "", fmt.Errorf("workspace %s reports no native process", workspace)
	}
	return w.Status.Allocation.Startup.Process.Name, nil
}

func unclaimedSpares(pools []workerPool, profileUID string) []workerPool {
	out := []workerPool{}
	for _, pool := range pools {
		if pool.SpareProfile == profileUID {
			out = append(out, pool)
		}
	}
	return out
}

// checkSpareClaim verifies a turn's worker came from a spare listed before
// the turn: same pool UID and Pod UID, Pod born before the Task, never
// restarted, and the native Worker carrying the claimed pool's labels. It
// then times the replenishment back to the requested count.
// claimView is what a running turn's Actor was placed on, captured while
// the prompt runs: a no-window detach retires the Actor right after.
type claimView struct {
	pool   string
	labels map[string]string
	pools  []workerPool
	err    error
}

func (l *latency) captureClaim(ctx context.Context, t *turn) *claimView {
	view := &claimView{}
	process, err := l.processName(ctx, t.Workspace)
	if err == nil {
		view.pool, view.labels, err = l.actorWorker(ctx, process)
	}
	if err == nil {
		view.pools, err = l.workerPools(ctx)
	}
	view.err = err
	return view
}

func (l *latency) checkSpareClaim(ctx context.Context, scenario string, rep int, ordinal string, t *turn, before []workerPool, profileUID string, want int, view *claimView) error {
	if view == nil || view.err != nil {
		l.violate("%s %s spare claim could not be observed while running: %v", scenario, t.Name, view)
		return nil
	}
	poolName, workerLabels, pools := view.pool, view.labels, view.pools
	var claimed *workerPool
	for i := range pools {
		if pools[i].Name == poolName {
			claimed = &pools[i]
		}
	}
	var spare *workerPool
	for i := range before {
		if claimed != nil && before[i].UID == claimed.UID {
			spare = &before[i]
		}
	}
	checks := map[string]bool{"claimedPoolWasUnclaimedSpare": spare != nil && claimed != nil}
	if spare != nil && claimed != nil && len(spare.Pods) == 1 && len(claimed.Pods) >= 1 {
		pod := claimed.Pods[0]
		checks["samePodUID"] = pod.UID == spare.Pods[0].UID
		checks["podBornBeforeTask"] = pod.Created.Before(t.CreatedAt)
		checks["podNeverRestarted"] = pod.Restarts == 0 && spare.Pods[0].Restarts == 0
		checks["claimMarkerPresent"] = claimed.SpareClaim != "" && claimed.SpareProfile == ""
		synced := workerLabels != nil
		for key, value := range claimed.Labels {
			if workerLabels[key] != value {
				synced = false
			}
		}
		checks["workerLabelsSyncedToClaimedPool"] = synced
	}
	l.emit("spareClaim", map[string]any{"scenario": scenario, "rep": rep, "turn": ordinal, "checks": checks, "claimed": claimed, "spare": spare})
	for check, ok := range checks {
		if !ok {
			l.violate("%s %s spare claim failed %s", scenario, t.Name, check)
		}
	}
	if len(checks) == 1 {
		l.violate("%s %s did not run on a pre-created spare", scenario, t.Name)
	}
	// Replenishment back to the requested unclaimed count.
	started := time.Now()
	deadline, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	err := wait(deadline, func() (bool, error) {
		current, err := l.workerPools(deadline)
		if err != nil {
			return false, err
		}
		ready := 0
		for _, pool := range unclaimedSpares(current, profileUID) {
			if len(pool.Pods) == 1 && pool.Pods[0].Running {
				ready++
			}
		}
		return ready >= want, nil
	})
	l.emit("spareReplenished", map[string]any{"scenario": scenario, "rep": rep, "turn": ordinal, "afterSettleMs": ms(time.Since(started)), "ok": err == nil})
	if err != nil {
		l.violate("%s spares were not replenished to %d within 3m after %s", scenario, want, t.Name)
	}
	return nil
}

// waitSpares waits until the profile has want unclaimed spares with a
// running worker Pod, and returns them.
func (l *latency) waitSpares(ctx context.Context, profileUID string, want int) ([]workerPool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	var spares []workerPool
	err := wait(ctx, func() (bool, error) {
		pools, err := l.workerPools(ctx)
		if err != nil {
			return false, err
		}
		spares = nil
		for _, pool := range unclaimedSpares(pools, profileUID) {
			if len(pool.Pods) == 1 && pool.Pods[0].Running {
				spares = append(spares, pool)
			}
		}
		return len(spares) >= want, nil
	})
	if err != nil {
		return nil, fmt.Errorf("%d running spares for profile %s: %w", want, profileUID, err)
	}
	return spares, nil
}

// spareScenario is detachScenario on a profile with spare workers.
func (l *latency) spareScenario(ctx context.Context, rep int, profileUID string, want int) (map[string]any, error) {
	views := map[string]*claimView{}
	l.onAccepted = func(t *turn) { views[t.Name] = l.captureClaim(ctx, t) }
	defer func() { l.onAccepted = nil }()
	return l.detachLike(ctx, "spare", "lat-spare", rep, func(ordinal string, t *turn, before []workerPool) error {
		return l.checkSpareClaim(ctx, "spare", rep, ordinal, t, before, profileUID, want, views[t.Name])
	}, func() ([]workerPool, error) { return l.waitSpares(ctx, profileUID, want) })
}

// sharedPoolOf returns the shared pool hosting a workspace's Actor.
func (l *latency) sharedPoolOf(ctx context.Context, workspace string) (*workerPool, error) {
	process, err := l.processName(ctx, workspace)
	if err != nil {
		return nil, err
	}
	poolName, _, err := l.actorWorker(ctx, process)
	if err != nil {
		return nil, err
	}
	pools, err := l.workerPools(ctx)
	if err != nil {
		return nil, err
	}
	for i := range pools {
		if pools[i].Name == poolName {
			return &pools[i], nil
		}
	}
	return nil, fmt.Errorf("workspace %s Actor runs on unknown pool %s", workspace, poolName)
}

func ledgerHolds(pool *workerPool, actorUIDs ...string) bool {
	if pool == nil || pool.Ledger == nil {
		return false
	}
	for _, uid := range actorUIDs {
		found := false
		for _, entry := range pool.Ledger.Entries {
			found = found || entry.ActorUID == uid
		}
		if !found {
			return false
		}
	}
	return true
}

// waitFreeSharedSlot waits for an admitting shared pool of the profile with
// a running worker and a free slot (warm capacity).
func (l *latency) waitFreeSharedSlot(ctx context.Context, profileUID string, slots int) (*workerPool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	var found *workerPool
	err := wait(ctx, func() (bool, error) {
		pools, err := l.workerPools(ctx)
		if err != nil {
			return false, err
		}
		for i := range pools {
			pool := &pools[i]
			if pool.Ledger == nil || pool.Ledger.Scope.ProfileUID != profileUID || pool.Ledger.State != "Admitting" || len(pool.Pods) != 1 || !pool.Pods[0].Running {
				continue
			}
			if int(pool.Ledger.Scope.MaxActors)-len(pool.Ledger.Entries) >= slots {
				found = pool
				return true, nil
			}
		}
		return false, nil
	})
	return found, err
}

// egressWatch polls an Actor's default EgressPolicy until it is gone and
// records when it was first observed with no rules (deny-all).
type egressWatch struct {
	mu        sync.Mutex
	firstDeny time.Time
	lastRules int
	done      chan struct{}
}

func (l *latency) watchEgress(ctx context.Context, process string) *egressWatch {
	w := &egressWatch{done: make(chan struct{}), lastRules: -1}
	go func() {
		defer close(w.done)
		for {
			policy, err := l.native.GetActorEgressPolicy(ctx, &pb.GetActorEgressPolicyRequest{Actor: &pb.ObjectRef{Atespace: l.namespace, Name: process}, Name: "default"})
			if status.Code(err) == codes.NotFound || ctx.Err() != nil {
				return
			}
			if err == nil {
				w.mu.Lock()
				w.lastRules = len(policy.GetRules())
				if w.lastRules == 0 && w.firstDeny.IsZero() {
					w.firstDeny = time.Now()
				}
				w.mu.Unlock()
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	return w
}

type turnResult struct {
	t   *turn
	err error
}

func (l *latency) concurrentTurns(ctx context.Context, class string, specs [][2]string, create bool) ([]*turn, error) {
	results := make([]turnResult, len(specs))
	var group sync.WaitGroup
	for i, spec := range specs {
		group.Add(1)
		go func(i int, name, session string) {
			defer group.Done()
			t, err := l.createTurn(ctx, name, session, class, create)
			if err == nil {
				_, err = l.waitSettled(ctx, t)
			}
			results[i] = turnResult{t, err}
		}(i, spec[0], spec[1])
	}
	group.Wait()
	turns := []*turn{}
	for _, result := range results {
		if result.err != nil {
			return nil, result.err
		}
		turns = append(turns, result.t)
	}
	return turns, nil
}

// sharedScenario runs two concurrent Sessions of one namespace and profile on
// a shared worker. Session A's warm window expires and its Actor is
// suspended per Actor while Session B keeps taking turns on the same worker
// Pod; A then resumes from its Tag onto a free shared slot.
func (l *latency) sharedScenario(ctx context.Context, rep int, profileUID string, sessions int) (map[string]any, error) {
	scenario, class := "shared", "lat-shared"
	sessionA, sessionB := fmt.Sprintf("lat-shared-%d-a", rep), fmt.Sprintf("lat-shared-%d-b", rep)
	free, err := l.waitFreeSharedSlot(ctx, profileUID, sessions)
	if err != nil {
		l.violate("no warm shared pool with %d free slots before repetition %d: %v", sessions, rep, err)
	}
	l.emit("sharedBefore", map[string]any{"scenario": scenario, "rep": rep, "freePool": free})
	// Sessions beyond A and B only take a concurrent first turn, filling
	// more slots of the same worker; their windows then expire with A's.
	specs := [][2]string{{sessionA + "-t1", sessionA}, {sessionB + "-t1", sessionB}}
	for i := 2; i < sessions; i++ {
		extra := fmt.Sprintf("lat-shared-%d-%c", rep, 'a'+i)
		specs = append(specs, [2]string{extra + "-t1", extra})
	}
	turns, err := l.concurrentTurns(ctx, class, specs, true)
	if err != nil {
		return nil, err
	}
	a1, b1 := turns[0], turns[1]
	for _, t := range turns {
		l.sharedWorkspaces[t.Workspace] = true
	}
	snapshots := map[string]*snapshot{}
	for _, t := range turns {
		w, err := l.workspace(ctx, t.Workspace)
		if err != nil {
			return nil, err
		}
		if snapshots[t.Name], err = l.snapshot(ctx, w, t.Identity["runtimePoolName"]); err != nil {
			return nil, err
		}
		l.logTurnKind(scenario, rep, t.Name[len(t.Name)-4:], "cold", t, nil, snapshots[t.Name])
	}
	sa, sb := snapshots[a1.Name], snapshots[b1.Name]
	pool, err := l.sharedPoolOf(ctx, a1.Workspace)
	if err != nil {
		return nil, err
	}
	allSame, actorUIDs := true, []string{}
	for _, t := range turns {
		allSame = allSame && snapshots[t.Name].WorkerPodUID == sa.WorkerPodUID
		actorUIDs = append(actorUIDs, snapshots[t.Name].ActorUID)
	}
	shareChecks := map[string]bool{
		"sameWorkerPod":    sa.WorkerPodUID != "" && sa.WorkerPodUID == sb.WorkerPodUID,
		"allOnOneWorker":   allSame,
		"ledgerListsAll":   ledgerHolds(pool, actorUIDs...),
		"sharedPool":       pool.SharedPool != "" && pool.Ledger != nil,
		"ledgerListsBoth":  ledgerHolds(pool, sa.ActorUID, sb.ActorUID),
		"ledgerNamespace":  pool.Ledger != nil && pool.Ledger.Scope.Namespace == l.namespace,
		"distinctActors":   sa.ActorUID != sb.ActorUID,
		"warmSlotWasFree":  free != nil && free.UID == pool.UID,
		"neverRestartedA1": len(pool.Pods) == 1 && pool.Pods[0].Restarts == 0,
	}
	entries := 0
	if pool.Ledger != nil {
		entries = len(pool.Ledger.Entries)
	}
	l.emit("sharedPlacement", map[string]any{"scenario": scenario, "rep": rep, "checks": shareChecks, "pool": pool, "actorsOnWorker": entries, "sessions": len(turns)})
	for check, ok := range shareChecks {
		if !ok {
			l.violate("shared repetition %d placement failed %s", rep, check)
		}
	}
	// B takes a warm turn right away, so its window outlives A's.
	b2, err := l.createTurn(ctx, sessionB+"-t2", sessionB, class, false)
	if err != nil {
		return nil, err
	}
	w, err := l.waitSettled(ctx, b2)
	if err != nil {
		return nil, err
	}
	sb2, err := l.snapshot(ctx, w, b2.Identity["runtimePoolName"])
	if err != nil {
		return nil, err
	}
	l.logTurnKind(scenario, rep, "b-t2", "warm", b2, sb, sb2)
	// Watch A's egress and wait for retention to begin its suspension.
	processA, err := l.processName(ctx, a1.Workspace)
	if err != nil {
		return nil, err
	}
	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	egress := l.watchEgress(watchCtx, processA)
	deadlineCtx, cancel := context.WithTimeout(ctx, l.window+3*time.Minute)
	err = wait(deadlineCtx, func() (bool, error) {
		current, err := l.workspace(deadlineCtx, a1.Workspace)
		if err != nil {
			return false, err
		}
		return current.Spec.DesiredState == "Suspended", nil
	})
	cancel()
	if err != nil {
		return nil, fmt.Errorf("session A's warm window did not expire: %w", err)
	}
	suspendRequested := time.Now()
	// B's turn while A's Actor is being suspended on the same worker.
	type bTurn struct {
		t   *turn
		err error
	}
	bDone := make(chan bTurn, 1)
	go func() {
		t, err := l.createTurn(ctx, sessionB+"-t3", sessionB, class, false)
		if err == nil {
			_, err = l.waitSettled(ctx, t)
		}
		bDone <- bTurn{t, err}
	}()
	suspendedA, _, err := l.waitSuspended(ctx, a1.Workspace, a1.Identity["runtimePoolName"], sa, 5*time.Minute)
	suspendedAt := time.Now()
	if err != nil {
		return nil, err
	}
	l.emit("suspended", map[string]any{"scenario": scenario, "rep": rep, "after": "window", "workspace": a1.Workspace,
		"observedAfterSettleMs": ms(suspendedAt.Sub(suspendRequested)), "snapshot": suspendedA})
	b3result := <-bDone
	if b3result.err != nil {
		return nil, fmt.Errorf("session B's turn during A's suspension: %w", b3result.err)
	}
	b3 := b3result.t
	w, err = l.workspace(ctx, b3.Workspace)
	if err != nil {
		return nil, err
	}
	sb3, err := l.snapshot(ctx, w, b3.Identity["runtimePoolName"])
	if err != nil {
		return nil, err
	}
	l.logTurnKind(scenario, rep, "b-t3", "warm-during-neighbour-suspend", b3, sb2, sb3)
	stopWatch()
	<-egress.done
	egress.mu.Lock()
	firstDeny, lastRules := egress.firstDeny, egress.lastRules
	egress.mu.Unlock()
	poolAfter, err := l.sharedPoolOf(ctx, b3.Workspace)
	if err != nil {
		return nil, err
	}
	isolation := map[string]bool{
		"neighbourSameActor":          sb3.ActorUID == sb.ActorUID && sb3.Sequence == sb.Sequence,
		"neighbourSameWorkerPod":      sb3.WorkerPodUID == sb.WorkerPodUID,
		"neighbourWorkerNotRestarted": len(poolAfter.Pods) == 1 && poolAfter.Pods[0].UID == sb.WorkerPodUID && poolAfter.Pods[0].Restarts == 0,
		"suspendedActorLeftLedger":    !ledgerHolds(poolAfter, sa.ActorUID) && ledgerHolds(poolAfter, sb.ActorUID),
		"egressDenyAllBeforeReported": !firstDeny.IsZero() && firstDeny.Before(suspendedAt),
		"newTagForA":                  len(newTags(sa, suspendedA)) > 0,
	}
	l.emit("sharedSuspend", map[string]any{"scenario": scenario, "rep": rep, "checks": isolation, "lastObservedEgressRules": lastRules,
		"denyAllAfterSuspendRequestMs": func() int64 {
			if firstDeny.IsZero() {
				return -1
			}
			return ms(firstDeny.Sub(suspendRequested))
		}(), "suspendedObservedAfterRequestMs": ms(suspendedAt.Sub(suspendRequested)), "suspended": suspendedA, "pool": poolAfter})
	for check, ok := range isolation {
		if !ok {
			l.violate("shared repetition %d per-Actor suspend failed %s", rep, check)
		}
	}
	// A resumes from its Tag onto a free shared slot.
	a2, err := l.createTurn(ctx, sessionA+"-t2", sessionA, class, false)
	if err != nil {
		return nil, err
	}
	if w, err = l.waitSettled(ctx, a2); err != nil {
		return nil, err
	}
	sa2, err := l.snapshot(ctx, w, a2.Identity["runtimePoolName"])
	if err != nil {
		return nil, err
	}
	l.logTurnKind(scenario, rep, "a-t2", "resume", a2, suspendedA, sa2)
	resumePool, err := l.sharedPoolOf(ctx, a2.Workspace)
	if err != nil {
		return nil, err
	}
	resumeChecks := map[string]bool{
		"resumedFromTag":       sa2.ResumePath && sa2.Sequence == sa.Sequence+1 && sa2.ActorUID != sa.ActorUID,
		"resumedOnSharedPool":  resumePool.SharedPool != "" && ledgerHolds(resumePool, sa2.ActorUID),
		"resumedBesideB":       sa2.WorkerPodUID == sb3.WorkerPodUID,
		"neighbourStillListed": ledgerHolds(resumePool, sb.ActorUID) || resumePool.UID != poolAfter.UID,
	}
	l.emit("sharedResume", map[string]any{"scenario": scenario, "rep": rep, "checks": resumeChecks, "pool": resumePool})
	for check, ok := range resumeChecks {
		if !ok {
			l.violate("shared repetition %d resume failed %s", rep, check)
		}
	}
	// Let every window expire so the next repetition starts from idle Actors.
	lasts := []struct {
		t *turn
		s *snapshot
	}{{a2, sa2}, {b3, sb3}}
	for _, t := range turns[2:] {
		lasts = append(lasts, struct {
			t *turn
			s *snapshot
		}{t, snapshots[t.Name]})
	}
	for _, last := range lasts {
		if final, waited, err := l.waitSuspended(ctx, last.t.Workspace, last.t.Identity["runtimePoolName"], last.s, l.window+5*time.Minute); err != nil {
			return nil, err
		} else {
			l.emit("suspended", map[string]any{"scenario": scenario, "rep": rep, "after": "window", "workspace": last.t.Workspace, "observedAfterSettleMs": ms(waited), "snapshot": final})
		}
	}
	return map[string]any{"sessions": []string{sessionA, sessionB}, "placement": shareChecks, "isolation": isolation, "resume": resumeChecks}, nil
}

// scopeScenario places one Session on a second shared profile in the same
// namespace: a different profile is a different scope and must never land
// on the first profile's shared worker.
func (l *latency) scopeScenario(ctx context.Context, rep int, otherProfileUID string) (map[string]any, error) {
	session := fmt.Sprintf("lat-scope-%d", rep)
	t, err := l.createTurn(ctx, session+"-t1", session, "lat-shared-scope", true)
	if err != nil {
		return nil, err
	}
	l.sharedWorkspaces[t.Workspace] = true
	w, err := l.waitSettled(ctx, t)
	if err != nil {
		return nil, err
	}
	s, err := l.snapshot(ctx, w, t.Identity["runtimePoolName"])
	if err != nil {
		return nil, err
	}
	l.logTurnKind("scope", rep, "t1", "cold", t, nil, s)
	pool, err := l.sharedPoolOf(ctx, t.Workspace)
	if err != nil {
		return nil, err
	}
	pools, err := l.workerPools(ctx)
	if err != nil {
		return nil, err
	}
	foreign := false
	for _, other := range pools {
		if other.Ledger == nil || other.UID == pool.UID {
			continue
		}
		for _, entry := range other.Ledger.Entries {
			foreign = foreign || entry.ActorUID == s.ActorUID
		}
	}
	checks := map[string]bool{
		"ownScopePool":       pool.Ledger != nil && pool.Ledger.Scope.ProfileUID == otherProfileUID,
		"notOnOtherScope":    !foreign,
		"ownPoolOnlyOwnUIDs": pool.Ledger != nil && len(pool.Ledger.Entries) >= 1,
	}
	l.emit("scopeIsolation", map[string]any{"scenario": "scope", "rep": rep, "checks": checks, "pool": pool})
	for check, ok := range checks {
		if !ok {
			l.violate("scope repetition %d failed %s", rep, check)
		}
	}
	if final, waited, err := l.waitSuspended(ctx, t.Workspace, t.Identity["runtimePoolName"], s, 5*time.Minute); err != nil {
		return nil, err
	} else {
		l.emit("suspended", map[string]any{"scenario": "scope", "rep": rep, "after": "t1", "workspace": t.Workspace, "observedAfterSettleMs": ms(waited), "snapshot": final})
	}
	return map[string]any{"session": session, "checks": checks}, nil
}
