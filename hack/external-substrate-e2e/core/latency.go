// Copyright (c) 2026. MIT License - see LICENSE file for details.

// Latency measurement of session-reused, DataOnly, Suspend-on-detach Substrate
// workspaces through the actual Core and deployed provider (core/latency.sh).
// Like the proof it creates only public objects (profiles, classes, Tasks) and
// observes workspaces, RuntimePools and native objects read-only. It reports
// identities, revisions and resourceVersions only: never Secret data,
// attachment token hashes or bootstrap nonces.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	profile "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Workspace annotations Core writes (ADR 0055 and its detach bookkeeping).
const (
	annotationSuspendAt          = "acp.workspace.orka.ai/suspend-at"
	annotationSuspendAfterDetach = "acp.workspace.orka.ai/suspend-after-detach"
	annotationSuspendPending     = "acp.workspace.orka.ai/suspend-pending"
	annotationLastDetached       = "acp.workspace.orka.ai/last-detached-at"
	annotationLastSettledTask    = "acp.workspace.orka.ai/last-settled-task-uid"
	annotationDetachAction       = "acp.workspace.orka.ai/detach-action"
	annotationSessionCommitted   = "acp.workspace.orka.ai/durable-session-committed"
	annotationWorkloadPublished  = "acp.workspace.orka.ai/workload-published-at"
	annotationRetirementAuth     = "acp.workspace.orka.ai/retirement-authorized-at"
	runtimePoolUIDLabel          = "orka.ai/runtime-pool-uid"
	latencyPollInterval          = 100 * time.Millisecond
)

type latency struct {
	*proof
	start      time.Time
	window     time.Duration
	events     []map[string]any
	violations []string
}

// emit prints one machine-readable event and keeps it for the final report.
func (l *latency) emit(kind string, fields map[string]any) {
	fields["event"] = kind
	fields["atMs"] = time.Since(l.start).Milliseconds()
	fields["at"] = time.Now().UTC().Format(time.RFC3339Nano)
	l.events = append(l.events, fields)
	encoded, _ := json.Marshal(fields)
	fmt.Println("LATENCY_EVENT", string(encoded))
}

func ms(d time.Duration) int64 { return d.Milliseconds() }

// violate records a failed expectation and lets the measurement continue, so
// one finding does not discard the remaining samples. Any violation fails
// the run after the final report.
func (l *latency) violate(format string, args ...any) {
	text := fmt.Sprintf(format, args...)
	l.violations = append(l.violations, text)
	l.emit("violation", map[string]any{"message": text})
}

// turn is one client-observed Task: every execution state and phase is
// stamped the first poll it was seen, relative to just before Create.
type turn struct {
	Name      string           `json:"name"`
	UID       string           `json:"uid"`
	Session   string           `json:"session"`
	CreatedAt time.Time        `json:"createdAt"`
	States    map[string]int64 `json:"statesFirstSeenMs"`
	// AcceptedMs is the first observation of an Accepted or later state;
	// DoneMs of phase Succeeded; SettledMs of the workspace settlement
	// receipt naming this Task.
	AcceptedMs int64             `json:"acceptedMs"`
	DoneMs     int64             `json:"doneMs"`
	SettledMs  int64             `json:"settledMs,omitempty"`
	Identity   map[string]string `json:"identity"`
	Workspace  string            `json:"workspace"`
}

var acceptedOrLater = map[string]bool{"Accepted": true, "Running": true, "Settling": true, "Succeeded": true}

func (l *latency) createTurn(ctx context.Context, name, session, class string, create bool) (*turn, error) {
	task := l.object("Task", name, map[string]any{"type": "agent", "agentRef": map[string]any{"name": "native-core"},
		"prompt":     "Run the deterministic external native workspace fixture (latency turn).",
		"timeout":    "5m",
		"sessionRef": map[string]any{"name": session, "create": create, "append": true},
		"execution": map[string]any{"workspace": map[string]any{"classRef": map[string]any{"name": class},
			"reusePolicy": "session", "onDetach": "Suspend"}},
		"agentRuntime": map[string]any{"maxTurns": int64(1), "allowedTools": []any{"Glob", "Grep", "Read"}, "allowBash": false}})
	t := &turn{Name: name, Session: session, States: map[string]int64{}, AcceptedMs: -1, DoneMs: -1}
	t.CreatedAt = time.Now()
	if err := l.c.Create(ctx, task); err != nil {
		return nil, fmt.Errorf("create Task %s: %w", name, err)
	}
	t.UID = string(task.GetUID())
	fmt.Printf("ACCEPTED_TASK namespace=%s name=%s uid=%s\n", l.namespace, name, t.UID)
	if err := wait(ctx, func() (bool, error) {
		if err := l.c.Get(ctx, client.ObjectKeyFromObject(task), task); err != nil {
			return false, err
		}
		elapsed := ms(time.Since(t.CreatedAt))
		state, phase := str(task, "status", "execution", "state"), str(task, "status", "phase")
		for _, key := range []string{"state:" + state, "phase:" + phase} {
			if _, seen := t.States[key]; !seen && !strings.HasSuffix(key, ":") {
				t.States[key] = elapsed
			}
		}
		if t.AcceptedMs < 0 && acceptedOrLater[state] {
			t.AcceptedMs = elapsed
		}
		if phase == "Failed" || phase == "Cancelled" {
			return false, fmt.Errorf("Task %s became %s: %s %s", name, phase, str(task, "status", "execution", "reason"), str(task, "status", "execution", "message"))
		}
		if phase == "Succeeded" {
			t.DoneMs = elapsed
			return true, nil
		}
		return false, nil
	}); err != nil {
		return nil, err
	}
	t.Identity = map[string]string{}
	for _, field := range []string{"runtimePoolName", "runtimePoolUID", "runtimeInstanceID", "runtimeSessionUID", "runtimeSessionSupervisorBootID", "promptID"} {
		t.Identity[field] = str(task, "status", "execution", field)
	}
	// One Session keeps its RuntimeSession UID across turns; each turn creates
	// a new generation of it (supervisor runtime-session-<Task UID>-g<n>).
	generation, _, _ := unstructured.NestedInt64(task.Object, "status", "execution", "runtimeSessionGeneration")
	t.Identity["runtimeSessionGeneration"] = strconv.FormatInt(generation, 10)
	t.Workspace = str(task, "status", "executionWorkspace", "workspaceRef", "name")
	if t.Workspace == "" {
		t.Workspace = task.GetLabels()["acp.workspace.orka.ai/execution-workspace"]
	}
	if t.Workspace == "" || t.Identity["runtimeInstanceID"] == "" || t.Identity["runtimeSessionUID"] == "" {
		return nil, fmt.Errorf("successful Task %s omitted its workspace or runtime identity", name)
	}
	return t, nil
}

// waitSettled stamps when the workspace's settlement receipt names the turn.
func (l *latency) waitSettled(ctx context.Context, t *turn) (*api.ExecutionWorkspace, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	var w *api.ExecutionWorkspace
	err := wait(ctx, func() (bool, error) {
		current, err := l.workspace(ctx, t.Workspace)
		if err != nil {
			return false, err
		}
		w = current
		if strings.HasPrefix(current.Annotations[annotationLastSettledTask], t.UID) {
			t.SettledMs = ms(time.Since(t.CreatedAt))
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return w, fmt.Errorf("workspace settlement of %s: %w", t.Name, err)
	}
	return w, nil
}

func (l *latency) workspace(ctx context.Context, name string) (*api.ExecutionWorkspace, error) {
	w := &api.ExecutionWorkspace{}
	return w, l.c.Get(ctx, client.ObjectKey{Namespace: l.namespace, Name: name}, w)
}

// snapshot is the public, non-secret view of a workspace, its pool and its
// native Actor used as evidence.
type snapshot struct {
	Sequence        int64             `json:"sequence"`
	Revision        string            `json:"workloadRevision"`
	ResumePath      bool              `json:"retainedData"`
	DesiredState    string            `json:"desiredState"`
	State           string            `json:"state"`
	Attached        bool              `json:"attached"`
	AttachmentEpoch int64             `json:"attachmentEpoch"`
	InstanceID      string            `json:"providerInstanceID,omitempty"`
	ActorUID        string            `json:"actorUID,omitempty"`
	ActorVersion    int64             `json:"actorVersion,omitempty"`
	ActorExists     bool              `json:"actorExists"`
	WorkerPodUID    string            `json:"workerPodUID,omitempty"`
	WorkerPod       string            `json:"workerPod,omitempty"`
	WorkerPodExists bool              `json:"workerPodExists"`
	Retirement      string            `json:"retirement,omitempty"`
	Annotations     map[string]string `json:"annotations"`
	Pool            map[string]any    `json:"runtimePool,omitempty"`
	PoolSecrets     map[string]string `json:"poolCredentialSecrets,omitempty"`
	Tags            []string          `json:"nativeTagUIDs"`
}

func (l *latency) snapshot(ctx context.Context, w *api.ExecutionWorkspace, poolName string) (*snapshot, error) {
	s := &snapshot{DesiredState: string(w.Spec.DesiredState), State: string(w.Status.State), Attached: w.Spec.Attachment != nil,
		AttachmentEpoch: w.Spec.AttachmentEpoch, Annotations: map[string]string{}}
	if w.Spec.Workload != nil {
		s.Sequence, s.Revision, s.ResumePath = w.Spec.Workload.Sequence, w.Spec.Workload.Revision, w.Spec.Workload.RetainedData != nil
	}
	if r := w.Spec.Retirement; r != nil {
		s.Retirement = fmt.Sprintf("%d/%s", r.Sequence, r.Action)
	}
	for _, key := range []string{annotationSuspendAt, annotationSuspendAfterDetach, annotationSuspendPending, annotationLastDetached, annotationDetachAction, annotationWorkloadPublished, annotationRetirementAuth} {
		if value, ok := w.Annotations[key]; ok {
			s.Annotations[strings.TrimPrefix(key, "acp.workspace.orka.ai/")] = value
		}
	}
	if _, ok := w.Annotations[annotationSessionCommitted]; ok {
		s.Annotations["durable-session-committed"] = "present"
	}
	if a := w.Status.Allocation; a != nil {
		s.InstanceID = a.Identity.InstanceID
		if a.Startup != nil && a.Startup.Process != nil {
			process := a.Startup.Process
			s.ActorUID, s.ActorVersion, s.WorkerPodUID = process.UID, process.Version, string(process.Worker.UID)
			s.WorkerPod = process.Worker.Namespace + "/" + process.Worker.Name
			actor, err := l.native.GetActor(ctx, &pb.GetActorRequest{Actor: &pb.ObjectRef{Atespace: process.Namespace, Name: process.Name}})
			switch {
			case status.Code(err) == codes.NotFound:
			case err != nil:
				return nil, err
			default:
				s.ActorExists = actor.GetMetadata().GetUid() == process.UID
				s.ActorVersion = actor.GetMetadata().GetVersion()
			}
			pod := &corev1.Pod{}
			err = l.c.Get(ctx, client.ObjectKey{Namespace: process.Worker.Namespace, Name: process.Worker.Name}, pod)
			if err != nil && !apierrors.IsNotFound(err) {
				return nil, err
			}
			s.WorkerPodExists = err == nil && pod.UID == process.Worker.UID && pod.DeletionTimestamp == nil
		}
	}
	if poolName != "" {
		pool := l.object("RuntimePool", poolName, nil)
		if err := l.c.Get(ctx, client.ObjectKeyFromObject(pool), pool); err == nil {
			specReplicas, _, _ := unstructured.NestedInt64(pool.Object, "spec", "desiredReplicas")
			desired, _, _ := unstructured.NestedInt64(pool.Object, "status", "desiredReplicas")
			current, _, _ := unstructured.NestedInt64(pool.Object, "status", "currentReplicas")
			s.Pool = map[string]any{"uid": string(pool.GetUID()), "specDesiredReplicas": specReplicas, "desiredReplicas": desired, "currentReplicas": current,
				"lifecycle": str(pool, "status", "lifecycle"), "admissionState": str(pool, "status", "admissionState"),
				"activeInstance": str(pool, "status", "activeInstance", "runtimeInstanceID"), "bootID": str(pool, "status", "activeInstance", "bootID")}
			var secrets corev1.SecretList
			if err := l.c.List(ctx, &secrets, client.InNamespace(l.namespace), client.MatchingLabels{runtimePoolUIDLabel: string(pool.GetUID())}); err != nil {
				return nil, err
			}
			s.PoolSecrets = map[string]string{}
			for _, secret := range secrets.Items {
				// Identity and version only; Secret data is never read into the report.
				s.PoolSecrets[secret.Name] = string(secret.UID) + "@" + secret.ResourceVersion
			}
		} else if !apierrors.IsNotFound(err) {
			return nil, err
		}
	}
	tags, err := l.native.ListTags(ctx, &pb.ListTagsRequest{Atespace: l.namespace, Options: &pb.ListOptions{PageSize: 1000}})
	if err != nil {
		return nil, err
	}
	s.Tags = []string{}
	for _, tag := range tags.GetTags() {
		s.Tags = append(s.Tags, tag.GetMetadata().GetUid())
	}
	return s, nil
}

// waitSuspended waits for the exact suspension of the instance snapshot
// before describes: Suspended state, the Actor and worker Pod gone.
func (l *latency) waitSuspended(ctx context.Context, name, poolName string, before *snapshot, bound time.Duration) (*snapshot, time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	started := time.Now()
	var last *snapshot
	err := wait(ctx, func() (bool, error) {
		w, err := l.workspace(ctx, name)
		if err != nil {
			return false, err
		}
		if w.Status.State == api.ExecutionWorkspaceStateFailed {
			return false, fmt.Errorf("workspace %s failed while suspending", name)
		}
		if w.Spec.DesiredState != api.ExecutionWorkspaceDesiredSuspended || w.Status.State != api.ExecutionWorkspaceStateSuspended {
			return false, nil
		}
		last, err = l.snapshot(ctx, w, poolName)
		if err != nil {
			return false, err
		}
		// Exactly the Actor and worker Pod that ran before the detach.
		return !last.ActorExists && !last.WorkerPodExists && l.instanceGone(ctx, w.Namespace, before), nil
	})
	if err != nil {
		return last, time.Since(started), fmt.Errorf("workspace %s suspension: %w", name, err)
	}
	return last, time.Since(started), nil
}

// instanceGone reports the exact Actor UID and worker Pod UID of an earlier
// snapshot absent. It is conservative: any read error counts as present.
func (l *latency) instanceGone(ctx context.Context, atespace string, s *snapshot) bool {
	if s == nil || s.ActorUID == "" {
		return true
	}
	actors, err := l.native.ListActors(ctx, &pb.ListActorsRequest{Atespace: atespace, Options: &pb.ListOptions{PageSize: 1000}})
	if err != nil {
		return false
	}
	for _, actor := range actors.GetActors() {
		if actor.GetMetadata().GetUid() == s.ActorUID {
			return false
		}
	}
	if namespace, name, ok := strings.Cut(s.WorkerPod, "/"); ok {
		pod := &corev1.Pod{}
		err := l.c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, pod)
		if err == nil && string(pod.UID) == s.WorkerPodUID {
			return false
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return false
		}
	}
	return true
}

// newTags returns the native Tag UIDs in after that before lacked.
func newTags(before, after *snapshot) []string {
	known := map[string]bool{}
	for _, uid := range before.Tags {
		known[uid] = true
	}
	added := []string{}
	for _, uid := range after.Tags {
		if !known[uid] {
			added = append(added, uid)
		}
	}
	return added
}

func (l *latency) logTurn(scenario string, rep int, ordinal string, t *turn, before, after *snapshot) {
	l.emit("turn", map[string]any{"scenario": scenario, "rep": rep, "turn": ordinal, "task": t, "workspaceBefore": before, "workspaceAfter": after})
}

// detachScenario: the profile has no warm window, so every detach suspends.
func (l *latency) detachScenario(ctx context.Context, rep int) (map[string]any, error) {
	session := fmt.Sprintf("lat-detach-%d", rep)
	t1, err := l.createTurn(ctx, session+"-t1", session, "lat-detach", true)
	if err != nil {
		return nil, err
	}
	w, err := l.waitSettled(ctx, t1)
	if err != nil {
		return nil, err
	}
	pool := t1.Identity["runtimePoolName"]
	afterT1, err := l.snapshot(ctx, w, pool)
	if err != nil {
		return nil, err
	}
	l.logTurn("detach", rep, "t1", t1, nil, afterT1)
	suspended, waited, err := l.waitSuspended(ctx, t1.Workspace, pool, afterT1, 5*time.Minute)
	if err != nil {
		return nil, err
	}
	l.emit("suspended", map[string]any{"scenario": "detach", "rep": rep, "after": "t1", "workspace": t1.Workspace, "observedAfterSettleMs": ms(waited), "snapshot": suspended})
	if _, present := suspended.Annotations["suspend-at"]; present {
		l.violate("window-unset workspace carries a suspend-at deadline")
	}
	if len(newTags(afterT1, suspended)) == 0 {
		l.violate("suspension of %s produced no native Tag", t1.Workspace)
	}
	t2, err := l.createTurn(ctx, session+"-t2", session, "lat-detach", false)
	if err != nil {
		return nil, err
	}
	if w, err = l.waitSettled(ctx, t2); err != nil {
		return nil, err
	}
	afterT2, err := l.snapshot(ctx, w, t2.Identity["runtimePoolName"])
	if err != nil {
		return nil, err
	}
	l.logTurn("detach", rep, "t2", t2, suspended, afterT2)
	if t2.Workspace != t1.Workspace || afterT2.Sequence != afterT1.Sequence+1 || !afterT2.ResumePath || afterT2.ActorUID == afterT1.ActorUID ||
		t2.Identity["runtimeInstanceID"] == t1.Identity["runtimeInstanceID"] {
		l.violate("continuation %s did not resume the same workspace from its Tag on a new sequence and Actor", t2.Name)
	}
	final, waited, err := l.waitSuspended(ctx, t2.Workspace, t2.Identity["runtimePoolName"], afterT2, 5*time.Minute)
	if err != nil {
		return nil, err
	}
	l.emit("suspended", map[string]any{"scenario": "detach", "rep": rep, "after": "t2", "workspace": t2.Workspace, "observedAfterSettleMs": ms(waited), "snapshot": final})
	return map[string]any{"session": session, "workspace": t1.Workspace, "t1": t1, "t2": t2, "resumedSequence": afterT2.Sequence}, nil
}

// warmScenario: the profile freezes a warm window. Turn 2 arrives inside it
// and must attach the running instance; then the window expires, retention
// suspends, and turn 3 resumes from the Tag.
func (l *latency) warmScenario(ctx context.Context, rep int) (map[string]any, error) {
	session := fmt.Sprintf("lat-warm-%d", rep)
	t1, err := l.createTurn(ctx, session+"-t1", session, "lat-warm", true)
	if err != nil {
		return nil, err
	}
	w, err := l.waitSettled(ctx, t1)
	if err != nil {
		return nil, err
	}
	pool := t1.Identity["runtimePoolName"]
	warm1, err := l.snapshot(ctx, w, pool)
	if err != nil {
		return nil, err
	}
	l.logTurn("warm", rep, "t1", t1, nil, warm1)
	if frozen, err := time.ParseDuration(warm1.Annotations["suspend-after-detach"]); err != nil || frozen != l.window {
		l.violate("workspace froze window %q, want %s", warm1.Annotations["suspend-after-detach"], l.window)
	}
	if _, open := warm1.Annotations["suspend-at"]; !open || warm1.Attached || warm1.DesiredState != "Ready" || !warm1.ActorExists || !warm1.WorkerPodExists {
		return nil, fmt.Errorf("settled turn 1 did not leave an open warm window on a running instance: %+v", warm1)
	}
	if replicas, _ := warm1.Pool["desiredReplicas"].(int64); replicas < 1 {
		l.violate("RuntimePool dropped its replicas inside the warm window: %v", warm1.Pool)
	}
	// Turn 2 inside the window.
	t2, err := l.createTurn(ctx, session+"-t2", session, "lat-warm", false)
	if err != nil {
		return nil, err
	}
	if w, err = l.waitSettled(ctx, t2); err != nil {
		return nil, err
	}
	warm2, err := l.snapshot(ctx, w, t2.Identity["runtimePoolName"])
	if err != nil {
		return nil, err
	}
	l.logTurn("warm", rep, "t2", t2, warm1, warm2)
	direct := map[string]bool{
		"sameWorkspace":            t2.Workspace == t1.Workspace,
		"sameWorkloadSequence":     warm2.Sequence == warm1.Sequence,
		"sameWorkloadRevision":     warm2.Revision == warm1.Revision,
		"sameProviderInstance":     warm2.InstanceID == warm1.InstanceID,
		"sameActorUID":             warm2.ActorUID == warm1.ActorUID && warm2.ActorExists,
		"sameWorkerPod":            warm2.WorkerPodUID == warm1.WorkerPodUID && warm2.WorkerPodExists,
		"sameRuntimeInstanceID":    t2.Identity["runtimeInstanceID"] == t1.Identity["runtimeInstanceID"],
		"sameSupervisorBoot":       t2.Identity["runtimeSessionSupervisorBootID"] == t1.Identity["runtimeSessionSupervisorBootID"],
		"samePool":                 t2.Identity["runtimePoolUID"] == t1.Identity["runtimePoolUID"],
		"poolCredentialsUnchanged": len(warm1.PoolSecrets) > 0 && fmt.Sprint(warm1.PoolSecrets) == fmt.Sprint(warm2.PoolSecrets),
		"sameSessionNewGeneration": t2.Identity["runtimeSessionUID"] == t1.Identity["runtimeSessionUID"] && t2.Identity["runtimeSessionGeneration"] != t1.Identity["runtimeSessionGeneration"] && t2.Identity["promptID"] != t1.Identity["promptID"],
		"newAttachmentEpoch":       warm2.AttachmentEpoch > warm1.AttachmentEpoch,
		"noNewTag":                 len(newTags(warm1, warm2)) == 0,
	}
	l.emit("warmAttach", map[string]any{"scenario": "warm", "rep": rep, "checks": direct})
	for check, ok := range direct {
		if !ok {
			l.violate("warm continuation %s failed %s", t2.Name, check)
		}
	}
	// The second detach opens its own window; sample the pool until expiry.
	deadline, err := time.Parse(time.RFC3339Nano, warm2.Annotations["suspend-at"])
	if err != nil {
		return nil, fmt.Errorf("turn 2 detach opened no fresh warm window: %v", warm2.Annotations)
	}
	if previous, _ := time.Parse(time.RFC3339Nano, warm1.Annotations["suspend-at"]); !deadline.After(previous) {
		l.violate("turn 2 detach reused the earlier warm deadline")
	}
	samples := []map[string]any{}
	for time.Until(deadline) > 5*time.Second {
		current, err := l.workspace(ctx, t2.Workspace)
		if err != nil {
			return nil, err
		}
		s, err := l.snapshot(ctx, current, pool)
		if err != nil {
			return nil, err
		}
		samples = append(samples, map[string]any{"untilDeadlineMs": ms(time.Until(deadline)), "desiredState": s.DesiredState, "state": s.State,
			"actorExists": s.ActorExists, "workerPodExists": s.WorkerPodExists, "pool": s.Pool})
		if replicas, _ := s.Pool["desiredReplicas"].(int64); replicas < 1 || !s.ActorExists || !s.WorkerPodExists || s.DesiredState != "Ready" {
			l.violate("warm instance was not held for the whole window: %+v", s)
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(min(20*time.Second, time.Until(deadline)-5*time.Second)):
		}
	}
	l.emit("windowHeld", map[string]any{"scenario": "warm", "rep": rep, "deadline": deadline.UTC().Format(time.RFC3339Nano), "samples": samples})
	suspended, _, err := l.waitSuspended(ctx, t2.Workspace, pool, warm2, time.Until(deadline)+5*time.Minute)
	if err != nil {
		return nil, err
	}
	expiryLateness := time.Since(deadline)
	l.emit("suspended", map[string]any{"scenario": "warm", "rep": rep, "after": "window", "workspace": t2.Workspace,
		"observedAfterDeadlineMs": ms(expiryLateness), "snapshot": suspended})
	// Idle retention can collect older workspaces' Tags meanwhile, so the
	// expiry is checked for a Tag UID that did not exist before.
	if len(newTags(warm2, suspended)) == 0 {
		l.violate("window expiry suspended %s without a new native Tag", t2.Workspace)
	}
	if _, present := suspended.Annotations["suspend-at"]; present {
		l.violate("suspension kept the warm deadline")
	}
	// Turn 3 resumes from the Tag.
	t3, err := l.createTurn(ctx, session+"-t3", session, "lat-warm", false)
	if err != nil {
		return nil, err
	}
	if w, err = l.waitSettled(ctx, t3); err != nil {
		return nil, err
	}
	warm3, err := l.snapshot(ctx, w, t3.Identity["runtimePoolName"])
	if err != nil {
		return nil, err
	}
	l.logTurn("warm", rep, "t3", t3, suspended, warm3)
	if t3.Workspace != t1.Workspace || warm3.Sequence != warm2.Sequence+1 || !warm3.ResumePath || warm3.ActorUID == warm2.ActorUID ||
		t3.Identity["runtimeInstanceID"] == t2.Identity["runtimeInstanceID"] {
		l.violate("turn 3 %s did not resume the workspace from its Tag", t3.Name)
	}
	// Let the last window expire too, so the next repetition runs alone.
	if deadline3, err := time.Parse(time.RFC3339Nano, warm3.Annotations["suspend-at"]); err == nil {
		if _, _, err := l.waitSuspended(ctx, t3.Workspace, t3.Identity["runtimePoolName"], warm3, time.Until(deadline3)+5*time.Minute); err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("turn 3 detach opened no warm window")
	}
	return map[string]any{"session": session, "workspace": t1.Workspace, "t1": t1, "t2": t2, "t3": t3, "warmChecks": direct,
		"windowExpiryObservedAfterDeadlineMs": ms(expiryLateness)}, nil
}

func (l *latency) ensureClass(ctx context.Context, name string, window *metav1.Duration) error {
	retention := (*profile.RetentionPolicy)(nil)
	if window != nil {
		retention = &profile.RetentionPolicy{SuspendAfterDetach: window}
	}
	parameters := &profile.SubstrateWorkspaceProfile{ObjectMeta: metav1.ObjectMeta{Namespace: l.namespace, Name: name},
		Spec: profile.SubstrateWorkspaceProfileSpec{TemplateRef: profile.SubstrateTemplateReference{Namespace: l.namespace, Name: "infrastructure"},
			Suspend: &profile.SubstrateSuspendPolicy{Mode: profile.SubstrateSuspendModeDataOnly}, Retention: retention}}
	if err := l.c.Create(ctx, parameters); err != nil {
		return fmt.Errorf("create profile %s: %w", name, err)
	}
	idle, lifetime := metav1.Duration{Duration: 30 * time.Minute}, metav1.Duration{Duration: time.Hour}
	class := &api.ExecutionWorkspaceClass{ObjectMeta: metav1.ObjectMeta{Namespace: l.namespace, Name: name},
		Spec: api.ExecutionWorkspaceClassSpec{ProviderRef: &api.ClusterObjectReference{Name: "substrate"},
			ParametersRef: &api.TypedObjectReference{Group: profile.GroupVersion.Group, Kind: "SubstrateWorkspaceProfile", Name: name},
			Mode:          api.ExecutionWorkspaceModeInteractive, RequiredFeatures: []api.ExecutionWorkspaceFeature{api.WorkspaceFeatureACPRuntime},
			AllowedReuseScopes: []api.WorkspaceReuseScope{api.WorkspaceReuseScopeSession}, Lifecycle: api.ExecutionWorkspaceLifecycle{
				DefaultOnDetach: api.WorkspaceOnDetachSuspend, AllowedOnDetach: []api.WorkspaceOnDetach{api.WorkspaceOnDetachSuspend},
				DetachTimeout: metav1.Duration{Duration: time.Minute}, IdleTimeout: &idle, MaxLifetime: &lifetime,
				DeletionPolicy: api.ExecutionWorkspaceDeletionPolicy{ProviderResources: api.WorkspaceDeletionActionDelete, PersistentVolumes: api.WorkspaceDeletionActionDelete, Checkpoints: api.WorkspaceDeletionActionDelete}}}}
	if err := l.c.Create(ctx, class); err != nil {
		return fmt.Errorf("create class %s: %w", name, err)
	}
	return wait(ctx, func() (bool, error) {
		if err := l.c.Get(ctx, client.ObjectKeyFromObject(class), class); err != nil {
			return false, err
		}
		for _, condition := range class.Status.Conditions {
			if condition.Type == "Ready" && condition.ObservedGeneration == class.Generation {
				if condition.Status == metav1.ConditionTrue {
					return true, nil
				}
				if condition.Reason != "" && condition.Reason != "Reconciling" {
					fmt.Printf("class %s not Ready yet: %s %s\n", name, condition.Reason, condition.Message)
				}
			}
		}
		return false, nil
	})
}

// measure runs LATENCY_SCENARIOS (default "detach,warm") LATENCY_REPEATS
// times (default 3) with warm window LATENCY_WINDOW (default 2m).
func (p *proof) measure(ctx context.Context) (map[string]any, error) {
	l := &latency{proof: p, start: time.Now(), window: 2 * time.Minute}
	if raw := os.Getenv("LATENCY_WINDOW"); raw != "" {
		window, err := time.ParseDuration(raw)
		if err != nil {
			return nil, err
		}
		l.window = window
	}
	repeats := 3
	if raw := os.Getenv("LATENCY_REPEATS"); raw != "" {
		var err error
		if repeats, err = strconv.Atoi(raw); err != nil || repeats < 1 {
			return nil, fmt.Errorf("LATENCY_REPEATS must be a positive integer")
		}
	}
	scenarios := strings.Split(os.Getenv("LATENCY_SCENARIOS"), ",")
	if os.Getenv("LATENCY_SCENARIOS") == "" {
		scenarios = []string{"detach", "warm"}
	}
	suffix := os.Getenv("LATENCY_RUN_ID")
	report := map[string]any{"namespace": p.namespace, "window": l.window.String(), "repeats": repeats, "scenarios": scenarios}
	results := map[string][]map[string]any{}
	for _, scenario := range scenarios {
		var class string
		switch scenario {
		case "detach":
			class = "lat-detach"
			if err := l.ensureClassOnce(ctx, class, nil); err != nil {
				return nil, err
			}
		case "warm":
			class = "lat-warm"
			if err := l.ensureClassOnce(ctx, class, &metav1.Duration{Duration: l.window}); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unknown latency scenario %q", scenario)
		}
		for rep := 1; rep <= repeats; rep++ {
			id := rep
			if suffix != "" {
				// A later run in the same namespace uses fresh Session and Task names.
				n, _ := strconv.Atoi(suffix)
				id = n*100 + rep
			}
			var result map[string]any
			var err error
			if scenario == "detach" {
				result, err = l.detachScenario(ctx, id)
			} else {
				result, err = l.warmScenario(ctx, id)
			}
			if err != nil {
				report["events"] = l.events
				encoded, _ := json.Marshal(report)
				fmt.Println("NATIVE_CORE_LATENCY_PARTIAL", string(encoded))
				return nil, fmt.Errorf("%s repetition %d: %w", scenario, rep, err)
			}
			results[scenario] = append(results[scenario], result)
		}
	}
	report["results"] = results
	report["totalMs"] = time.Since(l.start).Milliseconds()
	report["violations"] = l.violations
	if len(l.violations) != 0 {
		encoded, _ := json.Marshal(report)
		fmt.Println("NATIVE_CORE_LATENCY", string(encoded))
		return nil, fmt.Errorf("%d expectation(s) failed: %s", len(l.violations), strings.Join(l.violations, "; "))
	}
	return report, nil
}

// ensureClassOnce creates the profile/class pair unless an earlier run in
// this namespace already did; an existing pair must already be Ready.
func (l *latency) ensureClassOnce(ctx context.Context, name string, window *metav1.Duration) error {
	existing := &api.ExecutionWorkspaceClass{}
	if err := l.c.Get(ctx, client.ObjectKey{Namespace: l.namespace, Name: name}, existing); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	return l.ensureClass(ctx, name, window)
}
