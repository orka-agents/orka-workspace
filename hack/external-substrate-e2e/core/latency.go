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
	"sync"
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
	annotationStopRequested      = "acp.workspace.orka.ai/stop-requested-at"
	runtimePoolUIDLabel          = "orka.ai/runtime-pool-uid"
	latencyPollInterval          = 100 * time.Millisecond
)

type latency struct {
	*proof
	start      time.Time
	window     time.Duration
	events     []map[string]any
	violations []string
	// sharedWorkspaces names workspaces placed on shared workers.
	sharedWorkspaces map[string]bool
	// onAccepted, when set, runs once per turn when its prompt is accepted.
	onAccepted func(t *turn)
	// warmClass/warmLabel and sharedClass/sharedLabel name the class and
	// report label of the running warm and shared scenario variant.
	warmClass, warmLabel, sharedClass, sharedLabel string
	// agentFor names the Agent a class's turns use; unset classes use the
	// Codex-typed native-core Agent.
	agentFor map[string]string
	// writeClasses run write-intent turns against LATENCY_WRITE_REPO
	// (round 10): each turn's fixture prompt asks for one file change, and
	// the clean-room publisher pushes it to branch latency/<session>.
	writeClasses    map[string]bool
	skipFinalExpiry bool
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
	// Attach is the client-observed warm attach on the workspace: when Core
	// requested the attachment, when the provider observed that generation,
	// and when it acknowledged the epoch, in ms since Create.
	Attach map[string]int64 `json:"attach,omitempty"`
}

var acceptedOrLater = map[string]bool{"Accepted": true, "Running": true, "Settling": true, "Succeeded": true}

func (l *latency) createTurn(ctx context.Context, name, session, class string, create bool) (*turn, error) {
	agentName := "native-core"
	if mapped := l.agentFor[class]; mapped != "" {
		agentName = mapped
	}
	prompt := "Run the deterministic external native workspace fixture (latency turn)."
	tools := []any{"Glob", "Grep", "Read"}
	if l.writeClasses[class] {
		prompt += " [latency-write:" + session + "]"
		tools = append(tools, "Write", "Edit")
	}
	task := l.object("Task", name, map[string]any{"type": "agent", "agentRef": map[string]any{"name": agentName},
		"prompt":     prompt,
		"timeout":    "5m",
		"sessionRef": map[string]any{"name": session, "create": create, "append": true},
		"execution": map[string]any{"workspace": map[string]any{"classRef": map[string]any{"name": class},
			"reusePolicy": "session", "onDetach": "Suspend"}},
		"agentRuntime": map[string]any{"maxTurns": int64(1), "allowedTools": tools, "allowBash": false}})
	if l.writeClasses[class] {
		repo := os.Getenv("LATENCY_WRITE_REPO")
		ref := func(role string) map[string]any { return map[string]any{"name": "r10-cred-" + role, "key": "token"} }
		_ = unstructured.SetNestedField(task.Object, map[string]any{"intent": "write", "gitRepo": repo, "branch": "main",
			"readCredentialRef": ref("read"), "publicationGitRepo": repo, "publicationReadCredentialRef": ref("publication-read"),
			"publicationCredentialRef": ref("push"), "forgeCredentialRef": ref("forge"),
			"pushBranch": "latency/" + session, "prBaseBranch": "main", "createPR": false}, "spec", "workspace")
	}
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
			if l.onAccepted != nil {
				// The runtime is serving this prompt now; a no-window detach
				// may retire it as soon as the Task settles.
				t.Workspace = str(task, "status", "executionWorkspace", "workspaceRef", "name")
				if t.Workspace == "" {
					t.Workspace = task.GetLabels()["acp.workspace.orka.ai/execution-workspace"]
				}
				l.onAccepted(t)
			}
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
	for _, key := range []string{annotationSuspendAt, annotationSuspendAfterDetach, annotationSuspendPending, annotationLastDetached, annotationDetachAction, annotationWorkloadPublished, annotationRetirementAuth, annotationStopRequested} {
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
		// Exactly the Actor and worker Pod that ran before the detach. A
		// shared worker (provider ADR 0008) outlives its Actors, so only the
		// Actor's absence counts there.
		shared := l.sharedWorkspaces[name]
		return !last.ActorExists && (shared || !last.WorkerPodExists) && l.instanceGone(ctx, w.Namespace, before, shared), nil
	})
	if err != nil {
		return last, time.Since(started), fmt.Errorf("workspace %s suspension: %w", name, err)
	}
	return last, time.Since(started), nil
}

// instanceGone reports the exact Actor UID and worker Pod UID of an earlier
// snapshot absent. It is conservative: any read error counts as present.
func (l *latency) instanceGone(ctx context.Context, atespace string, s *snapshot, sharedWorker bool) bool {
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
	if namespace, name, ok := strings.Cut(s.WorkerPod, "/"); ok && !sharedWorker {
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
	kind := map[string]string{"t1": "cold", "t2": "warm", "t3": "resume"}[ordinal]
	l.logTurnKind(scenario, rep, ordinal, kind, t, before, after)
}

// logTurnKind records one turn; kind (cold, resume, warm, ...) groups it.
func (l *latency) logTurnKind(scenario string, rep int, ordinal, kind string, t *turn, before, after *snapshot) {
	l.emit("turn", map[string]any{"scenario": scenario, "rep": rep, "turn": ordinal, "kind": kind, "task": t, "workspaceBefore": before, "workspaceAfter": after})
}

// detachScenario: the profile has no warm window, so every detach suspends.
func (l *latency) detachScenario(ctx context.Context, rep int) (map[string]any, error) {
	return l.detachLike(ctx, "detach", "lat-detach", rep, nil, nil)
}

// detachLike runs a cold turn, its suspension on detach, and a turn resumed
// from the Tag on class. Before each turn prepare (when set) waits for and
// lists the warm capacity, which check then verifies the turn used.
func (l *latency) detachLike(ctx context.Context, scenario, class string, rep int,
	check func(ordinal string, t *turn, before []workerPool) error, prepare func() ([]workerPool, error)) (map[string]any, error) {
	session := fmt.Sprintf("lat-%s-%d", scenario, rep)
	before := func() ([]workerPool, error) {
		if prepare == nil {
			return nil, nil
		}
		return prepare()
	}
	capacity, err := before()
	if err != nil {
		return nil, err
	}
	t1, err := l.createTurn(ctx, session+"-t1", session, class, true)
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
	l.logTurnKind(scenario, rep, "t1", "cold", t1, nil, afterT1)
	if check != nil {
		if err := check("t1", t1, capacity); err != nil {
			return nil, err
		}
	}
	suspended, waited, err := l.waitSuspended(ctx, t1.Workspace, pool, afterT1, 5*time.Minute)
	if err != nil {
		return nil, err
	}
	l.emit("suspended", map[string]any{"scenario": scenario, "rep": rep, "after": "t1", "workspace": t1.Workspace, "observedAfterSettleMs": ms(waited), "snapshot": suspended})
	if _, present := suspended.Annotations["suspend-at"]; present {
		l.violate("window-unset workspace carries a suspend-at deadline")
	}
	if len(newTags(afterT1, suspended)) == 0 {
		l.violate("suspension of %s produced no native Tag", t1.Workspace)
	}
	if capacity, err = before(); err != nil {
		return nil, err
	}
	t2, err := l.createTurn(ctx, session+"-t2", session, class, false)
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
	l.logTurnKind(scenario, rep, "t2", "resume", t2, suspended, afterT2)
	if t2.Workspace != t1.Workspace || afterT2.Sequence != afterT1.Sequence+1 || !afterT2.ResumePath || afterT2.ActorUID == afterT1.ActorUID ||
		t2.Identity["runtimeInstanceID"] == t1.Identity["runtimeInstanceID"] {
		l.violate("continuation %s did not resume the same workspace from its Tag on a new sequence and Actor", t2.Name)
	}
	if check != nil {
		if err := check("t2", t2, capacity); err != nil {
			return nil, err
		}
	}
	final, waited, err := l.waitSuspended(ctx, t2.Workspace, t2.Identity["runtimePoolName"], afterT2, 5*time.Minute)
	if err != nil {
		return nil, err
	}
	l.emit("suspended", map[string]any{"scenario": scenario, "rep": rep, "after": "t2", "workspace": t2.Workspace, "observedAfterSettleMs": ms(waited), "snapshot": final})
	return map[string]any{"session": session, "workspace": t1.Workspace, "t1": t1, "t2": t2, "resumedSequence": afterT2.Sequence}, nil
}

// warmScenario: the profile freezes a warm window. Turn 2 arrives inside it
// and must attach the running instance; then the window expires, retention
// suspends, and turn 3 resumes from the Tag.
func (l *latency) warmScenario(ctx context.Context, rep int) (map[string]any, error) {
	session := fmt.Sprintf("%s-%d", l.warmClass, rep)
	t1, err := l.createTurn(ctx, session+"-t1", session, l.warmClass, true)
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
	l.logTurn(l.warmLabel, rep, "t1", t1, nil, warm1)
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
	attach := l.watchAttach(ctx, t1.Workspace, warm1.AttachmentEpoch)
	t2, err := l.createTurn(ctx, session+"-t2", session, l.warmClass, false)
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
	t2.Attach = attach.result(t2.CreatedAt)
	l.logTurn(l.warmLabel, rep, "t2", t2, warm1, warm2)
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
		// ADR 0059: a persistent-session hit keeps the generation and a miss
		// creates a new one; either way the Session and its UID are the same
		// and the prompt is new. Which one happened is recorded, not checked.
		"sameSessionNewPrompt": t2.Identity["runtimeSessionUID"] == t1.Identity["runtimeSessionUID"] && t2.Identity["promptID"] != t1.Identity["promptID"],
		"newAttachmentEpoch":   warm2.AttachmentEpoch > warm1.AttachmentEpoch,
		// Other Sessions' windows can expire meanwhile when the last window
		// is left idle, so the namespace-wide Tag check applies only otherwise.
		"noNewTag": l.skipFinalExpiry || len(newTags(warm1, warm2)) == 0,
	}
	l.emit("warmAttach", map[string]any{"scenario": l.warmLabel, "rep": rep, "checks": direct,
		"sameGeneration": t2.Identity["runtimeSessionGeneration"] == t1.Identity["runtimeSessionGeneration"],
		"generations":    []string{t1.Identity["runtimeSessionGeneration"], t2.Identity["runtimeSessionGeneration"]}})
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
	l.emit("windowHeld", map[string]any{"scenario": l.warmLabel, "rep": rep, "deadline": deadline.UTC().Format(time.RFC3339Nano), "samples": samples})
	suspended, _, err := l.waitSuspended(ctx, t2.Workspace, pool, warm2, time.Until(deadline)+5*time.Minute)
	if err != nil {
		return nil, err
	}
	expiryLateness := time.Since(deadline)
	l.emit("suspended", map[string]any{"scenario": l.warmLabel, "rep": rep, "after": "window", "workspace": t2.Workspace,
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
	t3, err := l.createTurn(ctx, session+"-t3", session, l.warmClass, false)
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
	l.logTurn(l.warmLabel, rep, "t3", t3, suspended, warm3)
	if t3.Workspace != t1.Workspace || warm3.Sequence != warm2.Sequence+1 || !warm3.ResumePath || warm3.ActorUID == warm2.ActorUID ||
		t3.Identity["runtimeInstanceID"] == t2.Identity["runtimeInstanceID"] {
		l.violate("turn 3 %s did not resume the workspace from its Tag", t3.Name)
	}
	// Let the last window expire too, so the next repetition runs alone,
	// unless LATENCY_SKIP_FINAL_EXPIRY leaves it to idle (long default windows).
	if l.skipFinalExpiry {
		return map[string]any{"session": session, "workspace": t1.Workspace, "t1": t1, "t2": t2, "t3": t3, "warmChecks": direct,
			"windowExpiryObservedAfterDeadlineMs": ms(expiryLateness)}, nil
	}
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

// classOptions are the profile inputs a scenario varies.
type classOptions struct {
	window      *metav1.Duration
	annotations map[string]string
	placement   *profile.SubstratePlacementPolicy
	// classAnnotations go on the ExecutionWorkspaceClass, for example Orka
	// ADR 0057's acp.workspace.orka.ai/default-suspend-after-detach.
	classAnnotations map[string]string
}

func (l *latency) ensureClass(ctx context.Context, name string, options classOptions) error {
	retention := (*profile.RetentionPolicy)(nil)
	if options.window != nil {
		retention = &profile.RetentionPolicy{SuspendAfterDetach: options.window}
	}
	parameters := &profile.SubstrateWorkspaceProfile{ObjectMeta: metav1.ObjectMeta{Namespace: l.namespace, Name: name, Annotations: options.annotations},
		Spec: profile.SubstrateWorkspaceProfileSpec{TemplateRef: profile.SubstrateTemplateReference{Namespace: l.namespace, Name: "infrastructure"},
			Suspend: &profile.SubstrateSuspendPolicy{Mode: profile.SubstrateSuspendModeDataOnly}, Retention: retention, Placement: options.placement}}
	if err := l.c.Create(ctx, parameters); err != nil {
		return fmt.Errorf("create profile %s: %w", name, err)
	}
	idle, lifetime := metav1.Duration{Duration: 30 * time.Minute}, metav1.Duration{Duration: time.Hour}
	class := &api.ExecutionWorkspaceClass{ObjectMeta: metav1.ObjectMeta{Namespace: l.namespace, Name: name, Annotations: options.classAnnotations},
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
	l := &latency{proof: p, start: time.Now(), window: 2 * time.Minute, sharedWorkspaces: map[string]bool{},
		warmClass: "lat-warm", warmLabel: "warm", sharedClass: "lat-shared", sharedLabel: "shared",
		skipFinalExpiry: os.Getenv("LATENCY_SKIP_FINAL_EXPIRY") == "1", agentFor: map[string]string{}, writeClasses: map[string]bool{}}
	// LATENCY_CLAUDE_CLASSES lists classes whose turns use a Claude-typed
	// Agent over the same deterministic fixture: Codex native sessions are
	// never reused (ADR 0059), so warm-reuse scenarios need a non-Codex type.
	if raw := os.Getenv("LATENCY_CLAUDE_CLASSES"); raw != "" {
		if err := l.ensureClaudeAgent(ctx); err != nil {
			return nil, err
		}
		for _, class := range strings.Split(raw, ",") {
			l.agentFor[strings.TrimSpace(class)] = "native-core-claude"
		}
	}
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
		window := &metav1.Duration{Duration: l.window}
		defaultWindow, err := time.ParseDuration(envString("LATENCY_DEFAULT_WINDOW", "5m"))
		if err != nil {
			return nil, fmt.Errorf("LATENCY_DEFAULT_WINDOW: %w", err)
		}
		noDefaultWindow := map[string]string{"acp.workspace.orka.ai/default-suspend-after-detach": "disabled"}
		spares := envInt("LATENCY_SPARE_WORKERS", 1)
		sharedSlots := envInt("LATENCY_SHARED_SPARES", 2)
		lifetime, err := time.ParseDuration(envString("LATENCY_SHARED_LIFETIME", "3h"))
		if err != nil {
			return nil, fmt.Errorf("LATENCY_SHARED_LIFETIME: %w", err)
		}
		shared := &profile.SubstratePlacementPolicy{SharedWorkers: &profile.SharedWorkersPolicy{
			MaxActorsPerWorker: int32(envInt("LATENCY_SHARED_ACTORS", 4)), MaxWorkerLifetime: &metav1.Duration{Duration: lifetime}}}
		sharedSessions := envInt("LATENCY_SHARED_SESSIONS", 2)
		var run func(rep int) (map[string]any, error)
		switch scenario {
		case "detach":
			// Immediate suspension on detach: opt the class out of ADR 0057's default window.
			if err := l.ensureClassOnce(ctx, "lat-detach", classOptions{classAnnotations: noDefaultWindow}); err != nil {
				return nil, err
			}
			run = func(rep int) (map[string]any, error) { return l.detachScenario(ctx, rep) }
		case "warm":
			if err := l.ensureClassOnce(ctx, "lat-warm", classOptions{window: window}); err != nil {
				return nil, err
			}
			run = func(rep int) (map[string]any, error) {
				l.window, l.warmClass, l.warmLabel = window.Duration, "lat-warm", "warm"
				return l.warmScenario(ctx, rep)
			}
		case "warmdefault":
			// ADR 0057: no profile or class window, so Core's default applies.
			if err := l.ensureClassOnce(ctx, "lat-warmd", classOptions{}); err != nil {
				return nil, err
			}
			run = func(rep int) (map[string]any, error) {
				l.window, l.warmClass, l.warmLabel = defaultWindow, "lat-warmd", "warmdefault"
				return l.warmScenario(ctx, rep)
			}
		case "spare":
			// Provider ADR 0007: private spares, no window.
			if err := l.ensureClassOnce(ctx, "lat-spare", classOptions{annotations: map[string]string{spareWorkersAnnotation: strconv.Itoa(spares)}, classAnnotations: noDefaultWindow}); err != nil {
				return nil, err
			}
			uid, err := l.profileUID(ctx, "lat-spare")
			if err != nil {
				return nil, err
			}
			run = func(rep int) (map[string]any, error) { return l.spareScenario(ctx, rep, uid, spares) }
		case "shared":
			// Provider ADR 0008: shared workers with warm free slots and a window.
			if err := l.ensureClassOnce(ctx, "lat-shared", classOptions{window: window, placement: shared,
				annotations: map[string]string{spareWorkersAnnotation: strconv.Itoa(sharedSlots)}}); err != nil {
				return nil, err
			}
			uid, err := l.profileUID(ctx, "lat-shared")
			if err != nil {
				return nil, err
			}
			run = func(rep int) (map[string]any, error) {
				l.window, l.sharedClass, l.sharedLabel = window.Duration, "lat-shared", "shared"
				return l.sharedScenario(ctx, rep, uid, sharedSessions)
			}
		case "writeprivate", "writeshared":
			// Round 10: write-intent turns, three per Session inside Core's
			// default window, so turns 2 and 3 are warm write follow-ups.
			class, options := "lat-write-p", classOptions{}
			if scenario == "writeshared" {
				class, options = "lat-write-s", classOptions{placement: shared}
			}
			l.writeClasses[class] = true
			if err := l.ensureClassOnce(ctx, class, options); err != nil {
				return nil, err
			}
			name := scenario
			run = func(rep int) (map[string]any, error) { return l.writeScenario(ctx, name, class, rep) }
		case "shareddefault":
			// Shared workers under Core's default window (ADR 0057).
			if err := l.ensureClassOnce(ctx, "lat-shared-d", classOptions{placement: shared,
				annotations: map[string]string{spareWorkersAnnotation: strconv.Itoa(sharedSlots)}}); err != nil {
				return nil, err
			}
			uid, err := l.profileUID(ctx, "lat-shared-d")
			if err != nil {
				return nil, err
			}
			run = func(rep int) (map[string]any, error) {
				l.window, l.sharedClass, l.sharedLabel = defaultWindow, "lat-shared-d", "shareddefault"
				return l.sharedScenario(ctx, rep, uid, sharedSessions)
			}
		case "scope":
			// A second shared profile in the same namespace is another scope.
			if err := l.ensureClassOnce(ctx, "lat-shared-scope", classOptions{placement: shared}); err != nil {
				return nil, err
			}
			uid, err := l.profileUID(ctx, "lat-shared-scope")
			if err != nil {
				return nil, err
			}
			run = func(rep int) (map[string]any, error) { return l.scopeScenario(ctx, rep, uid) }
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
			result, err := run(id)
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
func (l *latency) ensureClassOnce(ctx context.Context, name string, options classOptions) error {
	existing := &api.ExecutionWorkspaceClass{}
	if err := l.c.Get(ctx, client.ObjectKey{Namespace: l.namespace, Name: name}, existing); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	return l.ensureClass(ctx, name, options)
}

func (l *latency) profileUID(ctx context.Context, name string) (string, error) {
	parameters := &profile.SubstrateWorkspaceProfile{}
	if err := l.c.Get(ctx, client.ObjectKey{Namespace: l.namespace, Name: name}, parameters); err != nil {
		return "", err
	}
	return string(parameters.UID), nil
}

func envString(name, fallback string) string {
	if raw := os.Getenv(name); raw != "" {
		return raw
	}
	return fallback
}

func envInt(name string, fallback int) int {
	if raw := os.Getenv(name); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil {
			return value
		}
	}
	return fallback
}

// attachWatch records, from the workspace itself, when Core requested a new
// attachment epoch, when the provider observed that generation, and when it
// acknowledged the epoch.
type attachWatch struct {
	mu     sync.Mutex
	marks  map[string]time.Time
	cancel context.CancelFunc
	done   chan struct{}
}

func (l *latency) watchAttach(ctx context.Context, name string, epochBefore int64) *attachWatch {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	w := &attachWatch{marks: map[string]time.Time{}, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(w.done)
		var generation int64
		for ctx.Err() == nil {
			current, err := l.workspace(ctx, name)
			now := time.Now()
			if err == nil {
				w.mu.Lock()
				if _, seen := w.marks["attachRequested"]; !seen && current.Spec.Attachment != nil && current.Spec.AttachmentEpoch > epochBefore {
					w.marks["attachRequested"], generation = now, current.Generation
				}
				if _, seen := w.marks["attachRequested"]; seen {
					if _, observed := w.marks["generationObserved"]; !observed && current.Status.ObservedGeneration >= generation {
						w.marks["generationObserved"] = now
					}
					if _, acknowledged := w.marks["attachAcknowledged"]; !acknowledged && current.Status.AttachedEpoch >= current.Spec.AttachmentEpoch && current.Spec.AttachmentEpoch > epochBefore {
						w.marks["attachAcknowledged"] = now
					}
				}
				complete := len(w.marks) == 3
				w.mu.Unlock()
				if complete {
					return
				}
			}
			select {
			case <-ctx.Done():
			case <-time.After(50 * time.Millisecond):
			}
		}
	}()
	return w
}

func (w *attachWatch) result(start time.Time) map[string]int64 {
	w.cancel()
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	out := map[string]int64{}
	for key, at := range w.marks {
		out[key+"Ms"] = ms(at.Sub(start))
	}
	return out
}

// ensureClaudeAgent creates, once per namespace, the Claude-typed Agent that
// the fixture runtime serves exactly like the Codex-typed native-core Agent.
func (l *latency) ensureClaudeAgent(ctx context.Context) error {
	existing := l.object("Agent", "native-core-claude", nil)
	if err := l.c.Get(ctx, client.ObjectKeyFromObject(existing), existing); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	agent := l.object("Agent", "native-core-claude", map[string]any{"runtime": map[string]any{"contractVersion": "orka.harness.v2", "type": "claude", "defaultMaxTurns": int64(1),
		"defaultAllowedTools": []any{"Glob", "Grep", "Read"}, "defaultAllowBash": false}, "model": map[string]any{"name": "claude-sonnet-4-5"}})
	if err := l.c.Create(ctx, agent); err != nil {
		return fmt.Errorf("Claude fixture Agent admission: %w", err)
	}
	return nil
}

// writeScenario runs three write-intent turns of one Session back to back:
// a cold first turn and two warm follow-ups inside the default window. It
// records each turn's delivery so the forge read-back can check what was
// published.
func (l *latency) writeScenario(ctx context.Context, scenario, class string, rep int) (map[string]any, error) {
	session := fmt.Sprintf("lat-%s-%d", map[string]string{"writeprivate": "wp", "writeshared": "ws"}[scenario], rep)
	var previous *snapshot
	turns := []any{}
	for i, kind := range []string{"cold", "warm", "warm"} {
		ordinal := fmt.Sprintf("t%d", i+1)
		t, err := l.createTurn(ctx, session+"-"+ordinal, session, class, i == 0)
		if err != nil {
			return nil, err
		}
		w, err := l.waitSettled(ctx, t)
		if err != nil {
			return nil, err
		}
		after, err := l.snapshot(ctx, w, t.Identity["runtimePoolName"])
		if err != nil {
			return nil, err
		}
		l.logTurnKind(scenario, rep, ordinal, kind, t, previous, after)
		task := l.object("Task", t.Name, nil)
		var delivery map[string]any
		waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		err = wait(waitCtx, func() (bool, error) {
			if err := l.c.Get(waitCtx, client.ObjectKeyFromObject(task), task); err != nil {
				return false, err
			}
			delivery, _, _ = unstructured.NestedMap(task.Object, "status", "delivery")
			return delivery["outcome"] != nil && delivery["outcome"] != "", nil
		})
		cancel()
		if err != nil {
			l.violate("write turn %s published no delivery outcome: %v", t.Name, err)
		}
		l.emit("writeTurn", map[string]any{"scenario": scenario, "rep": rep, "turn": ordinal, "task": t.Name, "session": session,
			"generation": t.Identity["runtimeSessionGeneration"], "outcome": delivery["outcome"], "verifiedRemoteSHA": delivery["verifiedRemoteSHA"],
			"startingSHA": delivery["startingSHA"], "remoteBeforeSHA": delivery["remoteBeforeSHA"], "branch": delivery["branch"],
			"deliveredAt": delivery["lastTransitionTime"], "doneMs": t.DoneMs, "settledMs": t.SettledMs})
		if delivery["outcome"] != "VerifiedExact" {
			l.violate("write turn %s delivery %v", t.Name, delivery["outcome"])
		}
		turns = append(turns, t)
		previous = after
	}
	return map[string]any{"session": session, "turns": turns}, nil
}
