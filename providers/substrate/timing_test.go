package substrate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// timingLogs captures the provider's production JSON log lines.
type timingLogs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *timingLogs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *timingLogs) context(t *testing.T) context.Context {
	return ctrl.LoggerInto(t.Context(), zap.New(zap.WriteTo(l)))
}

func (l *timingLogs) raw() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func (l *timingLogs) lines(t *testing.T, message string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(l.raw()), "\n") {
		if line == "" {
			continue
		}
		entry := map[string]any{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("log line is not JSON: %v", err)
		}
		if entry["msg"] == message {
			out = append(out, entry)
		}
	}
	return out
}

func (l *timingLogs) only(t *testing.T, message string) map[string]any {
	t.Helper()
	entries := l.lines(t, message)
	if len(entries) != 1 {
		t.Fatalf("%q logged %d times, want exactly once", message, len(entries))
	}
	return entries[0]
}

func readyWith(t *testing.T, ctx context.Context, c client.Client, native *nativeFixture, request sdk.WorkloadRequest) sdk.AllocationObservation {
	t.Helper()
	for range 12 {
		// A new Lifecycle per pass is a provider restart: only the journal persists.
		observed, err := driver(c, native).EnsureAllocation(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == sdk.AllocationReady {
			return observed
		}
	}
	t.Fatal("never ready")
	return sdk.AllocationObservation{}
}

func retiredWith(t *testing.T, ctx context.Context, c client.Client, native *nativeFixture, request sdk.WorkloadRequest, identity sdk.InstanceIdentity, suspend bool) sdk.AllocationObservation {
	t.Helper()
	for range 12 {
		var out sdk.AllocationObservation
		var err error
		if suspend {
			out, err = driver(c, native).SuspendInstance(ctx, request.Key, identity, nil)
		} else {
			out, err = driver(c, native).StopInstance(ctx, request.Key, identity)
		}
		if err != nil {
			t.Fatal(err)
		}
		if out.State == sdk.AllocationStopped {
			return out
		}
	}
	t.Fatal("never stopped")
	return sdk.AllocationObservation{}
}

func journal(t *testing.T, c client.Client, native *nativeFixture, key sdk.AllocationKey) *journalRecord {
	t.Helper()
	_, record, err := driver(c, native).read(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

// requireOrdered checks that each recorded milestone is set and not before
// the previous one.
func requireOrdered(t *testing.T, names []string, times ...*metav1.MicroTime) {
	t.Helper()
	for i, value := range times {
		if value == nil {
			t.Fatalf("milestone %s was not recorded", names[i])
		}
		if i > 0 && value.Before(times[i-1]) {
			t.Fatalf("milestone %s precedes %s", names[i], names[i-1])
		}
	}
}

func requireKeys(t *testing.T, entry map[string]any, present, absent []string) {
	t.Helper()
	for _, key := range present {
		value, ok := entry[key]
		if !ok {
			t.Fatalf("log has no %s: %v", key, entry)
		}
		if strings.HasSuffix(key, "Ms") {
			if ms, ok := value.(float64); !ok || ms < 0 {
				t.Fatalf("%s is not a non-negative duration: %v", key, value)
			}
		}
	}
	for _, key := range absent {
		if _, ok := entry[key]; ok {
			t.Fatalf("log has unexpected %s: %v", key, entry)
		}
	}
}

// requireOnlyTimingFields fails if a timing log line carries anything beyond
// the documented Orka-level identity and durations.
func requireOnlyTimingFields(t *testing.T, entry map[string]any) {
	t.Helper()
	allowed := []string{"level", "ts", "msg", "workspaceNamespace", "workspaceName", "runtimePool", "sequence", "path", "workerSource", "operation",
		"workerPoolMs", "workerActiveMs", "templateCreateMs", "actorCreateMs", "templateUpdateMs", "workerToResumeMs", "resumeToChallengeMs", "challengeToReadyMs",
		"suspendMs", "tagMs", "workerTeardownMs", "actorDeleteMs", "totalMs"}
	for key := range entry {
		if !slices.Contains(allowed, key) {
			t.Fatalf("timing log carries undocumented field %s", key)
		}
	}
}

// requireNoNativeIdentifiers fails if the log text names any credential,
// bootstrap material, snapshot location, or provider-native identifier.
func requireNoNativeIdentifiers(t *testing.T, logs string, records ...*journalRecord) {
	t.Helper()
	secrets := []string{"public-nonce", "public-key", "rotated-public-nonce", "gs://"}
	for _, record := range records {
		secrets = append(secrets, record.Actor.Name, record.Actor.UID, record.Template.Name, record.Template.UID, record.CreateTemplate.UID, record.RuntimePool.Name, record.RuntimePool.UID, record.Anchor.Name, record.Anchor.UID, record.EgressPolicy.UID, record.ChallengeSHA256, record.Observation.Identity.AllocationID, record.Observation.Identity.InstanceID)
		if record.Worker != nil {
			secrets = append(secrets, record.Worker.Name, record.Worker.UID, record.Worker.Pod, record.Worker.PodUID)
		}
		for _, checkpoint := range []*checkpointRecord{record.Checkpoint, record.InheritedCheckpoint} {
			if checkpoint != nil {
				secrets = append(secrets, checkpoint.Tag.Name, checkpoint.Tag.UID, checkpoint.SnapshotURI, checkpoint.SourceUID)
			}
		}
	}
	for _, secret := range secrets {
		if secret != "" && strings.Contains(logs, secret) {
			t.Fatalf("timing log exposes %q", secret)
		}
	}
}

func TestLifecycleTimingColdPathRecordsMilestonesAndLogsReadyOnce(t *testing.T) {
	c, native, request := fixture(t, false)
	logs := &timingLogs{}
	ctx := logs.context(t)
	observed := readyWith(t, ctx, c, native, request)
	// Later passes, restarts, and read-only observation never log again.
	readyWith(t, ctx, c, native, request)
	readyWith(t, ctx, c, native, request)
	if _, err := driver(c, native).Observe(ctx, request.Key); err != nil {
		t.Fatal(err)
	}
	record := journal(t, c, native, request.Key)
	timing := record.Timing
	requireOrdered(t, []string{"allocationStarted", "workerPoolCreated", "templateCreated", "actorCreated", "workerActive", "resumeIssued", "challengeObserved", "ready"},
		timing.AllocationStartedAt, timing.WorkerPoolCreatedAt, timing.TemplateCreatedAt, timing.ActorCreatedAt, timing.WorkerActiveAt, timing.ResumeIssuedAt, timing.ChallengeObservedAt, timing.ReadyAt)
	if timing.TemplateUpdateObservedAt != nil {
		t.Fatal("a cold boot recorded a restore template update")
	}
	// StartedAt reads back with second precision; both name the same instant.
	if !record.StartedAt.Equal(&metav1.Time{Time: timing.AllocationStartedAt.Truncate(time.Second)}) {
		t.Fatal("precise allocation start differs from StartedAt")
	}
	entry := logs.only(t, "Substrate runtime ready")
	requireKeys(t, entry, []string{"workspaceNamespace", "workspaceName", "runtimePool", "sequence", "path", "workerPoolMs", "workerActiveMs", "templateCreateMs", "actorCreateMs", "workerToResumeMs", "resumeToChallengeMs", "challengeToReadyMs", "totalMs"}, []string{"templateUpdateMs"})
	if entry["path"] != "cold" || entry["workerSource"] != "fresh" || entry["sequence"] != float64(1) || entry["workspaceNamespace"] != request.Key.Namespace || entry["workspaceName"] != request.Key.Name || entry["runtimePool"] != request.Runtime.PoolBinding.Name {
		t.Fatalf("ready log identifies the wrong allocation: %v", entry)
	}
	requireOnlyTimingFields(t, entry)
	requireNoNativeIdentifiers(t, logs.raw(), record)
	if observed.State != sdk.AllocationReady {
		t.Fatal("never ready")
	}
}

func TestLifecycleTimingStopRecordsRetirementAndLogsStoppedOnce(t *testing.T) {
	c, native, request := fixture(t, false)
	logs := &timingLogs{}
	ctx := logs.context(t)
	first := readyWith(t, ctx, c, native, request)
	retiredWith(t, ctx, c, native, request, first.Identity, false)
	retiredWith(t, ctx, c, native, request, first.Identity, false)
	record := journal(t, c, native, request.Key)
	timing := record.Timing
	requireOrdered(t, []string{"ready", "retirementStarted", "workerAbsent", "actorDeleteIssued", "stopped"},
		timing.ReadyAt, timing.RetirementStartedAt, timing.WorkerAbsentAt, timing.ActorDeleteIssuedAt, timing.StoppedAt)
	if timing.SuspendIssuedAt != nil || timing.SuspendedObservedAt != nil || timing.TagCompletedAt != nil {
		t.Fatal("a stop recorded suspension milestones")
	}
	entry := logs.only(t, "Substrate runtime stopped")
	requireKeys(t, entry, []string{"workspaceNamespace", "workspaceName", "runtimePool", "sequence", "operation", "workerTeardownMs", "actorDeleteMs", "totalMs"}, []string{"suspendMs", "tagMs", "path"})
	if entry["operation"] != "stop" {
		t.Fatalf("stopped log names the wrong operation: %v", entry)
	}
	requireOnlyTimingFields(t, entry)
	requireNoNativeIdentifiers(t, logs.raw(), record)
}

func TestLifecycleTimingSuspendAndResumeFromTag(t *testing.T) {
	c, native, request := fixture(t, true)
	logs := &timingLogs{}
	ctx := logs.context(t)
	first := readyWith(t, ctx, c, native, request)
	stopped := retiredWith(t, ctx, c, native, request, first.Identity, true)
	// A completed suspension is idempotent and does not log again.
	retiredWith(t, ctx, c, native, request, first.Identity, true)
	suspended := journal(t, c, native, request.Key)
	timing := suspended.Timing
	requireOrdered(t, []string{"ready", "retirementStarted", "suspendIssued", "suspendedObserved", "tagCompleted", "workerAbsent", "actorDeleteIssued", "stopped"},
		timing.ReadyAt, timing.RetirementStartedAt, timing.SuspendIssuedAt, timing.SuspendedObservedAt, timing.TagCompletedAt, timing.WorkerAbsentAt, timing.ActorDeleteIssuedAt, timing.StoppedAt)
	if suspended.Checkpoint == nil || !suspended.Checkpoint.CreatedAt.Equal(&metav1.Time{Time: timing.TagCompletedAt.Truncate(time.Second)}) {
		t.Fatal("Tag completion differs from the checkpoint's creation time")
	}
	stoppedEntry := logs.only(t, "Substrate runtime stopped")
	requireKeys(t, stoppedEntry, []string{"operation", "suspendMs", "tagMs", "workerTeardownMs", "actorDeleteMs", "totalMs"}, nil)
	if stoppedEntry["operation"] != "suspend" {
		t.Fatalf("stopped log names the wrong operation: %v", stoppedEntry)
	}
	requireOnlyTimingFields(t, stoppedEntry)

	next := *request.DeepCopy()
	next.Sequence++
	next.PreviousInstance = &first.Identity
	next.RetainedData = stopped.RetainedData
	next.Runtime.Template.Spec.Containers[0].Env[0].Value = "rotated-public-nonce"
	next.Revision, _ = sdk.WorkloadRevision(next)
	if err := admit(c)(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	readyWith(t, ctx, c, native, next)
	readyWith(t, ctx, c, native, next)
	resumed := journal(t, c, native, request.Key)
	timing = resumed.Timing
	requireOrdered(t, []string{"allocationStarted", "workerPoolCreated", "templateCreated", "actorCreated", "templateUpdateObserved", "workerActive", "resumeIssued", "challengeObserved", "ready"},
		timing.AllocationStartedAt, timing.WorkerPoolCreatedAt, timing.TemplateCreatedAt, timing.ActorCreatedAt, timing.TemplateUpdateObservedAt, timing.WorkerActiveAt, timing.ResumeIssuedAt, timing.ChallengeObservedAt, timing.ReadyAt)
	if timing.RetirementStartedAt != nil || timing.StoppedAt != nil {
		t.Fatal("a successor inherited its predecessor's retirement milestones")
	}
	readyEntries := logs.lines(t, "Substrate runtime ready")
	if len(readyEntries) != 2 {
		t.Fatalf("ready logged %d times across two sequences, want 2", len(readyEntries))
	}
	resumeEntry := readyEntries[1]
	requireKeys(t, resumeEntry, []string{"path", "workerPoolMs", "workerActiveMs", "templateCreateMs", "actorCreateMs", "templateUpdateMs", "workerToResumeMs", "resumeToChallengeMs", "challengeToReadyMs", "totalMs"}, nil)
	if resumeEntry["path"] != "resume" || resumeEntry["workerSource"] != "fresh" || resumeEntry["sequence"] != float64(2) || readyEntries[0]["path"] != "cold" {
		t.Fatalf("ready logs name the wrong path: %v", readyEntries)
	}
	requireOnlyTimingFields(t, resumeEntry)

	// The history archive keeps the predecessor's milestones.
	_, history, err := driver(c, native).readAt(t.Context(), request.Key, historyKey(request.Key, request.Sequence))
	if err != nil {
		t.Fatal(err)
	}
	if history.Timing.StoppedAt == nil || !timingEqual(history.Timing, suspended.Timing) {
		t.Fatal("history archive lost the predecessor's lifecycle timing")
	}
	requireNoNativeIdentifiers(t, logs.raw(), suspended, resumed)
}

func timingEqual(a, b lifecycleTiming) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return bytes.Equal(left, right)
}

func TestLifecycleTimingJournalCompatibility(t *testing.T) {
	c, native, request := fixture(t, false)
	logs := &timingLogs{}
	ctx := logs.context(t)
	readyWith(t, ctx, c, native, request)
	key := journalKey(request.Key)
	cm := &corev1.ConfigMap{}
	if err := c.Get(t.Context(), key, cm); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cm.Data[journalDataKey], `"timing":{`) {
		t.Fatal("new journal did not persist lifecycle timing")
	}
	// A journal written by this code passes every journal validation and
	// decodes to exactly what was saved.
	_, written, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := protectedJournalRecord(cm); err != nil {
		t.Fatalf("journal protection rejected a timed journal: %v", err)
	}
	if err := validateJournalRecord(written); err != nil {
		t.Fatal(err)
	}
	encoded, err := encode(written)
	if err != nil {
		t.Fatal(err)
	}
	if encoded != cm.Data[journalDataKey] {
		t.Fatal("timed journal does not round-trip byte for byte")
	}
	// Untimed records encode exactly as before.
	untimed := *written
	untimed.Timing = lifecycleTiming{}
	if data, err := encode(&untimed); err != nil || strings.Contains(data, `"timing"`) {
		t.Fatalf("untimed journal encodes a timing key: %v", err)
	}

	// A journal written before timing existed still decodes and keeps working;
	// an already-Ready instance is never stamped or logged late.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(cm.Data[journalDataKey]), &raw); err != nil {
		t.Fatal(err)
	}
	delete(raw, "timing")
	legacy, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	cm.Data[journalDataKey] = string(legacy)
	if err := c.Update(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), key, cm); err != nil {
		t.Fatal(err)
	}
	if _, _, err := protectedJournalRecord(cm); err != nil {
		t.Fatalf("journal protection rejected an untimed journal: %v", err)
	}
	old := journal(t, c, native, request.Key)
	if old.Timing != (lifecycleTiming{}) || old.StartedAt.IsZero() {
		t.Fatal("untimed journal decoded with timing or without its start")
	}
	readyWith(t, ctx, c, native, request)
	if journal(t, c, native, request.Key).Timing.ReadyAt != nil {
		t.Fatal("an instance already Ready before timing was stamped late")
	}
	if entries := logs.lines(t, "Substrate runtime ready"); len(entries) != 1 {
		t.Fatalf("ready logged %d times, want once", len(entries))
	}
}

func TestLifecycleTimingPhasesNeedBothBounds(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	at := func(ms int) *metav1.MicroTime {
		value := metav1.NewMicroTime(base.Add(time.Duration(ms) * time.Millisecond))
		return &value
	}
	record := &journalRecord{Request: sdk.WorkloadRequest{Sequence: 3, Key: sdk.AllocationKey{Namespace: "tenant", Name: "workspace"}}, Operation: "ensure", StartedAt: metav1.Time{Time: base}}
	record.Timing = lifecycleTiming{AllocationStartedAt: at(0), WorkerPoolCreatedAt: at(40), ResumeIssuedAt: at(900), ChallengeObservedAt: at(1500), ReadyAt: at(1750)}
	values := readyTimingValues(record)
	got := map[string]any{}
	for i := 0; i < len(values); i += 2 {
		got[values[i].(string)] = values[i+1]
	}
	want := map[string]any{"workspaceNamespace": "tenant", "workspaceName": "workspace", "sequence": int64(3), "path": "cold", "workerSource": "fresh", "workerPoolMs": int64(40), "resumeToChallengeMs": int64(600), "challengeToReadyMs": int64(250), "totalMs": int64(1750)}
	if len(got) != len(want) {
		t.Fatalf("phases = %v, want %v", got, want)
	}
	for key, value := range want {
		if got[key] != value {
			t.Fatalf("%s = %v, want %v", key, got[key], value)
		}
	}
	// Without the precise start, StartedAt bounds the allocation.
	record.Timing.AllocationStartedAt = nil
	values = readyTimingValues(record)
	if !slices.Contains(values, any("totalMs")) {
		t.Fatal("legacy start did not bound the allocation")
	}

	// A suspension journaled before timing falls back to the checkpoint's time.
	stopped := &journalRecord{Request: record.Request, Operation: "suspend", Checkpoint: &checkpointRecord{CreatedAt: metav1.Time{Time: base.Add(3 * time.Second)}}}
	stopped.Timing = lifecycleTiming{RetirementStartedAt: at(0), SuspendIssuedAt: at(100), SuspendedObservedAt: at(2100), WorkerAbsentAt: at(5000), StoppedAt: at(5600)}
	values = stoppedTimingValues(stopped)
	got = map[string]any{}
	for i := 0; i < len(values); i += 2 {
		got[values[i].(string)] = values[i+1]
	}
	if got["suspendMs"] != int64(2000) || got["tagMs"] != int64(900) || got["workerTeardownMs"] != int64(2000) || got["totalMs"] != int64(5600) || got["actorDeleteMs"] != nil {
		t.Fatalf("suspension phases = %v", got)
	}
	// A CreatedAt that truncates to before the suspension was observed bounds
	// no Tag phase rather than a negative one.
	stopped.Checkpoint.CreatedAt = metav1.Time{Time: base.Add(2 * time.Second)}
	values = stoppedTimingValues(stopped)
	if slices.Contains(values, any("tagMs")) || slices.Contains(values, any("workerTeardownMs")) || !slices.Contains(values, any("suspendMs")) {
		t.Fatalf("truncated checkpoint time bounded a Tag phase: %v", values)
	}
	// A capture that finished before the upgrade recorded no observed
	// suspension; its CreatedAt still bounds the worker teardown that
	// finished afterwards.
	stopped.Timing.SuspendIssuedAt, stopped.Timing.SuspendedObservedAt = nil, nil
	values = stoppedTimingValues(stopped)
	got = map[string]any{}
	for i := 0; i < len(values); i += 2 {
		got[values[i].(string)] = values[i+1]
	}
	if got["workerTeardownMs"] != int64(3000) || got["tagMs"] != nil || got["suspendMs"] != nil {
		t.Fatalf("pre-upgrade capture phases = %v", got)
	}
}

// A capture whose catalog write committed but whose journal save was lost is
// recovered by a later pass. The checkpoint keeps the catalog's first
// CreatedAt, so the later pass's time is not recorded as the Tag's
// completion, and the stopped log bounds the Tag phases by CreatedAt.
func TestLifecycleTimingRecoveredCheckpointKeepsCatalogCompletion(t *testing.T) {
	c, native, request := fixture(t, true)
	logs := &timingLogs{}
	ctx := logs.context(t)
	first := readyWith(t, ctx, c, native, request)
	var lost *checkpointRecord
	wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if cm, ok := obj.(*corev1.ConfigMap); ok && lost == nil && cm.Data[journalDataKey] != "" {
			var record journalRecord
			if json.Unmarshal([]byte(cm.Data[journalDataKey]), &record) == nil && record.Checkpoint != nil {
				lost = record.Checkpoint
				return fmt.Errorf("lost journal save after catalog registration")
			}
		}
		return inner.Update(ctx, obj, opts...)
	}})
	for range 12 {
		if _, err := driver(wrapped, native).SuspendInstance(ctx, request.Key, first.Identity, nil); err != nil {
			break
		}
	}
	if lost == nil {
		t.Fatal("the capture's journal save was never lost")
	}
	_, catalog, err := driver(c, native).readCatalog(t.Context(), catalogKey(request.Key.Namespace, lost.Tag.UID))
	if err != nil {
		t.Fatalf("catalog registration did not commit before the lost journal save: %v", err)
	}
	retiredWith(t, ctx, c, native, request, first.Identity, true)
	suspended := journal(t, c, native, request.Key)
	if suspended.Checkpoint == nil || !suspended.Checkpoint.CreatedAt.Equal(&catalog.Checkpoint.CreatedAt) {
		t.Fatal("recovery did not keep the catalog's first completion time")
	}
	if suspended.Timing.TagCompletedAt != nil {
		t.Fatalf("recovery recorded its own time %v as the Tag's completion", suspended.Timing.TagCompletedAt)
	}
	entry := logs.only(t, "Substrate runtime stopped")
	requireOnlyTimingFields(t, entry)
	createdAt := suspended.Checkpoint.CreatedAt.Time
	if observed := suspended.Timing.SuspendedObservedAt; observed != nil && !createdAt.Before(observed.Time) {
		requireKeys(t, entry, []string{"tagMs", "workerTeardownMs"}, nil)
		if entry["tagMs"] != float64(createdAt.Sub(observed.Time).Milliseconds()) {
			t.Fatalf("tagMs = %v, want the phase bounded by the catalog's CreatedAt", entry["tagMs"])
		}
	} else {
		requireKeys(t, entry, nil, []string{"tagMs", "workerTeardownMs"})
	}
}
