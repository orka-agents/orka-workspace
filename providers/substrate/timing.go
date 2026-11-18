package substrate

import (
	"context"
	"time"

	sdk "github.com/orka-agents/orka-workspace/sdk"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

// lifecycleTiming records when this provider's own clock reached each
// lifecycle milestone of one sequence. Every milestone is written by the same
// journal save that already persists its step, so timing adds no API writes;
// a field named ObservedAt is the first save that saw a step which has no save
// of its own. Times are microsecond-precise because metav1.Time keeps only
// seconds. Native API timestamps are never used: their clock differs.
type lifecycleTiming struct {
	// AllocationStartedAt is the sub-second copy of StartedAt.
	AllocationStartedAt *metav1.MicroTime `json:"allocationStartedAt,omitempty"`
	// WorkerPoolCreatedAt is when the private WorkerPool's UID was first saved.
	WorkerPoolCreatedAt *metav1.MicroTime `json:"workerPoolCreatedAt,omitempty"`
	TemplateCreatedAt   *metav1.MicroTime `json:"templateCreatedAt,omitempty"`
	ActorCreatedAt      *metav1.MicroTime `json:"actorCreatedAt,omitempty"`
	// TemplateUpdateObservedAt is when a restored Actor was first seen on its
	// rotated bootstrap template; the update itself has no save.
	TemplateUpdateObservedAt *metav1.MicroTime `json:"templateUpdateObservedAt,omitempty"`
	// WorkerActiveAt is when the active, empty private worker was fenced.
	WorkerActiveAt      *metav1.MicroTime `json:"workerActiveAt,omitempty"`
	ResumeIssuedAt      *metav1.MicroTime `json:"resumeIssuedAt,omitempty"`
	ChallengeObservedAt *metav1.MicroTime `json:"challengeObservedAt,omitempty"`
	ReadyAt             *metav1.MicroTime `json:"readyAt,omitempty"`

	RetirementStartedAt *metav1.MicroTime `json:"retirementStartedAt,omitempty"`
	SuspendIssuedAt     *metav1.MicroTime `json:"suspendIssuedAt,omitempty"`
	SuspendedObservedAt *metav1.MicroTime `json:"suspendedObservedAt,omitempty"`
	// TagCompletedAt is the sub-second copy of Checkpoint.CreatedAt. It stays
	// unset when a later pass recovers a catalog entry whose journal save was
	// lost; the timing log then uses CreatedAt.
	TagCompletedAt *metav1.MicroTime `json:"tagCompletedAt,omitempty"`
	// WorkerAbsentAt is when the exact worker Pods and private pool were
	// first proven absent.
	WorkerAbsentAt      *metav1.MicroTime `json:"workerAbsentAt,omitempty"`
	ActorDeleteIssuedAt *metav1.MicroTime `json:"actorDeleteIssuedAt,omitempty"`
	StoppedAt           *metav1.MicroTime `json:"stoppedAt,omitempty"`
}

// milestone returns this process's current time as it reads back from the
// journal: microsecond precision and no monotonic reading, so an in-memory
// record still equals its decoded copy.
func milestone() *metav1.MicroTime {
	now := metav1.NewMicroTime(time.Now().Truncate(time.Microsecond))
	return &now
}

// stamp sets a milestone once; later passes keep the first time.
func stamp(field **metav1.MicroTime) {
	if *field == nil {
		*field = milestone()
	}
}

// allocationStart prefers the precise start and falls back to StartedAt,
// which journals written before timing carry with second precision.
func allocationStart(record *journalRecord) *metav1.MicroTime {
	if record.Timing.AllocationStartedAt != nil {
		return record.Timing.AllocationStartedAt
	}
	if record.StartedAt.IsZero() {
		return nil
	}
	return &metav1.MicroTime{Time: record.StartedAt.Time}
}

// phase appends a duration in milliseconds when both bounds were recorded.
func phase(values []any, key string, from, to *metav1.MicroTime) []any {
	if from == nil || to == nil {
		return values
	}
	return append(values, key, to.Sub(from.Time).Milliseconds())
}

// timingSubject names the workspace by its Orka identity only. Provider-native
// names (Actor, template, private pool, worker Pod, Tag) never enter the log.
func timingSubject(record *journalRecord) []any {
	values := []any{"workspaceNamespace", record.Request.Key.Namespace, "workspaceName", record.Request.Key.Name, "sequence", record.Request.Sequence}
	if record.Request.Runtime != nil && record.Request.Runtime.PoolBinding.Name != "" {
		values = append(values, "runtimePool", record.Request.Runtime.PoolBinding.Name)
	}
	return values
}

func allocationPath(record *journalRecord) string {
	if record.InheritedCheckpoint != nil {
		return "resume"
	}
	return "cold"
}

// workerSource is spare when the private WorkerPool was a claimed spare, so
// workerPoolMs and workerActiveMs measure the claim instead of worker birth.
func workerSource(record *journalRecord) string {
	if spare := record.SpareWorker; spare != nil && !spare.Released && record.RuntimePool.UID != "" {
		return "spare"
	}
	return "fresh"
}

func readyTimingValues(record *journalRecord) []any {
	t := &record.Timing
	start := allocationStart(record)
	values := append(timingSubject(record), "path", allocationPath(record), "workerSource", workerSource(record))
	values = phase(values, "workerPoolMs", start, t.WorkerPoolCreatedAt)
	values = phase(values, "workerActiveMs", t.WorkerPoolCreatedAt, t.WorkerActiveAt)
	values = phase(values, "templateCreateMs", t.WorkerPoolCreatedAt, t.TemplateCreatedAt)
	values = phase(values, "actorCreateMs", t.TemplateCreatedAt, t.ActorCreatedAt)
	values = phase(values, "templateUpdateMs", t.ActorCreatedAt, t.TemplateUpdateObservedAt)
	values = phase(values, "workerToResumeMs", t.WorkerActiveAt, t.ResumeIssuedAt)
	values = phase(values, "resumeToChallengeMs", t.ResumeIssuedAt, t.ChallengeObservedAt)
	values = phase(values, "challengeToReadyMs", t.ChallengeObservedAt, t.ReadyAt)
	return phase(values, "totalMs", start, t.ReadyAt)
}

func stoppedTimingValues(record *journalRecord) []any {
	t := &record.Timing
	values := append(timingSubject(record), "operation", record.Operation)
	values = phase(values, "suspendMs", t.SuspendIssuedAt, t.SuspendedObservedAt)
	tagCompleted := t.TagCompletedAt
	// A recovered checkpoint, or one captured before this journal recorded
	// timing, has only its second-precision CreatedAt. That can truncate to
	// before the suspension was observed; such a bound would report a negative
	// Tag phase, so neither Tag-bounded phase is reported then. Without an
	// observed suspension there is no Tag phase to distort, and CreatedAt
	// still bounds the worker teardown.
	if tagCompleted == nil && record.Checkpoint != nil && !record.Checkpoint.CreatedAt.IsZero() &&
		(t.SuspendedObservedAt == nil || !record.Checkpoint.CreatedAt.Before(&metav1.Time{Time: t.SuspendedObservedAt.Time})) {
		tagCompleted = &metav1.MicroTime{Time: record.Checkpoint.CreatedAt.Time}
	}
	values = phase(values, "tagMs", t.SuspendedObservedAt, tagCompleted)
	// A suspension tears its worker down after the Tag completes; a stop does
	// so as soon as retirement starts.
	teardownStart := t.RetirementStartedAt
	if record.Operation == "suspend" {
		teardownStart = tagCompleted
	}
	values = phase(values, "workerTeardownMs", teardownStart, t.WorkerAbsentAt)
	values = phase(values, "actorDeleteMs", t.ActorDeleteIssuedAt, t.StoppedAt)
	return phase(values, "totalMs", t.RetirementStartedAt, t.StoppedAt)
}

// logRuntimeReady runs once, after the save that first persisted ReadyAt.
func logRuntimeReady(ctx context.Context, record *journalRecord) {
	ctrl.LoggerFrom(ctx).Info("Substrate runtime ready", readyTimingValues(record)...)
}

// logRuntimeStopped runs once, after the save that first persisted StoppedAt.
func logRuntimeStopped(ctx context.Context, record *journalRecord) {
	ctrl.LoggerFrom(ctx).Info("Substrate runtime stopped", stoppedTimingValues(record)...)
}

// firstReady reports whether this pass's Ready observation is the sequence's
// first. A journal that was already Ready before timing existed has no
// ReadyAt; it is not stamped late.
func firstReady(record *journalRecord, observed sdk.AllocationObservation) bool {
	return observed.State == sdk.AllocationReady && record.Observation.State != sdk.AllocationReady && record.Timing.ReadyAt == nil
}
