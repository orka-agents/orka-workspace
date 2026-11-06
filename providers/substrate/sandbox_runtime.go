package substrate

import (
	"context"
	"fmt"
	"strings"

	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
)

// sandboxRuntimeStamp is the immutable runtime provenance of a data
// checkpoint: the sandbox class and compatibility version its snapshot was
// taken under (Snapshot.sandbox_runtime). Digest covers the exact
// compatibility attributes.
type sandboxRuntimeStamp struct {
	Class         string `json:"class"`
	SchemaVersion string `json:"schemaVersion"`
	Digest        string `json:"digest"`
}

func stampSandboxRuntime(class string, version *pb.VersionedSandboxCompat) (*sandboxRuntimeStamp, error) {
	if class == "" || version == nil || version.GetSchemaVersion() == "" {
		return nil, nil
	}
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(version)
	if err != nil {
		return nil, err
	}
	return &sandboxRuntimeStamp{Class: class, SchemaVersion: version.GetSchemaVersion(), Digest: digest(data)}, nil
}

// checkpointSandboxRuntime stamps a data snapshot. An unstamped snapshot is
// accepted (the field is optional upstream) but cannot classify a later
// cross-runtime restore; a stamp from another class fails closed.
func checkpointSandboxRuntime(snapshot *pb.Snapshot) (*sandboxRuntimeStamp, error) {
	runtime := snapshot.GetSandboxRuntime()
	if runtime == nil {
		return nil, nil
	}
	if runtime.GetSandboxClass() != gvisorSandboxClass {
		return nil, fmt.Errorf("native data snapshot was not taken under gVisor")
	}
	return stampSandboxRuntime(runtime.GetSandboxClass(), runtime.GetVersion())
}

func (s *sandboxRuntimeStamp) String() string {
	if s == nil {
		return "unrecorded"
	}
	return s.Class + "/" + s.SchemaVersion + "/" + strings.TrimPrefix(s.Digest, "sha256:")[:12]
}

// nativeRestoreRuntimeMismatchPrefix starts the failure recorded when a data
// restore crashed before its supervisor booted and the checkpoint was taken
// under a different sandbox runtime than the one that restored it. Agent
// Substrate runs every gVisor restore without memory under the worker's
// default runsc and does not check that it can read another version's
// filesystem checkpoint.
const nativeRestoreRuntimeMismatchPrefix = "the native data restore crashed under a different sandbox runtime"

// recordRestoreRuntime saves the runtime stamped on the Actor's worker
// assignment the first time this restore is observed with one. It reports
// whether it saved the record.
func (d *Lifecycle) recordRestoreRuntime(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord, actor *pb.Actor) (bool, error) {
	if record.InheritedCheckpoint == nil || record.RestoreRuntime != nil {
		return false, nil
	}
	assigned := actor.GetStatus().GetWorkerAssignment().GetSandboxRuntime()
	if assigned == nil {
		return false, nil
	}
	stamp, err := stampSandboxRuntime(assigned.GetSandboxClass(), assigned.GetVersion())
	if err != nil || stamp == nil {
		// An unreadable stamp is no evidence; the runtime stays unknown.
		return false, nil
	}
	record.RestoreRuntime = stamp
	return true, d.save(ctx, cm, record)
}

// fencedWorkerRuntime reads the default runtime that the fenced Worker runs
// the checkpoint's sandbox class under. ResumeActor places by selector, not by
// Worker, so the reading counts only while the private pool holds exactly one
// Worker and it is the fenced, active registration: then no other Worker could
// have performed the restore. It returns nil otherwise, or when there is no
// restore or fence.
func (d *Lifecycle) fencedWorkerRuntime(ctx context.Context, record *journalRecord) *sandboxRuntimeStamp {
	if record.InheritedCheckpoint == nil || record.InheritedCheckpoint.SandboxRuntime == nil || record.Worker == nil || record.Worker.Epoch == 0 {
		return nil
	}
	worker, err := d.privatePoolWorker(ctx, record)
	if err != nil || worker == nil || worker.GetMetadata().GetName() != record.Worker.Name || worker.GetMetadata().GetUid() != record.Worker.UID ||
		worker.GetStatus().GetRegisteredEpoch() != record.Worker.Epoch || worker.GetStatus().GetState() != pb.WorkerState_WORKER_STATE_ACTIVE {
		return nil
	}
	class := record.InheritedCheckpoint.SandboxRuntime.Class
	for _, entry := range worker.GetStatus().GetSandboxClasses() {
		if entry.GetSandboxClass() == class {
			stamp, err := stampSandboxRuntime(class, entry.GetDefaultRuntime().GetVersion())
			if err != nil {
				return nil
			}
			return stamp
		}
	}
	return nil
}

// restoreRuntimeMismatch classifies a crashed restore from evidence of the
// restore attempt itself, in order: the Actor's current assignment, the
// assignment recorded while it booted, or - when the restore crashed before
// any assignment was observed - the fenced Worker's default runtime read just
// before Resume, but only if the same Worker registration still reports that
// same default now, so it cannot have changed during the restore. Otherwise
// the runtime is unknown. It returns the failure to record, or "" when the
// runtimes match or either is unknown.
func (d *Lifecycle) restoreRuntimeMismatch(ctx context.Context, record *journalRecord, actor *pb.Actor) string {
	if record.InheritedCheckpoint == nil || record.InheritedCheckpoint.SandboxRuntime == nil || record.ChallengeSHA256 != "" {
		return ""
	}
	want := record.InheritedCheckpoint.SandboxRuntime
	got := record.RestoreRuntime
	if assigned := actor.GetStatus().GetWorkerAssignment().GetSandboxRuntime(); assigned != nil {
		if current, _ := stampSandboxRuntime(assigned.GetSandboxClass(), assigned.GetVersion()); current != nil {
			got = current
		}
	}
	if got == nil && record.PreResumeRuntime != nil {
		if now := d.fencedWorkerRuntime(ctx, record); now != nil && *now == *record.PreResumeRuntime {
			got = record.PreResumeRuntime
		}
	}
	if got == nil || *got == *want {
		return ""
	}
	return fmt.Sprintf("%s (checkpoint %s, restore %s); the checkpoint is retained for explicit recovery under a compatible runtime and uncertain work is never replayed", nativeRestoreRuntimeMismatchPrefix, want, got)
}
