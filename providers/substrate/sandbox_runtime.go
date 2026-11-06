package substrate

import (
	"context"
	"fmt"
	"strings"

	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	"google.golang.org/protobuf/proto"
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

// restoreRuntimeMismatch classifies a crashed restore. It returns the failure
// to record, or "" when the runtimes match or either is unknown.
func (d *Lifecycle) restoreRuntimeMismatch(ctx context.Context, record *journalRecord, actor *pb.Actor) string {
	if record.InheritedCheckpoint == nil || record.InheritedCheckpoint.SandboxRuntime == nil || record.ChallengeSHA256 != "" {
		return ""
	}
	want := record.InheritedCheckpoint.SandboxRuntime
	var got *sandboxRuntimeStamp
	if assigned := actor.GetStatus().GetWorkerAssignment().GetSandboxRuntime(); assigned != nil {
		got, _ = stampSandboxRuntime(assigned.GetSandboxClass(), assigned.GetVersion())
	}
	if got == nil && record.Worker != nil {
		worker, err := d.control.GetWorker(ctx, &pb.GetWorkerRequest{Worker: &pb.ObjectRef{Name: record.Worker.Name}})
		if err == nil && worker.GetMetadata().GetUid() == record.Worker.UID {
			for _, class := range worker.GetStatus().GetSandboxClasses() {
				if class.GetSandboxClass() == want.Class {
					got, _ = stampSandboxRuntime(class.GetSandboxClass(), class.GetDefaultRuntime().GetVersion())
				}
			}
		}
	}
	if got == nil || *got == *want {
		return ""
	}
	return fmt.Sprintf("%s (checkpoint %s, restore %s); the checkpoint is retained for explicit recovery under a compatible runtime and uncertain work is never replayed", nativeRestoreRuntimeMismatchPrefix, want, got)
}
