package substrate

import (
	"context"
	"reflect"
	"slices"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
)

// passSnapshot is the journal one reconcile pass read and verified, and the
// observation it verified against native state. Only this workspace's own
// reconcile writes its journal, and the workqueue never runs two of them at
// once, so the snapshot stays current until this pass writes.
type passSnapshot struct {
	cm       *corev1.ConfigMap
	record   *journalRecord
	observed sdk.AllocationObservation
}

// steadyReady reports whether EnsureAllocation would change nothing after
// this pass's own Ready observation: every fence it checks holds on what this
// pass read and verified, it would issue no native effect, and its journal
// save would rewrite identical content. The warm follow-up turn, whose only
// change is a new attachment epoch, then publishes its observation without
// repeating the whole verification. Any doubt returns false, and the pass
// runs EnsureAllocation as before.
//
//nolint:gocyclo // Each EnsureAllocation step that could act is ruled out in turn.
func (d *Lifecycle) steadyReady(ctx context.Context, snap *passSnapshot, workspace *api.ExecutionWorkspace, provider *api.ExecutionWorkspaceProvider, request sdk.WorkloadRequest) bool {
	if snap == nil || snap.observed.State != sdk.AllocationReady {
		return false
	}
	record, cm := snap.record, snap.cm
	// The request, configuration and admission fences EnsureAllocation
	// starts with, on the workspace and provider this pass read; the status
	// publication is optimistic-locked on that workspace read.
	if validateRequest(request) != nil || d.config.admitEgressCredentials(request) != nil || d.control == nil || d.endpoint() == "" {
		return false
	}
	if admittedWorkspace(workspace, request) != nil || admittedProvider(provider, request) != nil || d.admitted(ctx, request) != nil {
		return false
	}
	if record.Request.Sequence != request.Sequence || record.Request.Revision != request.Revision || record.Operation != "ensure" || record.Failure != "" {
		return false
	}
	// requireJournal: protected, API-assigned and exactly marked.
	if cm.UID == "" || !slices.Contains(cm.Finalizers, journalProtectionFinalizer) || journalMarked(workspace, cm, record) != nil {
		return false
	}
	// Infrastructure and boot are complete, so ensureInfrastructure and
	// ensureNative would only re-verify what observeReady verified.
	if record.ChallengeSHA256 == "" || !record.BootRequested || record.Worker == nil || record.Template.UID == "" || record.Actor.UID == "" || record.Anchor.UID == "" ||
		record.RuntimePool.Name == "" || record.RuntimePool.UID == "" || len(record.RuntimePoolSpec) == 0 || len(record.SourceSelector) == 0 || record.RuntimePoolDeleting {
		return false
	}
	if spare := record.SpareWorker; spare != nil && spare.Released {
		return false
	}
	// No import, restore-runtime stamp, first-ready stamp or suspended
	// reservation release is left to write.
	if record.Request.RestoreFrom != nil && record.Request.Sequence == 1 || record.InheritedCheckpoint != nil && record.RestoreRuntime == nil ||
		record.SuspendedReservation || record.Timing.ReadyAt == nil || record.Observation.State != sdk.AllocationReady || !reflect.DeepEqual(record.Observation, snap.observed) {
		return false
	}
	// ensureNative also verifies the template the Actor was created from.
	// For a resume that is the inherited checkpoint's, which
	// verifyInheritedCheckpoint already verified in this pass.
	if record.CreateTemplate != record.Template && (record.InheritedCheckpoint == nil || record.CreateTemplate != record.InheritedCheckpoint.Template || record.InheritedCheckpoint.Atespace != record.Atespace) {
		if d.verifyTemplate(ctx, record.Atespace, record.CreateTemplate) != nil {
			return false
		}
	}
	return true
}
