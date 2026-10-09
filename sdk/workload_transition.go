// Copyright (c) 2026. MIT License - see LICENSE file for details.

package workspaceprovider

import (
	"fmt"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
)

// ValidateWorkloadTransition checks core's sequence update against the last
// provider observation. Providers must apply the same check against their durable
// journal, then verify retained backend resources before any cold resume effects.
// A same-request retry is harmless, including after retirement; it never grants
// authority to restart that retired request.
func ValidateWorkloadTransition(previous *WorkloadRequest, next WorkloadRequest, observed *AllocationObservation, requireRetainedData bool) error {
	if err := next.Validate(); err != nil {
		return err
	}
	if previous == nil {
		if next.Sequence != 1 || next.PreviousInstance != nil || next.RetainedData != nil {
			return ErrRequestConflict
		}
		return nil
	}
	if err := previous.Validate(); err != nil {
		return fmt.Errorf("invalid previous request: %w", err)
	}
	if next.Key != previous.Key {
		return ErrStaleIdentity
	}
	if next.Sequence == previous.Sequence && next.Revision == previous.Revision {
		return nil
	}
	if next.Sequence != previous.Sequence+1 || next.PreviousInstance == nil {
		return ErrRequestConflict
	}
	if observed == nil || observed.Key != previous.Key || observed.Sequence != previous.Sequence ||
		observed.Identity.RequestRevision != previous.Revision || !observed.Identity.Valid() ||
		*next.PreviousInstance != observed.Identity {
		return ErrStaleIdentity
	}
	if observed.State != AllocationStopped || observed.Startup != nil {
		return ErrInstanceRunning
	}
	if requireRetainedData && (next.RetainedData == nil || observed.RetainedData == nil) {
		return fmt.Errorf("cold resume requires verified retained data")
	}
	if observed.RetainedData != nil || next.RetainedData != nil {
		if observed.RetainedData == nil || next.RetainedData == nil ||
			*observed.RetainedData != *next.RetainedData || !observed.RetainedData.Valid() ||
			observed.RetainedData.SourceInstance != observed.Identity {
			return fmt.Errorf("retained data does not match the exact stopped instance")
		}
	}
	// A workload incarnation may rotate its pool and bootstrap material. Class
	// identity and provider parameters remain frozen for the workspace lifetime.
	if (previous.Runtime == nil) != (next.Runtime == nil) ||
		(previous.Runtime != nil && previous.Runtime.ClassBinding != next.Runtime.ClassBinding) ||
		!sameParameters(previous, &next) || !sameCheckpoint(previous.RestoreFrom, next.RestoreFrom) {
		return ErrRequestConflict
	}
	return nil
}

func sameCheckpoint(a, b *workspacev1alpha1.WorkloadCheckpointReference) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

func sameParameters(a, b *WorkloadRequest) bool {
	if (a.ParametersRef == nil) != (b.ParametersRef == nil) || (a.ParametersBinding == nil) != (b.ParametersBinding == nil) {
		return false
	}
	return (a.ParametersRef == nil || *a.ParametersRef == *b.ParametersRef) &&
		(a.ParametersBinding == nil || *a.ParametersBinding == *b.ParametersBinding)
}

// ValidateWorkspaceWorkload checks the persisted handoff's immutable object
// bindings. Current-generation core admission remains a separate prerequisite.
func ValidateWorkspaceWorkload(workspace *workspacev1alpha1.ExecutionWorkspace) error {
	if workspace == nil || workspace.Spec.Workload == nil {
		return fmt.Errorf("workspace has no workload request")
	}
	request := workspace.Spec.Workload
	if err := request.Validate(); err != nil {
		return err
	}
	if request.Key.Namespace != workspace.Namespace || request.Key.Name != workspace.Name ||
		request.Key.WorkspaceUID != workspace.UID || request.Key.ProviderUID != workspace.Spec.ProviderBinding.UID {
		return ErrStaleIdentity
	}
	if request.Runtime != nil && request.Runtime.ClassBinding != workspace.Spec.ClassBinding {
		return ErrStaleIdentity
	}
	return nil
}

// ValidateWorkloadRetirement checks core's exact-instance authorization. Provider
// controllers must call this before responding to desiredState with a physical
// retirement operation. An absent authorization means drain is still pending.
func ValidateWorkloadRetirement(request *WorkloadRequest, observed *AllocationObservation, retirement *WorkloadRetirement, action WorkloadRetirementAction) error {
	if request == nil || observed == nil || retirement == nil {
		return fmt.Errorf("exact-instance retirement authorization is pending")
	}
	if err := request.Validate(); err != nil {
		return err
	}
	if action != workspacev1alpha1.WorkloadRetirementStop && action != workspacev1alpha1.WorkloadRetirementSuspend && action != workspacev1alpha1.WorkloadRetirementDelete {
		return fmt.Errorf("unknown retirement action")
	}
	if retirement.Sequence != request.Sequence || observed.Sequence != request.Sequence || observed.Key != request.Key ||
		retirement.Identity != observed.Identity || !retirement.Identity.Valid() || retirement.Identity.RequestRevision != request.Revision {
		return ErrStaleIdentity
	}
	if retirement.Action != action && !(retirement.Action == workspacev1alpha1.WorkloadRetirementDelete && action == workspacev1alpha1.WorkloadRetirementStop) {
		return fmt.Errorf("retirement action is not authorized")
	}
	return nil
}
