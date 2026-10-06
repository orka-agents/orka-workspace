// Copyright (c) 2026. MIT License - see LICENSE file for details.

package workspaceprovider

import (
	"context"
	"errors"
	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
)

const LifecycleContractV1 = workspacev1alpha1.LifecycleContractV1

type AllocationKey = workspacev1alpha1.AllocationKey
type WorkloadRequest = workspacev1alpha1.WorkloadRequest
type RuntimeWorkload = workspacev1alpha1.RuntimeWorkload
type InstanceIdentity = workspacev1alpha1.InstanceIdentity
type PodReference = workspacev1alpha1.PodReference
type StartupEvidence = workspacev1alpha1.StartupEvidence
type AllocationState = workspacev1alpha1.AllocationState
type AllocationObservation = workspacev1alpha1.AllocationObservation
type PersistentVolumeEvidence = workspacev1alpha1.PersistentVolumeEvidence
type WorkloadRetirement = workspacev1alpha1.WorkloadRetirement
type WorkloadRetirementAction = workspacev1alpha1.WorkloadRetirementAction
type RetainedDataReference = workspacev1alpha1.RetainedDataReference

const (
	AllocationPending = workspacev1alpha1.AllocationPending
	AllocationReady   = workspacev1alpha1.AllocationReady
	AllocationStopped = workspacev1alpha1.AllocationStopped
	AllocationDeleted = workspacev1alpha1.AllocationDeleted
)

var (
	ErrNotFound             = errors.New("allocation not found")
	ErrStaleIdentity        = workspacev1alpha1.ErrStaleIdentity
	ErrRequestConflict      = workspacev1alpha1.ErrRequestConflict
	ErrInstanceRunning      = errors.New("instance has not stopped")
	ErrWorkspaceNotAdmitted = errors.New("workspace is not currently admitted by Orka core")
)

func WorkloadRevision(request WorkloadRequest) (string, error) {
	return workspacev1alpha1.WorkloadRevision(request)
}

func ValidateStartup(request WorkloadRequest, observed AllocationObservation) error {
	return workspacev1alpha1.ValidateStartup(request, observed)
}

// Lifecycle is derived from RuntimePool workload materialization and cleanup.
// Every method is retry-safe after a lost response. EnsureAllocation persists
// intent before backend effects and never recreates a stopped/deleted request.
// Observe is read-only. StopInstance must reject any mismatched fence. Delete
// requires observed termination and records data disposition before completion.
// Providers keep a deletion tombstone until the owning workspace is finalized.
type Lifecycle interface {
	EnsureAllocation(context.Context, WorkloadRequest) (AllocationObservation, error)
	Observe(context.Context, AllocationKey) (AllocationObservation, error)
	StopInstance(context.Context, AllocationKey, InstanceIdentity) (AllocationObservation, error)
	DeleteAllocation(context.Context, AllocationKey, InstanceIdentity, workspacev1alpha1.ExecutionWorkspaceDeletionPolicy) (AllocationObservation, error)
}

// AttachmentController is optional. Acknowledgement must not wait for runtime
// admission; revocation must fence the requested epoch even during an outage.
type AttachmentController interface {
	ActivateAttachment(context.Context, *workspacev1alpha1.ExecutionWorkspace) error
	RevokeAttachment(context.Context, *workspacev1alpha1.ExecutionWorkspace, int64) error
}

// SuspensionController is optional. It preserves verified durable data and must
// confirm termination of the exact instance before publishing retained lineage.
// EnsureAllocation consumes that lineage on a later numbered request, cold boots
// with fresh public bootstrap material, and keeps previous retirement tombstones.
type SuspensionController interface {
	SuspendInstance(context.Context, AllocationKey, InstanceIdentity) (AllocationObservation, error)
}
