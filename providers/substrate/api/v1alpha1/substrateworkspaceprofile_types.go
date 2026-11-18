/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package v1alpha1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SubstrateTemplateReference names the operator-owned native infrastructure
// ActorTemplate. The provider derives its runtime template from this object's
// worker placement, default Actor size, and snapshot storage; its containers do
// not execute ACP work.
type SubstrateTemplateReference struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Namespace is the native infrastructure namespace, defaulting to the
	// profile namespace. It is not a cross-namespace profile reference.
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// SubstrateSuspendMode identifies the only admitted snapshot scope.
// +kubebuilder:validation:Enum=DataOnly
type SubstrateSuspendMode string

// SubstrateSuspendModeDataOnly captures only workspace DurableDir data. Native
// Tags retain verified VOLUMES snapshots; a fresh Actor and supervisor boot
// perform cold continuation. Full-memory restore remains prohibited by ADR 0030.
const SubstrateSuspendModeDataOnly SubstrateSuspendMode = "DataOnly"

// SubstrateSuspendPolicy requires VOLUMES-fidelity snapshots with golden
// snapshots disabled in the derived ActorTemplate. The provider must verify
// native Tag provenance and confirm source termination before reporting
// suspension, as defined by ADR 0031.
type SubstrateSuspendPolicy struct {
	Mode SubstrateSuspendMode `json:"mode"`
}

// RetentionPolicy limits suspended occupancy per class and namespace. A class
// permitting suspension must separately configure expiry; a count cap requires
// maxLifetime because quota contention can defer suspension.
// +kubebuilder:validation:XValidation:rule="!has(self.suspendAfterDetach) || (duration(self.suspendAfterDetach) > duration('0s') && duration(self.suspendAfterDetach) <= duration('1h'))",message="suspendAfterDetach must be positive and at most 1h"
type RetentionPolicy struct {
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxSuspendedWorkspaces *int32 `json:"maxSuspendedWorkspaces,omitempty"`

	// SuspendAfterDetach is the warm window defined by Orka ADR 0055. Orka
	// keeps a detached session-reused workspace Ready, with its Actor and
	// supervisor running but its attachment revoked, for this long before the
	// frozen Suspend applies. Unset suspends immediately on detach. It requires
	// suspend.mode DataOnly and must be positive and at most 1h; Orka core
	// additionally requires it to be shorter than the class idleTimeout and
	// maxLifetime. There is no default, so existing profiles keep their hash.
	// +optional
	SuspendAfterDetach *metav1.Duration `json:"suspendAfterDetach,omitempty"`
}

// MaxSuspendAfterDetach bounds the warm window a profile may request.
const MaxSuspendAfterDetach = time.Hour

// SharedWorkersPolicy opts a profile in to shared native workers under the
// interim termination evidence of provider ADR 0008 and Orka ADR 0056. Up to
// maxActorsPerWorker data-only Actors of one namespace, profile and Actor size
// share one gVisor worker, which is recycled after maxWorkerLifetime. Taking
// this field changes the profile hash: sharing changes the security model, so
// it can only be adopted by creating a new profile.
// +kubebuilder:validation:XValidation:rule="!has(self.maxWorkerLifetime) || (duration(self.maxWorkerLifetime) >= duration('1h') && duration(self.maxWorkerLifetime) <= duration('24h'))",message="maxWorkerLifetime must be at least 1h and at most 24h"
type SharedWorkersPolicy struct {
	// MaxActorsPerWorker is the number of Actors one shared worker is sized
	// for and admits.
	// +kubebuilder:validation:Minimum=2
	// +kubebuilder:validation:Maximum=16
	MaxActorsPerWorker int32 `json:"maxActorsPerWorker"`

	// MaxWorkerLifetime bounds how long a shared worker lives. It stops
	// admitting Actors before then and is deleted, with exact worker Pod
	// absence proof for every Actor it still hosts, when it is reached.
	// Defaults to 24h, which is also the maximum.
	// +optional
	MaxWorkerLifetime *metav1.Duration `json:"maxWorkerLifetime,omitempty"`
}

// SubstratePlacementPolicy chooses how Actors are placed on native workers.
// Omission keeps one Actor per private worker.
type SubstratePlacementPolicy struct {
	// +optional
	SharedWorkers *SharedWorkersPolicy `json:"sharedWorkers,omitempty"`
}

const (
	// MinSharedActorsPerWorker and MaxSharedActorsPerWorker bound
	// sharedWorkers.maxActorsPerWorker.
	MinSharedActorsPerWorker = 2
	MaxSharedActorsPerWorker = 16
	// MinSharedWorkerLifetime and MaxSharedWorkerLifetime bound
	// sharedWorkers.maxWorkerLifetime; the maximum is also the default.
	MinSharedWorkerLifetime = time.Hour
	MaxSharedWorkerLifetime = 24 * time.Hour
)

// WorkerLifetime returns the effective maximum worker lifetime.
func (p *SharedWorkersPolicy) WorkerLifetime() time.Duration {
	if p == nil || p.MaxWorkerLifetime == nil {
		return MaxSharedWorkerLifetime
	}
	return p.MaxWorkerLifetime.Duration
}

// SubstrateWorkspaceProfileSpec contains only Substrate infrastructure inputs.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="SubstrateWorkspaceProfile spec is immutable; create a new profile"
// +kubebuilder:validation:XValidation:rule="!has(self.retention) || !has(self.retention.suspendAfterDetach) || (has(self.suspend) && self.suspend.mode == 'DataOnly')",message="retention.suspendAfterDetach requires suspend.mode DataOnly"
// +kubebuilder:validation:XValidation:rule="!has(self.placement) || !has(self.placement.sharedWorkers) || (has(self.suspend) && self.suspend.mode == 'DataOnly')",message="placement.sharedWorkers requires suspend.mode DataOnly"
type SubstrateWorkspaceProfileSpec struct {
	TemplateRef SubstrateTemplateReference `json:"templateRef"`

	// Suspend enables verified data-only cold continuation. Omission leaves
	// suspension unsupported; it does not permit a weaker memory checkpoint.
	// +optional
	Suspend *SubstrateSuspendPolicy `json:"suspend,omitempty"`

	// +optional
	Retention *RetentionPolicy `json:"retention,omitempty"`

	// Placement opts in to shared workers. Omission keeps one Actor per
	// private worker, and the profile hash is unchanged.
	// +optional
	Placement *SubstratePlacementPolicy `json:"placement,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:categories=orka
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// SubstrateWorkspaceProfile is resolved in its ExecutionWorkspaceClass
// namespace through a TypedObjectReference. Legacy ACP profiles remain owned by
// the old provider; creating this kind creates a new immutable parameter identity.
type SubstrateWorkspaceProfile struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec SubstrateWorkspaceProfileSpec `json:"spec"`
}

// +kubebuilder:object:root=true

type SubstrateWorkspaceProfileList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SubstrateWorkspaceProfile `json:"items"`
}

func init() { SchemeBuilder.Register(&SubstrateWorkspaceProfile{}, &SubstrateWorkspaceProfileList{}) }
