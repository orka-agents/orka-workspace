/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// SandboxSuspendMode identifies the only admitted cold-resume scope.
// +kubebuilder:validation:Enum=DataOnly
type SandboxSuspendMode string

// SandboxSuspendModeDataOnly preserves workspace PVC data. It never preserves
// process memory, supervisor state, or credentials.
const SandboxSuspendModeDataOnly SandboxSuspendMode = "DataOnly"

// SandboxDurableVolume freezes the requested workspace PVC shape. The provider
// owns its mount path and verifies the realized PVC identity before bootstrap.
type SandboxDurableVolume struct {
	// StorageClassName selects a class, or the cluster default when empty.
	// Resolution must pin the live class UID and verify its deletion policy.
	// +optional
	StorageClassName string `json:"storageClassName,omitempty"`

	// AccessModes defaults to ReadWriteOnce when omitted or empty. Every mode
	// must permit the runtime to write repository/workspace data.
	// +listType=set
	// +kubebuilder:validation:items:Enum=ReadWriteOnce;ReadWriteOncePod;ReadWriteMany
	// +kubebuilder:default={ReadWriteOnce}
	// +optional
	AccessModes []string `json:"accessModes,omitempty"`

	// Capacity is a positive Kubernetes storage quantity, for example 1Gi.
	// +kubebuilder:validation:MinLength=1
	Capacity string `json:"capacity"`
}

// SandboxSuspendPolicy permits data-only cold suspension. The claim creates a
// dedicated PVC instead of adopting warm capacity. Suspension must terminate
// the exact Pod; resume starts a fresh Pod and repeats identity verification
// and secure bootstrap while preserving only the workspace PVC.
type SandboxSuspendPolicy struct {
	Mode   SandboxSuspendMode   `json:"mode"`
	Volume SandboxDurableVolume `json:"volume"`
}

// RetentionPolicy limits suspended occupancy per class and namespace. The
// owning class must separately provide an expiry bound. A configured count cap
// requires maxLifetime because capacity contention can defer suspension.
type RetentionPolicy struct {
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxSuspendedWorkspaces *int32 `json:"maxSuspendedWorkspaces,omitempty"`
}

// SandboxWorkspaceProfileSpec contains only agent-sandbox parameters.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="SandboxWorkspaceProfile spec is immutable; create a new profile"
type SandboxWorkspaceProfileSpec struct {
	// Suspend enables PVC-backed cold resume. Omission keeps suspension
	// unsupported and leaves ordinary delete/recreate behavior unchanged.
	// +optional
	Suspend *SandboxSuspendPolicy `json:"suspend,omitempty"`

	// +optional
	Retention *RetentionPolicy `json:"retention,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:categories=orka
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// SandboxWorkspaceProfile is resolved in the ExecutionWorkspaceClass namespace
// through its TypedObjectReference. It cannot supply a different namespace.
type SandboxWorkspaceProfile struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Default an empty functional spec so later additions are also subject to
	// the immutable-spec transition rule.
	// +kubebuilder:default={}
	// +optional
	Spec SandboxWorkspaceProfileSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

type SandboxWorkspaceProfileList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SandboxWorkspaceProfile `json:"items"`
}

func init() { SchemeBuilder.Register(&SandboxWorkspaceProfile{}, &SandboxWorkspaceProfileList{}) }
