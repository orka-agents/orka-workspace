/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// SubstrateTemplateReference names the operator-owned native infrastructure
// ActorTemplate. The provider derives its runtime template from this object's
// placement, runsc, and snapshot storage; its containers do not execute ACP work.
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
// Tags retain verified Data snapshots; a fresh Actor and supervisor boot perform
// cold continuation. Full-memory restore remains prohibited by ADR 0030.
const SubstrateSuspendModeDataOnly SubstrateSuspendMode = "DataOnly"

// SubstrateSuspendPolicy requires Data/Data/ColdBoot snapshot settings in the
// derived ActorTemplate. The provider must verify native Tag provenance and
// confirm source termination before reporting suspension, as defined by ADR 0031.
type SubstrateSuspendPolicy struct {
	Mode SubstrateSuspendMode `json:"mode"`
}

// RetentionPolicy limits suspended occupancy per class and namespace. A class
// permitting suspension must separately configure expiry; a count cap requires
// maxLifetime because quota contention can defer suspension.
type RetentionPolicy struct {
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxSuspendedWorkspaces *int32 `json:"maxSuspendedWorkspaces,omitempty"`
}

// SubstrateWorkspaceProfileSpec contains only Substrate infrastructure inputs.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="SubstrateWorkspaceProfile spec is immutable; create a new profile"
type SubstrateWorkspaceProfileSpec struct {
	TemplateRef SubstrateTemplateReference `json:"templateRef"`

	// Suspend enables verified data-only cold continuation. Omission leaves
	// suspension unsupported; it does not permit a weaker memory checkpoint.
	// +optional
	Suspend *SubstrateSuspendPolicy `json:"suspend,omitempty"`

	// +optional
	Retention *RetentionPolicy `json:"retention,omitempty"`
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
