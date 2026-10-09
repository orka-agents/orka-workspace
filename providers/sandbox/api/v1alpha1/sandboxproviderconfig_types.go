/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// SandboxProviderConfigSpec is the immutable configuration for an agent-sandbox
// installation. The kind selects the backend; there is no shared backend enum.
// Connection and controller deployment settings remain installation-owned.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="SandboxProviderConfig spec is immutable; create a new config"
type SandboxProviderConfigSpec struct{}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,categories=orka
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// SandboxProviderConfig is the cluster-scoped parameter kind referenced by an
// agent-sandbox ExecutionWorkspaceProvider. Existing ACP configuration objects
// remain with their original provider; this kind does not adopt them.
type SandboxProviderConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec SandboxProviderConfigSpec `json:"spec"`
}

// +kubebuilder:object:root=true

type SandboxProviderConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SandboxProviderConfig `json:"items"`
}

func init() { SchemeBuilder.Register(&SandboxProviderConfig{}, &SandboxProviderConfigList{}) }
