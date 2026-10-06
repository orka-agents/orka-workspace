/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// SubstrateProviderConfigSpec is the immutable configuration for a Substrate
// installation. The kind selects the backend; there is no shared backend enum.
// Native control transport and credential references remain installation-owned.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="SubstrateProviderConfig spec is immutable; create a new config"
type SubstrateProviderConfigSpec struct{}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,categories=orka
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// SubstrateProviderConfig is the cluster-scoped parameter kind referenced by a
// Substrate ExecutionWorkspaceProvider. It does not transfer ownership from a
// legacy ACP provider or adopt that provider's retained resources.
type SubstrateProviderConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec SubstrateProviderConfigSpec `json:"spec"`
}

// +kubebuilder:object:root=true

type SubstrateProviderConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SubstrateProviderConfig `json:"items"`
}

func init() { SchemeBuilder.Register(&SubstrateProviderConfig{}, &SubstrateProviderConfigList{}) }
