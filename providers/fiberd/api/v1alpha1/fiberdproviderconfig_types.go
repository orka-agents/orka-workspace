/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// FiberdProviderConfigSpec is intentionally empty. The in-memory development
// provider needs only a concrete cluster-scoped identity for reference and
// REST-mapping conformance.
type FiberdProviderConfigSpec struct{}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,categories=orka

// FiberdProviderConfig is the cluster-scoped configuration identity used by the
// development-only fiberd workspace provider.
type FiberdProviderConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +optional
	Spec FiberdProviderConfigSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// FiberdProviderConfigList contains FiberdProviderConfig objects.
type FiberdProviderConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FiberdProviderConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&FiberdProviderConfig{}, &FiberdProviderConfigList{})
}
