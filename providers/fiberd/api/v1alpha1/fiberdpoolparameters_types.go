/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// FiberdPoolParametersSpec is intentionally empty. Pool capacity is expressed by
// the generic ExecutionWorkspacePool resource; this object provides an
// adapter-owned namespaced parameters identity for conformance.
type FiberdPoolParametersSpec struct{}

// +kubebuilder:object:root=true
// +kubebuilder:resource:categories=orka

// FiberdPoolParameters is the namespaced parameters identity used by the
// development-only fiberd workspace pool reconciler.
type FiberdPoolParameters struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +optional
	Spec FiberdPoolParametersSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// FiberdPoolParametersList contains FiberdPoolParameters objects.
type FiberdPoolParametersList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FiberdPoolParameters `json:"items"`
}

func init() {
	SchemeBuilder.Register(&FiberdPoolParameters{}, &FiberdPoolParametersList{})
}
