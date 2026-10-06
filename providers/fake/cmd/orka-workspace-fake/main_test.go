// Copyright (c) 2026. MIT License - see LICENSE file for details.

package main

import (
	"testing"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestProviderBinarySchemeSupportsTypedNetworkPolicyInventory(t *testing.T) {
	scheme, err := newProviderScheme()
	if err != nil {
		t.Fatal(err)
	}
	policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "runtime", Name: "core-network"}, Spec: networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"runtime": "selected"}},
		PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
	}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(policy).Build()
	policies := &networkingv1.NetworkPolicyList{}
	if err := c.List(t.Context(), policies, client.InNamespace(policy.Namespace)); err != nil {
		t.Fatalf("deployed provider cannot inventory admitted network isolation: %v", err)
	}
	if len(policies.Items) != 1 || policies.Items[0].Name != policy.Name {
		t.Fatalf("typed network inventory = %+v", policies.Items)
	}
}
