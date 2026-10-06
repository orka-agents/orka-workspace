// Copyright (c) 2026. MIT License - see LICENSE file for details.

package substrate

import (
	"reflect"
	"strings"
	"testing"

	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestNativeRejectsUnsupportedNetworkPolicyBeforeEffects(t *testing.T) {
	for _, test := range []struct {
		name        string
		policy      networkingv1.NetworkPolicySpec
		noNamespace bool
		want        string
	}{
		{name: "default ingress isolation", want: "ingress policy is unsupported"},
		{name: "default ingress and egress", policy: networkingv1.NetworkPolicySpec{Egress: []networkingv1.NetworkPolicyEgressRule{{}}}, want: "ingress policy is unsupported"},
		{name: "explicit ingress isolation", policy: networkingv1.NetworkPolicySpec{PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}}, want: "ingress policy is unsupported"},
		{name: "both isolation directions", policy: networkingv1.NetworkPolicySpec{PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress}}, want: "ingress policy is unsupported"},
		{name: "inactive egress rules", policy: networkingv1.NetworkPolicySpec{PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}, Egress: []networkingv1.NetworkPolicyEgressRule{{}}}, want: "inactive egress rules"},
		{name: "inactive ingress rules", policy: networkingv1.NetworkPolicySpec{PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress}, Ingress: []networkingv1.NetworkPolicyIngressRule{{}}}, want: "ingress policy is unsupported"},
		{name: "relative peer without frozen namespace", policy: networkingv1.NetworkPolicySpec{PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress}, Egress: []networkingv1.NetworkPolicyEgressRule{{To: []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{}}}}}}, noNamespace: true, want: "frozen runtime namespace"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, native, request := fixture(t, false)
			request.Runtime.NetworkPolicy = test.policy.DeepCopy()
			if test.noNamespace {
				request.Runtime.Template.Namespace = ""
			}
			var err error
			request.Revision, err = sdk.WorkloadRevision(request)
			if err != nil {
				t.Fatal(err)
			}
			if err := admit(c)(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("unsupported policy was not rejected: %v", err)
			}
			if native.boots != 0 || len(native.actors) != 0 || len(native.templates) != 1 || len(native.workers) != 0 {
				t.Fatal("unsupported policy created native compute or a template")
			}
			if err := c.Get(t.Context(), journalKey(request.Key), &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
				t.Fatalf("unsupported policy created an allocation journal: %v", err)
			}
			policies := &networkingv1.NetworkPolicyList{}
			if err := c.List(t.Context(), policies); err != nil {
				t.Fatal(err)
			}
			if len(policies.Items) != 0 {
				t.Fatal("unsupported policy created network infrastructure")
			}
		})
	}
}

func TestNativeWorkerEgressPreservesNamespaceRelativePeers(t *testing.T) {
	c, native, request := fixture(t, false)
	explicitNamespace := &metav1.LabelSelector{MatchLabels: map[string]string{"environment": "trusted"}}
	request.Runtime.NetworkPolicy.Egress = []networkingv1.NetworkPolicyEgressRule{{To: []networkingv1.NetworkPolicyPeer{
		{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "runtime-peer"}}},
		{PodSelector: &metav1.LabelSelector{}, NamespaceSelector: explicitNamespace},
		{IPBlock: &networkingv1.IPBlock{CIDR: "192.0.2.0/24"}},
	}}}
	var err error
	request.Revision, err = sdk.WorkloadRevision(request)
	if err != nil {
		t.Fatal(err)
	}
	original := request.DeepCopy()
	if err := admit(c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	ready(t, c, native, request)
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.Placement.Namespace == request.Runtime.Template.Namespace {
		t.Fatal("fixture did not exercise differing worker and runtime namespaces")
	}
	policy := &networkingv1.NetworkPolicy{}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: record.Placement.Namespace, Name: record.NetworkPolicy.Name}, policy); err != nil {
		t.Fatal(err)
	}
	peers := policy.Spec.Egress[0].To
	wantNamespace := &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelMetadataName: request.Runtime.Template.Namespace}}
	if !reflect.DeepEqual(peers[0].NamespaceSelector, wantNamespace) || !reflect.DeepEqual(peers[0].PodSelector, original.Runtime.NetworkPolicy.Egress[0].To[0].PodSelector) {
		t.Fatalf("relative Pod peer changed its admitted namespace or selector: %#v", peers[0])
	}
	if !reflect.DeepEqual(peers[1].NamespaceSelector, explicitNamespace) || !reflect.DeepEqual(peers[2], original.Runtime.NetworkPolicy.Egress[0].To[2]) {
		t.Fatal("translation changed an explicit namespace or IP peer")
	}
	if !reflect.DeepEqual(request.Runtime.NetworkPolicy, original.Runtime.NetworkPolicy) || !reflect.DeepEqual(record.Request.Runtime.NetworkPolicy, original.Runtime.NetworkPolicy) {
		t.Fatal("translation rewrote admitted network intent")
	}
}
