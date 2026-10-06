// Copyright (c) 2026. MIT License - see LICENSE file for details.

package sandbox

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klabels "k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Core owns the policies and Sandbox preserves the admitted namespace and Pod
// labels. Check their combined permissions before allocation and again against
// the realized Pod, whose upstream labels may select additional policies.
func (d *Lifecycle) verifyNetworkPolicies(ctx context.Context, runtime *workspaceprovider.RuntimeWorkload, namespace string, podLabels map[string]string) error {
	admitted := runtime.NetworkPolicy
	if admitted == nil {
		return nil
	}
	for _, direction := range admitted.PolicyTypes {
		if direction != networkingv1.PolicyTypeIngress && direction != networkingv1.PolicyTypeEgress {
			return fmt.Errorf("admitted network policy has unsupported direction %q", direction)
		}
	}
	if runtime.Template.Spec.HostNetwork {
		return fmt.Errorf("Sandbox cannot enforce admitted NetworkPolicy on a host-networked runtime")
	}
	selector, err := metav1.LabelSelectorAsSelector(&admitted.PodSelector)
	if err != nil {
		return fmt.Errorf("admitted network selector is invalid: %w", err)
	}
	if !selector.Matches(klabels.Set(podLabels)) {
		return fmt.Errorf("admitted network selector does not select the runtime")
	}
	policies := &networkingv1.NetworkPolicyList{}
	if err := d.client.List(ctx, policies, client.InNamespace(namespace)); err != nil {
		return fmt.Errorf("read runtime NetworkPolicies: %w", err)
	}
	requireIngress, requireEgress := policyDirections(admitted)
	ingressIsolated, egressIsolated := false, false
	for _, policy := range policies.Items {
		selector, err := metav1.LabelSelectorAsSelector(&policy.Spec.PodSelector)
		if err != nil {
			return fmt.Errorf("NetworkPolicy %q selector is invalid: %w", policy.Name, err)
		}
		if !selector.Matches(klabels.Set(podLabels)) {
			continue
		}
		if policy.DeletionTimestamp != nil {
			return fmt.Errorf("runtime NetworkPolicy %q is being deleted", policy.Name)
		}
		isolatesIngress, isolatesEgress := policyDirections(&policy.Spec)
		ingressIsolated = ingressIsolated || isolatesIngress
		egressIsolated = egressIsolated || isolatesEgress
		if requireIngress && isolatesIngress {
			for _, rule := range policy.Spec.Ingress {
				if !containsRule(admitted.Ingress, rule) {
					return fmt.Errorf("runtime labels select unadmitted ingress permissions in NetworkPolicy %q", policy.Name)
				}
			}
		}
		if requireEgress && isolatesEgress {
			for _, rule := range policy.Spec.Egress {
				if !containsRule(admitted.Egress, rule) {
					return fmt.Errorf("runtime labels select unadmitted egress permissions in NetworkPolicy %q", policy.Name)
				}
			}
		}
	}
	if (requireIngress && !ingressIsolated) || (requireEgress && !egressIsolated) {
		return fmt.Errorf("runtime is missing admitted network isolation")
	}
	return nil
}

// Kubernetes defaults omitted policyTypes to Ingress, plus Egress when the
// policy includes egress rules. An explicit Egress policy with no rules denies
// all egress and must still count as isolation.
func policyDirections(spec *networkingv1.NetworkPolicySpec) (ingress, egress bool) {
	if len(spec.PolicyTypes) == 0 {
		return true, len(spec.Egress) > 0
	}
	return slices.Contains(spec.PolicyTypes, networkingv1.PolicyTypeIngress), slices.Contains(spec.PolicyTypes, networkingv1.PolicyTypeEgress)
}

func containsRule[T any](rules []T, expected T) bool {
	return slices.ContainsFunc(rules, func(rule T) bool { return reflect.DeepEqual(rule, expected) })
}
