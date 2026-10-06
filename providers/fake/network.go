// Copyright (c) 2026. MIT License - see LICENSE file for details.

package fake

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Core owns the policies and fake preserves the admitted namespace and Pod
// labels. Check their combined permissions before allocation and again against
// the realized Pod, whose upstream labels may select additional policies.
func (d *Lifecycle) verifyNetworkPolicies(ctx context.Context, runtime *workspaceprovider.RuntimeWorkload, namespace string, podLabels map[string]string) error {
	if runtime == nil || runtime.NetworkPolicy == nil {
		return nil
	}
	normalized := workspaceprovider.NormalizedNetworkPolicySpec(*runtime.NetworkPolicy)
	admitted := &normalized
	for _, direction := range admitted.PolicyTypes {
		if direction != networkingv1.PolicyTypeIngress && direction != networkingv1.PolicyTypeEgress {
			return fmt.Errorf("admitted network policy has unsupported direction %q", direction)
		}
	}
	if runtime.Template.Spec.HostNetwork {
		return fmt.Errorf("fake cannot enforce admitted NetworkPolicy on a host-networked runtime")
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
	// A direction not isolated by the admitted policy is unrestricted, rather
	// than denied by its empty rule list. Extra policies in that direction can
	// only preserve or narrow the admitted permissions. Check additive grants
	// against the rule envelope only for directions admission isolates.
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
		actual := workspaceprovider.NormalizedNetworkPolicySpec(policy.Spec)
		for _, direction := range actual.PolicyTypes {
			if direction != networkingv1.PolicyTypeIngress && direction != networkingv1.PolicyTypeEgress {
				return fmt.Errorf("runtime NetworkPolicy %q has unsupported direction %q", policy.Name, direction)
			}
		}
		isolatesIngress, isolatesEgress := policyDirections(&actual)
		ingressIsolated = ingressIsolated || isolatesIngress
		egressIsolated = egressIsolated || isolatesEgress
		if requireIngress && isolatesIngress {
			for _, rule := range actual.Ingress {
				if !slices.ContainsFunc(admitted.Ingress, func(allowed networkingv1.NetworkPolicyIngressRule) bool {
					return peersWithin(rule.From, allowed.From) && portsWithin(rule.Ports, allowed.Ports)
				}) {
					return fmt.Errorf("runtime labels select unadmitted ingress permissions in NetworkPolicy %q", policy.Name)
				}
			}
		}
		if requireEgress && isolatesEgress {
			for _, rule := range actual.Egress {
				if !slices.ContainsFunc(admitted.Egress, func(allowed networkingv1.NetworkPolicyEgressRule) bool {
					return peersWithin(rule.To, allowed.To) && portsWithin(rule.Ports, allowed.Ports)
				}) {
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

// Peers and ports are independent OR lists within one rule, combined with AND.
// A rule can narrow either list without adding permissions. Keep the two lists
// within the same admitted rule so combining peers and ports cannot cross-grant
// a permission that separate admitted rules never allowed.
func peersWithin(actual, allowed []networkingv1.NetworkPolicyPeer) bool {
	if len(allowed) == 0 {
		return true // An empty peer list permits every peer.
	}
	if len(actual) == 0 {
		return false
	}
	for _, peer := range actual {
		// Selector implication and IP-block exclusions stay conservative. Exact
		// peer identity proves this member of the admitted union is allowed.
		if !slices.ContainsFunc(allowed, func(candidate networkingv1.NetworkPolicyPeer) bool { return reflect.DeepEqual(peer, candidate) }) {
			return false
		}
	}
	return true
}

func portsWithin(actual, allowed []networkingv1.NetworkPolicyPort) bool {
	if len(allowed) == 0 {
		return true // An empty port list permits every port and protocol.
	}
	if len(actual) == 0 {
		return false
	}
	for _, port := range actual {
		if !slices.ContainsFunc(allowed, func(candidate networkingv1.NetworkPolicyPort) bool { return portWithin(port, candidate) }) {
			return false
		}
	}
	return true
}

func portWithin(actual, allowed networkingv1.NetworkPolicyPort) bool {
	if !reflect.DeepEqual(actual.Protocol, allowed.Protocol) {
		return false
	}
	if allowed.Port == nil {
		return allowed.EndPort == nil // Every port for this protocol.
	}
	if actual.Port == nil {
		return false
	}
	if actual.Port.Type != intstr.Int || allowed.Port.Type != intstr.Int {
		return reflect.DeepEqual(actual, allowed) // Named-port resolution is not known.
	}
	start, end := actual.Port.IntVal, actual.Port.IntVal
	if actual.EndPort != nil {
		end = *actual.EndPort
	}
	allowedStart, allowedEnd := allowed.Port.IntVal, allowed.Port.IntVal
	if allowed.EndPort != nil {
		allowedEnd = *allowed.EndPort
	}
	return start > 0 && end >= start && end <= 65535 && allowedStart > 0 && allowedEnd >= allowedStart && allowedEnd <= 65535 && start >= allowedStart && end <= allowedEnd
}
