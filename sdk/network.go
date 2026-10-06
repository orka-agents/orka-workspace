package workspaceprovider

import (
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
)

// NormalizedNetworkPolicySpec returns a copy with Kubernetes API defaults made
// explicit for placement comparisons. It never changes a frozen workload request.
func NormalizedNetworkPolicySpec(spec networkingv1.NetworkPolicySpec) networkingv1.NetworkPolicySpec {
	result := *spec.DeepCopy()
	if len(result.PolicyTypes) == 0 {
		result.PolicyTypes = []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}
		if len(result.Egress) != 0 {
			result.PolicyTypes = append(result.PolicyTypes, networkingv1.PolicyTypeEgress)
		}
	}
	defaultPorts := func(ports []networkingv1.NetworkPolicyPort) {
		for i := range ports {
			if ports[i].Protocol == nil {
				ports[i].Protocol = new(corev1.ProtocolTCP)
			}
		}
	}
	for i := range result.Ingress {
		defaultPorts(result.Ingress[i].Ports)
	}
	for i := range result.Egress {
		defaultPorts(result.Egress[i].Ports)
	}
	return result
}
