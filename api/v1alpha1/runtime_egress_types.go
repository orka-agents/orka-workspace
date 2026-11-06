// Copyright (c) 2026. MIT License - see LICENSE file for details.

package v1alpha1

import (
	"fmt"
	"net/netip"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// RuntimeEgressProtocol selects how a host-name egress rule is enforced by a
// backend egress gateway.
// +kubebuilder:validation:Enum=HTTP;HTTPS;TLSPassthrough
type RuntimeEgressProtocol string

const (
	// RuntimeEgressHTTP allows cleartext HTTP/1.1 and h2c to the host. TLS to
	// the same host is not allowed by this rule.
	RuntimeEgressHTTP RuntimeEgressProtocol = "HTTP"
	// RuntimeEgressHTTPS allows HTTPS that the backend gateway terminates and
	// inspects. The runtime must trust the backend's interception CA.
	RuntimeEgressHTTPS RuntimeEgressProtocol = "HTTPS"
	// RuntimeEgressTLSPassthrough allows TLS selected by SNI and forwarded
	// without decryption. The runtime verifies the origin's own certificate.
	RuntimeEgressTLSPassthrough RuntimeEgressProtocol = "TLSPassthrough"
)

// MaxRuntimeEgressRules bounds one admitted allowlist.
const MaxRuntimeEgressRules = 256

// RuntimeEgressRule allows one protocol to one host pattern on explicit TCP
// ports. Rules only add permissions.
type RuntimeEgressRule struct {
	Protocol RuntimeEgressProtocol `json:"protocol"`
	// Host is a lowercase fully qualified DNS name without a trailing dot, or
	// such a name whose leftmost label is "*" (matching exactly one label).
	// Bare "*", single-label wildcards, IP addresses, ports and URLs are invalid.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Host string `json:"host"`
	// Ports are destination TCP ports, without repeats. Every rule names its
	// ports explicitly; there is no all-ports form.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:Minimum=1
	// +kubebuilder:validation:items:Maximum=65535
	// +listType=set
	Ports []int32 `json:"ports"`
}

// RuntimeEgressPolicy is the admitted default-deny host-name allowlist for a
// runtime whose backend enforces egress at a gateway rather than through
// Kubernetes NetworkPolicy. An empty rule list denies all egress, including
// DNS resolution. Core must include every origin the runtime itself needs, such
// as its in-cluster provider authentication proxy.
type RuntimeEgressPolicy struct {
	// +kubebuilder:validation:MaxItems=256
	// +listType=atomic
	// +optional
	Rules []RuntimeEgressRule `json:"rules,omitempty"`
}

// Validate rejects malformed or ambiguous allowlists. A host and port may be
// granted by only one rule, whatever its protocol, so enforcement never needs
// to choose between overlapping grants.
func (p RuntimeEgressPolicy) Validate() error {
	if len(p.Rules) > MaxRuntimeEgressRules {
		return fmt.Errorf("runtime egress allows at most %d rules", MaxRuntimeEgressRules)
	}
	type destination struct {
		host string
		port int32
	}
	seen := map[destination]bool{}
	for i, rule := range p.Rules {
		switch rule.Protocol {
		case RuntimeEgressHTTP, RuntimeEgressHTTPS, RuntimeEgressTLSPassthrough:
		default:
			return fmt.Errorf("runtime egress rule %d has unsupported protocol %q", i, rule.Protocol)
		}
		if err := ValidateRuntimeEgressHost(rule.Host); err != nil {
			return fmt.Errorf("runtime egress rule %d: %w", i, err)
		}
		if len(rule.Ports) == 0 || len(rule.Ports) > 16 {
			return fmt.Errorf("runtime egress rule %d requires one to sixteen explicit ports", i)
		}
		ports := map[int32]bool{}
		for _, port := range rule.Ports {
			if port < 1 || port > 65535 || ports[port] {
				return fmt.Errorf("runtime egress rule %d has an invalid or repeated port", i)
			}
			ports[port] = true
			key := destination{host: rule.Host, port: port}
			if seen[key] {
				return fmt.Errorf("runtime egress grants %s port %d more than once", rule.Host, port)
			}
			seen[key] = true
		}
	}
	return nil
}

// ValidateRuntimeEgressHost accepts a lowercase DNS name with at least two
// labels, or a leftmost-label wildcard over such a name. It never accepts a
// wildcard that covers a whole top-level domain or every name.
func ValidateRuntimeEgressHost(host string) error {
	if host == "" || host != strings.ToLower(host) || strings.HasSuffix(host, ".") {
		return fmt.Errorf("egress host %q must be a lowercase DNS name without a trailing dot", host)
	}
	name := strings.TrimPrefix(host, "*.")
	if strings.Contains(name, "*") {
		return fmt.Errorf("egress host %q may use a wildcard only as its complete leftmost label", host)
	}
	if _, err := netip.ParseAddr(name); err == nil {
		return fmt.Errorf("egress host %q must be a DNS name, not an IP address", host)
	}
	if len(validation.IsDNS1123Subdomain(name)) != 0 || !strings.Contains(name, ".") {
		return fmt.Errorf("egress host %q must be a fully qualified DNS name", host)
	}
	return nil
}
