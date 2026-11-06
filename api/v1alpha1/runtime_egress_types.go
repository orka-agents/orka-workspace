// Copyright (c) 2026. MIT License - see LICENSE file for details.

package v1alpha1

import (
	"fmt"
	"net/netip"
	"slices"
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
	// ReplaceHeaders asks the backend gateway to replace the value of each
	// named request header with a credential it resolves from a Kubernetes
	// Secret, so the runtime never holds that credential. A header is replaced
	// only when the runtime's request carries it (with any placeholder value);
	// requests without it are forwarded unchanged. Only exact-host HTTP and
	// HTTPS rules may carry replacements, and only providers advertising
	// runtime.egress-credential-injection enforce them. On HTTP the credential
	// leaves the gateway in cleartext, so use it only for in-cluster origins.
	// +kubebuilder:validation:MaxItems=4
	// +listType=map
	// +listMapKey=header
	// +optional
	ReplaceHeaders []RuntimeEgressCredentialHeader `json:"replaceHeaders,omitempty"`
}

// MaxRuntimeEgressCredentialHeaders bounds header replacements on one rule.
const MaxRuntimeEgressCredentialHeaders = 4

// maxRuntimeEgressCredentialPrefixBytes bounds a credential header prefix.
const maxRuntimeEgressCredentialPrefixBytes = 64

// RuntimeEgressCredentialHeader replaces one HTTP request header with a
// credential. The credential is referenced, never embedded.
type RuntimeEgressCredentialHeader struct {
	// Header is the case-insensitive HTTP request header to replace when the
	// request carries it. Hop-by-hop, routing, framing and cookie headers are
	// rejected.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	Header string `json:"header"`
	// Prefix is prepended verbatim to the credential value, including any
	// separator, for example "Bearer " for Authorization.
	// +kubebuilder:validation:MaxLength=64
	// +optional
	Prefix string `json:"prefix,omitempty"`
	// SecretKeyRef names the Kubernetes Secret key holding the credential.
	SecretKeyRef RuntimeEgressSecretKeyReference `json:"secretKeyRef"`
}

// RuntimeEgressSecretKeyReference names one key of a Kubernetes Secret. The
// backend's credential provider reads it at request time; the value never
// enters the workload request, provider journal or runtime.
type RuntimeEgressSecretKeyReference struct {
	// Namespace defaults to the workload's namespace. Only the workspace
	// intent writer (Orka core) sets another namespace, such as its private
	// runtime namespace; providers accept it only when configured to.
	// +kubebuilder:validation:MaxLength=63
	// +optional
	Namespace string `json:"namespace,omitempty"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Key string `json:"key"`
}

// HasCredentialInjection reports whether any rule asks the gateway to
// replace a request header with a credential.
func (p *RuntimeEgressPolicy) HasCredentialInjection() bool {
	if p == nil {
		return false
	}
	return slices.ContainsFunc(p.Rules, func(rule RuntimeEgressRule) bool { return len(rule.ReplaceHeaders) != 0 })
}

// reservedRuntimeEgressCredentialHeaders cannot carry an injected credential:
// they are hop-by-hop, select routing or message framing, or carry cookies
// whose value a destination may reflect into responses or other origins.
var reservedRuntimeEgressCredentialHeaders = map[string]bool{
	"connection": true, "keep-alive": true, "proxy-connection": true, "proxy-authenticate": true,
	"proxy-authorization": true, "te": true, "trailer": true, "transfer-encoding": true, "upgrade": true,
	"host": true, "content-length": true, "content-type": true, "content-encoding": true, "expect": true,
	"cookie": true, "set-cookie": true, "forwarded": true, "via": true, "x-forwarded-for": true,
	"x-forwarded-host": true, "x-forwarded-proto": true, "x-real-ip": true, "ate-target-actor": true,
}

// ValidateRuntimeEgressCredentialHeaders checks one rule's header
// replacements against the protocol and host that carry them.
func ValidateRuntimeEgressCredentialHeaders(protocol RuntimeEgressProtocol, host string, headers []RuntimeEgressCredentialHeader) error {
	if len(headers) == 0 {
		return nil
	}
	if protocol != RuntimeEgressHTTP && protocol != RuntimeEgressHTTPS {
		return fmt.Errorf("credential header replacement requires an HTTP or HTTPS rule; the gateway cannot read %s traffic", protocol)
	}
	if strings.HasPrefix(host, "*") {
		return fmt.Errorf("credential header replacement requires an exact host, not %q", host)
	}
	if len(headers) > MaxRuntimeEgressCredentialHeaders {
		return fmt.Errorf("credential header replacement allows at most %d headers per rule", MaxRuntimeEgressCredentialHeaders)
	}
	seen := map[string]bool{}
	for i, header := range headers {
		name := strings.ToLower(header.Header)
		if !validRuntimeEgressHeaderName(header.Header) || len(header.Header) > 128 {
			return fmt.Errorf("credential header %d must be an HTTP header name", i)
		}
		if reservedRuntimeEgressCredentialHeaders[name] || strings.HasPrefix(name, "x-envoy-") || strings.HasPrefix(name, "x-ate-") {
			return fmt.Errorf("credential header %q is reserved", header.Header)
		}
		if seen[name] {
			return fmt.Errorf("credential header %q is replaced more than once", header.Header)
		}
		seen[name] = true
		if len(header.Prefix) > maxRuntimeEgressCredentialPrefixBytes || strings.IndexFunc(header.Prefix, func(r rune) bool { return r != ' ' && (r < '!' || r > '~') }) >= 0 {
			return fmt.Errorf("credential header %q prefix must be at most %d printable ASCII bytes", header.Header, maxRuntimeEgressCredentialPrefixBytes)
		}
		ref := header.SecretKeyRef
		if ref.Namespace != "" && len(validation.IsDNS1123Label(ref.Namespace)) != 0 {
			return fmt.Errorf("credential header %q Secret namespace is invalid", header.Header)
		}
		if len(validation.IsDNS1123Subdomain(ref.Name)) != 0 {
			return fmt.Errorf("credential header %q Secret name is invalid", header.Header)
		}
		if len(validation.IsConfigMapKey(ref.Key)) != 0 {
			return fmt.Errorf("credential header %q Secret key is invalid", header.Header)
		}
	}
	return nil
}

// validRuntimeEgressHeaderName accepts an RFC 9110 field-name token.
func validRuntimeEgressHeaderName(value string) bool {
	if value == "" {
		return false
	}
	for _, c := range []byte(value) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0) {
			return false
		}
	}
	return true
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
		if err := ValidateRuntimeEgressCredentialHeaders(rule.Protocol, rule.Host, rule.ReplaceHeaders); err != nil {
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
	return validateRuntimeEgressOverlaps(p.Rules)
}

// validateRuntimeEgressOverlaps rejects an exact host that a wildcard rule on
// the same port also matches, when the two use different protocols. Gateways
// prefer the exact rule, so the overlap would silently replace the wildcard's
// protocol for that host instead of only adding permissions.
func validateRuntimeEgressOverlaps(rules []RuntimeEgressRule) error {
	for _, wildcard := range rules {
		suffix, ok := strings.CutPrefix(wildcard.Host, "*")
		if !ok {
			continue
		}
		for _, exact := range rules {
			label, matches := strings.CutSuffix(exact.Host, suffix)
			if exact.Protocol == wildcard.Protocol || !matches || label == "" || strings.ContainsAny(label, ".*") {
				continue
			}
			for _, port := range exact.Ports {
				if slices.Contains(wildcard.Ports, port) {
					return fmt.Errorf("runtime egress grants %s port %d as %s, which overrides %s from %s", exact.Host, port, exact.Protocol, wildcard.Protocol, wildcard.Host)
				}
			}
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
	// Gateways reject an all-numeric final label, which only an address uses.
	if strings.Trim(name[strings.LastIndexByte(name, '.')+1:], "0123456789") == "" {
		return fmt.Errorf("egress host %q must not end in an all-numeric label", host)
	}
	return nil
}
