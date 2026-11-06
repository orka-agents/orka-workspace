// Copyright (c) 2026. MIT License - see LICENSE file for details.

package v1alpha1

import (
	"strings"
	"testing"

	networkingv1 "k8s.io/api/networking/v1"
)

func TestRuntimeEgressAcceptsOnlyExplicitHostNameRules(t *testing.T) {
	valid := func(rules ...RuntimeEgressRule) RuntimeEgressPolicy { return RuntimeEgressPolicy{Rules: rules} }
	for _, tc := range []struct {
		name   string
		policy RuntimeEgressPolicy
		ok     bool
	}{
		{"deny all", valid(), true},
		{"exact HTTPS", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "api.example.com", Ports: []int32{443}}), true},
		{"in-cluster HTTP", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTP, Host: "auth-proxy.orka-system.svc.cluster.local", Ports: []int32{8080}}), true},
		{"leftmost wildcard passthrough", valid(RuntimeEgressRule{Protocol: RuntimeEgressTLSPassthrough, Host: "*.example.com", Ports: []int32{443, 8443}}), true},
		{"same host different ports", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTP, Host: "a.example.com", Ports: []int32{80}}, RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "a.example.com", Ports: []int32{443}}), true},
		{"bare wildcard", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "*", Ports: []int32{443}}), false},
		{"top-level wildcard", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "*.com", Ports: []int32{443}}), false},
		{"inner wildcard", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "api.*.example.com", Ports: []int32{443}}), false},
		{"partial wildcard", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "api*.example.com", Ports: []int32{443}}), false},
		{"single label", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTP, Host: "localhost", Ports: []int32{80}}), false},
		{"uppercase", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "API.example.com", Ports: []int32{443}}), false},
		{"trailing dot", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "api.example.com.", Ports: []int32{443}}), false},
		{"IPv4", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "10.0.0.1", Ports: []int32{443}}), false},
		{"URL", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "https://api.example.com", Ports: []int32{443}}), false},
		{"host with port", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "api.example.com:443", Ports: []int32{443}}), false},
		{"no ports", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "api.example.com"}), false},
		{"port zero", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "api.example.com", Ports: []int32{0}}), false},
		{"port too large", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "api.example.com", Ports: []int32{65536}}), false},
		{"repeated port", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "api.example.com", Ports: []int32{443, 443}}), false},
		{"unknown protocol", valid(RuntimeEgressRule{Protocol: "TCP", Host: "api.example.com", Ports: []int32{443}}), false},
		{"empty protocol", valid(RuntimeEgressRule{Host: "api.example.com", Ports: []int32{443}}), false},
		{"overlapping protocols", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTP, Host: "a.example.com", Ports: []int32{443}}, RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "a.example.com", Ports: []int32{443}}), false},
		{"all-numeric final label", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "api.123", Ports: []int32{443}}), false},
		{"dotted numeric name", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "01.2.3.4", Ports: []int32{443}}), false},
		{"numeric wildcard suffix", valid(RuntimeEgressRule{Protocol: RuntimeEgressTLSPassthrough, Host: "*.example.123", Ports: []int32{443}}), false},
		{"numeric inner label", valid(RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "123.example.com", Ports: []int32{443}}), true},
		{"wildcard overridden by exact protocol", valid(
			RuntimeEgressRule{Protocol: RuntimeEgressTLSPassthrough, Host: "*.example.com", Ports: []int32{443}},
			RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "api.example.com", Ports: []int32{443}}), false},
		{"wildcard and exact same protocol", valid(
			RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "*.example.com", Ports: []int32{443}},
			RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "api.example.com", Ports: []int32{443}}), true},
		{"wildcard and exact other port", valid(
			RuntimeEgressRule{Protocol: RuntimeEgressTLSPassthrough, Host: "*.example.com", Ports: []int32{8443}},
			RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "api.example.com", Ports: []int32{443}}), true},
		{"wildcard does not cover deeper names", valid(
			RuntimeEgressRule{Protocol: RuntimeEgressTLSPassthrough, Host: "*.example.com", Ports: []int32{443}},
			RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "a.b.example.com", Ports: []int32{443}}), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.policy.Validate(); (err == nil) != tc.ok {
				t.Fatalf("accepted=%v, want %v: %v", err == nil, tc.ok, err)
			}
		})
	}
	many := RuntimeEgressPolicy{}
	for i := range MaxRuntimeEgressRules + 1 {
		many.Rules = append(many.Rules, RuntimeEgressRule{Protocol: RuntimeEgressHTTPS, Host: "h" + strings.Repeat("x", i%8) + string(rune('a'+i%26)) + ".example.com", Ports: []int32{int32(1 + i)}})
	}
	if err := many.Validate(); err == nil {
		t.Fatal("accepted an unbounded allowlist")
	}
}

func TestRuntimeRequestRejectsMixedNetworkShapesAndInvalidEgress(t *testing.T) {
	request := validationRuntimeRequest(t)
	request.Runtime.Egress = &RuntimeEgressPolicy{Rules: []RuntimeEgressRule{{Protocol: RuntimeEgressHTTPS, Host: "api.example.com", Ports: []int32{443}}}}
	setValidationRevision(t, &request)
	if err := request.Validate(); err != nil {
		t.Fatalf("rejected a valid egress allowlist: %v", err)
	}
	mixed := *request.DeepCopy()
	mixed.Runtime.NetworkPolicy = &networkingv1.NetworkPolicySpec{PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress}}
	setValidationRevision(t, &mixed)
	if err := mixed.Validate(); err == nil {
		t.Fatal("accepted both NetworkPolicy and host-name egress")
	}
	invalid := *request.DeepCopy()
	invalid.Runtime.Egress.Rules[0].Host = "*"
	setValidationRevision(t, &invalid)
	if err := invalid.Validate(); err == nil {
		t.Fatal("accepted a bare wildcard egress host")
	}
}

func TestRoutedNativeStartupBindsRouterTargetToExactProcess(t *testing.T) {
	process := func(target string) *NativeProcessEvidence {
		return &NativeProcessEvidence{Namespace: "native", Name: "actor", UID: "actor-uid", Version: 1, Worker: PodReference{Namespace: "workers", Name: "worker", UID: "pod-uid"}, ChallengeSHA256: "sha256:" + strings.Repeat("d", 64), RouterTarget: target}
	}
	for _, tc := range []struct {
		name, endpoint string
		target         string
		bootstrap      int32
		ok             bool
	}{
		{"router default HTTPS port", "https://atenet-router.ate-system.svc", "native/actor", 80, true},
		{"router explicit port", "https://atenet-router.ate-system.svc:443", "native/actor", 80, true},
		{"router custom port", "https://router.example:8443", "native/actor", 80, true},
		{"cleartext router", "http://atenet-router.ate-system.svc:80", "native/actor", 80, false},
		{"wrong bootstrap port", "https://atenet-router.ate-system.svc", "native/actor", 443, false},
		{"other actor", "https://atenet-router.ate-system.svc", "native/other", 80, false},
		{"other namespace", "https://atenet-router.ate-system.svc", "other/actor", 80, false},
		{"missing namespace", "https://atenet-router.ate-system.svc", "actor", 80, false},
		{"extra segment", "https://atenet-router.ate-system.svc", "native/actor/x", 80, false},
		// Without a router target the legacy shape keeps its exact endpoint-port check.
		{"unrouted HTTPS keeps endpoint port", "https://atenet-router.ate-system.svc", "", 80, false},
		{"unrouted HTTP actor", "http://actor.native.example:80", "", 80, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := validationRuntimeRequest(t)
			request.Runtime.BootstrapPort = tc.bootstrap
			request.Runtime.RequiredFeatures = []ExecutionWorkspaceFeature{WorkspaceFeatureNativeProcess}
			setValidationRevision(t, &request)
			identity := InstanceIdentity{AllocationID: "allocation", InstanceID: "instance", RequestRevision: request.Revision}
			startup := &StartupEvidence{ContractVersion: LifecycleContractV1, Identity: identity, Endpoint: tc.endpoint, Process: process(tc.target)}
			observation := AllocationObservation{Key: request.Key, Sequence: request.Sequence, Identity: identity, State: AllocationReady, Startup: startup}
			if err := ValidateStartup(request, observation); (err == nil) != tc.ok {
				t.Fatalf("accepted=%v, want %v: %v", err == nil, tc.ok, err)
			}
		})
	}
}

func TestRuntimeEgressCredentialHeadersAreBoundedAndReferenceOnly(t *testing.T) {
	ref := RuntimeEgressSecretKeyReference{Name: "provider-token", Key: "token"}
	header := func(name string) RuntimeEgressCredentialHeader {
		return RuntimeEgressCredentialHeader{Header: name, Prefix: "Bearer ", SecretKeyRef: ref}
	}
	rule := func(protocol RuntimeEgressProtocol, host string, headers ...RuntimeEgressCredentialHeader) RuntimeEgressPolicy {
		return RuntimeEgressPolicy{Rules: []RuntimeEgressRule{{Protocol: protocol, Host: host, Ports: []int32{8080}, ReplaceHeaders: headers}}}
	}
	many := make([]RuntimeEgressCredentialHeader, 0, MaxRuntimeEgressCredentialHeaders+1)
	for i := range MaxRuntimeEgressCredentialHeaders + 1 {
		many = append(many, header("X-Api-Key-"+string(rune('a'+i))))
	}
	for _, tc := range []struct {
		name   string
		policy RuntimeEgressPolicy
		ok     bool
	}{
		{"HTTP authorization", rule(RuntimeEgressHTTP, "proxy.orka-system.svc.cluster.local", header("Authorization")), true},
		{"HTTPS api key", rule(RuntimeEgressHTTPS, "api.example.com", header("x-api-key")), true},
		{"other namespace", rule(RuntimeEgressHTTP, "proxy.orka-system.svc", RuntimeEgressCredentialHeader{Header: "Authorization", SecretKeyRef: RuntimeEgressSecretKeyReference{Namespace: "orka-runtimes", Name: "p", Key: "token"}}), true},
		{"TLS passthrough", rule(RuntimeEgressTLSPassthrough, "api.example.com", header("Authorization")), false},
		{"wildcard host", rule(RuntimeEgressHTTPS, "*.example.com", header("Authorization")), false},
		{"cookie", rule(RuntimeEgressHTTPS, "api.example.com", header("Cookie")), false},
		{"hop-by-hop", rule(RuntimeEgressHTTPS, "api.example.com", header("Proxy-Authorization")), false},
		{"connection", rule(RuntimeEgressHTTPS, "api.example.com", header("connection")), false},
		{"host", rule(RuntimeEgressHTTPS, "api.example.com", header("Host")), false},
		{"router target", rule(RuntimeEgressHTTPS, "api.example.com", header("ate-target-actor")), false},
		{"envoy internal", rule(RuntimeEgressHTTPS, "api.example.com", header("x-envoy-original-path")), false},
		{"forwarded port", rule(RuntimeEgressHTTPS, "api.example.com", header("X-Forwarded-Port")), false},
		{"forwarded prefix", rule(RuntimeEgressHTTPS, "api.example.com", header("x-forwarded-prefix")), false},
		{"pseudo header", rule(RuntimeEgressHTTPS, "api.example.com", header(":authority")), false},
		{"empty header", rule(RuntimeEgressHTTPS, "api.example.com", header("")), false},
		{"space in header", rule(RuntimeEgressHTTPS, "api.example.com", header("X Api")), false},
		{"case-insensitive duplicate", rule(RuntimeEgressHTTPS, "api.example.com", header("Authorization"), header("authorization")), false},
		{"too many", rule(RuntimeEgressHTTPS, "api.example.com", many...), false},
		{"control prefix", rule(RuntimeEgressHTTPS, "api.example.com", RuntimeEgressCredentialHeader{Header: "Authorization", Prefix: "Bearer\r\n", SecretKeyRef: ref}), false},
		{"long prefix", rule(RuntimeEgressHTTPS, "api.example.com", RuntimeEgressCredentialHeader{Header: "Authorization", Prefix: strings.Repeat("a", 65), SecretKeyRef: ref}), false},
		{"missing secret", rule(RuntimeEgressHTTPS, "api.example.com", RuntimeEgressCredentialHeader{Header: "Authorization"}), false},
		{"invalid secret key", rule(RuntimeEgressHTTPS, "api.example.com", RuntimeEgressCredentialHeader{Header: "Authorization", SecretKeyRef: RuntimeEgressSecretKeyReference{Name: "p", Key: "a/b"}}), false},
		{"invalid namespace", rule(RuntimeEgressHTTPS, "api.example.com", RuntimeEgressCredentialHeader{Header: "Authorization", SecretKeyRef: RuntimeEgressSecretKeyReference{Namespace: "Bad_NS", Name: "p", Key: "k"}}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.policy.Validate(); (err == nil) != tc.ok {
				t.Fatalf("accepted=%v, want %v: %v", err == nil, tc.ok, err)
			}
			if got := tc.policy.HasCredentialInjection(); !got {
				t.Fatal("did not report credential injection")
			}
		})
	}
	if (*RuntimeEgressPolicy)(nil).HasCredentialInjection() || (&RuntimeEgressPolicy{Rules: []RuntimeEgressRule{{Protocol: RuntimeEgressHTTP, Host: "a.example.com", Ports: []int32{80}}}}).HasCredentialInjection() {
		t.Fatal("reported credential injection without replacements")
	}
}

func TestRuntimeRequestCredentialInjectionRequiresFeature(t *testing.T) {
	request := validationRuntimeRequest(t)
	request.Runtime.Egress = &RuntimeEgressPolicy{Rules: []RuntimeEgressRule{{Protocol: RuntimeEgressHTTP, Host: "proxy.orka-system.svc", Ports: []int32{8080},
		ReplaceHeaders: []RuntimeEgressCredentialHeader{{Header: "Authorization", Prefix: "Bearer ", SecretKeyRef: RuntimeEgressSecretKeyReference{Name: "provider", Key: "token"}}}}}}
	setValidationRevision(t, &request)
	if err := request.Validate(); err == nil {
		t.Fatal("accepted credential replacement without requiring the provider feature")
	}
	request.Runtime.RequiredFeatures = append(request.Runtime.RequiredFeatures, WorkspaceFeatureEgressCredentialInjection)
	setValidationRevision(t, &request)
	if err := request.Validate(); err != nil {
		t.Fatalf("rejected credential replacement with the provider feature: %v", err)
	}
}
