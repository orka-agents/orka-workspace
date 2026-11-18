// Copyright (c) 2026. MIT License - see LICENSE file for details.

package main

import (
	"testing"

	provider "github.com/orka-agents/orka-workspace/providers/substrate"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
)

func TestUnconfiguredNativeControlRunsOnlyProviderStatus(t *testing.T) {
	// Native control without router-based Actor routing is not configured.
	unrouted := provider.Config{APIEndpoint: "api.ate-system.svc:443", CAFile: "/run/substrate-serving-ca/trust-bundle.pem", BearerTokenFile: "/run/substrate-token/token"}
	if unrouted.Validate() == nil {
		t.Fatal("native control without router routing was accepted")
	}
	if setups := controllerSetups(nil, unrouted, nil, "", provider.DefaultMaxConcurrentReconciles); len(setups) != 2 {
		t.Fatalf("unconfigured provider registered %d controllers, want provider status and the Kubernetes-only shared worker bounds loop", len(setups))
	}
	// The shipped Deployment configures both native control and the router.
	shipped := unrouted
	shipped.RouterEndpoint = "https://atenet-router.ate-system.svc:443"
	shipped.RouterCAFile = "/run/substrate-serving-ca/trust-bundle.pem"
	shipped.RouterCertFile = "/run/substrate-podidentity/credential-bundle.pem"
	shipped.RouterKeyFile = "/run/substrate-podidentity/credential-bundle.pem"
	if err := shipped.Validate(); err != nil {
		t.Fatal(err)
	}
	if setups := controllerSetups(nil, shipped, pb.NewControlClient(nil), "", provider.DefaultMaxConcurrentReconciles); len(setups) != 5 {
		t.Fatalf("configured provider registered %d controllers, want provider, shared worker bounds, workspace, checkpoint, and spare workers", len(setups))
	}
}

func TestValidConfigWhoseDialFailsKeepsStatusAndSharedBounds(t *testing.T) {
	// Validates, but its TLS material cannot be read.
	missing := t.TempDir() + "/absent"
	config := provider.Config{APIEndpoint: "api.ate-system.svc:443", CAFile: missing, BearerTokenFile: missing, RouterEndpoint: "https://atenet-router.ate-system.svc:443", RouterCAFile: missing, RouterCertFile: missing, RouterKeyFile: missing}
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	native, unavailable, closer := nativeControl(config)
	if native != nil || closer != nil || unavailable == "" {
		t.Fatalf("a failed dial did not degrade: native=%v reason=%q", native, unavailable)
	}
	if setups := controllerSetups(nil, config, native, unavailable, provider.DefaultMaxConcurrentReconciles); len(setups) != 2 {
		t.Fatalf("a failed dial registered %d controllers, want provider status and the Kubernetes-only shared worker bounds loop", len(setups))
	}
}
