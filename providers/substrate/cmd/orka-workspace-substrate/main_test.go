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
	if setups := controllerSetups(nil, unrouted, nil); len(setups) != 1 {
		t.Fatalf("unconfigured provider registered %d controllers, want only provider status", len(setups))
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
	if setups := controllerSetups(nil, shipped, pb.NewControlClient(nil)); len(setups) != 3 {
		t.Fatalf("configured provider registered %d controllers, want provider, workspace, and checkpoint", len(setups))
	}
}
