// Copyright (c) 2026. MIT License - see LICENSE file for details.

package main

import (
	"testing"

	provider "github.com/orka-agents/orka-workspace/providers/substrate"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
)

func TestUnconfiguredNativeControlRunsOnlyProviderStatus(t *testing.T) {
	// The shipped Deployment keeps direct egress unacknowledged.
	shipped := provider.Config{APIEndpoint: "api.ate-system.svc:443", CAFile: "/native-control/ca.crt", BearerTokenFile: "/native-control/token", ActorDNSSuffix: "actors.resources.substrate.ate.dev"}
	if shipped.Validate() == nil {
		t.Fatal("shipped configuration unexpectedly acknowledged native control")
	}
	if setups := controllerSetups(nil, shipped, nil); len(setups) != 1 {
		t.Fatalf("unconfigured provider registered %d controllers, want only provider status", len(setups))
	}
	if setups := controllerSetups(nil, shipped, pb.NewControlClient(nil)); len(setups) != 3 {
		t.Fatalf("configured provider registered %d controllers, want provider, workspace, and checkpoint", len(setups))
	}
}
