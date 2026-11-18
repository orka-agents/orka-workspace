// Copyright (c) 2026. MIT License - see LICENSE file for details.

package main

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	provider "github.com/orka-agents/orka-workspace/providers/substrate"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
)

func TestUnconfiguredNativeControlRunsOnlyProviderStatus(t *testing.T) {
	// Native control without router-based Actor routing is not configured.
	unrouted := provider.Config{APIEndpoint: "api.ate-system.svc:443", CAFile: "/run/substrate-serving-ca/trust-bundle.pem", BearerTokenFile: "/run/substrate-token/token"}
	if unrouted.Validate() == nil {
		t.Fatal("native control without router routing was accepted")
	}
	if setups := controllerSetups(nil, unrouted, nil, ""); len(setups) != 2 {
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
	if setups := controllerSetups(nil, shipped, pb.NewControlClient(nil), ""); len(setups) != 5 {
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
	if setups := controllerSetups(nil, config, native, unavailable); len(setups) != 2 {
		t.Fatalf("a failed dial registered %d controllers, want provider status and the Kubernetes-only shared worker bounds loop", len(setups))
	}
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

func TestDegradedProviderRestartsOnceNativeControlIsReachable(t *testing.T) {
	failures := 2
	r := nativeRedialer{interval: time.Millisecond, dial: func() (io.Closer, error) {
		if failures > 0 {
			failures--
			return nil, errors.New("certificate not yet mounted")
		}
		return nopCloser{}, nil
	}}
	if err := r.Start(t.Context()); !errors.Is(err, errNativeControlRecovered) {
		t.Fatalf("a recovered dial did not stop the degraded provider: %v", err)
	}
	if failures != 0 {
		t.Fatal("the redialer stopped before native control was reachable")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	never := nativeRedialer{interval: time.Millisecond, dial: func() (io.Closer, error) { return nil, errors.New("unreachable") }}
	if err := never.Start(ctx); err != nil {
		t.Fatalf("a stopped manager made the redialer fail: %v", err)
	}
}
