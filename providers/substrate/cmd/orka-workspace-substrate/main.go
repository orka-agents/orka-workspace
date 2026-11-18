package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	provider "github.com/orka-agents/orka-workspace/providers/substrate"
	profile "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func main() {
	var leader bool
	var leaderNamespace string
	var config provider.Config
	var maxConcurrentReconciles int
	flag.BoolVar(&leader, "leader-elect", true, "elect one active provider")
	flag.StringVar(&leaderNamespace, "leader-election-namespace", "orka-workspace-system", "namespace of provider leader Lease")
	flag.StringVar(&config.APIEndpoint, "native-api-endpoint", "api.ate-system.svc:443", "pinned Substrate control TLS endpoint")
	flag.StringVar(&config.CAFile, "native-ca-file", "", "trusted control CA file")
	flag.StringVar(&config.CertFile, "native-cert-file", "", "rotating control client certificate file")
	flag.StringVar(&config.KeyFile, "native-key-file", "", "rotating control client key file")
	flag.StringVar(&config.BearerTokenFile, "native-token-file", "", "rotating control bearer token file")
	flag.StringVar(&config.RouterEndpoint, "native-router-endpoint", "https://atenet-router.ate-system.svc:443", "native ingress router HTTPS origin; Actors are selected by the ate-target-actor header")
	flag.StringVar(&config.RouterCAFile, "native-router-ca-file", "", "trusted native router serving CA file")
	flag.StringVar(&config.RouterCertFile, "native-router-cert-file", "", "rotating router client certificate file; its SPIFFE ID must be in the router ingress allowlist")
	flag.StringVar(&config.RouterKeyFile, "native-router-key-file", "", "rotating router client key file; may equal --native-router-cert-file for a credential bundle")
	flag.StringVar(&config.RouterServerName, "native-router-server-name", "", "router TLS server name; defaults to the endpoint host")
	flag.DurationVar(&config.BootTimeout, "native-boot-timeout", 6*time.Minute, "fail a native allocation that has no process evidence after this long")
	flag.BoolVar(&config.DedicatedInstall, "substrate-dedicated-install", false, "assert that only this Orka creates Actors and ActorTemplates on the Agent Substrate install; required before any profile may share workers (ADR 0008)")
	flag.DurationVar(&config.EgressPolicyCacheTTL, "substrate-egress-policy-cache-ttl", 10*time.Second, "must equal the Agent Substrate egress gateway's --egress-policy-cache-ttl; shared workers wait longer than it after revoking egress before suspending an Actor")
	flag.StringVar(&config.EgressCredentialProvider, "egress-credential-provider", "", "credential provider class the Substrate egress gateway was installed with (only k8s.io); empty disables egress credential injection")
	flag.IntVar(&maxConcurrentReconciles, "max-concurrent-reconciles", provider.DefaultMaxConcurrentReconciles, fmt.Sprintf("ExecutionWorkspaces reconciled in parallel, from 1 to %d; one workspace is never reconciled twice at once", provider.MaxMaxConcurrentReconciles))
	flag.Func("egress-credential-namespace", "namespace, besides each workload's own, whose Secrets egress credential rules may name; repeatable (Orka core needs its runtime namespace)", func(value string) error {
		config.EgressCredentialNamespaces = append(config.EgressCredentialNamespaces, strings.TrimSpace(value))
		return nil
	})
	options := zap.Options{}
	options.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&options)))
	// Configuration errors, including invalid egress credentials, never exit
	// before run: config.Validate covers them, and run degrades to the
	// provider status reconciler and the Kubernetes-only shared worker bounds
	// so existing shared pools stay bounded. Only flag parsing and flag ranges fail fast,
	// which a previously working install with the same arguments cannot hit.
	if err := provider.ValidateMaxConcurrentReconciles(maxConcurrentReconciles); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := run(leader, leaderNamespace, config, maxConcurrentReconciles); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(leader bool, leaderNamespace string, config provider.Config, maxConcurrentReconciles int) error {
	// Without acknowledged native control, run only the provider reconciler so
	// it withdraws any stale advertisement, and the Kubernetes-only shared
	// worker bounds loop, instead of crash-looping.
	native, unavailable, closer, redial := nativeControl(config)
	if closer != nil {
		defer closer.Close()
	}
	scheme := runtime.NewScheme()
	for _, register := range []func(*runtime.Scheme) error{corev1.AddToScheme, api.AddToScheme, profile.AddToScheme} {
		if err := register(scheme); err != nil {
			return err
		}
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: scheme, LeaderElection: leader, LeaderElectionID: "orka-workspace-substrate.workspace.orka.ai", LeaderElectionNamespace: leaderNamespace, HealthProbeBindAddress: ":8081", Metrics: metricsserver.Options{BindAddress: "0"}})
	if err != nil {
		return err
	}
	c, err := client.New(mgr.GetConfig(), client.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	for _, setup := range controllerSetups(c, config, native, unavailable, maxConcurrentReconciles) {
		if err := setup(mgr); err != nil {
			return err
		}
	}
	if redial {
		// A failed dial may be transient, such as TLS material mounted late.
		// Keep retrying; once native control is reachable, stop so the
		// restarted process runs every lifecycle controller.
		if err := mgr.Add(nativeRedialer{dial: func() (io.Closer, error) { return provider.Dial(config) }, interval: nativeRedialInterval}); err != nil {
			return err
		}
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return err
	}
	return mgr.Start(ctrl.SetupSignalHandler())
}

// nativeControl dials native control. An invalid configuration, or a valid
// one whose dial fails (for example unreadable TLS material), degrades the
// same way: it is logged, no native client is returned, and the reason is
// reported so the provider status withdraws its advertisement.
//
// redial is true only for a failed dial, which may be transient; an invalid
// configuration (including invalid egress credentials) is not retried.
func nativeControl(config provider.Config) (native pb.ControlClient, unavailable string, closer io.Closer, redial bool) {
	if err := config.Validate(); err != nil {
		ctrl.Log.Info("native control is not configured; publishing unconfigured provider status only", "reason", err.Error())
		return nil, "native control is not configured: " + err.Error(), nil, false
	}
	conn, err := provider.Dial(config)
	if err != nil {
		ctrl.Log.Error(err, "native control could not be reached; withdrawing the provider advertisement and running only Kubernetes-only shared worker bounds")
		return nil, "native control could not be reached: " + err.Error(), nil, true
	}
	return pb.NewControlClient(conn), "", conn, false
}

// controllerSetups registers lifecycle controllers only with native control.
// The Kubernetes-only shared worker bounds loop always runs, so existing
// shared pools stay bounded while native configuration or connection is
// broken.
func controllerSetups(c client.Client, config provider.Config, native pb.ControlClient, nativeUnavailable string, maxConcurrentReconciles int) []func(ctrl.Manager) error {
	setups := []func(ctrl.Manager) error{(&provider.ProviderReconciler{Client: c, Config: config, NativeUnavailable: nativeUnavailable}).SetupWithManager, (&provider.SharedWorkerBounds{Client: c, Config: config}).SetupWithManager}
	if native == nil {
		// No native sweep runs, and no lifecycle controller admits anything;
		// the bounds loop retires existing shared pools past the outage
		// bound.
		provider.SetNativeDegraded(nativeUnavailable)
		return setups
	}
	return append(setups, (&provider.ExecutionWorkspaceReconciler{Client: c, Control: native, Config: config, MaxConcurrentReconciles: maxConcurrentReconciles}).SetupWithManager, (&provider.CheckpointReconciler{Client: c, Control: native, Config: config}).SetupWithManager, (&provider.SpareWorkerReplenisher{Client: c, Control: native, Config: config}).SetupWithManager)
}

const nativeRedialInterval = 30 * time.Second

// errNativeControlRecovered stops a degraded provider once native control is
// reachable, so its restart runs with every lifecycle controller.
var errNativeControlRecovered = errors.New("native control became reachable; restarting with lifecycle controllers")

// nativeRedialer retries a native dial that failed at startup. It runs on
// every replica, so a degraded replica recovers whether or not it leads.
type nativeRedialer struct {
	dial     func() (io.Closer, error)
	interval time.Duration
}

func (r nativeRedialer) Start(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			conn, err := r.dial()
			if err != nil {
				ctrl.Log.V(1).Info("native control is still unreachable", "reason", err.Error())
				continue
			}
			_ = conn.Close()
			ctrl.Log.Info("native control is reachable; restarting to start lifecycle controllers")
			return errNativeControlRecovered
		}
	}
}

func (nativeRedialer) NeedLeaderElection() bool { return false }
