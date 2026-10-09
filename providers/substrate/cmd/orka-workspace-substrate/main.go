package main

import (
	"flag"
	"fmt"
	"os"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	provider "github.com/orka-agents/orka-workspace/providers/substrate"
	profile "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
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
	flag.BoolVar(&leader, "leader-elect", true, "elect one active provider")
	flag.StringVar(&leaderNamespace, "leader-election-namespace", "orka-workspace-system", "namespace of provider leader Lease")
	flag.StringVar(&config.APIEndpoint, "native-api-endpoint", "api.ate-system.svc:443", "pinned Substrate control TLS endpoint")
	flag.StringVar(&config.CAFile, "native-ca-file", "", "trusted control CA file")
	flag.StringVar(&config.CertFile, "native-cert-file", "", "rotating control client certificate file")
	flag.StringVar(&config.KeyFile, "native-key-file", "", "rotating control client key file")
	flag.StringVar(&config.BearerTokenFile, "native-token-file", "", "rotating control bearer token file")
	flag.StringVar(&config.ActorDNSSuffix, "actor-dns-suffix", "actors.resources.substrate.ate.dev", "DNS suffix routed directly to native actor router")
	flag.BoolVar(&config.DirectEgressEnabled, "native-direct-egress", false, "acknowledge provider egress gateway disabled and WorkerPool NetworkPolicy enforcement")
	options := zap.Options{}
	options.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&options)))
	if err := run(leader, leaderNamespace, config); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(leader bool, leaderNamespace string, config provider.Config) error {
	// Without acknowledged native control, run only the provider reconciler so
	// it withdraws any stale advertisement instead of crash-looping.
	var native pb.ControlClient
	if err := config.Validate(); err != nil {
		ctrl.Log.Info("native control is not configured; publishing unconfigured provider status only", "reason", err.Error())
	} else {
		conn, err := provider.Dial(config)
		if err != nil {
			return err
		}
		defer conn.Close()
		native = pb.NewControlClient(conn)
	}
	scheme := runtime.NewScheme()
	for _, register := range []func(*runtime.Scheme) error{corev1.AddToScheme, networkingv1.AddToScheme, api.AddToScheme, profile.AddToScheme} {
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
	for _, setup := range controllerSetups(c, config, native) {
		if err := setup(mgr); err != nil {
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

// controllerSetups registers lifecycle controllers only with native control.
func controllerSetups(c client.Client, config provider.Config, native pb.ControlClient) []func(ctrl.Manager) error {
	setups := []func(ctrl.Manager) error{(&provider.ProviderReconciler{Client: c, Config: config}).SetupWithManager}
	if native == nil {
		return setups
	}
	return append(setups, (&provider.ExecutionWorkspaceReconciler{Client: c, Control: native, Config: config}).SetupWithManager, (&provider.CheckpointReconciler{Client: c, Control: native, Config: config}).SetupWithManager)
}
