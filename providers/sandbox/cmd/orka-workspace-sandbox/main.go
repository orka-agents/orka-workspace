package main

import (
	"flag"
	"fmt"
	"os"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	provider "github.com/orka-agents/orka-workspace/providers/sandbox"
	sandboxv1alpha1 "github.com/orka-agents/orka-workspace/providers/sandbox/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime"
	nativev1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func main() {
	var leaderElection bool
	var leaderNamespace string
	flag.BoolVar(&leaderElection, "leader-elect", true, "elect one active controller")
	flag.StringVar(&leaderNamespace, "leader-election-namespace", "orka-workspace-system", "namespace for the Sandbox provider leader Lease")
	options := zap.Options{}
	options.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&options)))
	if err := run(leaderElection, leaderNamespace); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(leaderElection bool, leaderNamespace string) error {
	scheme := runtime.NewScheme()
	for _, register := range []func(*runtime.Scheme) error{corev1.AddToScheme, networkingv1.AddToScheme, storagev1.AddToScheme, workspacev1alpha1.AddToScheme, sandboxv1alpha1.AddToScheme, nativev1beta1.AddToScheme, extv1beta1.AddToScheme} {
		if err := register(scheme); err != nil {
			return err
		}
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		LeaderElection:          leaderElection,
		LeaderElectionID:        "orka-workspace-sandbox.workspace.orka.ai",
		LeaderElectionNamespace: leaderNamespace,
		HealthProbeBindAddress:  ":8081",
		Metrics:                 metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		return err
	}
	// Journal CAS reads must bypass the manager cache, including after a lost
	// update response. Watches still use the manager's shared cache.
	c, err := client.New(mgr.GetConfig(), client.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	for _, setup := range []func(ctrl.Manager) error{
		(&provider.ProviderReconciler{Client: c}).SetupWithManager,
		(&provider.ExecutionWorkspaceReconciler{Client: c}).SetupWithManager,
	} {
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
