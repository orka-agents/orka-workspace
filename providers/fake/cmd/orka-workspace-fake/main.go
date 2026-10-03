package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	"github.com/orka-agents/orka-workspace/conformance"
	provider "github.com/orka-agents/orka-workspace/providers/fake"
	fakev1alpha1 "github.com/orka-agents/orka-workspace/providers/fake/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func main() {
	var conformanceMode bool
	var leaderElection bool
	var leaderNamespace string
	flag.BoolVar(&conformanceMode, "conformance", false, "run fixture lifecycle conformance with an isolated Kubernetes fake client and exit")
	flag.BoolVar(&leaderElection, "leader-elect", true, "elect one active controller")
	flag.StringVar(&leaderNamespace, "leader-election-namespace", "orka-workspace-system", "namespace for the fake provider leader Lease")
	options := zap.Options{}
	options.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&options)))
	if err := run(conformanceMode, leaderElection, leaderNamespace); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(conformanceMode, leaderElection bool, leaderNamespace string) error {
	scheme := runtime.NewScheme()
	for _, register := range []func(*runtime.Scheme) error{corev1.AddToScheme, workspacev1alpha1.AddToScheme, fakev1alpha1.AddToScheme} {
		if err := register(scheme); err != nil {
			return err
		}
	}
	if conformanceMode {
		ctx := context.Background()
		c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&workspacev1alpha1.ExecutionWorkspace{}).Build()
		request, err := provider.ConformanceFixture(ctx, c)
		if err != nil {
			return err
		}
		if err := conformance.Check(ctx, func() workspaceprovider.Lifecycle { return provider.New(c) }, request); err != nil {
			return err
		}
		fmt.Println("fake fixture lifecycle conformance passed")
		return nil
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		LeaderElection:          leaderElection,
		LeaderElectionID:        "orka-workspace-fake.workspace.orka.ai",
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
		(&provider.FakeExecutionWorkspaceProviderReconciler{Client: c}).SetupWithManager,
		(&provider.FakeExecutionWorkspacePoolReconciler{Client: c}).SetupWithManager,
		(&provider.FakeExecutionWorkspaceReconciler{Client: c, APIReader: c}).SetupWithManager,
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
