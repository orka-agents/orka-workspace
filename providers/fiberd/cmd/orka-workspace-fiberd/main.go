package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	"github.com/orka-agents/orka-workspace/conformance"
	provider "github.com/orka-agents/orka-workspace/providers/fiberd"
	fiberdv1alpha1 "github.com/orka-agents/orka-workspace/providers/fiberd/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

type homeFlags struct {
	namespace, pod, target, nodeID, issuerKey, issuerURL, template, images string
	lease                                                                  time.Duration
}

func (h homeFlags) config() provider.HomeConfig {
	nodeID := h.nodeID
	if nodeID == "" {
		nodeID = h.pod
	}
	var images []string
	for _, image := range strings.Split(h.images, ",") {
		if image = strings.TrimSpace(image); image != "" {
			images = append(images, image)
		}
	}
	return provider.HomeConfig{Pod: types.NamespacedName{Namespace: h.namespace, Name: h.pod}, Target: h.target, NodeID: nodeID,
		IssuerKeyFile: h.issuerKey, IssuerURL: h.issuerURL, TemplateDigest: h.template, SupervisorImages: images, Lease: h.lease}
}

func main() {
	var conformanceMode, homeConformance bool
	var leaderElection bool
	var leaderNamespace string
	var home homeFlags
	flag.BoolVar(&conformanceMode, "conformance", false, "run fixture lifecycle conformance with an isolated Kubernetes fake client and exit")
	flag.BoolVar(&homeConformance, "conformance-home", false, "with --conformance, clone the runtime fixture on the configured fiberd home instead of in memory")
	flag.StringVar(&home.namespace, "home-namespace", "", "namespace of the fiberd home Pod")
	flag.StringVar(&home.pod, "home-pod", "", "name of the fiberd home Pod")
	flag.StringVar(&home.target, "fiberd-target", "", "Fibers API host:port; defaults to the home Pod IP on 8484")
	flag.StringVar(&home.nodeID, "node-id", "", "the home's grant audience; defaults to the home Pod name")
	flag.StringVar(&home.issuerKey, "issuer-key-file", "", "private JWK the provider mints per-instance grants with")
	flag.StringVar(&home.issuerURL, "issuer-url", "", "issuer URL the home trusts")
	flag.StringVar(&home.template, "template-digest", "sha256:orka-acp-supervisor", "template digest every grant names")
	flag.StringVar(&home.images, "supervisor-images", "", "comma-separated pinned supervisor images the home's rootfs serves")
	flag.DurationVar(&home.lease, "grant-lease", 24*time.Hour, "lease of each per-instance grant")
	flag.BoolVar(&leaderElection, "leader-elect", true, "elect one active controller")
	flag.StringVar(&leaderNamespace, "leader-election-namespace", "orka-workspace-system", "namespace for the fiberd provider leader Lease")
	options := zap.Options{}
	options.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&options)))
	if err := run(conformanceMode, homeConformance, home, leaderElection, leaderNamespace); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newProviderScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	for _, register := range []func(*runtime.Scheme) error{corev1.AddToScheme, networkingv1.AddToScheme, workspacev1alpha1.AddToScheme, fiberdv1alpha1.AddToScheme} {
		if err := register(scheme); err != nil {
			return nil, err
		}
	}
	return scheme, nil
}

func run(conformanceMode, homeConformance bool, home homeFlags, leaderElection bool, leaderNamespace string) error {
	scheme, err := newProviderScheme()
	if err != nil {
		return err
	}
	if conformanceMode {
		ctx := context.Background()
		checks := []func(context.Context, func() workspaceprovider.Lifecycle, workspaceprovider.WorkloadRequest) error{
			conformance.Check,
			func(ctx context.Context, factory func() workspaceprovider.Lifecycle, request workspaceprovider.WorkloadRequest) error {
				return conformance.CheckReplacement(ctx, factory, request, nil)
			},
			func(ctx context.Context, factory func() workspaceprovider.Lifecycle, request workspaceprovider.WorkloadRequest) error {
				return conformance.CheckSuspension(ctx, factory, request, nil)
			},
		}
		for _, check := range checks {
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&workspacev1alpha1.ExecutionWorkspace{}).Build()
			request, err := provider.ConformanceFixture(ctx, c)
			if err != nil {
				return err
			}
			if err := check(ctx, func() workspaceprovider.Lifecycle { return provider.New(c, nil) }, request); err != nil {
				return err
			}
		}
		fmt.Println("fiberd fixture lifecycle conformance passed")
		return runtimeConformance(ctx, scheme, homeConformance, home)
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		LeaderElection:          leaderElection,
		LeaderElectionID:        "orka-workspace-fiberd.workspace.orka.ai",
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
	fibers, err := provider.NewHome(context.Background(), c, home.config())
	if err != nil {
		return err
	}
	for _, setup := range []func(ctrl.Manager) error{
		(&provider.FiberdExecutionWorkspaceProviderReconciler{Client: c}).SetupWithManager,
		(&provider.FiberdExecutionWorkspacePoolReconciler{Client: c}).SetupWithManager,
		(&provider.FiberdExecutionWorkspaceReconciler{Client: c, APIReader: c, Fibers: fibers}).SetupWithManager,
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

// runtimeConformance runs lifecycle and replacement conformance on supervisor
// fibers. Journals use a fake client; the home is in memory or real.
func runtimeConformance(ctx context.Context, scheme *runtime.Scheme, onHome bool, home homeFlags) error {
	image := provider.ConformanceImage
	newFibers := func() (provider.Fibers, error) { return provider.NewMemoryFibers(image), nil }
	if onHome {
		cfg := home.config()
		if len(cfg.SupervisorImages) == 0 {
			return fmt.Errorf("--conformance-home needs --supervisor-images")
		}
		image = cfg.SupervisorImages[0]
		reader, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
		if err != nil {
			return err
		}
		newFibers = func() (provider.Fibers, error) { return provider.NewHome(ctx, reader, cfg) }
	}
	for _, replacement := range []bool{false, true} {
		name := map[bool]string{false: "lifecycle", true: "replacement"}[replacement]
		c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&workspacev1alpha1.ExecutionWorkspace{}).Build()
		fibers, err := newFibers()
		if err != nil {
			return err
		}
		nonce := fmt.Sprintf("%x", time.Now().UnixNano())
		request, err := provider.RuntimeConformanceFixture(ctx, c, image, nonce)
		if err != nil {
			return err
		}
		factory := func() workspaceprovider.Lifecycle { return provider.New(c, fibers) }
		cctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
		start := time.Now()
		if replacement {
			err = conformance.CheckReplacement(cctx, factory, request, func(ctx context.Context, next workspaceprovider.WorkloadRequest) error {
				return provider.PublishRequest(ctx, c, next)
			})
		} else {
			err = conformance.Check(cctx, factory, request)
		}
		cancel()
		if err != nil {
			return fmt.Errorf("runtime %s conformance: %w", name, err)
		}
		fmt.Printf("fiberd runtime %s conformance passed in %s (home %v)\n", name, time.Since(start).Round(time.Millisecond), onHome)
	}
	return nil
}
