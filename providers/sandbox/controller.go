package sandbox

import (
	"context"
	"slices"
	"time"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	profilev1alpha1 "github.com/orka-agents/orka-workspace/providers/sandbox/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const providerHeartbeatPeriod = 20 * time.Second

// backendResources are the pinned agent-sandbox APIs every allocation uses.
var backendResources = map[string][]string{
	"agents.x-k8s.io/v1beta1":            {"sandboxes"},
	"extensions.agents.x-k8s.io/v1beta1": {"sandboxclaims", "sandboxtemplates"},
}

type ProviderReconciler struct {
	client.Client
	// Discovery proves the pinned backend APIs are served before capabilities
	// are advertised. Nil skips the check.
	Discovery discovery.ServerResourcesInterface
}

// backendServed reports false only when discovery definitively says a pinned
// backend API is not served; that answer wins over a transient error for
// another group. Transient errors alone keep the previous advertisement until
// its heartbeat expires, so a discovery blip cannot close admission.
func (r *ProviderReconciler) backendServed() (bool, error) {
	if r.Discovery == nil {
		return true, nil
	}
	var transient error
	for groupVersion, resources := range backendResources {
		list, err := r.Discovery.ServerResourcesForGroupVersion(groupVersion)
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			transient = err
			continue
		}
		for _, name := range resources {
			if !slices.ContainsFunc(list.APIResources, func(resource metav1.APIResource) bool { return resource.Name == name }) {
				return false, nil
			}
		}
	}
	return transient == nil, transient
}

func (r *ProviderReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	provider := &workspacev1alpha1.ExecutionWorkspaceProvider{}
	if err := r.Get(ctx, req.NamespacedName, provider); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if provider.Spec.ControllerName != ControllerName || provider.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}
	configured := false
	configUID := ""
	ref := provider.Spec.ParametersRef
	if ref.Group == profilev1alpha1.GroupVersion.Group && ref.Kind == "SandboxProviderConfig" && ref.Name != "" {
		config := &profilev1alpha1.SandboxProviderConfig{}
		if err := r.Get(ctx, types.NamespacedName{Name: ref.Name}, config); err != nil {
			if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		} else {
			configUID = string(config.UID)
			configured = config.DeletionTimestamp == nil && configUID != "" && (provider.Status.PinnedParametersUID == "" || provider.Status.PinnedParametersUID == configUID)
		}
	}
	served, err := r.backendServed()
	if err != nil {
		return ctrl.Result{}, err
	}
	if configured && !served {
		ctrl.LoggerFrom(ctx).Info("withholding provider advertisement: pinned agent-sandbox APIs are not served")
		configured = false
	}
	before := provider.DeepCopy()
	provider.Status.ObservedGeneration = 0
	provider.Status.Adapter = nil
	provider.Status.Backend = nil
	provider.Status.SupportedContracts = nil
	provider.Status.SupportedFeatures = nil
	provider.Status.LastHeartbeat = nil
	if configured {
		provider.Status.PinnedParametersUID = configUID
		provider.Status.ObservedGeneration = provider.Generation
		provider.Status.Adapter = &workspacev1alpha1.ExecutionWorkspaceAdapterStatus{Version: AdapterVersion}
		provider.Status.Backend = &workspacev1alpha1.ExecutionWorkspaceBackendStatus{Version: "1.0.3", APIVersions: []string{"agents.x-k8s.io/v1beta1", "extensions.agents.x-k8s.io/v1beta1"}}
		provider.Status.SupportedContracts = []string{workspacev1alpha1.ContractVersionV1, workspaceprovider.LifecycleContractV1}
		provider.Status.SupportedFeatures = []workspacev1alpha1.ExecutionWorkspaceFeature{workspacev1alpha1.WorkspaceFeatureACPRuntime, workspacev1alpha1.WorkspaceFeatureSuspend}
		now := metav1.Now()
		provider.Status.LastHeartbeat = &now
	}
	if err := r.Status().Patch(ctx, provider, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: providerHeartbeatPeriod}, nil
}

func (r *ProviderReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&workspacev1alpha1.ExecutionWorkspaceProvider{}).WithEventFilter(predicate.And(workspaceprovider.ControllerNamePredicate(ControllerName), predicate.GenerationChangedPredicate{})).Named("sandbox-execution-workspace-provider").Complete(r)
}

type ExecutionWorkspaceReconciler struct{ client.Client }

func (r *ExecutionWorkspaceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, req.NamespacedName, workspace); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	provider := &workspacev1alpha1.ExecutionWorkspaceProvider{}
	if err := r.Get(ctx, types.NamespacedName{Name: workspace.Spec.ProviderBinding.Name}, provider); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if provider.UID != workspace.Spec.ProviderBinding.UID || provider.Spec.ControllerName != ControllerName {
		return ctrl.Result{}, nil
	}
	return r.reconcileLifecycle(ctx, workspace)
}

func (r *ExecutionWorkspaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := (&JournalProtectionReconciler{Client: r.Client}).SetupWithManager(mgr); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).For(&workspacev1alpha1.ExecutionWorkspace{}).WithEventFilter(predicate.GenerationChangedPredicate{}).Named("sandbox-execution-workspace").Complete(r)
}
