package substrate

import (
	"context"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	"time"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	profilev1alpha1 "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const providerHeartbeatPeriod = 20 * time.Second

type ProviderReconciler struct {
	client.Client
	Config Config
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
	transportReady := r.Config.Validate() == nil
	configUID := ""
	ref := provider.Spec.ParametersRef
	if ref.Group == profilev1alpha1.GroupVersion.Group && ref.Kind == "SubstrateProviderConfig" && ref.Name != "" {
		config := &profilev1alpha1.SubstrateProviderConfig{}
		if err := r.Get(ctx, types.NamespacedName{Name: ref.Name}, config); err != nil {
			if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		} else {
			configUID = string(config.UID)
			configured = transportReady && config.DeletionTimestamp == nil && configUID != "" && (provider.Status.PinnedParametersUID == "" || provider.Status.PinnedParametersUID == configUID)
		}
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
		provider.Status.Backend = &workspacev1alpha1.ExecutionWorkspaceBackendStatus{Version: UpstreamCommit, APIVersions: []string{"ateapi.v0.1.0"}}
		provider.Status.SupportedContracts = []string{workspacev1alpha1.ContractVersionV1, workspaceprovider.LifecycleContractV1}
		provider.Status.SupportedFeatures = []workspacev1alpha1.ExecutionWorkspaceFeature{workspacev1alpha1.WorkspaceFeatureACPRuntime, workspacev1alpha1.WorkspaceFeatureNativeProcess, workspacev1alpha1.WorkspaceFeatureSuspend, workspacev1alpha1.WorkspaceFeatureCheckpoint, workspacev1alpha1.WorkspaceFeatureRestore}
		now := metav1.Now()
		provider.Status.LastHeartbeat = &now
	}
	if err := r.Status().Patch(ctx, provider, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: providerHeartbeatPeriod}, nil
}

func (r *ProviderReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&workspacev1alpha1.ExecutionWorkspaceProvider{}).WithEventFilter(predicate.And(workspaceprovider.ControllerNamePredicate(ControllerName), predicate.GenerationChangedPredicate{})).Named("substrate-execution-workspace-provider").Complete(r)
}

type ExecutionWorkspaceReconciler struct {
	client.Client
	Control pb.ControlClient
	Config  Config
}

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
	return ctrl.NewControllerManagedBy(mgr).For(&workspacev1alpha1.ExecutionWorkspace{}).WithEventFilter(predicate.GenerationChangedPredicate{}).Named("substrate-execution-workspace").Complete(r)
}
