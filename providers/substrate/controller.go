package substrate

import (
	"context"
	"fmt"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	"strconv"
	"time"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	profilev1alpha1 "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const providerHeartbeatPeriod = 20 * time.Second

type ProviderReconciler struct {
	client.Client
	Config Config
	// NativeUnavailable, when set, is why native control could not be
	// reached even though Config validates (for example unreadable TLS
	// material). Nothing is advertised while it is set.
	NativeUnavailable string
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
	transportReady := r.Config.Validate() == nil && r.NativeUnavailable == ""
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
		provider.Status.Backend = &workspacev1alpha1.ExecutionWorkspaceBackendStatus{Version: UpstreamCommit, APIVersions: []string{BackendAPIVersion}}
		provider.Status.SupportedContracts = []string{workspacev1alpha1.ContractVersionV1, workspaceprovider.LifecycleContractV1}
		provider.Status.SupportedFeatures = []workspacev1alpha1.ExecutionWorkspaceFeature{workspacev1alpha1.WorkspaceFeatureACPRuntime, workspacev1alpha1.WorkspaceFeatureNativeProcess, workspacev1alpha1.WorkspaceFeatureSuspend, workspacev1alpha1.WorkspaceFeatureCheckpoint, workspacev1alpha1.WorkspaceFeatureRestore, workspacev1alpha1.WorkspaceFeatureEgressAllowlist}
		if r.Config.EgressCredentialInjectionEnabled() {
			provider.Status.SupportedFeatures = append(provider.Status.SupportedFeatures, workspacev1alpha1.WorkspaceFeatureEgressCredentialInjection)
		}
		now := metav1.Now()
		provider.Status.LastHeartbeat = &now
	}
	if err := r.Status().Patch(ctx, provider, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, err
	}
	r.reportNativeCredentials(ctx, provider)
	return ctrl.Result{RequeueAfter: providerHeartbeatPeriod}, nil
}

// nativeCredentialsEventReason is the Warning Event reason that surfaces the
// native control plane rejecting the provider's own credentials.
const nativeCredentialsEventReason = "NativeCredentialsRejected"

// reportNativeCredentials surfaces, on every heartbeat, a rejection of the
// provider's own native credentials as a Warning Event on the provider. The
// core/v1 Event is created, or its last timestamp and message patched, under
// the events create and patch verbs the provider already holds. It is best
// effort and never fails the heartbeat.
func (r *ProviderReconciler) reportNativeCredentials(ctx context.Context, provider *workspacev1alpha1.ExecutionWorkspaceProvider) {
	reason := NativeCredentialsRejected()
	if reason == "" {
		return
	}
	now := metav1.Now()
	message := "Substrate " + reason + "; shared workers whose ActorTemplates cannot be checked are strongly retired once this outlasts its bound"
	namespace := provider.Namespace
	if namespace == "" {
		namespace = metav1.NamespaceDefault
	}
	event := &corev1.Event{
		ObjectMeta:          metav1.ObjectMeta{Namespace: namespace, Name: provider.Name + ".native-credentials-rejected"},
		InvolvedObject:      corev1.ObjectReference{APIVersion: workspacev1alpha1.GroupVersion.String(), Kind: "ExecutionWorkspaceProvider", Name: provider.Name, Namespace: provider.Namespace, UID: provider.UID},
		Reason:              nativeCredentialsEventReason,
		Message:             message,
		Type:                corev1.EventTypeWarning,
		Source:              corev1.EventSource{Component: ControllerName},
		ReportingController: ControllerName,
		FirstTimestamp:      now,
		LastTimestamp:       now,
		Count:               1,
	}
	err := r.Create(ctx, event)
	if apierrors.IsAlreadyExists(err) {
		patch := client.RawPatch(types.MergePatchType, []byte(`{"lastTimestamp":"`+now.UTC().Format(time.RFC3339)+`","message":`+strconv.Quote(message)+`}`))
		err = r.Patch(ctx, &corev1.Event{ObjectMeta: event.ObjectMeta}, patch)
	}
	if err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "Substrate provider native credential rejection could not be reported as an Event", "reason", reason)
	}
}

func (r *ProviderReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&workspacev1alpha1.ExecutionWorkspaceProvider{}).WithEventFilter(predicate.And(workspaceprovider.ControllerNamePredicate(ControllerName), predicate.GenerationChangedPredicate{})).Named("substrate-execution-workspace-provider").Complete(r)
}

// Workspace reconcile concurrency. The workqueue never runs two reconciles
// of one workspace at once; reconciles of different workspaces share only
// thread-safe clients and resourceVersion-preconditioned writes (spare
// claims, shared ledgers, the suspended-occupancy ledger), so they may run
// in parallel. Each pass makes several blocking native calls, so a single
// worker serializes concurrent cold starts behind each other.
const (
	DefaultMaxConcurrentReconciles = 8
	MaxMaxConcurrentReconciles     = 32
)

// ValidateMaxConcurrentReconciles bounds the workspace reconcile concurrency.
func ValidateMaxConcurrentReconciles(n int) error {
	if n < 1 || n > MaxMaxConcurrentReconciles {
		return fmt.Errorf("--max-concurrent-reconciles must be from 1 to %d", MaxMaxConcurrentReconciles)
	}
	return nil
}

type ExecutionWorkspaceReconciler struct {
	client.Client
	Control pb.ControlClient
	Config  Config
	// MaxConcurrentReconciles bounds parallel reconciles of different
	// workspaces; zero selects DefaultMaxConcurrentReconciles.
	MaxConcurrentReconciles int

	polls pendingPolls
	// challengeClient replaces the router challenge reader in tests.
	challengeClient challengeGetter
}

func (r *ExecutionWorkspaceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, req.NamespacedName, workspace); err != nil {
		r.polls.reset(req.NamespacedName)
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	provider := &workspacev1alpha1.ExecutionWorkspaceProvider{}
	if err := r.Get(ctx, types.NamespacedName{Name: workspace.Spec.ProviderBinding.Name}, provider); err != nil {
		r.polls.reset(req.NamespacedName)
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if provider.UID != workspace.Spec.ProviderBinding.UID || provider.Spec.ControllerName != ControllerName {
		r.polls.reset(req.NamespacedName)
		return ctrl.Result{}, nil
	}
	return r.reconcileRead(ctx, workspace, provider)
}

func (r *ExecutionWorkspaceReconciler) maxConcurrentReconciles() int {
	if r.MaxConcurrentReconciles <= 0 {
		return DefaultMaxConcurrentReconciles
	}
	return r.MaxConcurrentReconciles
}

func (r *ExecutionWorkspaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := (&JournalProtectionReconciler{Client: r.Client}).SetupWithManager(mgr); err != nil {
		return err
	}
	if err := ValidateMaxConcurrentReconciles(r.maxConcurrentReconciles()); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).For(&workspacev1alpha1.ExecutionWorkspace{}).WithEventFilter(predicate.GenerationChangedPredicate{}).WithOptions(controller.Options{MaxConcurrentReconciles: r.maxConcurrentReconciles()}).Named("substrate-execution-workspace").Complete(r)
}
