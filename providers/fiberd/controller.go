package fiberd

import (
	"context"
	"fmt"
	"sort"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	fiberdworkspacev1alpha1 "github.com/orka-agents/orka-workspace/providers/fiberd/api/v1alpha1"
	"github.com/orka-agents/orka-workspace/sdk"
)

const (
	FiberdWorkspaceControllerName = "fiberd.workspace.orka.ai"
	fiberdWorkspaceAdapterVersion = "0.1.0-dev"
	fiberdProviderConfigKind      = "FiberdProviderConfig"
	fiberdPoolParametersKind      = "FiberdPoolParameters"
	fiberdProviderHeartbeatPeriod = 20 * time.Second
)

// FiberdExecutionWorkspaceProviderReconciler advertises the development Pod
// provider. It owns only registrations with the fiberd controller name.
type FiberdExecutionWorkspaceProviderReconciler struct {
	client.Client
	Now func() time.Time
}

func (r *FiberdExecutionWorkspaceProviderReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	provider := &workspacev1alpha1.ExecutionWorkspaceProvider{}
	if err := r.Get(ctx, req.NamespacedName, provider); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if provider.Spec.ControllerName != FiberdWorkspaceControllerName || !provider.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	before := provider.DeepCopy()
	configured, err := r.providerConfigAvailable(ctx, provider)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !configured {
		if err := r.clearProviderAdvertisement(ctx, provider); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: fiberdProviderHeartbeatPeriod}, nil
	}
	now := time.Now().UTC()
	if r.Now != nil {
		now = r.Now().UTC()
	}
	provider.Status.ObservedGeneration = provider.Generation
	provider.Status.Adapter = &workspacev1alpha1.ExecutionWorkspaceAdapterStatus{Version: fiberdWorkspaceAdapterVersion}
	provider.Status.Backend = &workspacev1alpha1.ExecutionWorkspaceBackendStatus{Version: "fiberd-gvisor-v1", APIVersions: []string{"fiberd.workspace.orka.ai/v1"}}
	provider.Status.SupportedContracts = []string{workspacev1alpha1.ContractVersionV1, workspaceprovider.LifecycleContractV1}
	// Supervisors run as gVisor fibers on a fiberd home: native processes whose
	// infrastructure worker is the home Pod. Bootstrap and admission stay in Core.
	provider.Status.SupportedFeatures = []workspacev1alpha1.ExecutionWorkspaceFeature{
		workspacev1alpha1.WorkspaceFeaturePools,
		workspacev1alpha1.WorkspaceFeatureACPRuntime,
		workspacev1alpha1.WorkspaceFeatureNativeProcess,
	}
	heartbeat := metav1.NewTime(now)
	provider.Status.LastHeartbeat = &heartbeat
	if err := r.Status().Patch(ctx, provider, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: fiberdProviderHeartbeatPeriod}, nil
}

func (r *FiberdExecutionWorkspaceProviderReconciler) clearProviderAdvertisement(
	ctx context.Context,
	provider *workspacev1alpha1.ExecutionWorkspaceProvider,
) error {
	before := provider.DeepCopy()
	provider.Status.ObservedGeneration = 0
	provider.Status.Adapter = nil
	provider.Status.Backend = nil
	provider.Status.SupportedContracts = nil
	provider.Status.SupportedFeatures = nil
	provider.Status.LastHeartbeat = nil

	return r.Status().Patch(ctx, provider, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

func (r *FiberdExecutionWorkspaceProviderReconciler) providerConfigAvailable(
	ctx context.Context,
	provider *workspacev1alpha1.ExecutionWorkspaceProvider,
) (bool, error) {
	ref := provider.Spec.ParametersRef
	if ref.Group != fiberdworkspacev1alpha1.GroupVersion.Group || ref.Kind != fiberdProviderConfigKind || ref.Name == "" {
		return false, nil
	}

	config := &unstructured.Unstructured{}
	config.SetGroupVersionKind(fiberdworkspacev1alpha1.GroupVersion.WithKind(fiberdProviderConfigKind))
	if err := r.Get(ctx, types.NamespacedName{Name: ref.Name}, config); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get fiberd provider config %q: %w", ref.Name, err)
	}
	if config.GetNamespace() != "" || config.GetDeletionTimestamp() != nil || config.GetUID() == "" {
		return false, nil
	}
	if provider.Status.PinnedParametersUID != "" && provider.Status.PinnedParametersUID != string(config.GetUID()) {
		return false, nil
	}
	provider.Status.PinnedParametersUID = string(config.GetUID())
	return true, nil
}

func (r *FiberdExecutionWorkspaceProviderReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&workspacev1alpha1.ExecutionWorkspaceProvider{}).
		// Every reconcile writes a fresh LastHeartbeat. Without the generation filter that
		// status write feeds back as an update event and replaces the timed heartbeat with a
		// continuous reconcile loop; RequeueAfter still drives the heartbeat.
		WithEventFilter(predicate.And(
			workspaceprovider.ControllerNamePredicate(FiberdWorkspaceControllerName),
			predicate.GenerationChangedPredicate{},
		)).
		Named("fiberd-execution-workspace-provider").
		Complete(r)
}

// FiberdExecutionWorkspacePoolReconciler derives deterministic pool counts from
// concrete workspaces. It does not create provider-native resources.
type FiberdExecutionWorkspacePoolReconciler struct {
	client.Client
}

func (r *FiberdExecutionWorkspacePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pool := &workspacev1alpha1.ExecutionWorkspacePool{}
	if err := r.Get(ctx, req.NamespacedName, pool); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	owned, err := fiberdProviderOwns(ctx, r.Client, pool.Spec.ProviderRef.Name)
	if err != nil || !owned {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: fiberdProviderHeartbeatPeriod}, retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		// Derive every published value from the object being patched so a
		// conflict retry never stamps stale capacity with a newer generation.
		current := &workspacev1alpha1.ExecutionWorkspacePool{}
		if err := r.Get(ctx, req.NamespacedName, current); err != nil {
			return err
		}
		parametersReady, err := r.poolParametersAvailable(ctx, current)
		if err != nil {
			return err
		}
		allocated, suspended, err := r.countPoolWorkspaces(ctx, current)
		if err != nil {
			return err
		}
		used := allocated + suspended
		total := min(current.Spec.Capacity.MinReady+used, current.Spec.Capacity.MaxSize)
		// A downsize never evicts active or suspended workspaces; total may temporarily exceed maxSize.
		total = max(total, used)
		if !parametersReady {
			total = used
		}
		before := current.DeepCopy()
		current.Status.ObservedGeneration = current.Generation
		current.Status.Available = max(total-used, 0)
		current.Status.Allocated = allocated
		current.Status.Suspended = suspended
		current.Status.Total = total
		workspaceprovider.SetCondition(&current.Status.Conditions, metav1.Condition{
			Type:               string(workspacev1alpha1.ConditionPoolReady),
			Status:             conditionStatus(parametersReady),
			Reason:             conditionReason(parametersReady, string(workspacev1alpha1.ReasonProgressing)),
			Message:            chooseMessage(parametersReady, "fiberd pool capacity is reconciled", "fiberd pool parameters are missing, deleting, or not FiberdPoolParameters"),
			ObservedGeneration: current.Generation,
		})
		admitted := parametersReady && used < current.Spec.Capacity.MaxSize
		workspaceprovider.SetCondition(&current.Status.Conditions, metav1.Condition{
			Type:               string(workspacev1alpha1.ConditionPoolAdmitted),
			Status:             conditionStatus(admitted),
			Reason:             conditionReason(admitted, string(workspacev1alpha1.ReasonCapacityUnavailable)),
			Message:            chooseMessage(admitted, "pool has allocation capacity", "pool capacity is exhausted or unconfigured"),
			ObservedGeneration: current.Generation,
		})
		return r.Status().Patch(ctx, current, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	})
}

// The immutable parametersRef names the pool's namespaced adapter-owned
// configuration identity. Missing, deleting, or foreign objects are not ready.
func (r *FiberdExecutionWorkspacePoolReconciler) poolParametersAvailable(
	ctx context.Context,
	pool *workspacev1alpha1.ExecutionWorkspacePool,
) (bool, error) {
	ref := pool.Spec.ParametersRef
	if ref.Group != fiberdworkspacev1alpha1.GroupVersion.Group || ref.Kind != fiberdPoolParametersKind || ref.Name == "" {
		return false, nil
	}
	parameters := &fiberdworkspacev1alpha1.FiberdPoolParameters{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: ref.Name}, parameters); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get fiberd pool parameters %q: %w", ref.Name, err)
	}
	return parameters.DeletionTimestamp == nil && parameters.UID != "", nil
}

func (r *FiberdExecutionWorkspacePoolReconciler) countPoolWorkspaces(
	ctx context.Context,
	pool *workspacev1alpha1.ExecutionWorkspacePool,
) (int32, int32, error) {
	var workspaces workspacev1alpha1.ExecutionWorkspaceList
	if err := r.List(ctx, &workspaces, client.InNamespace(pool.Namespace)); err != nil {
		return 0, 0, err
	}
	var allocated, suspended int32
	for i := range workspaces.Items {
		workspace := &workspaces.Items[i]
		if !workspaceHasCoreAdmission(workspace) || workspace.Spec.CoreAdmission.PoolBinding == nil ||
			workspace.Spec.CoreAdmission.PoolBinding.UID != pool.UID || workspaceComputeCapacityReleased(workspace) {
			continue
		}
		if workspace.Status.State == workspacev1alpha1.ExecutionWorkspaceStateSuspended {
			suspended++
			continue
		}
		allocated++
	}
	return allocated, suspended, nil
}

func (r *FiberdExecutionWorkspacePoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&workspacev1alpha1.ExecutionWorkspacePool{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Named("fiberd-execution-workspace-pool").
		Complete(r)
}

// FiberdExecutionWorkspaceReconciler projects the durable fixture lifecycle into
// shared status and materializes admitted Pods. Core admits ACP runtimes.
type FiberdExecutionWorkspaceReconciler struct {
	client.Client
	APIReader  client.Reader
	RESTMapper apimeta.RESTMapper
	Fibers     Fibers
}

func (r *FiberdExecutionWorkspaceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, req.NamespacedName, workspace); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	owned, err := fiberdProviderOwns(ctx, r.Client, workspace.Spec.ProviderBinding.Name)
	if err != nil || !owned {
		return ctrl.Result{}, err
	}

	return r.reconcileLifecycle(ctx, workspace)
}

func (r *FiberdExecutionWorkspaceReconciler) workspaceReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *FiberdExecutionWorkspaceReconciler) workspaceWithinPoolCapacity(
	ctx context.Context,
	workspace *workspacev1alpha1.ExecutionWorkspace,
) (bool, error) {
	var poolBinding *workspacev1alpha1.ImmutableObjectBinding
	if workspaceCurrentlyAdmittedByCore(workspace) {
		poolBinding = workspace.Spec.CoreAdmission.PoolBinding
	}
	pool, grandfathered, err := r.resolveWorkspaceCapacityPool(ctx, workspace, poolBinding)
	if err != nil {
		return false, err
	}
	if grandfathered || pool == nil {
		return true, nil
	}
	candidates, err := r.fiberdPoolCapacityCandidates(ctx, workspace.Namespace, pool)
	if err != nil {
		return false, err
	}
	return workspaceWinsFiberdPoolCapacity(workspace, candidates, pool.Spec.Capacity.MaxSize), nil
}

func (r *FiberdExecutionWorkspaceReconciler) resolveWorkspaceCapacityPool(
	ctx context.Context,
	workspace *workspacev1alpha1.ExecutionWorkspace,
	poolBinding *workspacev1alpha1.ImmutableObjectBinding,
) (*workspacev1alpha1.ExecutionWorkspacePool, bool, error) {
	poolName := ""
	if poolBinding != nil {
		// Core already reserved this immutable pool identity. Use that binding rather
		// than the mutable live class so deletion or replacement cannot strand the
		// workspace before the adapter publishes its first status.
		poolName = poolBinding.Name
	} else {
		class := &workspacev1alpha1.ExecutionWorkspaceClass{}
		classKey := types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Spec.ClassBinding.Name}
		if err := r.workspaceReader().Get(ctx, classKey, class); err != nil {
			return nil, false, fmt.Errorf("get fiberd workspace class for capacity: %w", err)
		}
		if class.Spec.PoolRef == nil {
			return nil, false, nil
		}
		poolName = class.Spec.PoolRef.Name
	}

	pool := &workspacev1alpha1.ExecutionWorkspacePool{}
	poolKey := types.NamespacedName{Namespace: workspace.Namespace, Name: poolName}
	if err := r.workspaceReader().Get(ctx, poolKey, pool); err != nil {
		if poolBinding != nil && apierrors.IsNotFound(err) {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("get fiberd workspace pool for capacity: %w", err)
	}
	return pool, poolBinding != nil && pool.UID != poolBinding.UID, nil
}

type fiberdPoolCapacityCandidate struct {
	name        string
	uid         types.UID
	created     metav1.Time
	provisioned bool
}

func (r *FiberdExecutionWorkspaceReconciler) fiberdPoolCapacityCandidates(
	ctx context.Context,
	namespace string,
	pool *workspacev1alpha1.ExecutionWorkspacePool,
) ([]fiberdPoolCapacityCandidate, error) {
	workspaces := &workspacev1alpha1.ExecutionWorkspaceList{}
	if err := r.workspaceReader().List(ctx, workspaces, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list fiberd workspace pool allocations: %w", err)
	}
	candidates := make([]fiberdPoolCapacityCandidate, 0, len(workspaces.Items))
	for i := range workspaces.Items {
		candidateWorkspace := &workspaces.Items[i]
		if !workspaceHasCoreAdmission(candidateWorkspace) || candidateWorkspace.Spec.CoreAdmission.PoolBinding == nil ||
			candidateWorkspace.Spec.CoreAdmission.PoolBinding.UID != pool.UID {
			continue
		}
		provisioned := candidateWorkspace.Status.State != "" ||
			candidateWorkspace.Spec.CoreAdmission.PoolBinding.Generation < pool.Generation
		if !provisioned && (!candidateWorkspace.DeletionTimestamp.IsZero() || workspaceHasMaintenanceIntent(candidateWorkspace)) {
			continue
		}
		if workspaceComputeCapacityReleased(candidateWorkspace) {
			continue
		}
		candidates = append(candidates, fiberdPoolCapacityCandidate{
			name:        candidateWorkspace.Name,
			uid:         candidateWorkspace.UID,
			created:     candidateWorkspace.CreationTimestamp,
			provisioned: provisioned,
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].provisioned != candidates[j].provisioned {
			return candidates[i].provisioned
		}
		if !candidates[i].created.Equal(&candidates[j].created) {
			return candidates[i].created.Before(&candidates[j].created)
		}
		if candidates[i].name != candidates[j].name {
			return candidates[i].name < candidates[j].name
		}
		return string(candidates[i].uid) < string(candidates[j].uid)
	})
	return candidates, nil
}

func workspaceWinsFiberdPoolCapacity(
	workspace *workspacev1alpha1.ExecutionWorkspace,
	candidates []fiberdPoolCapacityCandidate,
	maxSize int32,
) bool {
	provisionedCount := 0
	for _, candidate := range candidates {
		if !candidate.provisioned {
			break
		}
		provisionedCount++
		if candidate.name == workspace.Name && candidate.uid == workspace.UID {
			// Downsizing never evicts or freezes already provisioned workspaces.
			return true
		}
	}
	availableSlots := int(maxSize) - provisionedCount
	if availableSlots <= 0 {
		return false
	}
	for _, candidate := range candidates[provisionedCount:] {
		if availableSlots == 0 {
			break
		}
		availableSlots--
		if candidate.name == workspace.Name && candidate.uid == workspace.UID {
			return true
		}
	}
	return false
}

func (r *FiberdExecutionWorkspaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := (&JournalProtectionReconciler{Client: r.Client}).SetupWithManager(mgr); err != nil {
		return err
	}
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	if r.RESTMapper == nil {
		r.RESTMapper = mgr.GetRESTMapper()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&workspacev1alpha1.ExecutionWorkspace{}).
		Named("fiberd-execution-workspace").
		Complete(r)
}

func fiberdProviderOwns(ctx context.Context, c client.Client, name string) (bool, error) {
	provider := &workspacev1alpha1.ExecutionWorkspaceProvider{}
	if err := c.Get(ctx, types.NamespacedName{Name: name}, provider); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	return provider.Spec.ControllerName == FiberdWorkspaceControllerName, nil
}

func fiberdDeletedDisposition(policy workspacev1alpha1.ExecutionWorkspaceDeletionPolicy) *workspacev1alpha1.ExecutionWorkspaceDisposition {
	retainedOrDeleted := func(action workspacev1alpha1.WorkspaceDeletionAction) workspacev1alpha1.ExecutionWorkspaceDispositionState {
		if action == workspacev1alpha1.WorkspaceDeletionActionRetain {
			return workspacev1alpha1.DispositionRetained
		}
		return workspacev1alpha1.DispositionDeleted
	}
	disposition := &workspacev1alpha1.ExecutionWorkspaceDisposition{
		Compute:           workspacev1alpha1.DispositionDeleted,
		WorkspaceData:     workspacev1alpha1.DispositionDeleted,
		PersistentVolumes: retainedOrDeleted(policy.PersistentVolumes),
		Checkpoints:       retainedOrDeleted(policy.Checkpoints),
		ProviderResources: retainedOrDeleted(policy.ProviderResources),
	}
	// Core owns attachment and runtime credentials. Deleting provider compute
	// does not attest revocation or deletion of those independently owned Secrets.
	setAccessDisposition(disposition, workspacev1alpha1.DispositionNotApplicable)
	setEphemeralDisposition(disposition, workspacev1alpha1.DispositionNotApplicable)
	return disposition
}

func setAccessDisposition(
	disposition *workspacev1alpha1.ExecutionWorkspaceDisposition,
	state workspacev1alpha1.ExecutionWorkspaceDispositionState,
) {
	disposition.AccessCredentials = state
}

func setEphemeralDisposition(
	disposition *workspacev1alpha1.ExecutionWorkspaceDisposition,
	state workspacev1alpha1.ExecutionWorkspaceDispositionState,
) {
	disposition.EphemeralSecrets = state
}
