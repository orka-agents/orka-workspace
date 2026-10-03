package fake

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
	fakeworkspacev1alpha1 "github.com/orka-agents/orka-workspace/providers/fake/api/v1alpha1"
	"github.com/orka-agents/orka-workspace/sdk"
)

const (
	FakeWorkspaceControllerName = "fake.workspace.orka.ai/v1"
	fakeWorkspaceAdapterVersion = "0.1.0-dev"
	fakeProviderConfigKind      = "FakeProviderConfig"
	fakeProviderHeartbeatPeriod = 20 * time.Second
)

// FakeExecutionWorkspaceProviderReconciler is a status-only reference adapter
// used by envtest and development. It owns only providers with the fake controllerName.
type FakeExecutionWorkspaceProviderReconciler struct {
	client.Client
	Now func() time.Time
}

func (r *FakeExecutionWorkspaceProviderReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	provider := &workspacev1alpha1.ExecutionWorkspaceProvider{}
	if err := r.Get(ctx, req.NamespacedName, provider); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if provider.Spec.ControllerName != FakeWorkspaceControllerName || !provider.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	configured, err := r.providerConfigAvailable(ctx, provider)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !configured {
		if err := r.clearProviderAdvertisement(ctx, provider); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: fakeProviderHeartbeatPeriod}, nil
	}
	now := time.Now().UTC()
	if r.Now != nil {
		now = r.Now().UTC()
	}
	before := provider.DeepCopy()
	provider.Status.ObservedGeneration = provider.Generation
	provider.Status.Adapter = &workspacev1alpha1.ExecutionWorkspaceAdapterStatus{Version: fakeWorkspaceAdapterVersion}
	provider.Status.Backend = &workspacev1alpha1.ExecutionWorkspaceBackendStatus{Version: "status-fixture-v1", APIVersions: []string{"fake.workspace.orka.ai/v1"}}
	provider.Status.SupportedContracts = []string{workspacev1alpha1.ContractVersionV1}
	// This adapter models status transitions only. It provides no executable runtime,
	// network endpoint, TLS, files, reset or resume capability.
	provider.Status.SupportedFeatures = []workspacev1alpha1.ExecutionWorkspaceFeature{
		workspacev1alpha1.WorkspaceFeaturePools,
	}
	heartbeat := metav1.NewTime(now)
	provider.Status.LastHeartbeat = &heartbeat
	if err := r.Status().Patch(ctx, provider, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: fakeProviderHeartbeatPeriod}, nil
}

func (r *FakeExecutionWorkspaceProviderReconciler) clearProviderAdvertisement(
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

func (r *FakeExecutionWorkspaceProviderReconciler) providerConfigAvailable(
	ctx context.Context,
	provider *workspacev1alpha1.ExecutionWorkspaceProvider,
) (bool, error) {
	ref := provider.Spec.ParametersRef
	if ref.Group != fakeworkspacev1alpha1.GroupVersion.Group || ref.Kind != fakeProviderConfigKind || ref.Name == "" {
		return false, nil
	}

	config := &unstructured.Unstructured{}
	config.SetGroupVersionKind(fakeworkspacev1alpha1.GroupVersion.WithKind(fakeProviderConfigKind))
	if err := r.Get(ctx, types.NamespacedName{Name: ref.Name}, config); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get fake provider config %q: %w", ref.Name, err)
	}
	return config.GetNamespace() == "" && config.GetDeletionTimestamp() == nil, nil
}

func (r *FakeExecutionWorkspaceProviderReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&workspacev1alpha1.ExecutionWorkspaceProvider{}).
		// Every reconcile writes a fresh LastHeartbeat. Without the generation filter that
		// status write feeds back as an update event and replaces the timed heartbeat with a
		// continuous reconcile loop; RequeueAfter still drives the heartbeat.
		WithEventFilter(predicate.And(
			workspaceprovider.ControllerNamePredicate(FakeWorkspaceControllerName),
			predicate.GenerationChangedPredicate{},
		)).
		Named("fake-execution-workspace-provider").
		Complete(r)
}

// FakeExecutionWorkspacePoolReconciler derives deterministic pool counts from
// concrete workspaces. It does not create provider-native resources.
type FakeExecutionWorkspacePoolReconciler struct {
	client.Client
}

func (r *FakeExecutionWorkspacePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pool := &workspacev1alpha1.ExecutionWorkspacePool{}
	if err := r.Get(ctx, req.NamespacedName, pool); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	owned, err := fakeProviderOwns(ctx, r.Client, pool.Spec.ProviderRef.Name)
	if err != nil || !owned {
		return ctrl.Result{}, err
	}
	allocated, suspended, err := r.countPoolWorkspaces(ctx, pool)
	if err != nil {
		return ctrl.Result{}, err
	}
	used := allocated + suspended
	total := min(pool.Spec.Capacity.MinReady+used, pool.Spec.Capacity.MaxSize)
	// A downsize never evicts active or suspended workspaces; total may temporarily exceed maxSize.
	total = max(total, used)
	available := max(total-used, 0)
	return ctrl.Result{RequeueAfter: fakeProviderHeartbeatPeriod}, retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current := &workspacev1alpha1.ExecutionWorkspacePool{}
		if err := r.Get(ctx, req.NamespacedName, current); err != nil {
			return err
		}
		before := current.DeepCopy()
		current.Status.ObservedGeneration = current.Generation
		current.Status.Available = available
		current.Status.Allocated = allocated
		current.Status.Suspended = suspended
		current.Status.Total = total
		workspaceprovider.SetCondition(&current.Status.Conditions, metav1.Condition{
			Type:               string(workspacev1alpha1.ConditionPoolReady),
			Status:             metav1.ConditionTrue,
			Reason:             string(workspacev1alpha1.ReasonReady),
			Message:            "fake pool capacity is reconciled",
			ObservedGeneration: current.Generation,
		})
		admitted := used < current.Spec.Capacity.MaxSize
		workspaceprovider.SetCondition(&current.Status.Conditions, metav1.Condition{
			Type:               string(workspacev1alpha1.ConditionPoolAdmitted),
			Status:             conditionStatus(admitted),
			Reason:             conditionReason(admitted, string(workspacev1alpha1.ReasonCapacityUnavailable)),
			Message:            chooseMessage(admitted, "pool has allocation capacity", "pool capacity is exhausted"),
			ObservedGeneration: current.Generation,
		})
		return r.Status().Patch(ctx, current, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	})
}

func (r *FakeExecutionWorkspacePoolReconciler) countPoolWorkspaces(
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

func (r *FakeExecutionWorkspacePoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&workspacev1alpha1.ExecutionWorkspacePool{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Named("fake-execution-workspace-pool").
		Complete(r)
}

// FakeExecutionWorkspaceReconciler projects the durable fixture lifecycle into
// shared status. It cannot launch a workload or admit an ACP runtime.
type FakeExecutionWorkspaceReconciler struct {
	client.Client
	APIReader  client.Reader
	RESTMapper apimeta.RESTMapper
}

func (r *FakeExecutionWorkspaceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, req.NamespacedName, workspace); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	owned, err := fakeProviderOwns(ctx, r.Client, workspace.Spec.ProviderBinding.Name)
	if err != nil || !owned {
		return ctrl.Result{}, err
	}

	return r.reconcileLifecycle(ctx, workspace)
}

func (r *FakeExecutionWorkspaceReconciler) workspaceReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *FakeExecutionWorkspaceReconciler) workspaceWithinPoolCapacity(
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
	candidates, err := r.fakePoolCapacityCandidates(ctx, workspace.Namespace, pool)
	if err != nil {
		return false, err
	}
	return workspaceWinsFakePoolCapacity(workspace, candidates, pool.Spec.Capacity.MaxSize), nil
}

func (r *FakeExecutionWorkspaceReconciler) resolveWorkspaceCapacityPool(
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
			return nil, false, fmt.Errorf("get fake workspace class for capacity: %w", err)
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
		return nil, false, fmt.Errorf("get fake workspace pool for capacity: %w", err)
	}
	return pool, poolBinding != nil && pool.UID != poolBinding.UID, nil
}

type fakePoolCapacityCandidate struct {
	name        string
	uid         types.UID
	created     metav1.Time
	provisioned bool
}

func (r *FakeExecutionWorkspaceReconciler) fakePoolCapacityCandidates(
	ctx context.Context,
	namespace string,
	pool *workspacev1alpha1.ExecutionWorkspacePool,
) ([]fakePoolCapacityCandidate, error) {
	workspaces := &workspacev1alpha1.ExecutionWorkspaceList{}
	if err := r.workspaceReader().List(ctx, workspaces, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list fake workspace pool allocations: %w", err)
	}
	candidates := make([]fakePoolCapacityCandidate, 0, len(workspaces.Items))
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
		candidates = append(candidates, fakePoolCapacityCandidate{
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

func workspaceWinsFakePoolCapacity(
	workspace *workspacev1alpha1.ExecutionWorkspace,
	candidates []fakePoolCapacityCandidate,
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

func (r *FakeExecutionWorkspaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	if r.RESTMapper == nil {
		r.RESTMapper = mgr.GetRESTMapper()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&workspacev1alpha1.ExecutionWorkspace{}).
		Named("fake-execution-workspace").
		Complete(r)
}

func fakeProviderOwns(ctx context.Context, c client.Client, name string) (bool, error) {
	provider := &workspacev1alpha1.ExecutionWorkspaceProvider{}
	if err := c.Get(ctx, types.NamespacedName{Name: name}, provider); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	return provider.Spec.ControllerName == FakeWorkspaceControllerName, nil
}

func fakeDeletedDisposition(policy workspacev1alpha1.ExecutionWorkspaceDeletionPolicy) *workspacev1alpha1.ExecutionWorkspaceDisposition {
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
	setAccessDisposition(disposition, workspacev1alpha1.DispositionRevoked)
	setEphemeralDisposition(disposition, workspacev1alpha1.DispositionDeleted)
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
