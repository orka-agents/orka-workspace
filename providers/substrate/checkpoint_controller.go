package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const checkpointFinalizer = ControllerName + "/checkpoint-reference"

type CheckpointReconciler struct {
	client.Client
	Control pb.ControlClient
	Config  Config
}

func (d *Lifecycle) readExport(ctx context.Context, checkpoint *api.ExecutionWorkspaceCheckpoint) (*corev1.ConfigMap, *checkpointExport, error) {
	cm := &corev1.ConfigMap{}
	if err := d.client.Get(ctx, exportKey(checkpoint.Namespace, checkpoint.UID), cm); err != nil {
		return nil, nil, err
	}
	var index checkpointExport
	if err := json.Unmarshal([]byte(cm.Data[exportDataKey]), &index); err != nil {
		return nil, nil, fmt.Errorf("checkpoint export index is unreadable")
	}
	if cm.UID == "" || index.Version != catalogVersion || index.Checkpoint.Name != checkpoint.Name || index.Checkpoint.UID != checkpoint.UID || index.SourceWorkspace != checkpoint.Spec.WorkspaceRef || index.ProviderUID == "" || cm.Labels[exportLabel] != string(checkpoint.UID) || cm.Labels[providerLabel] != string(index.ProviderUID) || !reflect.DeepEqual(cm.OwnerReferences, []metav1.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: "ExecutionWorkspaceCheckpoint", Name: checkpoint.Name, UID: checkpoint.UID}}) {
		return nil, nil, sdk.ErrStaleIdentity
	}
	return cm, &index, nil
}

func (r *CheckpointReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	d := New(r.Client, r.Control, r.Config)
	if name, ok := strings.CutPrefix(req.Name, "catalog/"); ok {
		return r.reconcileCatalog(ctx, d, types.NamespacedName{Namespace: req.Namespace, Name: name})
	}
	checkpoint := &api.ExecutionWorkspaceCheckpoint{}
	if err := r.Get(ctx, req.NamespacedName, checkpoint); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	indexCM, index, err := d.readExport(ctx, checkpoint)
	if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	if checkpoint.DeletionTimestamp != nil {
		if !controllerutil.ContainsFinalizer(checkpoint, checkpointFinalizer) {
			return ctrl.Result{}, nil
		}
		if index == nil && checkpoint.Status.Digest != "" && checkpoint.Status.Phase != "Deleting" {
			return ctrl.Result{}, fmt.Errorf("checkpoint export catalog disappeared; cleanup is closed")
		}
		if index != nil {
			transferred, err := d.checkpointTransfersComplete(ctx, checkpoint, &index.Artifact)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !transferred {
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			if err := d.releaseCatalog(ctx, checkpoint.Namespace, &index.Artifact, publicArtifactOwner(checkpoint)); err != nil {
				return ctrl.Result{}, err
			}
			complete, err := d.collectCatalog(ctx, types.NamespacedName{Namespace: checkpoint.Namespace, Name: index.Artifact.Name})
			if err != nil {
				return ctrl.Result{}, err
			}
			if !complete {
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			if checkpoint.Status.Phase != "Deleting" {
				return r.phase(ctx, checkpoint, "Deleting", "ReferenceReleased", "the exact checkpoint reference was released after inheriting workspaces acquired their own references")
			}
			uid := indexCM.UID
			if err := r.Delete(ctx, indexCM, client.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: time.Millisecond}, nil
		}
		controllerutil.RemoveFinalizer(checkpoint, checkpointFinalizer)
		return ctrl.Result{}, r.Update(ctx, checkpoint)
	}
	if checkpoint.UID == "" {
		return ctrl.Result{}, fmt.Errorf("checkpoint requires an exact Kubernetes UID")
	}
	if index == nil {
		if checkpoint.Status.Digest != "" {
			return ctrl.Result{}, fmt.Errorf("checkpoint export catalog disappeared; reselection is closed")
		}
		workspace := &api.ExecutionWorkspace{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: checkpoint.Namespace, Name: checkpoint.Spec.WorkspaceRef.Name}, workspace); err != nil {
			if apierrors.IsNotFound(err) && !controllerutil.ContainsFinalizer(checkpoint, checkpointFinalizer) {
				return ctrl.Result{}, nil
			}
			return r.phase(ctx, checkpoint, "Pending", "SourceUnavailable", "the pinned source workspace is unavailable")
		}
		provider := &api.ExecutionWorkspaceProvider{}
		if err := r.Get(ctx, types.NamespacedName{Name: workspace.Spec.ProviderBinding.Name}, provider); err != nil {
			return ctrl.Result{}, err
		}
		if provider.Spec.ControllerName != ControllerName {
			return ctrl.Result{}, nil
		}
		if workspace.UID != checkpoint.Spec.WorkspaceRef.UID || provider.UID != workspace.Spec.ProviderBinding.UID {
			return r.phase(ctx, checkpoint, "Failed", "SourceChanged", "the exact source workspace or provider binding changed")
		}
		if provider.Spec.LifecycleState == api.ExecutionWorkspaceProviderDisabled {
			return r.phase(ctx, checkpoint, "Pending", "Disabled", "checkpoint export is disabled")
		}
		if controllerutil.AddFinalizer(checkpoint, checkpointFinalizer) {
			return ctrl.Result{RequeueAfter: time.Millisecond}, r.Update(ctx, checkpoint)
		}
		key := sdk.AllocationKey{Namespace: workspace.Namespace, Name: workspace.Name, WorkspaceUID: workspace.UID, ProviderUID: provider.UID}
		cm, record, err := d.read(ctx, key)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := d.requireJournalReadOnly(ctx, cm, record); err != nil {
			return ctrl.Result{}, err
		}
		if !record.SuspendEnabled {
			return r.phase(ctx, checkpoint, "Failed", "UnsupportedSource", "checkpoint export requires a DataOnly workspace")
		}
		recovery := checkpoint.Spec.RecoverLastCheckpoint && (workspace.Status.State == api.ExecutionWorkspaceStateFailed || workspace.Status.State == api.ExecutionWorkspaceStateQuarantined)
		selected, ref := record.Checkpoint, record.CheckpointCatalog
		if recovery && selected == nil {
			selected, ref = record.InheritedCheckpoint, record.InheritedCatalog
		}
		if selected == nil || ref == nil {
			return r.phase(ctx, checkpoint, "Pending", "AwaitingCheckpoint", "waiting for a verified Data checkpoint; attached work is never interrupted")
		}
		if !recovery && (record.Operation != "suspend" || record.Observation.State != sdk.AllocationStopped || record.Observation.RetainedData == nil || workspace.Spec.Attachment != nil || workspace.Status.State != api.ExecutionWorkspaceStateSuspended || workspace.Spec.DesiredState != api.ExecutionWorkspaceDesiredSuspended) {
			return r.phase(ctx, checkpoint, "Pending", "AwaitingSuspension", "waiting for idle DataOnly suspension; failed work requires explicit recovery")
		}
		_, artifact, err := d.readCatalogReference(ctx, checkpoint.Namespace, ref)
		if err != nil {
			return ctrl.Result{}, err
		}
		if artifact.ClassBinding != workspace.Spec.ClassBinding || artifact.ProviderBinding != workspace.Spec.ProviderBinding || !artifact.Owners[workspaceArtifactOwner(key)] || !reflect.DeepEqual(artifact.Checkpoint, *selected) {
			return r.phase(ctx, checkpoint, "Failed", "ArtifactUnavailable", "the exact source no longer owns the verified Data artifact")
		}
		index = &checkpointExport{Version: catalogVersion, Checkpoint: api.ObjectIdentityReference{Name: checkpoint.Name, UID: checkpoint.UID}, SourceWorkspace: checkpoint.Spec.WorkspaceRef, ProviderUID: provider.UID, Artifact: *ref}
		data, err := json.Marshal(index)
		if err != nil {
			return ctrl.Result{}, err
		}
		keyCM := exportKey(checkpoint.Namespace, checkpoint.UID)
		indexCM = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: keyCM.Namespace, Name: keyCM.Name, Labels: map[string]string{exportLabel: string(checkpoint.UID), providerLabel: string(provider.UID)}, OwnerReferences: []metav1.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: "ExecutionWorkspaceCheckpoint", Name: checkpoint.Name, UID: checkpoint.UID}}}, Data: map[string]string{exportDataKey: string(data)}}
		if err := r.Create(ctx, indexCM); err != nil {
			return ctrl.Result{}, err
		}
		// The exact UID index freezes selection before the public digest or
		// retained reference is published. A retry cannot choose newer data.
		checkpoint.Status.Digest = artifact.Digest
		checkpoint.Status.ClassBinding = &artifact.ClassBinding
		checkpoint.Status.CreatedAt = artifact.Checkpoint.CreatedAt.DeepCopy()
		return r.phase(ctx, checkpoint, "Pending", "CapturingReference", "recorded the immutable Data checkpoint selected for export")
	}
	_, artifact, err := d.readCatalogReference(ctx, checkpoint.Namespace, &index.Artifact)
	if err != nil {
		return ctrl.Result{}, err
	}
	if index.ProviderUID != artifact.ProviderBinding.UID || (checkpoint.Status.Digest != "" && checkpoint.Status.Digest != artifact.Digest) || (checkpoint.Status.ClassBinding != nil && *checkpoint.Status.ClassBinding != artifact.ClassBinding) {
		return ctrl.Result{}, sdk.ErrStaleIdentity
	}
	if err := d.acquireCatalog(ctx, checkpoint.Namespace, &index.Artifact, "workspace:"+checkpoint.Namespace+"/"+index.SourceWorkspace.Name+":"+string(index.SourceWorkspace.UID), publicArtifactOwner(checkpoint)); err != nil {
		return r.phase(ctx, checkpoint, "Failed", "ReferenceUnavailable", "the selected Data artifact was released before export completed")
	}
	if err := d.verifyCheckpoint(ctx, &artifact.Checkpoint); err != nil {
		return r.phase(ctx, checkpoint, "Failed", "VerificationFailed", "the retained Data artifact or immutable restore template changed")
	}
	checkpoint.Status.Digest = artifact.Digest
	checkpoint.Status.ClassBinding = &artifact.ClassBinding
	checkpoint.Status.CreatedAt = artifact.Checkpoint.CreatedAt.DeepCopy()
	return r.phase(ctx, checkpoint, "Ready", "DataRetained", "verified Data is retained independently of its source workspace")
}

func (d *Lifecycle) checkpointTransfersComplete(ctx context.Context, checkpoint *api.ExecutionWorkspaceCheckpoint, ref *catalogReference) (bool, error) {
	_, artifact, err := d.readCatalogReference(ctx, checkpoint.Namespace, ref)
	if err != nil {
		return false, err
	}
	workspaces := &api.ExecutionWorkspaceList{}
	if err := d.client.List(ctx, workspaces, client.InNamespace(checkpoint.Namespace)); err != nil {
		return false, err
	}
	for _, workspace := range workspaces.Items {
		request := workspace.Spec.Workload
		if request == nil || request.RestoreFrom == nil || request.RestoreFrom.Name != checkpoint.Name || request.RestoreFrom.UID != checkpoint.UID || request.RestoreFrom.Digest != artifact.Digest || workspace.Spec.ProviderBinding.UID != artifact.ProviderBinding.UID || !workspaceHasCoreAdmission(&workspace) || workspaceHasMaintenanceIntent(&workspace) || workspace.DeletionTimestamp != nil {
			continue
		}
		if request.Key.WorkspaceUID != workspace.UID || request.Runtime == nil || request.Runtime.ClassBinding != artifact.ClassBinding || request.Key.ProviderUID != artifact.ProviderBinding.UID {
			return false, sdk.ErrStaleIdentity
		}
		if !artifact.Owners[workspaceArtifactOwner(request.Key)] {
			return false, nil
		}
	}
	return true, nil
}

func (r *CheckpointReconciler) phase(ctx context.Context, checkpoint *api.ExecutionWorkspaceCheckpoint, phase, reason, message string) (ctrl.Result, error) {
	before := &api.ExecutionWorkspaceCheckpoint{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(checkpoint), before); err != nil {
		return ctrl.Result{}, err
	}
	if before.UID != checkpoint.UID {
		return ctrl.Result{}, sdk.ErrStaleIdentity
	}
	checkpoint.Status.Phase = phase
	status := metav1.ConditionFalse
	if phase == "Ready" {
		status = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&checkpoint.Status.Conditions, metav1.Condition{Type: "Ready", Status: status, Reason: reason, Message: message, ObservedGeneration: checkpoint.Generation, LastTransitionTime: metav1.Now()})
	if reflect.DeepEqual(before.Status, checkpoint.Status) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	checkpoint.ResourceVersion = before.ResourceVersion
	return ctrl.Result{RequeueAfter: 30 * time.Second}, r.Status().Patch(ctx, checkpoint, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

func (r *CheckpointReconciler) reconcileCatalog(ctx context.Context, d *Lifecycle, key types.NamespacedName) (ctrl.Result, error) {
	cm, artifact, err := d.readCatalog(ctx, key)
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	provider := &api.ExecutionWorkspaceProvider{}
	if err := r.Get(ctx, types.NamespacedName{Name: artifact.ProviderBinding.Name}, provider); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	}
	if provider.UID == artifact.ProviderBinding.UID && provider.Spec.ControllerName != ControllerName {
		return ctrl.Result{}, nil
	}
	for owner, active := range artifact.Owners {
		if !active {
			continue
		}
		object, ownerKey, uid, err := ownerObject(owner)
		if err != nil {
			return ctrl.Result{}, err
		}
		if ownerKey.Namespace != key.Namespace {
			return ctrl.Result{}, fmt.Errorf("checkpoint owner namespace differs from the catalog")
		}
		err = r.Get(ctx, ownerKey, object)
		if err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		if apierrors.IsNotFound(err) || object.GetUID() != uid {
			artifact.Owners[owner] = false
			if activeOwners(artifact) == 0 {
				artifact.Deleting = true
			}
			return ctrl.Result{RequeueAfter: time.Millisecond}, d.saveCatalog(ctx, cm, artifact)
		}
	}
	_, err = d.collectCatalog(ctx, key)
	return ctrl.Result{RequeueAfter: 30 * time.Second}, err
}

func (r *CheckpointReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).Named("substrate-workspace-checkpoint").For(&api.ExecutionWorkspaceCheckpoint{}).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, object client.Object) []reconcile.Request {
			if object.GetLabels()[catalogLabel] != catalogVersion {
				return nil
			}
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: object.GetNamespace(), Name: "catalog/" + object.GetName()}}}
		})).Complete(r)
}
