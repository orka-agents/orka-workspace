// Copyright (c) 2026. MIT License - see LICENSE file for details.

package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const exportProtectionFinalizer = ControllerName + "/checkpoint-export"
const exportReleasedAnnotation = ControllerName + "/checkpoint-export-released"

func exportRecord(cm *corev1.ConfigMap) (*checkpointExport, error) {
	raw := cm.Data[exportDataKey]
	if len(raw) == 0 || len(raw) > 512<<10 {
		return nil, fmt.Errorf("checkpoint export index size is invalid")
	}
	index := &checkpointExport{}
	if err := json.Unmarshal([]byte(raw), index); err != nil {
		return nil, fmt.Errorf("checkpoint export index is unreadable")
	}
	owner := metav1.OwnerReference{APIVersion: api.GroupVersion.String(), Kind: "ExecutionWorkspaceCheckpoint", Name: index.Checkpoint.Name, UID: index.Checkpoint.UID}
	if cm.UID == "" || cm.Namespace == "" || index.Version != catalogVersion || index.Checkpoint.Name == "" || index.Checkpoint.UID == "" ||
		index.SourceWorkspace.Name == "" || index.SourceWorkspace.UID == "" || index.ProviderUID == "" ||
		index.Artifact.Name == "" || index.Artifact.UID == "" || index.Artifact.Digest == "" ||
		client.ObjectKeyFromObject(cm) != exportKey(cm.Namespace, index.Checkpoint.UID) ||
		cm.Labels[exportLabel] != string(index.Checkpoint.UID) || cm.Labels[providerLabel] != string(index.ProviderUID) ||
		!reflect.DeepEqual(cm.OwnerReferences, []metav1.OwnerReference{owner}) {
		return nil, sdk.ErrStaleIdentity
	}
	if released := cm.Annotations[exportReleasedAnnotation]; released != "" &&
		(released != exportReleaseProof(cm) || slices.Contains(cm.Finalizers, exportProtectionFinalizer)) {
		return nil, fmt.Errorf("checkpoint export release acknowledgement is invalid")
	}
	seen := map[api.AllocationKey]bool{}
	for _, transfer := range index.Transfers {
		if !index.Retiring || transfer.Key.Validate() != nil || transfer.Key.Namespace != cm.Namespace || transfer.Key.ProviderUID != index.ProviderUID ||
			transfer.Sequence != 1 || transfer.Revision == "" || seen[transfer.Key] {
			return nil, fmt.Errorf("checkpoint export transfer fence is invalid")
		}
		seen[transfer.Key] = true
	}
	return index, nil
}

func exportReleaseProof(cm *corev1.ConfigMap) string {
	return digest([]byte(string(cm.UID) + "\n" + cm.Data[exportDataKey]))
}

func requireExportProtection(cm *corev1.ConfigMap) error {
	if cm.Annotations[exportReleasedAnnotation] != "" || !slices.Contains(cm.Finalizers, exportProtectionFinalizer) {
		return fmt.Errorf("checkpoint export index is not protected for retained-reference acquisition")
	}
	return nil
}

// Protection is established before publishing the digest or acquiring a catalog
// reference. An already-deleting legacy index cannot safely gain a finalizer.
func (d *Lifecycle) protectExport(ctx context.Context, cm *corev1.ConfigMap) (bool, error) {
	if _, err := exportRecord(cm); err != nil {
		return false, err
	}
	if cm.Annotations[exportReleasedAnnotation] != "" || slices.Contains(cm.Finalizers, exportProtectionFinalizer) {
		return false, nil
	}
	if !cm.DeletionTimestamp.IsZero() {
		return false, fmt.Errorf("unprotected checkpoint export index is already deleting; cleanup is closed")
	}
	cm.Finalizers = append(cm.Finalizers, exportProtectionFinalizer)
	return true, d.client.Update(ctx, cm)
}

func (r *CheckpointReconciler) releaseExport(ctx context.Context, cm *corev1.ConfigMap) error {
	if cm.Annotations[exportReleasedAnnotation] == "" {
		if cm.Annotations == nil {
			cm.Annotations = map[string]string{}
		}
		cm.Annotations[exportReleasedAnnotation] = exportReleaseProof(cm)
		cm.Finalizers = slices.DeleteFunc(cm.Finalizers, func(value string) bool { return value == exportProtectionFinalizer })
		if err := r.Update(ctx, cm); err != nil {
			return err
		}
	}
	if !cm.DeletionTimestamp.IsZero() {
		return nil
	}
	uid := cm.UID
	return client.IgnoreNotFound(r.Delete(ctx, cm, client.Preconditions{UID: &uid}))
}

// A protected index may outlive its checkpoint during namespace/foreground GC.
// Its immutable identities still authorize release, never a new export/import.
func (r *CheckpointReconciler) reconcileExport(ctx context.Context, d *Lifecycle, key client.ObjectKey) (ctrl.Result, error) {
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, key, cm); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	index, err := exportRecord(cm)
	if err != nil {
		return ctrl.Result{}, err
	}
	if changed, err := d.protectExport(ctx, cm); changed || err != nil {
		return ctrl.Result{RequeueAfter: time.Millisecond}, err
	}
	checkpoint := &api.ExecutionWorkspaceCheckpoint{}
	err = r.Get(ctx, client.ObjectKey{Namespace: cm.Namespace, Name: index.Checkpoint.Name}, checkpoint)
	if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	if err == nil && checkpoint.UID == index.Checkpoint.UID && (checkpoint.DeletionTimestamp.IsZero() || controllerutil.ContainsFinalizer(checkpoint, checkpointFinalizer)) {
		return r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(checkpoint)})
	}
	if cm.Annotations[exportReleasedAnnotation] != "" {
		return ctrl.Result{}, r.releaseExport(ctx, cm)
	}
	neverAcquired, err := d.exportNeverAcquired(ctx, cm, index)
	if err != nil {
		return ctrl.Result{}, err
	}
	if neverAcquired {
		// There is no native release authority here. Retire only this exact
		// index after verifying its recorded provider lifetime still exists.
		providers := &api.ExecutionWorkspaceProviderList{}
		if err := r.List(ctx, providers); err != nil {
			return ctrl.Result{}, err
		}
		owned := slices.ContainsFunc(providers.Items, func(provider api.ExecutionWorkspaceProvider) bool {
			return provider.UID == index.ProviderUID && provider.Spec.ControllerName == ControllerName
		})
		if !owned {
			return ctrl.Result{}, sdk.ErrStaleIdentity
		}
		if !index.Retiring {
			index.Retiring = true
			return ctrl.Result{RequeueAfter: time.Millisecond}, d.saveExport(ctx, cm, index)
		}
		return ctrl.Result{}, r.releaseExport(ctx, cm)
	}
	_, artifact, err := d.readCatalogReference(ctx, cm.Namespace, &index.Artifact)
	if err != nil {
		return ctrl.Result{}, err
	}
	provider := &api.ExecutionWorkspaceProvider{}
	if err := r.Get(ctx, client.ObjectKey{Name: artifact.ProviderBinding.Name}, provider); err != nil {
		return ctrl.Result{}, err
	}
	if validateExportArtifact(cm.Namespace, index, artifact) != nil || provider.UID != index.ProviderUID || provider.Spec.ControllerName != ControllerName {
		return ctrl.Result{}, sdk.ErrStaleIdentity
	}
	checkpoint = &api.ExecutionWorkspaceCheckpoint{ObjectMeta: metav1.ObjectMeta{Namespace: cm.Namespace, Name: index.Checkpoint.Name, UID: index.Checkpoint.UID}, Spec: api.ExecutionWorkspaceCheckpointSpec{WorkspaceRef: index.SourceWorkspace}}
	if !index.Retiring {
		return ctrl.Result{RequeueAfter: time.Millisecond}, d.freezeExportTransfers(ctx, cm, index)
	}
	complete, err := d.checkpointTransfersComplete(ctx, checkpoint, index)
	if err != nil || !complete {
		return ctrl.Result{RequeueAfter: time.Second}, err
	}
	if err := d.releaseCatalog(ctx, cm.Namespace, &index.Artifact, publicArtifactOwner(checkpoint)); err != nil {
		return ctrl.Result{}, err
	}
	complete, err = d.collectCatalog(ctx, client.ObjectKey{Namespace: cm.Namespace, Name: index.Artifact.Name})
	if err != nil || !complete {
		return ctrl.Result{RequeueAfter: time.Second}, err
	}
	return ctrl.Result{}, r.releaseExport(ctx, cm)
}

func (d *Lifecycle) freezeExportTransfers(ctx context.Context, cm *corev1.ConfigMap, index *checkpointExport) error {
	_, artifact, err := d.readCatalogReference(ctx, cm.Namespace, &index.Artifact)
	if err != nil {
		return err
	}
	if err := validateExportArtifact(cm.Namespace, index, artifact); err != nil {
		return err
	}
	checkpoint := &api.ExecutionWorkspaceCheckpoint{ObjectMeta: metav1.ObjectMeta{Namespace: cm.Namespace, Name: index.Checkpoint.Name, UID: index.Checkpoint.UID}}
	// An old cleanup may already have released its public owner. It must never
	// adopt new candidates on retry. Independently acquired owners remain valid.
	if artifact.Owners[publicArtifactOwner(checkpoint)] {
		workspaces := &api.ExecutionWorkspaceList{}
		if err := d.client.List(ctx, workspaces, client.InNamespace(cm.Namespace)); err != nil {
			return err
		}
		for _, workspace := range workspaces.Items {
			request := workspace.Spec.Workload
			if request == nil || request.RestoreFrom == nil || request.RestoreFrom.Name != checkpoint.Name || request.RestoreFrom.UID != checkpoint.UID ||
				request.RestoreFrom.Digest != artifact.Digest || workspace.Spec.ProviderBinding.UID != artifact.ProviderBinding.UID ||
				!workspaceHasCoreAdmission(&workspace) || workspaceHasMaintenanceIntent(&workspace) || workspace.DeletionTimestamp != nil {
				continue
			}
			if request.Key.WorkspaceUID != workspace.UID || request.Runtime == nil || request.Runtime.ClassBinding != artifact.ClassBinding ||
				request.Key.ProviderUID != artifact.ProviderBinding.UID || request.Validate() != nil || request.Sequence != 1 {
				return sdk.ErrStaleIdentity
			}
			index.Transfers = append(index.Transfers, checkpointTransfer{Key: request.Key, Sequence: request.Sequence, Revision: request.Revision})
		}
	}
	sort.Slice(index.Transfers, func(i, j int) bool {
		return workspaceArtifactOwner(index.Transfers[i].Key) < workspaceArtifactOwner(index.Transfers[j].Key)
	})
	index.Retiring = true
	return d.saveExport(ctx, cm, index)
}

func (d *Lifecycle) saveExport(ctx context.Context, cm *corev1.ConfigMap, index *checkpointExport) error {
	data, err := json.Marshal(index)
	if err != nil {
		return err
	}
	if len(data) > 512<<10 {
		return fmt.Errorf("checkpoint export transfer fence exceeds its record limit")
	}
	cm.Data[exportDataKey] = string(data)
	return d.client.Update(ctx, cm)
}

func (d *Lifecycle) exportNeverAcquired(ctx context.Context, cm *corev1.ConfigMap, index *checkpointExport) (bool, error) {
	if index.AcquisitionIssued == nil || *index.AcquisitionIssued {
		return false, nil // Legacy/issued acquisition may have committed.
	}
	if len(index.Transfers) != 0 {
		return false, fmt.Errorf("never-acquired export has an invalid transfer fence")
	}
	_, artifact, err := d.readCatalogReference(ctx, cm.Namespace, &index.Artifact)
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	checkpoint := &api.ExecutionWorkspaceCheckpoint{ObjectMeta: metav1.ObjectMeta{Namespace: cm.Namespace, Name: index.Checkpoint.Name, UID: index.Checkpoint.UID}}
	if artifact.Owners[publicArtifactOwner(checkpoint)] {
		return false, fmt.Errorf("never-acquired export unexpectedly owns retained data")
	}
	return false, nil
}

func (index *checkpointExport) allowsRetiringTransfer(request sdk.WorkloadRequest) bool {
	return index.Retiring && slices.Contains(index.Transfers, checkpointTransfer{Key: request.Key, Sequence: request.Sequence, Revision: request.Revision})
}

// Catalog tombstones must outlive an unreleased export index even after its
// checkpoint vanishes, so either controller ordering preserves release evidence.
func (d *Lifecycle) catalogExportReleasePending(ctx context.Context, cm *corev1.ConfigMap, artifact *checkpointArtifact) (bool, error) {
	indexes := &corev1.ConfigMapList{}
	if err := d.client.List(ctx, indexes, client.InNamespace(cm.Namespace), client.MatchingLabels{providerLabel: string(artifact.ProviderBinding.UID)}, client.HasLabels{exportLabel}); err != nil {
		return false, err
	}
	ref := artifactReference(cm, artifact)
	for i := range indexes.Items {
		export := &indexes.Items[i]
		index, err := exportRecord(export)
		if err != nil {
			return false, err
		}
		if index.Artifact.Name != cm.Name {
			continue
		}
		if index.Artifact != *ref || validateExportArtifact(cm.Namespace, index, artifact) != nil {
			return false, sdk.ErrStaleIdentity
		}
		if export.Annotations[exportReleasedAnnotation] == "" {
			return true, nil
		}
	}
	return false, nil
}

func validateExportArtifact(namespace string, index *checkpointExport, artifact *checkpointArtifact) error {
	sourceOwner := "workspace:" + namespace + "/" + index.SourceWorkspace.Name + ":" + string(index.SourceWorkspace.UID)
	if _, recorded := artifact.Owners[sourceOwner]; !recorded || artifact.ProviderBinding.UID != index.ProviderUID {
		return sdk.ErrStaleIdentity
	}
	return nil
}
