package substrate

import (
	"context"
	"fmt"
	"reflect"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func (d *Lifecycle) verifyInheritedCheckpoint(ctx context.Context, record *journalRecord) error {
	if record.InheritedCheckpoint == nil && record.InheritedCatalog == nil {
		return nil
	}
	if record.InheritedCheckpoint == nil || record.InheritedCatalog == nil {
		return fmt.Errorf("native restore lost its exact catalog ownership")
	}
	_, artifact, err := d.readCatalogReference(ctx, record.Request.Key.Namespace, record.InheritedCatalog)
	if err != nil {
		return err
	}
	if !artifact.Owners[workspaceArtifactOwner(record.Request.Key)] || artifact.Deleting || artifact.ClassBinding != record.Request.Runtime.ClassBinding || artifact.ProviderBinding.UID != record.Request.Key.ProviderUID || !reflect.DeepEqual(artifact.Checkpoint, *record.InheritedCheckpoint) {
		return sdk.ErrStaleIdentity
	}
	return d.verifyCheckpoint(ctx, record.InheritedCheckpoint)
}

func (d *Lifecycle) importCheckpoint(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	ref := record.Request.RestoreFrom
	if ref == nil || record.Request.Sequence != 1 {
		return nil
	}
	if !record.SuspendEnabled {
		return fmt.Errorf("checkpoint restore requires a DataOnly profile")
	}
	owner := workspaceArtifactOwner(record.Request.Key)
	publicOwner := "checkpoint:" + record.Request.Key.Namespace + "/" + ref.Name + ":" + string(ref.UID)
	var artifact *checkpointArtifact
	var reference *catalogReference
	if record.InheritedCatalog != nil {
		_, existing, err := d.readCatalogReference(ctx, record.Request.Key.Namespace, record.InheritedCatalog)
		if err != nil {
			return err
		}
		artifact, reference = existing, record.InheritedCatalog
	} else {
		// Ownership may have committed before the import journal write was
		// lost. Recover it even if the public checkpoint has since finalized.
		items, err := d.listCatalogs(ctx, record.Request.Key.Namespace)
		if err != nil {
			return err
		}
		for _, item := range items {
			catalogCM, candidate, err := d.readCatalog(ctx, client.ObjectKeyFromObject(&item))
			if err != nil {
				return err
			}
			_, exported := candidate.Owners[publicOwner]
			if candidate.Digest == ref.Digest && candidate.Owners[owner] && exported {
				if artifact != nil {
					return fmt.Errorf("checkpoint import has ambiguous ownership")
				}
				artifact, reference = candidate, artifactReference(catalogCM, candidate)
			}
		}
		if artifact == nil {
			checkpoint := &api.ExecutionWorkspaceCheckpoint{}
			if err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Request.Key.Namespace, Name: ref.Name}, checkpoint); err != nil {
				return err
			}
			indexCM, index, err := d.readExport(ctx, checkpoint)
			if err != nil {
				return err
			}
			if err := requireExportProtection(indexCM); err != nil {
				return err
			}
			if index.AcquisitionIssued != nil && !*index.AcquisitionIssued {
				return fmt.Errorf("checkpoint export never issued its retained-reference acquisition")
			}
			ready := meta.FindStatusCondition(checkpoint.Status.Conditions, "Ready")
			readyForImport := checkpoint.DeletionTimestamp == nil && !index.Retiring && checkpoint.Status.Phase == "Ready" && ready != nil && ready.Status == metav1.ConditionTrue && ready.ObservedGeneration == checkpoint.Generation
			retiringTransfer := checkpoint.DeletionTimestamp != nil && controllerutil.ContainsFinalizer(checkpoint, checkpointFinalizer) && checkpoint.Status.Phase == "Deleting" && index.allowsRetiringTransfer(record.Request)
			if checkpoint.UID != ref.UID || checkpoint.Status.Digest != ref.Digest || (!readyForImport && !retiringTransfer) || checkpoint.Spec.WorkspaceRef.UID == record.Request.Key.WorkspaceUID || checkpoint.Status.ClassBinding == nil || *checkpoint.Status.ClassBinding != record.Request.Runtime.ClassBinding {
				return fmt.Errorf("restoreFrom does not identify a Ready checkpoint at the exact UID, digest and class revision")
			}
			_, selected, err := d.readCatalogReference(ctx, checkpoint.Namespace, &index.Artifact)
			if err != nil {
				return err
			}
			artifact, reference = selected, &index.Artifact
		}
	}
	workspace := &api.ExecutionWorkspace{}
	if err := d.client.Get(ctx, clientKey(record.Request.Key), workspace); err != nil {
		return err
	}
	if workspace.UID != record.Request.Key.WorkspaceUID || artifact.SourceWorkspace.UID == workspace.UID || artifact.Digest != ref.Digest || artifact.ClassBinding != record.Request.Runtime.ClassBinding || artifact.ProviderBinding != workspace.Spec.ProviderBinding || artifact.Checkpoint.Atespace != record.Atespace || artifact.Placement != record.Placement || !compatibleRestore(artifact.TemplateSpec, record.TemplateSpec) || !compatibleRuntimePool(artifact.RuntimePoolSpec, record.RuntimePoolSpec) {
		return fmt.Errorf("checkpoint restore changed source identity, namespace, class, provider, infrastructure or durable layout")
	}
	if err := d.acquireCatalog(ctx, record.Request.Key.Namespace, reference, publicOwner, owner); err != nil {
		return err
	}
	if err := d.verifyCheckpoint(ctx, &artifact.Checkpoint); err != nil {
		return err
	}
	if record.InheritedCatalog != nil {
		if record.InheritedCheckpoint == nil || record.InheritedCheckpoint.Tag != artifact.Checkpoint.Tag || record.CreateTemplate != artifact.Checkpoint.Template {
			return sdk.ErrStaleIdentity
		}
		return nil
	}
	if record.Template.CreateIssued || record.Actor.CreateIssued {
		return fmt.Errorf("native restore lost import intent after materialization")
	}
	checkpoint := artifact.Checkpoint
	record.InheritedCheckpoint = &checkpoint
	record.InheritedCatalog = reference
	record.CreateTemplate = checkpoint.Template
	return d.save(ctx, cm, record)
}
