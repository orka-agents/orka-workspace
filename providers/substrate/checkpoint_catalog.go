package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	catalogLabel     = "substrate.workspace.orka.ai/checkpoint-catalog"
	catalogFinalizer = ControllerName + "/checkpoint-data"
	catalogDataKey   = "checkpoint.json"
	catalogVersion   = "substrate.workspace.checkpoint.v1"
	exportLabel      = "substrate.workspace.orka.ai/checkpoint-uid"
	exportDataKey    = "export.json"
)

// Owners are exact Kubernetes object lifetimes. A false entry is a release
// tombstone; it keeps cleanup evidence until that object's finalization ends.
type checkpointArtifact struct {
	Version         string                      `json:"version"`
	Digest          string                      `json:"digest"`
	Checkpoint      checkpointRecord            `json:"checkpoint"`
	TemplateSpec    *pb.ActorTemplate           `json:"templateSpec"`
	RuntimePoolSpec json.RawMessage             `json:"runtimePoolSpec"`
	Placement       api.PodReference            `json:"placement"`
	SourceWorkspace api.ObjectIdentityReference `json:"sourceWorkspace"`
	ClassBinding    api.ImmutableObjectBinding  `json:"classBinding"`
	ProviderBinding api.ImmutableObjectBinding  `json:"providerBinding"`
	Owners          map[string]bool             `json:"owners"`
	Deleting        bool                        `json:"deleting,omitempty"`
	Collected       bool                        `json:"collected,omitempty"`
}

// This index is named from the public object's UID, never from its name or
// status digest. Native identities remain in the private artifact catalog.
type checkpointExport struct {
	Version         string                      `json:"version"`
	Checkpoint      api.ObjectIdentityReference `json:"checkpoint"`
	SourceWorkspace api.ObjectIdentityReference `json:"sourceWorkspace"`
	ProviderUID     types.UID                   `json:"providerUID"`
	Artifact        catalogReference            `json:"artifact"`
}

func catalogKey(namespace, tagUID string) types.NamespacedName {
	return types.NamespacedName{Namespace: namespace, Name: "substrate-data-" + strings.TrimPrefix(digest([]byte(tagUID)), "sha256:")[:40]}
}
func exportKey(namespace string, uid types.UID) types.NamespacedName {
	return types.NamespacedName{Namespace: namespace, Name: "substrate-export-" + strings.TrimPrefix(digest([]byte(uid)), "sha256:")[:40]}
}
func workspaceArtifactOwner(key sdk.AllocationKey) string {
	return "workspace:" + key.Namespace + "/" + key.Name + ":" + string(key.WorkspaceUID)
}
func publicArtifactOwner(checkpoint *api.ExecutionWorkspaceCheckpoint) string {
	return "checkpoint:" + checkpoint.Namespace + "/" + checkpoint.Name + ":" + string(checkpoint.UID)
}
func artifactDigest(checkpoint checkpointRecord) (string, error) {
	data, err := json.Marshal(checkpoint)
	return digest(data), err
}
func activeOwners(artifact *checkpointArtifact) int {
	n := 0
	for _, active := range artifact.Owners {
		if active {
			n++
		}
	}
	return n
}
func artifactReference(cm *corev1.ConfigMap, artifact *checkpointArtifact) *catalogReference {
	return &catalogReference{Name: cm.Name, UID: string(cm.UID), Digest: artifact.Digest}
}

func (d *Lifecycle) readCatalog(ctx context.Context, key types.NamespacedName) (*corev1.ConfigMap, *checkpointArtifact, error) {
	cm := &corev1.ConfigMap{}
	if err := d.client.Get(ctx, key, cm); err != nil {
		return nil, nil, err
	}
	raw := cm.Data[catalogDataKey]
	if len(raw) == 0 || len(raw) > 512<<10 {
		return nil, nil, fmt.Errorf("checkpoint catalog size is invalid")
	}
	artifact := &checkpointArtifact{}
	if err := json.Unmarshal([]byte(raw), artifact); err != nil {
		return nil, nil, fmt.Errorf("checkpoint catalog is unreadable")
	}
	if artifact.TemplateSpec == nil || !compatibleRuntimePool(artifact.RuntimePoolSpec, artifact.RuntimePoolSpec) {
		return nil, nil, fmt.Errorf("checkpoint catalog has no valid immutable template or worker specification")
	}
	hash, err := artifactDigest(artifact.Checkpoint)
	if err != nil {
		return nil, nil, err
	}
	templateHash, err := templateDigest(artifact.TemplateSpec)
	if err != nil {
		return nil, nil, err
	}
	if artifact.Version != catalogVersion || artifact.Checkpoint.Atespace == "" || artifact.Checkpoint.Tag.UID == "" || artifact.Checkpoint.Template.UID == "" || artifact.Checkpoint.CreatedAt.IsZero() || artifact.Placement.UID == "" || artifact.Digest != hash || artifact.Checkpoint.Template.Digest != templateHash || artifact.Owners == nil || artifact.ProviderBinding.UID == "" || cm.UID == "" || key != catalogKey(cm.Namespace, artifact.Checkpoint.Tag.UID) || cm.Labels[catalogLabel] != catalogVersion || cm.Labels[providerLabel] != string(artifact.ProviderBinding.UID) || (artifact.Collected && (!artifact.Deleting || activeOwners(artifact) != 0)) {
		return nil, nil, fmt.Errorf("checkpoint catalog identity or immutable provenance is invalid")
	}
	return cm, artifact, nil
}
func (d *Lifecycle) readCatalogReference(ctx context.Context, namespace string, ref *catalogReference) (*corev1.ConfigMap, *checkpointArtifact, error) {
	if ref == nil || ref.Name == "" || ref.UID == "" || ref.Digest == "" {
		return nil, nil, fmt.Errorf("checkpoint catalog reference is incomplete")
	}
	cm, artifact, err := d.readCatalog(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name})
	if err != nil {
		return nil, nil, err
	}
	if string(cm.UID) != ref.UID || artifact.Digest != ref.Digest {
		return nil, nil, sdk.ErrStaleIdentity
	}
	return cm, artifact, nil
}
func (d *Lifecycle) saveCatalog(ctx context.Context, cm *corev1.ConfigMap, artifact *checkpointArtifact) error {
	data, err := json.Marshal(artifact)
	if err != nil {
		return err
	}
	if len(data) > 512<<10 {
		return fmt.Errorf("checkpoint catalog exceeds its record limit")
	}
	cm.Data = map[string]string{catalogDataKey: string(data)}
	if cm.ResourceVersion == "" {
		cm.Labels = map[string]string{catalogLabel: catalogVersion, providerLabel: string(artifact.ProviderBinding.UID)}
		cm.Finalizers = []string{catalogFinalizer}
		return d.client.Create(ctx, cm)
	}
	return d.client.Update(ctx, cm)
}
func (d *Lifecycle) registerCheckpoint(ctx context.Context, record *journalRecord, checkpoint *checkpointRecord) (*catalogReference, error) {
	var reference *catalogReference
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		key := catalogKey(record.Request.Key.Namespace, checkpoint.Tag.UID)
		cm, artifact, err := d.readCatalog(ctx, key)
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		workspace := &api.ExecutionWorkspace{}
		if err := d.client.Get(ctx, clientKey(record.Request.Key), workspace); err != nil {
			return err
		}
		if workspace.UID != record.Request.Key.WorkspaceUID || workspace.Spec.ProviderBinding.UID != record.Request.Key.ProviderUID {
			return sdk.ErrStaleIdentity
		}
		owner := workspaceArtifactOwner(record.Request.Key)
		if artifact != nil {
			// Recover the first completion time after a catalog write whose
			// journal write or API response was lost.
			checkpoint.CreatedAt = artifact.Checkpoint.CreatedAt
			if artifact.Deleting || cm.DeletionTimestamp != nil || !reflect.DeepEqual(*checkpoint, artifact.Checkpoint) || artifact.ClassBinding != workspace.Spec.ClassBinding || artifact.ProviderBinding != workspace.Spec.ProviderBinding || !artifact.Owners[owner] || !proto.Equal(artifact.TemplateSpec, record.TemplateSpec) || !compatibleRuntimePool(artifact.RuntimePoolSpec, record.RuntimePoolSpec) {
				return sdk.ErrRequestConflict
			}
			reference = artifactReference(cm, artifact)
			return nil
		}
		hash, err := artifactDigest(*checkpoint)
		if err != nil {
			return err
		}
		artifact = &checkpointArtifact{Version: catalogVersion, Digest: hash, Checkpoint: *checkpoint, TemplateSpec: proto.Clone(record.TemplateSpec).(*pb.ActorTemplate), RuntimePoolSpec: append(json.RawMessage(nil), record.RuntimePoolSpec...), Placement: record.Placement, SourceWorkspace: api.ObjectIdentityReference{Name: workspace.Name, UID: workspace.UID}, ClassBinding: workspace.Spec.ClassBinding, ProviderBinding: workspace.Spec.ProviderBinding, Owners: map[string]bool{owner: true}}
		cm = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}}
		if err := d.saveCatalog(ctx, cm, artifact); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, err)
			}
			return err
		}
		reference = artifactReference(cm, artifact)
		return nil
	})
	return reference, err
}
func (d *Lifecycle) acquireCatalog(ctx context.Context, namespace string, ref *catalogReference, from, owner string) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		cm, artifact, err := d.readCatalogReference(ctx, namespace, ref)
		if err != nil {
			return err
		}
		if artifact.Deleting || cm.DeletionTimestamp != nil {
			return fmt.Errorf("checkpoint is being collected")
		}
		if artifact.Owners[owner] {
			return nil
		}
		if !artifact.Owners[from] {
			return fmt.Errorf("checkpoint source released its reference before acquisition")
		}
		if len(artifact.Owners) >= 2048 {
			return fmt.Errorf("checkpoint reference limit reached")
		}
		artifact.Owners[owner] = true
		return d.saveCatalog(ctx, cm, artifact)
	})
}
func (d *Lifecycle) releaseCatalog(ctx context.Context, namespace string, ref *catalogReference, owner string) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		cm, artifact, err := d.readCatalogReference(ctx, namespace, ref)
		if err != nil {
			return err
		}
		if !artifact.Owners[owner] {
			return nil
		}
		artifact.Owners[owner] = false
		if activeOwners(artifact) == 0 {
			artifact.Deleting = true
		}
		return d.saveCatalog(ctx, cm, artifact)
	})
}
func (d *Lifecycle) listCatalogs(ctx context.Context, namespace string) ([]corev1.ConfigMap, error) {
	list := &corev1.ConfigMapList{}
	options := []client.ListOption{client.MatchingLabels{catalogLabel: catalogVersion}}
	if namespace != "" {
		options = append(options, client.InNamespace(namespace))
	}
	if err := d.client.List(ctx, list, options...); err != nil {
		return nil, err
	}
	return list.Items, nil
}
func (d *Lifecycle) catalogReferenced(ctx context.Context, namespace, atespace, uid string, template bool, excluding string) (bool, error) {
	items, err := d.listCatalogs(ctx, namespace)
	if err != nil {
		return false, err
	}
	for _, item := range items {
		if item.Name == excluding {
			continue
		}
		_, artifact, err := d.readCatalog(ctx, client.ObjectKeyFromObject(&item))
		if err != nil {
			return false, err
		}
		if artifact.Collected || artifact.Checkpoint.Atespace != atespace {
			continue
		}
		ref := artifact.Checkpoint.Tag
		if template {
			ref = artifact.Checkpoint.Template
		}
		if uid != "" && ref.UID == uid {
			return true, nil
		}
	}
	return false, nil
}
func (d *Lifecycle) catalogTagReferenced(ctx context.Context, namespace, atespace, uid string) (bool, error) {
	return d.catalogReferenced(ctx, namespace, atespace, uid, false, "")
}
func (d *Lifecycle) catalogTemplateReferenced(ctx context.Context, namespace, atespace, uid string) (bool, error) {
	return d.catalogReferenced(ctx, namespace, atespace, uid, true, "")
}

func ownerObject(owner string) (client.Object, types.NamespacedName, types.UID, error) {
	kind, rest, ok := strings.Cut(owner, ":")
	path, uid, hasUID := strings.Cut(rest, ":")
	namespace, name, hasNamespace := strings.Cut(path, "/")
	if !ok || !hasUID || !hasNamespace || namespace == "" || name == "" || uid == "" {
		return nil, types.NamespacedName{}, "", fmt.Errorf("checkpoint owner identity is invalid")
	}
	var object client.Object
	switch kind {
	case "workspace":
		object = &api.ExecutionWorkspace{}
	case "checkpoint":
		object = &api.ExecutionWorkspaceCheckpoint{}
	default:
		return nil, types.NamespacedName{}, "", fmt.Errorf("checkpoint owner kind is invalid")
	}
	return object, types.NamespacedName{Namespace: namespace, Name: name}, types.UID(uid), nil
}

// Native deletion is retried by observation. Catalog tombstones outlive source
// finalization, so a missing live catalog cannot be mistaken for completed GC.
func (d *Lifecycle) collectCatalog(ctx context.Context, key types.NamespacedName) (bool, error) {
	cm, artifact, err := d.readCatalog(ctx, key)
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if activeOwners(artifact) != 0 {
		return true, nil
	}
	if !artifact.Deleting {
		artifact.Deleting = true
		return false, d.saveCatalog(ctx, cm, artifact)
	}
	if !artifact.Collected {
		checkpoint := &artifact.Checkpoint
		tag, err := d.control.GetTag(ctx, &pb.GetTagRequest{Tag: &pb.ObjectRef{Atespace: checkpoint.Atespace, Name: checkpoint.Tag.Name}})
		if err != nil && status.Code(err) != codes.NotFound {
			return false, err
		}
		if tag != nil {
			if err := d.verifyCheckpoint(ctx, checkpoint); err != nil {
				return false, err
			}
			if _, err := d.control.DeleteTag(ctx, &pb.DeleteTagRequest{Tag: &pb.ObjectRef{Atespace: checkpoint.Atespace, Name: checkpoint.Tag.Name}}); err != nil && status.Code(err) != codes.NotFound {
				return false, err
			}
			return false, nil
		}
		referenced, err := d.catalogReferenced(ctx, key.Namespace, checkpoint.Atespace, checkpoint.Template.UID, true, cm.Name)
		if err != nil {
			return false, err
		}
		if !referenced {
			template, err := d.control.GetActorTemplate(ctx, &pb.GetActorTemplateRequest{ActorTemplate: &pb.ObjectRef{Atespace: checkpoint.Atespace, Name: checkpoint.Template.Name}})
			if err != nil && status.Code(err) != codes.NotFound {
				return false, err
			}
			if template != nil {
				if err := d.verifyTemplate(ctx, checkpoint.Atespace, checkpoint.Template); err != nil {
					return false, err
				}
				if _, err := d.control.DeleteActorTemplate(ctx, &pb.DeleteActorTemplateRequest{ActorTemplate: &pb.ObjectRef{Atespace: checkpoint.Atespace, Name: checkpoint.Template.Name}}); err != nil && status.Code(err) != codes.NotFound {
					return false, err
				}
				return false, nil
			}
		}
		artifact.Collected = true
		if err := d.saveCatalog(ctx, cm, artifact); err != nil {
			return false, err
		}
	}
	for owner := range artifact.Owners {
		object, key, uid, err := ownerObject(owner)
		if err != nil {
			return false, err
		}
		if err := d.client.Get(ctx, key, object); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, err
		}
		if object.GetUID() == uid {
			return true, nil
		}
	}
	if controllerutil.RemoveFinalizer(cm, catalogFinalizer) {
		if err := d.client.Update(ctx, cm); err != nil {
			return false, err
		}
	}
	uid := cm.UID
	return true, client.IgnoreNotFound(d.client.Delete(ctx, cm, client.Preconditions{UID: &uid}))
}

func (d *Lifecycle) releaseRecordArtifacts(ctx context.Context, record *journalRecord) (bool, error) {
	namespace := record.Request.Key.Namespace
	refs := map[string]*catalogReference{}
	for _, ref := range []*catalogReference{record.CheckpointCatalog, record.InheritedCatalog} {
		if ref != nil {
			if _, _, err := d.readCatalogReference(ctx, namespace, ref); err != nil {
				return false, err
			}
			refs[ref.Name] = ref
		}
	}
	// Capture/import ownership can commit before its journal write is lost.
	items, err := d.listCatalogs(ctx, namespace)
	if err != nil {
		return false, err
	}
	owner := workspaceArtifactOwner(record.Request.Key)
	for _, item := range items {
		cm, artifact, err := d.readCatalog(ctx, client.ObjectKeyFromObject(&item))
		if err != nil {
			return false, err
		}
		if _, recorded := artifact.Owners[owner]; recorded {
			refs[cm.Name] = artifactReference(cm, artifact)
		}
	}
	for _, ref := range refs {
		if err := d.releaseCatalog(ctx, namespace, ref, owner); err != nil {
			return false, err
		}
		complete, err := d.collectCatalog(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name})
		if err != nil || !complete {
			return false, err
		}
	}
	return true, nil
}
