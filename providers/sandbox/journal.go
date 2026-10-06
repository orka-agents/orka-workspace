// Copyright (c) 2026. MIT License - see LICENSE file for details.

package sandbox

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	profilev1alpha1 "github.com/orka-agents/orka-workspace/providers/sandbox/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ControllerName            = "sandbox.workspace.orka.ai"
	AdapterVersion            = "0.1.0-dev"
	journalVersion            = "sandbox.workspace.journal.v1"
	journalDataKey            = "record.json"
	MaxJournalBytes           = 256 * 1024
	ownershipLabel            = "sandbox.workspace.orka.ai/workspace-uid"
	providerLabel             = "sandbox.workspace.orka.ai/provider-uid"
	allocationLabel           = "sandbox.workspace.orka.ai/allocation"
	journalRequiredAnnotation = "sandbox.workspace.orka.ai/journal-required"
	nativeOperationAnnotation = "sandbox.workspace.orka.ai/operation"
)

type objectReference struct {
	Name         string    `json:"name"`
	UID          types.UID `json:"uid,omitempty"`
	CreateIssued bool      `json:"createIssued,omitempty"`
}

type storageIdentity struct {
	ClaimName  string    `json:"claimName"`
	ClaimUID   types.UID `json:"claimUID"`
	VolumeName string    `json:"volumeName"`
	VolumeUID  types.UID `json:"volumeUID"`
}

type journalRecord struct {
	Version         string                                              `json:"version"`
	Request         workspaceprovider.WorkloadRequest                   `json:"request"`
	Observation     workspaceprovider.AllocationObservation             `json:"observation"`
	Operation       string                                              `json:"operation"`
	Namespace       string                                              `json:"namespace"`
	Anchor          objectReference                                     `json:"anchor"`
	Template        objectReference                                     `json:"template"`
	WarmPool        objectReference                                     `json:"warmPool"`
	Claim           objectReference                                     `json:"claim"`
	Sandbox         objectReference                                     `json:"sandbox"`
	Pod             *workspaceprovider.PodReference                     `json:"pod,omitempty"`
	PreviousPod     *workspaceprovider.PodReference                     `json:"previousPod,omitempty"`
	Volume          *profilev1alpha1.SandboxDurableVolume               `json:"volume,omitempty"`
	StorageClassUID types.UID                                           `json:"storageClassUID,omitempty"`
	Storage         *storageIdentity                                    `json:"storage,omitempty"`
	ResumePrepared  bool                                                `json:"resumePrepared,omitempty"`
	DeletionPolicy  *workspacev1alpha1.ExecutionWorkspaceDeletionPolicy `json:"deletionPolicy,omitempty"`
}

// Lifecycle uses an uncached client. Every native mutation follows durable CAS
// intent, and all identity records survive until workspace finalization.
type Lifecycle struct{ client client.Client }

var _ workspaceprovider.Lifecycle = (*Lifecycle)(nil)
var _ workspaceprovider.SuspensionController = (*Lifecycle)(nil)

func New(c client.Client) *Lifecycle { return &Lifecycle{client: c} }

func journalKey(key workspaceprovider.AllocationKey) types.NamespacedName {
	sum := sha256.Sum256([]byte(key.Namespace + "\x00" + key.Name + "\x00" + string(key.WorkspaceUID) + "\x00" + string(key.ProviderUID)))
	return types.NamespacedName{Namespace: key.Namespace, Name: "sandbox-workspace-" + hex.EncodeToString(sum[:20])}
}

func historyKey(key workspaceprovider.AllocationKey, sequence int64) types.NamespacedName {
	sum := sha256.Sum256([]byte(journalKey(key).Name + "\x00" + strconv.FormatInt(sequence, 10)))
	return types.NamespacedName{Namespace: key.Namespace, Name: "sandbox-history-" + hex.EncodeToString(sum[:20])}
}

func labels(record *journalRecord) map[string]string {
	return map[string]string{ownershipLabel: string(record.Request.Key.WorkspaceUID), providerLabel: string(record.Request.Key.ProviderUID), allocationLabel: record.Observation.Identity.AllocationID}
}

func workspaceOwner(key workspaceprovider.AllocationKey) metav1.OwnerReference {
	return metav1.OwnerReference{APIVersion: workspacev1alpha1.GroupVersion.String(), Kind: "ExecutionWorkspace", Name: key.Name, UID: key.WorkspaceUID}
}

func (d *Lifecycle) read(ctx context.Context, key workspaceprovider.AllocationKey) (*corev1.ConfigMap, *journalRecord, error) {
	return d.readAt(ctx, key, journalKey(key))
}

func (d *Lifecycle) readAt(ctx context.Context, key workspaceprovider.AllocationKey, name types.NamespacedName) (*corev1.ConfigMap, *journalRecord, error) {
	if err := key.Validate(); err != nil {
		return nil, nil, err
	}
	cm := &corev1.ConfigMap{}
	if err := d.client.Get(ctx, name, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, workspaceprovider.ErrNotFound
		}
		return nil, nil, err
	}
	raw := cm.Data[journalDataKey]
	if len(raw) == 0 || len(raw) > MaxJournalBytes {
		return nil, nil, fmt.Errorf("invalid Sandbox journal size")
	}
	var record journalRecord
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return nil, nil, fmt.Errorf("decode Sandbox journal: %w", err)
	}
	if record.Version != journalVersion || record.Request.Key != key || record.Observation.Key != key ||
		record.Observation.Sequence != record.Request.Sequence || !record.Observation.Identity.Valid() || record.Observation.Identity.RequestRevision != record.Request.Revision ||
		!reflect.DeepEqual(cm.Labels, labels(&record)) || !reflect.DeepEqual(cm.OwnerReferences, []metav1.OwnerReference{workspaceOwner(key)}) {
		return nil, nil, workspaceprovider.ErrStaleIdentity
	}
	if err := record.Request.Validate(); err != nil {
		return nil, nil, fmt.Errorf("invalid journal request: %w", err)
	}
	return cm, &record, nil
}

func encode(record *journalRecord) (string, error) {
	data, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	if len(data) > MaxJournalBytes {
		return "", fmt.Errorf("Sandbox journal exceeds %d bytes", MaxJournalBytes)
	}
	return string(data), nil
}

func (d *Lifecycle) save(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	data, err := encode(record)
	if err != nil {
		return err
	}
	cm.Data = map[string]string{journalDataKey: data}
	return d.client.Update(ctx, cm)
}

func (d *Lifecycle) archive(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	key := historyKey(record.Request.Key, record.Request.Sequence)
	archived := cm.DeepCopy()
	archived.Name = key.Name
	archived.ResourceVersion = ""
	archived.UID = ""
	archived.CreationTimestamp = metav1.Time{}
	archived.ManagedFields = nil
	if err := d.client.Create(ctx, archived); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		_, existing, err := d.readAt(ctx, record.Request.Key, key)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(existing, record) {
			return workspaceprovider.ErrRequestConflict
		}
	}
	return nil
}

func newRecord(request workspaceprovider.WorkloadRequest) (*journalRecord, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	id := "sb-" + hex.EncodeToString(nonce[:])
	namespace := request.Runtime.Template.Namespace
	if namespace == "" {
		namespace = request.Key.Namespace
	}
	return &journalRecord{Version: journalVersion, Request: request, Operation: "ensure", Namespace: namespace,
		Anchor: objectReference{Name: id + "-owner"}, Template: objectReference{Name: id + "-template"}, WarmPool: objectReference{Name: id + "-warm"}, Claim: objectReference{Name: id},
		Observation: workspaceprovider.AllocationObservation{Sequence: request.Sequence, Key: request.Key, Identity: workspaceprovider.InstanceIdentity{AllocationID: id, InstanceID: id + "-1", RequestRevision: request.Revision}, State: workspaceprovider.AllocationPending}}, nil
}

func (d *Lifecycle) admitted(ctx context.Context, request workspaceprovider.WorkloadRequest) error {
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: request.Key.Namespace, Name: request.Key.Name}, workspace); err != nil {
		return err
	}
	if workspace.UID != request.Key.WorkspaceUID || workspace.Spec.ProviderBinding.UID != request.Key.ProviderUID {
		return workspaceprovider.ErrStaleIdentity
	}
	if !workspace.DeletionTimestamp.IsZero() || workspace.Spec.Retirement != nil || workspace.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredReady || !workspaceCurrentlyAdmittedByCore(workspace) || workspace.Spec.Workload == nil || workspace.Spec.Workload.Revision != request.Revision || workspace.Spec.Workload.Sequence != request.Sequence {
		return workspaceprovider.ErrWorkspaceNotAdmitted
	}
	if err := workspaceprovider.ValidateWorkspaceWorkload(workspace); err != nil {
		return err
	}
	provider := &workspacev1alpha1.ExecutionWorkspaceProvider{}
	if err := d.client.Get(ctx, types.NamespacedName{Name: workspace.Spec.ProviderBinding.Name}, provider); err != nil {
		return err
	}
	if provider.UID != request.Key.ProviderUID || provider.Spec.ControllerName != ControllerName {
		return workspaceprovider.ErrStaleIdentity
	}
	if provider.Spec.LifecycleState == workspacev1alpha1.ExecutionWorkspaceProviderDisabled || !provider.DeletionTimestamp.IsZero() {
		return workspaceprovider.ErrWorkspaceNotAdmitted
	}
	return nil
}

// Missing journal recovery cannot infer absence from a provider read failure or
// forget a native anchor that was created before the first status publication.
func (d *Lifecycle) proveNoAllocation(ctx context.Context, key workspaceprovider.AllocationKey) error {
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: key.Namespace, Name: key.Name}, workspace); err != nil {
		return err
	}
	if workspace.UID != key.WorkspaceUID || workspace.Spec.ProviderBinding.UID != key.ProviderUID {
		return workspaceprovider.ErrStaleIdentity
	}
	if workspace.Status.Allocation != nil || workspace.Annotations[journalRequiredAnnotation] != "" {
		return fmt.Errorf("allocation journal is missing after lifecycle evidence")
	}
	var anchors corev1.ConfigMapList
	if err := d.client.List(ctx, &anchors, client.MatchingLabels{ownershipLabel: string(key.WorkspaceUID), providerLabel: string(key.ProviderUID)}); err != nil {
		return err
	}
	for _, anchor := range anchors.Items {
		if anchor.Data["journalName"] == journalKey(key).Name {
			return fmt.Errorf("allocation journal is missing while native ownership anchor remains")
		}
	}
	return nil
}

func (d *Lifecycle) requireJournal(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	if cm.UID == "" {
		return fmt.Errorf("journal has no API-assigned UID")
	}
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Request.Key.Namespace, Name: record.Request.Key.Name}, workspace); err != nil {
		return err
	}
	if workspace.UID != record.Request.Key.WorkspaceUID || workspace.Spec.ProviderBinding.UID != record.Request.Key.ProviderUID {
		return workspaceprovider.ErrStaleIdentity
	}
	expected := cm.Name + "/" + string(cm.UID)
	if value := workspace.Annotations[journalRequiredAnnotation]; value != "" {
		if value != expected {
			return fmt.Errorf("required journal identity changed: %w", workspaceprovider.ErrStaleIdentity)
		}
		return nil
	}
	if record.Anchor.CreateIssued {
		return fmt.Errorf("required journal marker disappeared after native creation intent")
	}
	before := workspace.DeepCopy()
	if workspace.Annotations == nil {
		workspace.Annotations = map[string]string{}
	}
	workspace.Annotations[journalRequiredAnnotation] = expected
	return d.client.Patch(ctx, workspace, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

func (d *Lifecycle) verifyEnsureIntent(ctx context.Context, record *journalRecord) error {
	_, current, err := d.read(ctx, record.Request.Key)
	if err != nil {
		return err
	}
	if current.Operation != "ensure" || current.Request.Sequence != record.Request.Sequence || current.Request.Revision != record.Request.Revision {
		return workspaceprovider.ErrRequestConflict
	}
	return nil
}
