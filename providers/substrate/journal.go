// Copyright (c) 2026. MIT License - see LICENSE file for details.
package substrate

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	profile "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ControllerName            = "v1.substrate.workspace.orka.ai"
	AdapterVersion            = "0.1.0-dev"
	journalVersion            = "substrate.workspace.journal.v1"
	journalDataKey            = "record.json"
	MaxJournalBytes           = 256 << 10
	ownershipLabel            = "substrate.workspace.orka.ai/workspace-uid"
	providerLabel             = "substrate.workspace.orka.ai/provider-uid"
	journalRequiredAnnotation = ControllerName + "/journal-required"
	workerPoolLabel           = "ate.dev/worker-pool"
	workerAllocationLabel     = "substrate.workspace.orka.ai/allocation-id"
	workerInstanceLabel       = "substrate.workspace.orka.ai/instance-id"
)

type nativeReference struct {
	Name         string `json:"name"`
	UID          string `json:"uid,omitempty"`
	Digest       string `json:"digest,omitempty"`
	CreateIssued bool   `json:"createIssued,omitempty"`
}
type workerFence struct {
	Name, UID, Namespace, Pool, Pod, PodUID string
	AllocationID, InstanceID                string
}
type checkpointIntent struct {
	Name                 string          `json:"name"`
	SuspendIssued        bool            `json:"suspendIssued,omitempty"`
	PriorSnapshotDigest  string          `json:"priorSnapshotDigest,omitempty"`
	SourceVersion        int64           `json:"sourceVersion,omitempty"`
	SourceSnapshotDigest string          `json:"sourceSnapshotDigest,omitempty"`
	Tag                  nativeReference `json:"tag"`
}
type checkpointRecord struct {
	Tag        nativeReference `json:"tag"`
	Atespace   string          `json:"atespace"`
	SourceName string          `json:"sourceName"`
	SourceUID  string          `json:"sourceUID"`
	Template   nativeReference `json:"template"`
	CreatedAt  metav1.Time     `json:"createdAt"`
}

type catalogReference struct {
	Name   string `json:"name"`
	UID    string `json:"uid"`
	Digest string `json:"digest"`
}
type journalRecord struct {
	Version             string                    `json:"version"`
	Request             sdk.WorkloadRequest       `json:"request"`
	Observation         sdk.AllocationObservation `json:"observation"`
	Operation           string                    `json:"operation"`
	Atespace            string                    `json:"atespace"`
	SuspendEnabled      bool                      `json:"suspendEnabled"`
	MaxSuspended        *int32                    `json:"maxSuspended,omitempty"`
	Placement           api.PodReference          `json:"placement"`
	SourceSelector      map[string]string         `json:"sourceSelector"`
	RuntimePool         nativeReference           `json:"runtimePool"`
	RuntimePoolSpec     json.RawMessage           `json:"runtimePoolSpec"`
	RuntimePoolLabels   map[string]string         `json:"runtimePoolLabels"`
	RuntimePoolDeleting bool                      `json:"runtimePoolDeleting,omitempty"`
	Anchor              nativeReference           `json:"anchor"`
	NetworkPolicy       nativeReference           `json:"networkPolicy"`
	Template            nativeReference           `json:"template"`
	TemplateSpec        *pb.ActorTemplate         `json:"templateSpec"`
	CreateTemplate      nativeReference           `json:"createTemplate"`
	Actor               nativeReference           `json:"actor"`
	BootRequested       bool                      `json:"bootRequested,omitempty"`
	Worker              *workerFence              `json:"worker,omitempty"`
	RetirementWorkers   []workerFence             `json:"retirementWorkers,omitempty"`
	WorkerDrained       bool                      `json:"workerDrained,omitempty"`
	WorkloadAbsent      bool                      `json:"workloadAbsent,omitempty"`
	ChallengeSHA256     string                    `json:"challengeSHA256,omitempty"`
	ChallengeVersion    int64                     `json:"challengeVersion,omitempty"`
	Pending             *checkpointIntent         `json:"pending,omitempty"`
	Checkpoint          *checkpointRecord         `json:"checkpoint,omitempty"`
	InheritedCheckpoint *checkpointRecord         `json:"inheritedCheckpoint,omitempty"`
	CheckpointCatalog   *catalogReference         `json:"checkpointCatalog,omitempty"`
	InheritedCatalog    *catalogReference         `json:"inheritedCatalog,omitempty"`
	DeleteIssued        bool                      `json:"deleteIssued,omitempty"`
	// SuspendedReservation marks a successor that still holds its suspended
	// predecessor's retention slot until it first reaches Ready.
	SuspendedReservation bool                                  `json:"suspendedReservation,omitempty"`
	DeletionPolicy       *api.ExecutionWorkspaceDeletionPolicy `json:"deletionPolicy,omitempty"`
}

// Lifecycle owns native resources; it never holds ACP credentials. Client must
// bypass the manager cache for marker/journal/Pod identity and lost-response CAS.
type Lifecycle struct {
	client          client.Client
	control         pb.ControlClient
	config          Config
	challengeClient challengeGetter
}

var _ sdk.Lifecycle = (*Lifecycle)(nil)
var _ sdk.SuspensionController = (*Lifecycle)(nil)

func New(c client.Client, control pb.ControlClient, config Config) *Lifecycle {
	return &Lifecycle{client: c, control: control, config: config}
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}
func randomName(prefix string) (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(nonce[:]), nil
}
func journalKey(key sdk.AllocationKey) types.NamespacedName {
	sum := sha256.Sum256([]byte(key.Namespace + "\x00" + key.Name + "\x00" + string(key.WorkspaceUID) + "\x00" + string(key.ProviderUID)))
	return types.NamespacedName{Namespace: key.Namespace, Name: "substrate-workspace-" + hex.EncodeToString(sum[:20])}
}
func historyKey(key sdk.AllocationKey, sequence int64) types.NamespacedName {
	sum := sha256.Sum256([]byte(journalKey(key).Name + "\x00" + strconv.FormatInt(sequence, 10)))
	return types.NamespacedName{Namespace: key.Namespace, Name: "substrate-history-" + hex.EncodeToString(sum[:20])}
}
func workspaceOwner(key sdk.AllocationKey) metav1.OwnerReference {
	return metav1.OwnerReference{APIVersion: api.GroupVersion.String(), Kind: "ExecutionWorkspace", Name: key.Name, UID: key.WorkspaceUID}
}
func labels(record *journalRecord) map[string]string {
	return map[string]string{ownershipLabel: string(record.Request.Key.WorkspaceUID), providerLabel: string(record.Request.Key.ProviderUID)}
}
func (d *Lifecycle) read(ctx context.Context, key sdk.AllocationKey) (*corev1.ConfigMap, *journalRecord, error) {
	return d.readAt(ctx, key, journalKey(key))
}
func (d *Lifecycle) readAt(ctx context.Context, key sdk.AllocationKey, name types.NamespacedName) (*corev1.ConfigMap, *journalRecord, error) {
	if err := key.Validate(); err != nil {
		return nil, nil, err
	}
	cm := &corev1.ConfigMap{}
	if err := d.client.Get(ctx, name, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, sdk.ErrNotFound
		}
		return nil, nil, err
	}
	raw := cm.Data[journalDataKey]
	if len(raw) == 0 || len(raw) > MaxJournalBytes {
		return nil, nil, fmt.Errorf("native journal size is invalid")
	}
	record := &journalRecord{}
	if err := json.Unmarshal([]byte(raw), record); err != nil {
		return nil, nil, fmt.Errorf("native journal is unreadable")
	}
	if record.Version != journalVersion || record.Request.Key != key || record.Observation.Key != key || record.Observation.Sequence != record.Request.Sequence || record.Observation.Identity.RequestRevision != record.Request.Revision || !record.Observation.Identity.Valid() || !reflect.DeepEqual(cm.Labels, labels(record)) || !reflect.DeepEqual(cm.OwnerReferences, []metav1.OwnerReference{workspaceOwner(key)}) {
		return nil, nil, sdk.ErrStaleIdentity
	}
	if err := validateJournalRecord(record); err != nil {
		return nil, nil, err
	}
	return cm, record, nil
}

// validateJournalRecord is shared by read-only lifecycle recovery and GC release.
func validateJournalRecord(record *journalRecord) error {
	if record.TemplateSpec == nil || record.Template.Name == "" || record.Actor.Name == "" || record.Atespace == "" || record.Placement.UID == "" || record.Anchor.Name == "" || record.NetworkPolicy.Name == "" {
		return fmt.Errorf("native journal has incomplete infrastructure intent")
	}
	if err := validateRequest(record.Request); err != nil {
		return err
	}
	if record.Worker != nil && (record.Worker.AllocationID != record.Observation.Identity.AllocationID || record.Worker.InstanceID != record.Observation.Identity.InstanceID) {
		return sdk.ErrStaleIdentity
	}
	if len(record.RetirementWorkers) > 16 {
		return sdk.ErrStaleIdentity
	}
	for _, worker := range record.RetirementWorkers {
		if worker.AllocationID != record.Observation.Identity.AllocationID || worker.InstanceID != record.Observation.Identity.InstanceID || worker.Namespace != record.Placement.Namespace || worker.Pool != record.RuntimePool.Name || worker.PodUID == "" {
			return sdk.ErrStaleIdentity
		}
	}
	return nil
}

func encode(record *journalRecord) (string, error) {
	data, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	if len(data) > MaxJournalBytes {
		return "", fmt.Errorf("native journal exceeds recovery record limit")
	}
	return string(data), nil
}
func (d *Lifecycle) save(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	if err := d.protectJournal(ctx, cm); err != nil {
		return err
	}
	data, err := encode(record)
	if err != nil {
		return err
	}
	cm.Data = map[string]string{journalDataKey: data}
	return d.client.Update(ctx, cm)
}
func (d *Lifecycle) archive(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	archived := cm.DeepCopy()
	archived.Name = historyKey(record.Request.Key, record.Request.Sequence).Name
	archived.ResourceVersion = ""
	archived.UID = ""
	archived.CreationTimestamp = metav1.Time{}
	archived.DeletionTimestamp = nil
	archived.DeletionGracePeriodSeconds = nil
	if !slices.Contains(archived.Finalizers, journalProtectionFinalizer) {
		archived.Finalizers = append(archived.Finalizers, journalProtectionFinalizer)
	}
	archived.ManagedFields = nil
	delete(archived.Annotations, journalReleasedAnnotation)
	if err := d.client.Create(ctx, archived); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		_, existing, err := d.readAt(ctx, record.Request.Key, historyKey(record.Request.Key, record.Request.Sequence))
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(existing, record) {
			return sdk.ErrRequestConflict
		}
	}
	return nil
}
func (d *Lifecycle) admitted(ctx context.Context, request sdk.WorkloadRequest) error {
	workspace := &api.ExecutionWorkspace{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: request.Key.Namespace, Name: request.Key.Name}, workspace); err != nil {
		return err
	}
	if workspace.UID != request.Key.WorkspaceUID || workspace.Spec.ProviderBinding.UID != request.Key.ProviderUID {
		return sdk.ErrStaleIdentity
	}
	if workspace.DeletionTimestamp != nil || workspace.Spec.Retirement != nil || workspace.Spec.DesiredState != api.ExecutionWorkspaceDesiredReady || !workspaceCurrentlyAdmittedByCore(workspace) || workspace.Spec.Workload == nil || workspace.Spec.Workload.Revision != request.Revision || workspace.Spec.Workload.Sequence != request.Sequence {
		return sdk.ErrWorkspaceNotAdmitted
	}
	if err := sdk.ValidateWorkspaceWorkload(workspace); err != nil {
		return err
	}
	provider := &api.ExecutionWorkspaceProvider{}
	if err := d.client.Get(ctx, types.NamespacedName{Name: workspace.Spec.ProviderBinding.Name}, provider); err != nil {
		return err
	}
	if provider.UID != request.Key.ProviderUID || provider.Spec.ControllerName != ControllerName {
		return sdk.ErrStaleIdentity
	}
	if provider.Spec.LifecycleState == api.ExecutionWorkspaceProviderDisabled || provider.DeletionTimestamp != nil {
		return sdk.ErrWorkspaceNotAdmitted
	}
	return nil
}
func (d *Lifecycle) proveNoAllocation(ctx context.Context, key sdk.AllocationKey) error {
	workspace := &api.ExecutionWorkspace{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: key.Namespace, Name: key.Name}, workspace); err != nil {
		return err
	}
	if workspace.UID != key.WorkspaceUID || workspace.Spec.ProviderBinding.UID != key.ProviderUID {
		return sdk.ErrStaleIdentity
	}
	if workspace.Status.Allocation != nil || workspace.Annotations[journalRequiredAnnotation] != "" {
		return fmt.Errorf("native lifecycle journal is missing after allocation evidence")
	}
	if workspace.Status.ExternalID != "" || len(workspace.Status.Endpoints) != 0 {
		return fmt.Errorf("native lifecycle journal is missing after provider evidence")
	}
	for key, value := range workspace.Annotations {
		if strings.HasPrefix(key, "orka.ai/substrate-") && value != "" {
			return fmt.Errorf("legacy native lifecycle evidence cannot be adopted without its journal")
		}
	}
	return nil
}
func (d *Lifecycle) requireJournal(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	if err := d.protectJournal(ctx, cm); err != nil {
		return err
	}
	if cm.UID == "" {
		return fmt.Errorf("native journal has no API-assigned UID")
	}
	workspace := &api.ExecutionWorkspace{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: record.Request.Key.Namespace, Name: record.Request.Key.Name}, workspace); err != nil {
		return err
	}
	if workspace.UID != record.Request.Key.WorkspaceUID || workspace.Spec.ProviderBinding.UID != record.Request.Key.ProviderUID {
		return sdk.ErrStaleIdentity
	}
	expected := cm.Name + "/" + string(cm.UID)
	if marker := workspace.Annotations[journalRequiredAnnotation]; marker != "" {
		if marker != expected {
			return sdk.ErrStaleIdentity
		}
		return nil
	}
	if record.Template.CreateIssued || record.Actor.CreateIssued {
		return fmt.Errorf("native journal marker disappeared after creation intent")
	}
	before := workspace.DeepCopy()
	if workspace.Annotations == nil {
		workspace.Annotations = map[string]string{}
	}
	workspace.Annotations[journalRequiredAnnotation] = expected
	return d.client.Patch(ctx, workspace, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}
func (d *Lifecycle) resolveProfile(ctx context.Context, record *journalRecord) error {
	request := record.Request
	if request.ParametersRef == nil || request.ParametersBinding == nil || request.ParametersRef.Group != profile.GroupVersion.Group || request.ParametersRef.Kind != "SubstrateWorkspaceProfile" {
		return fmt.Errorf("native runtime requires an immutable SubstrateWorkspaceProfile")
	}
	object := &profile.SubstrateWorkspaceProfile{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: request.Key.Namespace, Name: request.ParametersRef.Name}, object); err != nil {
		return err
	}
	if err := object.Validate(); err != nil {
		return err
	}
	value, err := runtime.DefaultUnstructuredConverter.ToUnstructured(object)
	if err != nil {
		return err
	}
	value["apiVersion"] = profile.GroupVersion.String()
	value["kind"] = "SubstrateWorkspaceProfile"
	hash, err := sdk.ParametersProfileHash(&unstructured.Unstructured{Object: value})
	if err != nil {
		return err
	}
	if object.DeletionTimestamp != nil || object.UID != request.ParametersBinding.UID || object.Generation != request.ParametersBinding.Generation || hash != request.ParametersBinding.ProfileHash {
		return sdk.ErrStaleIdentity
	}
	ref, err := object.Spec.TemplateRef.Resolve(object.Namespace)
	if err != nil {
		return err
	}
	record.Atespace = ref.Namespace
	record.SuspendEnabled = object.Spec.Suspend != nil
	for _, feature := range request.Runtime.RequiredFeatures {
		if !record.SuspendEnabled && (feature == api.WorkspaceFeatureSuspend || feature == api.WorkspaceFeatureCheckpoint || feature == api.WorkspaceFeatureRestore) {
			return fmt.Errorf("requested Data lifecycle features require a DataOnly profile")
		}
	}
	if object.Spec.Retention != nil {
		record.MaxSuspended = object.Spec.Retention.MaxSuspendedWorkspaces
	}
	workspace := &api.ExecutionWorkspace{}
	if err := d.client.Get(ctx, clientKey(request.Key), workspace); err != nil {
		return err
	}
	policy := workspace.Spec.Lifecycle.DeletionPolicy
	if policy.ProviderResources != api.WorkspaceDeletionActionDelete || policy.Checkpoints != api.WorkspaceDeletionActionDelete || policy.PersistentVolumes != api.WorkspaceDeletionActionDelete {
		return fmt.Errorf("native runtime requires delete disposition policy")
	}
	if record.SuspendEnabled {
		lifecycle := workspace.Spec.Lifecycle
		if workspace.Spec.Mode != api.ExecutionWorkspaceModeInteractive || workspace.Spec.SessionRef == nil || workspace.Spec.SessionRef.UID == "" || !slices.Contains(lifecycle.AllowedOnDetach, api.WorkspaceOnDetachSuspend) {
			return fmt.Errorf("native suspension requires interactive session reuse and allowed Suspend")
		}
		if (lifecycle.IdleTimeout == nil || lifecycle.IdleTimeout.Duration <= 0) && (lifecycle.MaxLifetime == nil || lifecycle.MaxLifetime.Duration <= 0) {
			return fmt.Errorf("native suspension requires a positive retention expiry")
		}
		if record.MaxSuspended != nil && (lifecycle.MaxLifetime == nil || lifecycle.MaxLifetime.Duration <= 0) {
			return fmt.Errorf("suspended count cap requires a positive maxLifetime")
		}
	}
	if err := d.compileTemplate(ctx, record, ref.Name); err != nil {
		return err
	}
	return d.prepareRuntimePool(ctx, record)
}
