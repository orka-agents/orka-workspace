// Copyright (c) 2026. MIT License - see LICENSE file for details.

// Package fake provides a Kubernetes-backed development provider and a
// deterministic status fixture for lifecycle conformance.
package fake

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	journalVersion  = "fake.workspace.journal.v2"
	journalLabel    = "fake.workspace.orka.ai/journal"
	journalDataKey  = "record.json"
	MaxJournalBytes = 256 * 1024
)

type journalRecord struct {
	Version        string                                              `json:"version"`
	Request        workspaceprovider.WorkloadRequest                   `json:"request"`
	Observation    workspaceprovider.AllocationObservation             `json:"observation"`
	Operation      string                                              `json:"operation"`
	DeletionPolicy *workspacev1alpha1.ExecutionWorkspaceDeletionPolicy `json:"deletionPolicy,omitempty"`
	Pod            *workspaceprovider.PodReference                     `json:"pod,omitempty"`
	CreateIssued   bool                                                `json:"createIssued,omitempty"`
}

// Lifecycle requires an uncached client. The current record is updated with
// resourceVersion CAS; prior sequence tombstones survive until owner deletion.
// Pod mode receives only public configuration, never bootstrap credentials.
type Lifecycle struct{ client client.Client }

var _ workspaceprovider.Lifecycle = (*Lifecycle)(nil)
var _ workspaceprovider.SuspensionController = (*Lifecycle)(nil)

func New(c client.Client) *Lifecycle { return &Lifecycle{client: c} }

func journalKey(key workspaceprovider.AllocationKey) types.NamespacedName {
	sum := sha256.Sum256([]byte(key.Namespace + "\x00" + key.Name + "\x00" + string(key.WorkspaceUID)))
	return types.NamespacedName{Namespace: key.Namespace, Name: "fake-workspace-" + hex.EncodeToString(sum[:20])}
}
func historyKey(key workspaceprovider.AllocationKey, sequence int64) types.NamespacedName {
	sum := sha256.Sum256([]byte(journalKey(key).Name + "\x00" + strconv.FormatInt(sequence, 10)))
	return types.NamespacedName{Namespace: key.Namespace, Name: "fake-history-" + hex.EncodeToString(sum[:20])}
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
			if name == journalKey(key) {
				required, guardErr := d.journalRequired(ctx, key)
				if guardErr != nil {
					return nil, nil, guardErr
				}
				if required {
					return nil, nil, fmt.Errorf("required allocation journal is missing; native state remains unknown")
				}
			}
			return nil, nil, workspaceprovider.ErrNotFound
		}
		return nil, nil, err
	}
	raw := cm.Data[journalDataKey]
	if len(raw) == 0 || len(raw) > MaxJournalBytes {
		return nil, nil, fmt.Errorf("invalid fake journal size")
	}
	var record journalRecord
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return nil, nil, fmt.Errorf("decode fake journal: %w", err)
	}
	if record.Version != journalVersion {
		return nil, nil, fmt.Errorf("unsupported fake journal version %q", record.Version)
	}
	if record.Request.Key != key || record.Observation.Key != key {
		return nil, nil, workspaceprovider.ErrStaleIdentity
	}
	if err := record.Request.Validate(); err != nil {
		return nil, nil, fmt.Errorf("invalid fake journal request: %w", err)
	}
	if !record.Observation.Identity.Valid() || record.Observation.Identity.RequestRevision != record.Request.Revision || record.Observation.Sequence != record.Request.Sequence {
		return nil, nil, workspaceprovider.ErrStaleIdentity
	}
	if err := d.validateJournalGuard(ctx, key, &record); err != nil {
		return nil, nil, err
	}
	if record.Observation.State == workspaceprovider.AllocationDeleted {
		if err := validateJournalRecord(&record); err != nil {
			return nil, nil, err
		}
		// Older journals claimed cleanup of credentials owned only by Core.
		// Correct the returned observation without rewriting their tombstones
		// or supplying any missing provider cleanup evidence.
		disposition := *record.Observation.Disposition
		disposition.AccessCredentials = workspacev1alpha1.DispositionNotApplicable
		disposition.EphemeralSecrets = workspacev1alpha1.DispositionNotApplicable
		record.Observation.Disposition = &disposition
	}
	return cm, &record, nil
}
func validateJournalRecord(record *journalRecord) error {
	if record.Observation.State == workspaceprovider.AllocationDeleted {
		if record.DeletionPolicy == nil {
			return fmt.Errorf("deleted fake journal has no deletion policy")
		}
		if err := workspaceprovider.ValidateDeletedDisposition(record.Observation.Disposition, *record.DeletionPolicy); err != nil {
			return fmt.Errorf("invalid deleted fake journal disposition: %w", err)
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
		return "", fmt.Errorf("fake journal exceeds %d bytes", MaxJournalBytes)
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
	key := historyKey(record.Request.Key, record.Request.Sequence)
	archived := cm.DeepCopy()
	archived.Name = key.Name
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
		_, existing, err := d.readAt(ctx, record.Request.Key, key)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(existing, record) {
			return fmt.Errorf("retired sequence tombstone changed: %w", workspaceprovider.ErrRequestConflict)
		}
	}
	return nil
}
func (d *Lifecycle) admitted(ctx context.Context, request workspaceprovider.WorkloadRequest) (*workspacev1alpha1.ExecutionWorkspace, error) {
	key := request.Key
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: key.Namespace, Name: key.Name}, workspace); err != nil {
		return nil, err
	}
	if workspace.UID != key.WorkspaceUID || workspace.Spec.ProviderBinding.UID != key.ProviderUID {
		return nil, workspaceprovider.ErrStaleIdentity
	}
	if !workspace.DeletionTimestamp.IsZero() || !workspaceCurrentlyAdmittedByCore(workspace) || workspaceHasMaintenanceIntent(workspace) {
		return nil, workspaceprovider.ErrWorkspaceNotAdmitted
	}
	if request.Runtime != nil && request.Runtime.ClassBinding != workspace.Spec.ClassBinding {
		return nil, workspaceprovider.ErrWorkspaceNotAdmitted
	}
	if workspace.Spec.Workload != nil {
		if workspace.Spec.Workload.Revision != request.Revision || workspace.Spec.Workload.Sequence != request.Sequence {
			return nil, workspaceprovider.ErrWorkspaceNotAdmitted
		}
	} else if request.Runtime != nil {
		return nil, workspaceprovider.ErrWorkspaceNotAdmitted
	}
	provider := &workspacev1alpha1.ExecutionWorkspaceProvider{}
	if err := d.client.Get(ctx, types.NamespacedName{Name: workspace.Spec.ProviderBinding.Name}, provider); err != nil {
		return nil, err
	}
	if provider.UID != key.ProviderUID || provider.Spec.ControllerName != FakeWorkspaceControllerName {
		return nil, workspaceprovider.ErrStaleIdentity
	}
	if provider.Spec.LifecycleState == workspacev1alpha1.ExecutionWorkspaceProviderDisabled || !provider.DeletionTimestamp.IsZero() {
		return nil, workspaceprovider.ErrWorkspaceNotAdmitted
	}
	return workspace, nil
}
func newRecord(request workspaceprovider.WorkloadRequest, allocation string) (*journalRecord, error) {
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	if allocation == "" {
		allocation = "fake-" + hex.EncodeToString(nonce[:16])
	}
	identity := workspaceprovider.InstanceIdentity{AllocationID: allocation, InstanceID: "fake-" + hex.EncodeToString(nonce[16:]), RequestRevision: request.Revision}
	record := &journalRecord{Version: journalVersion, Request: request, Operation: "ensure", Observation: workspaceprovider.AllocationObservation{Sequence: request.Sequence, Key: request.Key, Identity: identity, State: workspaceprovider.AllocationPending}}
	if request.Runtime != nil {
		namespace := request.Runtime.Template.Namespace
		if namespace == "" {
			namespace = request.Key.Namespace
		}
		record.Pod = &workspaceprovider.PodReference{Namespace: namespace, Name: identity.InstanceID}
	}
	return record, nil
}
func validateNext(previous *journalRecord, next workspaceprovider.WorkloadRequest) error {
	if previous.Observation.State == workspaceprovider.AllocationDeleted || previous.Operation == "delete" {
		return workspaceprovider.ErrRequestConflict
	}
	return workspaceprovider.ValidateWorkloadTransition(&previous.Request, next, &previous.Observation, previous.Observation.RetainedData != nil)
}

func (d *Lifecycle) EnsureAllocation(ctx context.Context, request workspaceprovider.WorkloadRequest) (workspaceprovider.AllocationObservation, error) {
	if err := request.Validate(); err != nil {
		return workspaceprovider.AllocationObservation{}, err
	}
	if err := validatePodRequest(request); err != nil {
		return workspaceprovider.AllocationObservation{}, err
	}
	var observed workspaceprovider.AllocationObservation
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		cm, record, err := d.read(ctx, request.Key)
		if err != nil && !errors.Is(err, workspaceprovider.ErrNotFound) {
			return err
		}
		if record != nil && request.Sequence < record.Request.Sequence {
			_, retired, err := d.readAt(ctx, request.Key, historyKey(request.Key, request.Sequence))
			if err != nil {
				return err
			}
			if retired.Request.Revision != request.Revision {
				return workspaceprovider.ErrRequestConflict
			}
			observed = retired.Observation
			return nil
		}
		if record != nil && request.Sequence == record.Request.Sequence {
			if record.Request.Revision != request.Revision {
				return workspaceprovider.ErrRequestConflict
			}
			observed = record.Observation
			if record.Operation != "ensure" || observed.State == workspaceprovider.AllocationStopped || observed.State == workspaceprovider.AllocationDeleted {
				return nil
			}
		}
		workspace, err := d.admitted(ctx, request)
		if err != nil {
			return err
		}
		if err := d.verifyRuntimeServiceAccount(ctx, request); err != nil {
			observed = workspaceprovider.AllocationObservation{}
			return err
		}
		if request.Runtime != nil {
			namespace := request.Runtime.Template.Namespace
			if namespace == "" {
				namespace = request.Key.Namespace
			}
			if err := d.verifyNetworkPolicies(ctx, request.Runtime, namespace, request.Runtime.Template.Labels); err != nil {
				observed = workspaceprovider.AllocationObservation{}
				return err
			}
		}
		if record == nil {
			// New journals carry protection atomically at creation.
			if request.Sequence != 1 {
				return workspaceprovider.ErrRequestConflict
			}
			record, err = newRecord(request, "")
			if err != nil {
				return err
			}
			data, err := encode(record)
			if err != nil {
				return err
			}
			key := journalKey(request.Key)
			cm = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, Labels: map[string]string{journalLabel: "v2"}, Finalizers: []string{journalProtectionFinalizer}, OwnerReferences: []metav1.OwnerReference{{APIVersion: workspacev1alpha1.GroupVersion.String(), Kind: "ExecutionWorkspace", Name: workspace.Name, UID: workspace.UID}}}, Data: map[string]string{journalDataKey: data}}
			if err := d.client.Create(ctx, cm); err != nil {
				if apierrors.IsAlreadyExists(err) {
					return apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, err)
				}
				return err
			}
		} else if request.Sequence != record.Request.Sequence {
			if err := validateNext(record, request); err != nil {
				return err
			}
			next, err := newRecord(request, record.Observation.Identity.AllocationID)
			if err != nil {
				return err
			}
			if _, err := encode(next); err != nil {
				return err
			}
			if err := d.archive(ctx, cm, record); err != nil {
				return err
			}
			if err := d.save(ctx, cm, next); err != nil {
				return err
			}
			record = next
		}
		if err := d.protectJournal(ctx, cm); err != nil {
			return err
		}
		if err := d.guardJournal(ctx, request.Key, record); err != nil {
			return err
		}
		if request.Runtime != nil {
			observed, err = d.ensurePod(ctx, cm, record)
			return err
		}
		record.Observation.State = workspaceprovider.AllocationReady
		record.Observation.Startup = &workspaceprovider.StartupEvidence{ContractVersion: workspaceprovider.LifecycleContractV1, Identity: record.Observation.Identity, Endpoint: "http://" + record.Observation.Identity.InstanceID + ".invalid"}
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
		observed = record.Observation
		return nil
	})
	return observed, err
}
func (d *Lifecycle) Observe(ctx context.Context, key workspaceprovider.AllocationKey) (workspaceprovider.AllocationObservation, error) {
	_, record, err := d.read(ctx, key)
	if err != nil {
		return workspaceprovider.AllocationObservation{}, err
	}
	if record.Observation.State == workspaceprovider.AllocationDeleted {
		if err := validatePodStorage(record.Request); err != nil {
			return workspaceprovider.AllocationObservation{}, err
		}
	}
	if record.Request.Runtime != nil && record.Operation == "ensure" {
		return d.observePod(ctx, record)
	}
	return record.Observation, nil
}
func (d *Lifecycle) StopInstance(ctx context.Context, key workspaceprovider.AllocationKey, identity workspaceprovider.InstanceIdentity) (workspaceprovider.AllocationObservation, error) {
	return d.transition(ctx, key, identity, "stop", nil)
}
func (d *Lifecycle) SuspendInstance(ctx context.Context, key workspaceprovider.AllocationKey, identity workspaceprovider.InstanceIdentity) (workspaceprovider.AllocationObservation, error) {
	return d.transition(ctx, key, identity, "suspend", nil)
}
func (d *Lifecycle) DeleteAllocation(ctx context.Context, key workspaceprovider.AllocationKey, identity workspaceprovider.InstanceIdentity, policy workspacev1alpha1.ExecutionWorkspaceDeletionPolicy) (workspaceprovider.AllocationObservation, error) {
	if err := workspaceprovider.ValidateDeletedDisposition(fakeDeletedDisposition(policy), policy); err != nil {
		return workspaceprovider.AllocationObservation{}, err
	}
	return d.transition(ctx, key, identity, "delete", &policy)
}
func (d *Lifecycle) transition(ctx context.Context, key workspaceprovider.AllocationKey, identity workspaceprovider.InstanceIdentity, operation string, policy *workspacev1alpha1.ExecutionWorkspaceDeletionPolicy) (workspaceprovider.AllocationObservation, error) {
	var observed workspaceprovider.AllocationObservation
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		cm, record, err := d.read(ctx, key)
		if err != nil {
			return err
		}
		if record.Observation.Identity != identity {
			return workspaceprovider.ErrStaleIdentity
		}
		// Existing persistent-storage Pods can still be stopped by exact UID, but fake
		// cannot confirm storage deletion or replay a legacy deletion claim.
		if operation == "delete" || record.Observation.State == workspaceprovider.AllocationDeleted {
			if err := validatePodStorage(record.Request); err != nil {
				return err
			}
		}
		if operation == "suspend" && record.Request.Runtime != nil {
			return fmt.Errorf("fake Pod workloads do not implement data suspension")
		}
		if policy != nil {
			if record.DeletionPolicy != nil && *record.DeletionPolicy != *policy {
				return workspaceprovider.ErrRequestConflict
			}
			if record.Observation.State != workspaceprovider.AllocationStopped && record.Observation.State != workspaceprovider.AllocationDeleted {
				return workspaceprovider.ErrInstanceRunning
			}
		}
		observed = record.Observation
		if observed.State == workspaceprovider.AllocationDeleted {
			return nil
		}
		if operation == "stop" && observed.State == workspaceprovider.AllocationStopped {
			return nil
		}
		if operation == "suspend" && observed.State == workspaceprovider.AllocationStopped {
			if observed.RetainedData != nil {
				return nil
			}
			return workspaceprovider.ErrRequestConflict
		}
		if record.Operation == "delete" && operation != "delete" {
			return workspaceprovider.ErrRequestConflict
		}
		if record.Pod != nil && record.Pod.UID == "" && record.CreateIssued {
			pod, err := d.pod(ctx, record)
			if err != nil && !apierrors.IsNotFound(err) {
				return err
			}
			if err == nil {
				record.Pod.UID = pod.UID
			}
		}
		record.Operation = operation
		record.DeletionPolicy = policy
		if operation != "delete" {
			record.Observation.State = workspaceprovider.AllocationPending
		}
		record.Observation.Startup = nil
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
		if record.Request.Runtime != nil && operation != "delete" {
			stopped, err := d.stopPod(ctx, record)
			if err != nil {
				return err
			}
			if !stopped {
				observed = record.Observation
				return nil
			}
		}
		record.Observation.State = workspaceprovider.AllocationStopped
		if operation == "suspend" {
			body, _ := json.Marshal(struct {
				Key      workspaceprovider.AllocationKey
				Sequence int64
				Identity workspaceprovider.InstanceIdentity
			}{key, record.Request.Sequence, identity})
			sum := sha256.Sum256(body)
			record.Observation.RetainedData = &workspaceprovider.RetainedDataReference{ID: identity.AllocationID, SourceInstance: identity, ProofSHA256: "sha256:" + hex.EncodeToString(sum[:])}
		}
		if operation == "delete" {
			record.Observation.State = workspaceprovider.AllocationDeleted
			record.Observation.RetainedData = nil
			record.Observation.Disposition = fakeDeletedDisposition(*policy)
		}
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
		observed = record.Observation
		return nil
	})
	return observed, err
}
