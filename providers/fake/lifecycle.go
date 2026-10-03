// Copyright (c) 2026. MIT License - see LICENSE file for details.

// Package fake implements a persistent status fixture. It does not start a
// runtime, listen on its synthetic endpoint, or implement ACP.
package fake

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

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
	journalVersion = "fake.workspace.journal.v1"
	journalLabel   = "fake.workspace.orka.ai/journal"
	journalDataKey = "record.json"
	// MaxJournalBytes bounds the entire serialized intent and progress record.
	MaxJournalBytes = 256 * 1024
)

type journalRecord struct {
	Version        string                                              `json:"version"`
	Request        workspaceprovider.WorkloadRequest                   `json:"request"`
	Observation    workspaceprovider.AllocationObservation             `json:"observation"`
	Operation      string                                              `json:"operation"`
	DeletionPolicy *workspacev1alpha1.ExecutionWorkspaceDeletionPolicy `json:"deletionPolicy,omitempty"`
}

// Lifecycle stores non-secret fixture intent and progress in Kubernetes
// ConfigMaps. Every update uses resourceVersion CAS. Use an uncached client so
// retries and new controller processes read the committed journal immediately.
type Lifecycle struct{ client client.Client }

var _ workspaceprovider.Lifecycle = (*Lifecycle)(nil)

func New(c client.Client) *Lifecycle { return &Lifecycle{client: c} }

func journalKey(key workspaceprovider.AllocationKey) types.NamespacedName {
	// A name is not authority: the complete UID-bearing key is checked on read.
	// Including workspace UID lets a replacement coexist with an old tombstone.
	sum := sha256.Sum256([]byte(key.Namespace + "\x00" + key.Name + "\x00" + string(key.WorkspaceUID)))
	return types.NamespacedName{Namespace: key.Namespace, Name: "fake-workspace-" + hex.EncodeToString(sum[:20])}
}

func (d *Lifecycle) read(ctx context.Context, key workspaceprovider.AllocationKey) (*corev1.ConfigMap, *journalRecord, error) {
	if err := key.Validate(); err != nil {
		return nil, nil, err
	}
	cm := &corev1.ConfigMap{}
	if err := d.client.Get(ctx, journalKey(key), cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, workspaceprovider.ErrNotFound
		}
		return nil, nil, err
	}
	raw := cm.Data[journalDataKey]
	if len(raw) == 0 || len(raw) > MaxJournalBytes {
		return nil, nil, fmt.Errorf("invalid fixture journal size")
	}
	var record journalRecord
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return nil, nil, fmt.Errorf("decode fixture journal: %w", err)
	}
	if record.Version != journalVersion {
		return nil, nil, fmt.Errorf("unsupported fixture journal version %q", record.Version)
	}
	if record.Request.Key != key || record.Observation.Key != key {
		return nil, nil, workspaceprovider.ErrStaleIdentity
	}
	if err := record.Request.Validate(); err != nil {
		return nil, nil, fmt.Errorf("invalid fixture journal request: %w", err)
	}
	if record.Observation.Identity.AllocationID == "" || record.Observation.Identity.InstanceID == "" || record.Observation.Identity.RequestRevision != record.Request.Revision {
		return nil, nil, workspaceprovider.ErrStaleIdentity
	}
	return cm, &record, nil
}

func encode(record *journalRecord) (string, error) {
	data, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	if len(data) > MaxJournalBytes {
		return "", fmt.Errorf("fixture journal exceeds %d bytes", MaxJournalBytes)
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

func (d *Lifecycle) admitted(ctx context.Context, key workspaceprovider.AllocationKey) (*workspacev1alpha1.ExecutionWorkspace, error) {
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

func (d *Lifecycle) EnsureAllocation(ctx context.Context, request workspaceprovider.WorkloadRequest) (workspaceprovider.AllocationObservation, error) {
	if request.Runtime != nil {
		return workspaceprovider.AllocationObservation{}, fmt.Errorf("fake provider is a status fixture and cannot materialize a runtime workload")
	}
	if err := request.Validate(); err != nil {
		return workspaceprovider.AllocationObservation{}, err
	}
	var observed workspaceprovider.AllocationObservation
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		cm, record, err := d.read(ctx, request.Key)
		if err != nil && err != workspaceprovider.ErrNotFound {
			return err
		}
		if record != nil {
			if record.Request.Revision != request.Revision {
				return workspaceprovider.ErrRequestConflict
			}
			observed = record.Observation
			if record.Operation != "ensure" || observed.State == workspaceprovider.AllocationStopped || observed.State == workspaceprovider.AllocationDeleted {
				return nil
			}
		}
		workspace, err := d.admitted(ctx, request.Key)
		if err != nil {
			return err
		}
		if record == nil {
			var nonce [32]byte
			if _, err := rand.Read(nonce[:]); err != nil {
				return err
			}
			identity := workspaceprovider.InstanceIdentity{AllocationID: "fake-" + hex.EncodeToString(nonce[:16]), InstanceID: "fixture-" + hex.EncodeToString(nonce[16:]), RequestRevision: request.Revision}
			record = &journalRecord{Version: journalVersion, Request: request, Operation: "ensure", Observation: workspaceprovider.AllocationObservation{Key: request.Key, Identity: identity, State: workspaceprovider.AllocationPending}}
			data, err := encode(record)
			if err != nil {
				return err
			}
			key := journalKey(request.Key)
			cm = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, Labels: map[string]string{journalLabel: "v1"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: workspacev1alpha1.GroupVersion.String(), Kind: "ExecutionWorkspace", Name: workspace.Name, UID: workspace.UID}}}, Data: map[string]string{journalDataKey: data}}
			if err := d.client.Create(ctx, cm); err != nil {
				if apierrors.IsAlreadyExists(err) {
					return apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, err)
				}
				return err
			}
		}
		// The fixture's backend effect is just its state transition. Intent and IDs
		// were committed first, so retries after either lost response reuse them.
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
	return record.Observation, nil
}

func (d *Lifecycle) StopInstance(ctx context.Context, key workspaceprovider.AllocationKey, identity workspaceprovider.InstanceIdentity) (workspaceprovider.AllocationObservation, error) {
	return d.transition(ctx, key, identity, nil)
}

func (d *Lifecycle) DeleteAllocation(ctx context.Context, key workspaceprovider.AllocationKey, identity workspaceprovider.InstanceIdentity, policy workspacev1alpha1.ExecutionWorkspaceDeletionPolicy) (workspaceprovider.AllocationObservation, error) {
	if err := workspaceprovider.ValidateDeletedDisposition(fakeDeletedDisposition(policy), policy); err != nil {
		return workspaceprovider.AllocationObservation{}, err
	}
	return d.transition(ctx, key, identity, &policy)
}

func (d *Lifecycle) transition(ctx context.Context, key workspaceprovider.AllocationKey, identity workspaceprovider.InstanceIdentity, policy *workspacev1alpha1.ExecutionWorkspaceDeletionPolicy) (workspaceprovider.AllocationObservation, error) {
	var observed workspaceprovider.AllocationObservation
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		cm, record, err := d.read(ctx, key)
		if err != nil {
			return err
		}
		if record.Observation.Identity != identity {
			return workspaceprovider.ErrStaleIdentity
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
		if observed.State == workspaceprovider.AllocationDeleted || (policy == nil && observed.State == workspaceprovider.AllocationStopped) {
			return nil
		}
		if policy == nil {
			record.Operation = "stop"
			record.Observation.State = workspaceprovider.AllocationPending
		} else {
			record.Operation = "delete"
			record.DeletionPolicy = policy
		}
		// Withdraw readiness in the same CAS as retirement intent. An interrupted
		// operation must never leave startup evidence usable after a restart.
		record.Observation.Startup = nil
		// Persist the requested exact fence and deletion policy before effects.
		if err := d.save(ctx, cm, record); err != nil {
			return err
		}
		if policy == nil {
			record.Observation.State = workspaceprovider.AllocationStopped
		} else {
			record.Observation.State = workspaceprovider.AllocationDeleted
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
