package sandbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const retentionDataKey = "retention.json"
const retentionVersion = "sandbox.workspace.retention.v1"
const retentionClassLabel = "sandbox.workspace.orka.ai/class-uid"

type retentionReservation struct {
	LedgerUID types.UID `json:"ledgerUID"`
	Sequence  int64     `json:"sequence"`
}

type retentionOccupant struct {
	JournalName  string    `json:"journalName"`
	JournalUID   types.UID `json:"journalUID"`
	AllocationID string    `json:"allocationID"`
	Sequence     int64     `json:"sequence"`
}

type retentionLedger struct {
	Version      string                       `json:"version"`
	Limit        int32                        `json:"limit"`
	Reservations map[string]retentionOccupant `json:"reservations"`
}

// The cap is per class UID and workspace namespace, even when several classes
// share a profile or their runtime Pods live in another namespace. CAS reserves
// a slot before suspension intent and its native effects. Uncertain outcomes
// keep that slot; only observed running compute or completed cleanup releases it.
func retentionKey(record *journalRecord) types.NamespacedName {
	sum := sha256.Sum256([]byte(string(record.Request.Key.ProviderUID) + "\x00" + string(record.Request.Runtime.ClassBinding.UID)))
	return types.NamespacedName{Namespace: record.Request.Key.Namespace, Name: "sandbox-retention-" + hex.EncodeToString(sum[:20])}
}

func retentionMetadata(record *journalRecord) metav1.ObjectMeta {
	key := retentionKey(record)
	class := record.Request.Runtime.ClassBinding
	return metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name,
		Labels:          map[string]string{providerLabel: string(record.Request.Key.ProviderUID), retentionClassLabel: string(class.UID)},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: workspacev1alpha1.GroupVersion.String(), Kind: "ExecutionWorkspaceClass", Name: class.Name, UID: class.UID}}}
}

func decodeRetention(cm *corev1.ConfigMap, record *journalRecord) (*retentionLedger, error) {
	expected := retentionMetadata(record)
	if cm.UID == "" || cm.DeletionTimestamp != nil || !reflect.DeepEqual(cm.Labels, expected.Labels) || !reflect.DeepEqual(cm.OwnerReferences, expected.OwnerReferences) || len(cm.Data) != 1 || len(cm.BinaryData) != 0 {
		return nil, workspaceprovider.ErrStaleIdentity
	}
	raw := cm.Data[retentionDataKey]
	if len(raw) == 0 || len(raw) > MaxJournalBytes {
		return nil, fmt.Errorf("invalid suspended occupancy journal size")
	}
	var ledger retentionLedger
	if err := json.Unmarshal([]byte(raw), &ledger); err != nil {
		return nil, fmt.Errorf("decode suspended occupancy journal: %w", err)
	}
	if ledger.Version != retentionVersion || (record.MaxSuspended != nil && ledger.Limit != *record.MaxSuspended) || ledger.Limit < 0 || ledger.Reservations == nil || len(ledger.Reservations) > int(ledger.Limit) {
		return nil, workspaceprovider.ErrStaleIdentity
	}
	for uid, occupant := range ledger.Reservations {
		if uid == "" || occupant.JournalName == "" || occupant.JournalUID == "" || occupant.AllocationID == "" || occupant.Sequence <= 0 {
			return nil, workspaceprovider.ErrStaleIdentity
		}
	}
	return &ledger, nil
}

func setRetention(cm *corev1.ConfigMap, ledger *retentionLedger) error {
	data, err := json.Marshal(ledger)
	if err != nil {
		return err
	}
	if len(data) > MaxJournalBytes {
		return fmt.Errorf("suspended occupancy journal exceeds %d bytes", MaxJournalBytes)
	}
	cm.Data = map[string]string{retentionDataKey: string(data)}
	return nil
}

func occupantFor(cm *corev1.ConfigMap, record *journalRecord, sequence int64) retentionOccupant {
	return retentionOccupant{JournalName: cm.Name, JournalUID: cm.UID, AllocationID: record.Observation.Identity.AllocationID, Sequence: sequence}
}

// A lost ledger or an older, unreserved suspended journal cannot reset the
// class count. Check current journals, excluding immutable sequence history.
func (d *Lifecycle) verifyRetentionScope(ctx context.Context, record *journalRecord, ledgerCM *corev1.ConfigMap, ledger *retentionLedger) error {
	var journals corev1.ConfigMapList
	if err := d.client.List(ctx, &journals, client.InNamespace(record.Request.Key.Namespace), client.MatchingLabels{providerLabel: string(record.Request.Key.ProviderUID)}); err != nil {
		return err
	}
	for i := range journals.Items {
		object := &journals.Items[i]
		if !strings.HasPrefix(object.Name, "sandbox-workspace-") {
			continue
		}
		var existing journalRecord
		if err := json.Unmarshal([]byte(object.Data[journalDataKey]), &existing); err != nil {
			return fmt.Errorf("cannot account for Sandbox journal %s: %w", object.Name, err)
		}
		if existing.Request.Runtime == nil || existing.Request.Runtime.ClassBinding.UID != record.Request.Runtime.ClassBinding.UID {
			continue
		}
		if _, _, err := d.readAt(ctx, existing.Request.Key, client.ObjectKeyFromObject(object)); err != nil {
			return err
		}
		if object.Name != journalKey(existing.Request.Key).Name {
			return workspaceprovider.ErrStaleIdentity
		}
		if existing.Observation.State == workspaceprovider.AllocationDeleted || existing.Observation.State == workspaceprovider.AllocationReady {
			continue
		}
		if existing.Operation != "suspend" && existing.RetentionReservation == nil {
			continue
		}
		reservation := existing.RetentionReservation
		if ledgerCM == nil || reservation == nil || reservation.LedgerUID != ledgerCM.UID || ledger.Reservations[string(existing.Request.Key.WorkspaceUID)] != occupantFor(object, &existing, reservation.Sequence) {
			return fmt.Errorf("suspended occupancy journal is missing or incomplete while retained lifecycle evidence remains")
		}
	}
	return nil
}

func (d *Lifecycle) verifySuspendedReservation(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	if record.MaxSuspended == nil || record.Volume == nil {
		return nil
	}
	reservation := record.RetentionReservation
	if reservation == nil || reservation.Sequence <= 0 || reservation.Sequence > record.Request.Sequence || (record.Operation == "suspend" && reservation.Sequence != record.Request.Sequence) {
		return fmt.Errorf("suspended workspace has no exact reserved occupancy identity")
	}
	ledgerCM := &corev1.ConfigMap{}
	if err := d.client.Get(ctx, retentionKey(record), ledgerCM); err != nil {
		return err
	}
	ledger, err := decodeRetention(ledgerCM, record)
	if err != nil {
		return err
	}
	if reservation.LedgerUID != ledgerCM.UID || ledger.Reservations[string(record.Request.Key.WorkspaceUID)] != occupantFor(cm, record, reservation.Sequence) {
		return workspaceprovider.ErrStaleIdentity
	}
	return nil
}

func (d *Lifecycle) reserveSuspended(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	if record.MaxSuspended == nil {
		return nil
	}
	if *record.MaxSuspended <= 0 {
		return fmt.Errorf("Sandbox class suspended workspace capacity is full")
	}
	if cm.UID == "" {
		return workspaceprovider.ErrStaleIdentity
	}
	if record.Operation == "suspend" {
		return d.verifySuspendedReservation(ctx, cm, record)
	}
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		key := retentionKey(record)
		ledgerCM := &corev1.ConfigMap{}
		err := d.client.Get(ctx, key, ledgerCM)
		if apierrors.IsNotFound(err) {
			if err := d.verifyRetentionScope(ctx, record, nil, nil); err != nil {
				return err
			}
			ledgerCM = &corev1.ConfigMap{ObjectMeta: retentionMetadata(record)}
			if err := setRetention(ledgerCM, &retentionLedger{Version: retentionVersion, Limit: *record.MaxSuspended, Reservations: map[string]retentionOccupant{}}); err != nil {
				return err
			}
			if err := d.client.Create(ctx, ledgerCM); err != nil {
				if apierrors.IsAlreadyExists(err) {
					return apierrors.NewConflict(corev1.Resource("configmaps"), key.Name, err)
				}
				return err
			}
		} else if err != nil {
			return err
		}
		ledger, err := decodeRetention(ledgerCM, record)
		if err != nil {
			return err
		}
		if err := d.verifyRetentionScope(ctx, record, ledgerCM, ledger); err != nil {
			return err
		}
		owner := string(record.Request.Key.WorkspaceUID)
		expected := occupantFor(cm, record, record.Request.Sequence)
		current, exists := ledger.Reservations[owner]
		if exists {
			if current.JournalName != expected.JournalName || current.JournalUID != expected.JournalUID || current.AllocationID != expected.AllocationID || current.Sequence > expected.Sequence {
				return workspaceprovider.ErrStaleIdentity
			}
		} else if len(ledger.Reservations) >= int(ledger.Limit) {
			return fmt.Errorf("Sandbox class suspended workspace capacity is full")
		}
		ledger.Reservations[owner] = expected
		if err := setRetention(ledgerCM, ledger); err != nil {
			return err
		}
		if err := d.client.Update(ctx, ledgerCM); err != nil {
			return err
		}
		record.RetentionReservation = &retentionReservation{LedgerUID: ledgerCM.UID, Sequence: record.Request.Sequence}
		return nil
	})
}

// Sequence fencing keeps a delayed resume release from freeing a newer
// suspension's slot. It is safe to retry release after its response was lost.
func (d *Lifecycle) releaseSuspended(ctx context.Context, cm *corev1.ConfigMap, record *journalRecord) error {
	reservation := record.RetentionReservation
	if reservation == nil {
		if record.Observation.State != workspaceprovider.AllocationDeleted {
			return nil
		}
		// A reservation response or subsequent intent CAS can be lost before
		// its reference reaches the workspace journal. Proven deletion can
		// release that exact journal/allocation/sequence entry without adopting
		// storage or inferring a reservation for an older suspended journal.
		reservation = &retentionReservation{Sequence: record.Request.Sequence}
	}
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		ledgerCM := &corev1.ConfigMap{}
		if err := d.client.Get(ctx, retentionKey(record), ledgerCM); err != nil {
			return client.IgnoreNotFound(err)
		}
		ledger, err := decodeRetention(ledgerCM, record)
		if err != nil {
			return err
		}
		if reservation.LedgerUID != "" && ledgerCM.UID != reservation.LedgerUID {
			return workspaceprovider.ErrStaleIdentity
		}
		owner := string(record.Request.Key.WorkspaceUID)
		current, exists := ledger.Reservations[owner]
		if !exists {
			return nil
		}
		expected := occupantFor(cm, record, reservation.Sequence)
		if current != expected {
			if current.JournalName == expected.JournalName && current.JournalUID == expected.JournalUID && current.AllocationID == expected.AllocationID && current.Sequence > expected.Sequence {
				return nil
			}
			return workspaceprovider.ErrStaleIdentity
		}
		delete(ledger.Reservations, owner)
		if err := setRetention(ledgerCM, ledger); err != nil {
			return err
		}
		return d.client.Update(ctx, ledgerCM)
	})
	if err != nil {
		return err
	}
	record.RetentionReservation = nil
	return d.save(ctx, cm, record)
}
