// Copyright (c) 2026. MIT License - see LICENSE file for details.

package sandbox

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const journalProtectionFinalizer = ControllerName + "/journal-protection"
const journalReleasedAnnotation = ControllerName + "/journal-released"
const coreWorkspaceFinalizer = "workspace.orka.ai/finalizer"

// Owner finalizers do not protect dependents from foreground garbage collection.
// Protect each recovery record before effects; deletionTimestamp still permits
// journal CAS updates while exact physical cleanup completes.
func (d *Lifecycle) protectJournal(ctx context.Context, cm *corev1.ConfigMap) error {
	if slices.Contains(cm.Finalizers, journalProtectionFinalizer) {
		return nil
	}
	if _, _, err := protectedJournalRecord(cm); err != nil {
		return err
	}
	if cm.Annotations[journalReleasedAnnotation] != "" {
		return fmt.Errorf("released allocation journal cannot authorize new effects")
	}
	if !cm.DeletionTimestamp.IsZero() {
		return fmt.Errorf("unprotected allocation journal is already deleting; recovery requires operator intervention")
	}
	cm.Finalizers = append(cm.Finalizers, journalProtectionFinalizer)
	return d.client.Update(ctx, cm)
}

// JournalProtectionReconciler retains recovery fences through Core finalization.
// Owner references remain nonblocking, so this protection cannot prevent Core
// from removing its finalizer. The timed retry also handles absent owners and
// namespace deletion without relying on a Workspace event still being available.
type JournalProtectionReconciler struct{ client.Client }

func (r *JournalProtectionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, req.NamespacedName, cm); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	protected := slices.Contains(cm.Finalizers, journalProtectionFinalizer)
	if !isProtectedJournal(cm) {
		return ctrl.Result{}, nil
	}
	record, historical, err := protectedJournalRecord(cm)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !protected && cm.Annotations[journalReleasedAnnotation] == journalReleaseProof(cm) {
		return ctrl.Result{}, nil
	}
	if !protected {
		// Backfill valid legacy journals outside the read-only Observe path.
		return ctrl.Result{RequeueAfter: providerHeartbeatPeriod}, (&Lifecycle{client: r.Client}).protectJournal(ctx, cm)
	}
	key := record.Request.Key
	workspace := &api.ExecutionWorkspace{}
	err = r.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: key.Name}, workspace)
	if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	if err == nil && workspace.UID == key.WorkspaceUID {
		if workspace.Spec.ProviderBinding.UID != key.ProviderUID {
			return ctrl.Result{}, sdk.ErrStaleIdentity
		}
		if workspace.DeletionTimestamp.IsZero() || slices.Contains(workspace.Finalizers, coreWorkspaceFinalizer) {
			return ctrl.Result{RequeueAfter: providerHeartbeatPeriod}, nil
		}
		if workspace.Status.ObservedGeneration != workspace.Generation || workspace.Status.State != api.ExecutionWorkspaceStateDeleted ||
			sdk.ValidateDeletedDisposition(workspace.Status.Disposition, workspace.Spec.Lifecycle.DeletionPolicy) != nil {
			return ctrl.Result{}, fmt.Errorf("workspace finalization has no current provider cleanup evidence")
		}
	}
	// A historical fence cannot certify cleanup of later compute or data.
	// The current journal releases all histories before itself, so a legitimate
	// current-journal collection cannot strand a protected historical sibling.
	if historical {
		if record.Observation.State != sdk.AllocationStopped || record.Observation.Startup != nil {
			return ctrl.Result{}, fmt.Errorf("historical allocation journal has no exact termination evidence")
		}
		current := &corev1.ConfigMap{}
		if err := r.Get(ctx, journalKey(key), current); err != nil {
			return ctrl.Result{}, fmt.Errorf("historical journal's current cleanup proof is unavailable: %w", err)
		}
		currentRecord, currentHistorical, err := protectedJournalRecord(current)
		if err != nil || currentHistorical || currentRecord.Request.Key != key || !journalCleanupComplete(currentRecord) {
			return ctrl.Result{}, fmt.Errorf("historical journal's allocation still requires physical cleanup")
		}
	} else {
		if !journalCleanupComplete(record) {
			return ctrl.Result{}, fmt.Errorf("orphan allocation journal still requires physical cleanup")
		}
		if err := r.releaseHistoricalJournals(ctx, record); err != nil {
			return ctrl.Result{}, err
		}
	}
	releaseJournalProtection(cm)
	return ctrl.Result{}, r.Update(ctx, cm) // resourceVersion CAS preserves other finalizers.
}

func journalCleanupComplete(record *journalRecord) bool {
	return record.Observation.State == sdk.AllocationDeleted && record.DeletionPolicy != nil &&
		sdk.ValidateDeletedDisposition(record.Observation.Disposition, *record.DeletionPolicy) == nil
}

func (r *JournalProtectionReconciler) releaseHistoricalJournals(ctx context.Context, current *journalRecord) error {
	key := current.Request.Key
	objects := &corev1.ConfigMapList{}
	if err := r.List(ctx, objects, client.InNamespace(key.Namespace), client.MatchingLabels{ownershipLabel: string(key.WorkspaceUID), providerLabel: string(key.ProviderUID)}); err != nil {
		return err
	}
	owner := metav1.OwnerReference{APIVersion: api.GroupVersion.String(), Kind: "ExecutionWorkspace", Name: key.Name, UID: key.WorkspaceUID}
	for i := range objects.Items {
		cm := &objects.Items[i]
		if client.ObjectKeyFromObject(cm) == journalKey(key) || !isProtectedJournal(cm) ||
			!reflect.DeepEqual(cm.OwnerReferences, []metav1.OwnerReference{owner}) {
			continue
		}
		record, historical, err := protectedJournalRecord(cm)
		if err != nil || !historical || record.Request.Key != key || record.Observation.State != sdk.AllocationStopped || record.Observation.Startup != nil {
			return fmt.Errorf("historical journal has no exact terminated-incarnation proof")
		}
		releaseJournalProtection(cm)
		if err := r.Update(ctx, cm); err != nil {
			return err
		}
	}
	return nil
}

func journalReleaseProof(cm *corev1.ConfigMap) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(cm.Data[journalDataKey])))
}

func releaseJournalProtection(cm *corev1.ConfigMap) {
	// Bind the release acknowledgement to these exact frozen journal bytes.
	// Late watch events can distinguish accepted cleanup from an unprotected
	// legacy journal, including records held by another controller's finalizer.
	if cm.Annotations == nil {
		cm.Annotations = map[string]string{}
	}
	cm.Annotations[journalReleasedAnnotation] = journalReleaseProof(cm)
	cm.Finalizers = slices.DeleteFunc(cm.Finalizers, func(value string) bool { return value == journalProtectionFinalizer })
}

func protectedJournalRecord(cm *corev1.ConfigMap) (*journalRecord, bool, error) {
	raw := cm.Data[journalDataKey]
	if len(raw) == 0 || len(raw) > MaxJournalBytes {
		return nil, false, fmt.Errorf("protected allocation journal size is invalid")
	}
	record := &journalRecord{}
	if err := json.Unmarshal([]byte(raw), record); err != nil {
		return nil, false, fmt.Errorf("protected allocation journal is unreadable")
	}
	key := record.Request.Key
	owner := metav1.OwnerReference{APIVersion: api.GroupVersion.String(), Kind: "ExecutionWorkspace", Name: key.Name, UID: key.WorkspaceUID}
	historical := client.ObjectKeyFromObject(cm) == historyKey(key, record.Request.Sequence)
	if record.Version != journalVersion || record.Request.Validate() != nil || cm.Namespace != key.Namespace ||
		(!historical && client.ObjectKeyFromObject(cm) != journalKey(key)) ||
		record.Observation.Key != key || record.Observation.Sequence != record.Request.Sequence ||
		!record.Observation.Identity.Valid() || record.Observation.Identity.RequestRevision != record.Request.Revision ||
		!reflect.DeepEqual(cm.Labels, labels(record)) || !reflect.DeepEqual(cm.OwnerReferences, []metav1.OwnerReference{owner}) {
		return nil, false, sdk.ErrStaleIdentity
	}
	if err := validateJournalRecord(record); err != nil {
		return nil, false, err
	}
	return record, historical, nil
}

func (r *JournalProtectionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&corev1.ConfigMap{}, builder.WithPredicates(predicate.NewPredicateFuncs(func(object client.Object) bool {
		cm, ok := object.(*corev1.ConfigMap)
		return ok && isProtectedJournal(cm)
	}))).
		Named("sandbox-workspace-journal-protection").Complete(r)
}

func isProtectedJournal(cm *corev1.ConfigMap) bool {
	return slices.Contains(cm.Finalizers, journalProtectionFinalizer) || cm.Labels[ownershipLabel] != "" && cm.Labels[providerLabel] != "" &&
		(strings.HasPrefix(cm.Name, "sandbox-workspace-") || strings.HasPrefix(cm.Name, "sandbox-history-"))
}
