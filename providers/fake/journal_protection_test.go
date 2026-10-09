// Copyright (c) 2026. MIT License - see LICENSE file for details.

package fake

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func journalProtectionMap(t *testing.T, f conditionContractFixture, name client.ObjectKey) *corev1.ConfigMap {
	t.Helper()
	cm := &corev1.ConfigMap{}
	if err := f.client.Get(t.Context(), name, cm); err != nil {
		t.Fatal(err)
	}
	return cm
}

func reconcileJournalProtection(t *testing.T, f conditionContractFixture, name client.ObjectKey) (ctrl.Result, error) {
	t.Helper()
	return (&JournalProtectionReconciler{Client: f.client}).Reconcile(t.Context(), ctrl.Request{NamespacedName: name})
}

func TestJournalProtectionKeepsRecoveryFenceThroughCoreFinalization(t *testing.T) {
	f := newConditionContractFixture(t)
	reconcileConditionContract(t, f)
	workspace := conditionContractWorkspace(t, f)
	identity := workspace.Status.Allocation.Identity
	name := journalKey(f.request.Key)
	cm := journalProtectionMap(t, f, name)
	if !slices.Contains(cm.Finalizers, journalProtectionFinalizer) || len(cm.OwnerReferences) != 1 ||
		(cm.OwnerReferences[0].BlockOwnerDeletion != nil && *cm.OwnerReferences[0].BlockOwnerDeletion) {
		t.Fatal("allocation did not durably protect a nonblocking exact-owner journal")
	}
	cm.Finalizers = append(cm.Finalizers, "other.example/retain")
	if err := f.client.Update(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	// Foreground GC and namespace cleanup can both DELETE the journal while
	// Core still holds the Workspace. The fake client models finalizer storage,
	// so explicitly issue the dependent DELETE that the real controllers issue.
	if err := f.client.Delete(t.Context(), workspace, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil {
		t.Fatal(err)
	}
	if err := f.client.Delete(t.Context(), cm, client.Preconditions{UID: &cm.UID}); err != nil {
		t.Fatal(err)
	}
	before := journalProtectionMap(t, f, name).DeepCopy()
	if before.DeletionTimestamp.IsZero() {
		t.Fatal("dependent deletion was not requested")
	}
	if result, err := reconcileJournalProtection(t, f, name); err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("Core finalizer did not retain the journal: %+v %v", result, err)
	}
	if !reflect.DeepEqual(before, journalProtectionMap(t, f, name)) {
		t.Fatal("waiting for Core finalization changed the recovery journal")
	}
	workspace = conditionContractWorkspace(t, f)
	workspace.Spec.DesiredState = api.ExecutionWorkspaceDesiredDeleted
	workspace.Spec.Retirement = &api.WorkloadRetirement{Sequence: f.request.Sequence, Identity: identity, Action: api.WorkloadRetirementDelete}
	workspace.Generation++
	if err := f.client.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	for range 25 {
		reconcileConditionContract(t, f)
		if sdk.ConditionIsTrue(conditionContractWorkspace(t, f).Status.Conditions, string(api.ConditionWorkspaceFinalized)) {
			break
		}
	}
	workspace = assertConditionContract(t, f, metav1.ConditionFalse, metav1.ConditionTrue)
	// Terminal tombstones must remain usable for delayed exact lifecycle retries.
	driver := f.lifecycle(f.client)
	if observation, err := driver.StopInstance(t.Context(), f.request.Key, identity); err != nil || observation.State != sdk.AllocationDeleted {
		t.Fatalf("late Stop lost its terminal tombstone: %+v %v", observation, err)
	}
	if observation, err := driver.DeleteAllocation(t.Context(), f.request.Key, identity, workspace.Spec.Lifecycle.DeletionPolicy); err != nil || observation.State != sdk.AllocationDeleted {
		t.Fatalf("late Delete lost its terminal tombstone: %+v %v", observation, err)
	}
	if result, err := reconcileJournalProtection(t, f, name); err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("provider cleanup prematurely released the Core fence: %+v %v", result, err)
	}
	workspace.Finalizers = nil // Core has accepted provider and its own cleanup.
	if err := f.client.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	if err := f.client.Get(t.Context(), client.ObjectKeyFromObject(workspace), &api.ExecutionWorkspace{}); !apierrors.IsNotFound(err) {
		t.Fatal("Core finalizer removal did not finish owner deletion")
	}
	if _, err := reconcileJournalProtection(t, f, name); err != nil {
		t.Fatal(err)
	}
	cm = journalProtectionMap(t, f, name)
	if slices.Contains(cm.Finalizers, journalProtectionFinalizer) || !reflect.DeepEqual(cm.Finalizers, []string{"other.example/retain"}) {
		t.Fatal("terminal journal release changed foreign finalizers")
	}
	before = cm.DeepCopy()
	if _, err := reconcileJournalProtection(t, f, name); err != nil || !reflect.DeepEqual(before, journalProtectionMap(t, f, name)) {
		t.Fatal("late release reconcile reprotected or changed an accepted journal")
	}
	secret := &corev1.Secret{}
	if err := f.client.Get(t.Context(), client.ObjectKeyFromObject(f.credential), secret); err != nil || !reflect.DeepEqual(secret, f.credential) {
		t.Fatal("journal protection changed Core credentials")
	}
}

func TestJournalProtectionLegacyObserveIsReadOnlyBeforeBackfill(t *testing.T) {
	f := newConditionContractFixture(t)
	reconcileConditionContract(t, f)
	name := journalKey(f.request.Key)
	cm := journalProtectionMap(t, f, name)
	cm.Finalizers = []string{"other.example/retain"}
	if err := f.client.Update(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	before := journalProtectionMap(t, f, name).DeepCopy()
	driver := f.lifecycle(f.client)
	if _, err := driver.Observe(t.Context(), f.request.Key); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, journalProtectionMap(t, f, name)) {
		t.Fatal("Observe mutated a valid unprotected legacy journal")
	}
	if _, err := reconcileJournalProtection(t, f, name); err != nil {
		t.Fatal(err)
	}
	cm = journalProtectionMap(t, f, name)
	if !slices.Contains(cm.Finalizers, journalProtectionFinalizer) || !slices.Contains(cm.Finalizers, "other.example/retain") || !reflect.DeepEqual(cm.Data, before.Data) {
		t.Fatal("legacy backfill changed identity data or foreign finalizers")
	}
	cm.Finalizers = []string{"other.example/retain"}
	if err := f.client.Update(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	identity := conditionContractWorkspace(t, f).Status.Allocation.Identity
	for range 20 {
		observation, err := driver.StopInstance(t.Context(), f.request.Key, identity)
		if err != nil {
			t.Fatal(err)
		}
		f.tick()
		if observation.State == sdk.AllocationStopped {
			break
		}
	}
	cm = journalProtectionMap(t, f, name)
	if !slices.Contains(cm.Finalizers, journalProtectionFinalizer) {
		t.Fatal("legacy Stop effected cleanup without durable journal protection")
	}
	cm.Finalizers = []string{"other.example/retain"}
	if err := f.client.Update(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	if err := f.client.Delete(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	before = journalProtectionMap(t, f, name).DeepCopy()
	if _, err := driver.Observe(t.Context(), f.request.Key); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, journalProtectionMap(t, f, name)) {
		t.Fatal("Observe mutated a deleting legacy journal")
	}
	if _, err := reconcileJournalProtection(t, f, name); err == nil {
		t.Fatal("already-deleting legacy journal claimed retrospective protection")
	}
}

type journalReleaseUnavailableClient struct {
	client.Client
	history client.ObjectKey
}

func (c *journalReleaseUnavailableClient) Update(ctx context.Context, object client.Object, options ...client.UpdateOption) error {
	if _, ok := object.(*corev1.ConfigMap); ok && client.ObjectKeyFromObject(object) == c.history {
		return errors.New("historical release unavailable")
	}
	return c.Client.Update(ctx, object, options...)
}

func TestJournalProtectionOrdersHistoricalReleaseAndRetainsIncompleteOrphans(t *testing.T) {
	for _, complete := range []bool{false, true} {
		name := "nonterminal"
		if complete {
			name = "terminal"
		}
		t.Run(name, func(t *testing.T) {
			f := newConditionContractFixture(t)
			reconcileConditionContract(t, f)
			workspace := conditionContractWorkspace(t, f)
			driver := f.lifecycle(f.client)
			identity := workspace.Status.Allocation.Identity
			stopped := false
			for range 20 {
				observation, err := driver.StopInstance(t.Context(), f.request.Key, identity)
				if err != nil {
					t.Fatal(err)
				}
				f.tick()
				if observation.State == sdk.AllocationStopped {
					stopped = true
					break
				}
			}
			if !stopped {
				t.Fatal("exact incarnation never stopped")
			}
			cm, record, err := driver.read(t.Context(), f.request.Key)
			if err != nil {
				t.Fatal(err)
			}
			// Seed the historical sibling through the same durable archive seam
			// used by a sequence transition; GC behavior uses the public reconciler.
			if err := driver.archive(t.Context(), cm, record); err != nil {
				t.Fatal(err)
			}
			history := historyKey(f.request.Key, f.request.Sequence)
			if complete {
				for range 25 {
					observation, err := driver.DeleteAllocation(t.Context(), f.request.Key, identity, workspace.Spec.Lifecycle.DeletionPolicy)
					if err != nil {
						t.Fatal(err)
					}
					f.tick()
					if observation.State == sdk.AllocationDeleted {
						break
					}
				}
			}
			for _, key := range []client.ObjectKey{journalKey(f.request.Key), history} {
				object := journalProtectionMap(t, f, key)
				object.Finalizers = append(object.Finalizers, "other.example/retain")
				if complete && key == history {
					// A legacy history might not have been backfilled before
					// terminal-current release. It still needs a release receipt.
					object.Finalizers = []string{"other.example/retain"}
				}
				if err := f.client.Update(t.Context(), object); err != nil {
					t.Fatal(err)
				}
				if err := f.client.Delete(t.Context(), object); err != nil {
					t.Fatal(err)
				}
			}
			workspace = conditionContractWorkspace(t, f)
			workspace.Finalizers = nil
			if err := f.client.Update(t.Context(), workspace); err != nil {
				t.Fatal(err)
			}
			if err := f.client.Delete(t.Context(), workspace); err != nil {
				t.Fatal(err)
			}
			if !complete {
				for _, key := range []client.ObjectKey{journalKey(f.request.Key), history} {
					if _, err := reconcileJournalProtection(t, f, key); err == nil || !slices.Contains(journalProtectionMap(t, f, key).Finalizers, journalProtectionFinalizer) {
						t.Fatal("unexpected owner loss discarded unresolved recovery evidence")
					}
				}
				return
			}
			blocked := &JournalProtectionReconciler{Client: &journalReleaseUnavailableClient{Client: f.client, history: history}}
			if _, err := blocked.Reconcile(t.Context(), ctrl.Request{NamespacedName: journalKey(f.request.Key)}); err == nil ||
				!slices.Contains(journalProtectionMap(t, f, journalKey(f.request.Key)).Finalizers, journalProtectionFinalizer) {
				t.Fatal("current cleanup proof released before a historical release succeeded")
			}
			if _, err := reconcileJournalProtection(t, f, journalKey(f.request.Key)); err != nil {
				t.Fatal(err)
			}
			for _, key := range []client.ObjectKey{journalKey(f.request.Key), history} {
				object := journalProtectionMap(t, f, key)
				if !reflect.DeepEqual(object.Finalizers, []string{"other.example/retain"}) {
					t.Fatal("terminal release stranded a protected sibling or removed another controller's finalizer")
				}
				before := object.DeepCopy()
				if _, err := reconcileJournalProtection(t, f, key); err != nil || !reflect.DeepEqual(before, journalProtectionMap(t, f, key)) {
					t.Fatal("late historical/current reconcile changed an accepted release")
				}
			}
			current := journalProtectionMap(t, f, journalKey(f.request.Key))
			current.Finalizers = nil
			if err := f.client.Update(t.Context(), current); err != nil {
				t.Fatal(err)
			}
			before := journalProtectionMap(t, f, history).DeepCopy()
			if _, err := reconcileJournalProtection(t, f, history); err != nil || !reflect.DeepEqual(before, journalProtectionMap(t, f, history)) {
				t.Fatal("legacy history reprotected after the terminal current journal disappeared")
			}
		})
	}
}

func TestJournalProtectionRetainsCorruptTerminalRecoveryRecord(t *testing.T) {
	f := newConditionContractFixture(t)
	reconcileConditionContract(t, f)
	workspace := conditionContractWorkspace(t, f)
	workspace.Spec.DesiredState = api.ExecutionWorkspaceDesiredDeleted
	workspace.Spec.Retirement = &api.WorkloadRetirement{Sequence: f.request.Sequence, Identity: workspace.Status.Allocation.Identity, Action: api.WorkloadRetirementDelete}
	workspace.Generation++
	if err := f.client.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	for range 25 {
		reconcileConditionContract(t, f)
		if sdk.ConditionIsTrue(conditionContractWorkspace(t, f).Status.Conditions, string(api.ConditionWorkspaceFinalized)) {
			break
		}
	}
	workspace = assertConditionContract(t, f, metav1.ConditionFalse, metav1.ConditionTrue)
	driver := f.lifecycle(f.client)
	cm, record, err := driver.read(t.Context(), f.request.Key)
	if err != nil {
		t.Fatal(err)
	}
	record.DeletionPolicy = nil
	if err := driver.save(t.Context(), cm, record); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Observe(t.Context(), f.request.Key); err == nil {
		t.Fatal("corrupted terminal record passed public lifecycle validation")
	}
	workspace.Finalizers = nil
	if err := f.client.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	if err := f.client.Delete(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	if _, err := reconcileJournalProtection(t, f, journalKey(f.request.Key)); err == nil ||
		!slices.Contains(journalProtectionMap(t, f, journalKey(f.request.Key)).Finalizers, journalProtectionFinalizer) {
		t.Fatal("GC release discarded a terminal record that public lifecycle recovery rejects")
	}
}
