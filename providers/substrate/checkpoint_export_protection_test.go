// Copyright (c) 2026. MIT License - see LICENSE file for details.

package substrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func checkpointIndex(t *testing.T, c client.Client, cp *api.ExecutionWorkspaceCheckpoint) *corev1.ConfigMap {
	t.Helper()
	cm := &corev1.ConfigMap{}
	if err := c.Get(t.Context(), exportKey(cp.Namespace, cp.UID), cm); err != nil {
		t.Fatal(err)
	}
	return cm
}

func indexRequest(cm *corev1.ConfigMap) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKey{Namespace: cm.Namespace, Name: "export/" + cm.Name}}
}

func TestCheckpointExportIndexProtectedBeforePublicationAndLostCreate(t *testing.T) {
	c, native, source, _ := suspendedSource(t)
	cp := newCheckpoint(t, c, source, false)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}
	if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	lost := false
	wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Create: func(ctx context.Context, inner client.WithWatch, object client.Object, options ...client.CreateOption) error {
		cm, ok := object.(*corev1.ConfigMap)
		if !ok || cm.Labels[exportLabel] == "" {
			return inner.Create(ctx, object, options...)
		}
		if !slices.Contains(cm.Finalizers, exportProtectionFinalizer) || len(cm.OwnerReferences) != 1 || cm.OwnerReferences[0].UID != cp.UID ||
			(cm.OwnerReferences[0].BlockOwnerDeletion != nil && *cm.OwnerReferences[0].BlockOwnerDeletion) {
			t.Fatal("index creation did not atomically protect its exact nonblocking checkpoint owner")
		}
		if err := inner.Create(ctx, object, options...); err != nil {
			return err
		}
		lost = true
		return fmt.Errorf("lost accepted index creation response")
	}})
	if _, err := checkpointController(wrapped, native).Reconcile(t.Context(), req); err == nil || !lost {
		t.Fatal("fixture did not lose an accepted index creation")
	}
	current := &api.ExecutionWorkspaceCheckpoint{}
	if err := c.Get(t.Context(), req.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	cm := checkpointIndex(t, c, cp)
	index, err := exportRecord(cm)
	if err != nil {
		t.Fatal(err)
	}
	_, artifact, err := driver(c, native).readCatalogReference(t.Context(), cp.Namespace, &index.Artifact)
	if err != nil || current.Status.Digest != "" || artifact.Owners[publicArtifactOwner(cp)] {
		t.Fatal("lost creation published status or acquired an unprotected public reference")
	}
	// Model the dependent DELETE issued by GC before the owner's finalizer.
	if err := c.Delete(t.Context(), cm, client.Preconditions{UID: &cm.UID}, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil {
		t.Fatal(err)
	}
	if held := checkpointIndex(t, c, cp); held.DeletionTimestamp.IsZero() || !slices.Contains(held.Finalizers, exportProtectionFinalizer) {
		t.Fatal("foreground dependent deletion lost the immutable export selection")
	}
	checkpointDeleted(t, c, native, current)
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(cm), &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
		t.Fatal("unacquired protected export did not retire after lost creation")
	}
	if native.boots != 1 || native.suspends != 1 || len(native.tags) != 1 || len(native.templates) != 2 {
		t.Fatal("export recovery replayed or collected its live source's retained data")
	}
}

func TestCheckpointExportIndexSurvivesForegroundAndNamespaceDeletion(t *testing.T) {
	for _, namespaceDeletion := range []bool{false, true} {
		name := "foreground checkpoint deletion"
		if namespaceDeletion {
			name = "namespace resource deletion"
		}
		t.Run(name, func(t *testing.T) {
			c, native, source, stopped := suspendedSource(t)
			cp := exportReady(t, c, native, newCheckpoint(t, c, source, false))
			allocationDeleted(t, c, native, source, stopped.Identity)
			cm := checkpointIndex(t, c, cp)
			if err := c.Delete(t.Context(), cm, client.Preconditions{UID: &cm.UID}); err != nil {
				t.Fatal(err)
			}
			if namespaceDeletion {
				_, index, err := driver(c, native).readExport(t.Context(), cp)
				if err != nil {
					t.Fatal(err)
				}
				catalog, _, err := driver(c, native).readCatalogReference(t.Context(), cp.Namespace, &index.Artifact)
				if err != nil {
					t.Fatal(err)
				}
				if err := c.Delete(t.Context(), catalog, client.Preconditions{UID: &catalog.UID}); err != nil {
					t.Fatal(err)
				}
			}
			// Lost native delete responses must keep the protected index until
			// observed absence and catalog CAS acceptance complete on retry.
			native.failAfter = "deleteTag"
			if err := c.Delete(t.Context(), cp, client.Preconditions{UID: &cp.UID}, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil {
				t.Fatal(err)
			}
			failed := false
			for range 25 {
				_, err := checkpointController(c, native).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
				if err != nil {
					failed = true
					if !slices.Contains(checkpointIndex(t, c, cp).Finalizers, exportProtectionFinalizer) {
						t.Fatal("uncertain native deletion released export protection")
					}
				}
				if apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(cp), &api.ExecutionWorkspaceCheckpoint{})) {
					break
				}
			}
			if !failed || !apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(cp), &api.ExecutionWorkspaceCheckpoint{})) ||
				!apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(cm), &corev1.ConfigMap{})) {
				t.Fatal("protected deletion did not recover and finish its exact checkpoint lifetime")
			}
			if len(native.tags) != 0 || len(native.templates) != 1 || native.boots != 1 || native.suspends != 1 {
				t.Fatal("checkpoint deletion stranded native data or replayed source execution")
			}
		})
	}
}

func TestCheckpointUnacquiredExportOrphanPreservesCatalogReleaseEvidence(t *testing.T) {
	c, native, source, stopped := suspendedSource(t)
	cp := newCheckpoint(t, c, source, false)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}
	for range 2 {
		if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
			t.Fatal(err)
		}
	}
	cm := checkpointIndex(t, c, cp)
	index, err := exportRecord(cm)
	if err != nil {
		t.Fatal(err)
	}
	_, artifact, err := driver(c, native).readCatalogReference(t.Context(), cp.Namespace, &index.Artifact)
	if err != nil {
		t.Fatal(err)
	}
	if _, recorded := artifact.Owners[publicArtifactOwner(cp)]; recorded {
		t.Fatal("fixture already acquired the public catalog reference")
	}
	if err := c.Get(t.Context(), req.NamespacedName, cp); err != nil {
		t.Fatal(err)
	}
	cp.Finalizers = nil
	if err := c.Update(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), cp, client.Preconditions{UID: &cp.UID}); err != nil {
		t.Fatal(err)
	}
	allocationDeleted(t, c, native, source, stopped.Identity)
	workspace := &api.ExecutionWorkspace{}
	if err := c.Get(t.Context(), clientKey(source.Key), workspace); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), workspace, client.Preconditions{UID: &workspace.UID}); err != nil {
		t.Fatal(err)
	}
	catalogReq := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: cm.Namespace, Name: "catalog/" + index.Artifact.Name}}
	if _, err := checkpointController(c, native).Reconcile(t.Context(), catalogReq); err != nil {
		t.Fatal(err)
	}
	if _, artifact, err := driver(c, native).readCatalogReference(t.Context(), cp.Namespace, &index.Artifact); err != nil || !artifact.Collected {
		t.Fatal("unacquired export lost the exact collected catalog needed to release its index")
	}
	for range 4 {
		if _, err := checkpointController(c, native).Reconcile(t.Context(), indexRequest(cm)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := checkpointController(c, native).Reconcile(t.Context(), catalogReq); err != nil {
		t.Fatal(err)
	}
	if !apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(cm), &corev1.ConfigMap{})) ||
		!apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKey{Namespace: cm.Namespace, Name: index.Artifact.Name}, &corev1.ConfigMap{})) ||
		len(native.tags) != 0 || len(native.templates) != 1 || native.boots != 1 {
		t.Fatal("unacquired orphan release stranded exact index/catalog artifacts or replayed work")
	}
}

func TestCheckpointExportOrphanRetainsCatalogBeforeExactRelease(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		t.Run(fmt.Sprintf("replacement=%v", replaced), func(t *testing.T) {
			c, native, source, stopped := suspendedSource(t)
			cp := exportReady(t, c, native, newCheckpoint(t, c, source, false))
			allocationDeleted(t, c, native, source, stopped.Identity)
			cm := checkpointIndex(t, c, cp)
			cm.Finalizers = append(cm.Finalizers, "other.example/retain")
			if err := c.Update(t.Context(), cm); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(t.Context(), cm, client.Preconditions{UID: &cm.UID}); err != nil {
				t.Fatal(err)
			}
			index, err := exportRecord(cm)
			if err != nil {
				t.Fatal(err)
			}
			cp.Finalizers = nil
			if err := c.Update(t.Context(), cp); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(t.Context(), cp, client.Preconditions{UID: &cp.UID}); err != nil {
				t.Fatal(err)
			}
			var replacement *api.ExecutionWorkspaceCheckpoint
			if replaced {
				replacement = cp.DeepCopy()
				replacement.UID, replacement.ResourceVersion, replacement.Status = "replacement-checkpoint", "", api.ExecutionWorkspaceCheckpointStatus{}
				if err := c.Create(t.Context(), replacement); err != nil {
					t.Fatal(err)
				}
			}
			// Run catalog recovery first. It may collect native data, but must
			// preserve the exact tombstone until the index accepts that release.
			catalogReq := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: cm.Namespace, Name: "catalog/" + index.Artifact.Name}}
			for range 8 {
				if _, err := checkpointController(c, native).Reconcile(t.Context(), catalogReq); err != nil {
					t.Fatal(err)
				}
			}
			_, artifact, err := driver(c, native).readCatalogReference(t.Context(), cm.Namespace, &index.Artifact)
			if err != nil || !artifact.Collected {
				t.Fatal("catalog-first orphan cleanup lost its exact collected tombstone")
			}
			for range 4 {
				if _, err := checkpointController(c, native).Reconcile(t.Context(), indexRequest(cm)); err != nil {
					t.Fatal(err)
				}
			}
			released := checkpointIndex(t, c, cp)
			if released.Annotations[exportReleasedAnnotation] != exportReleaseProof(released) || !reflect.DeepEqual(released.Finalizers, []string{"other.example/retain"}) {
				t.Fatal("orphan release did not preserve unrelated finalizers and bind an exact acknowledgement")
			}
			// Remove the source object so catalog GC can finish; the index's
			// receipt must remain sufficient after the catalog is absent.
			workspace := &api.ExecutionWorkspace{}
			if err := c.Get(t.Context(), clientKey(source.Key), workspace); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(t.Context(), workspace, client.Preconditions{UID: &workspace.UID}); err != nil {
				t.Fatal(err)
			}
			if _, err := checkpointController(c, native).Reconcile(t.Context(), catalogReq); err != nil {
				t.Fatal(err)
			}
			if err := c.Get(t.Context(), client.ObjectKey{Namespace: cm.Namespace, Name: index.Artifact.Name}, &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
				t.Fatal("accepted orphan release retained an otherwise collectible catalog")
			}
			before := released.DeepCopy()
			if _, err := checkpointController(c, native).Reconcile(t.Context(), indexRequest(cm)); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, checkpointIndex(t, c, cp)) {
				t.Fatal("late index event changed an already accepted release")
			}
			if replacement != nil {
				current := &api.ExecutionWorkspaceCheckpoint{}
				if err := c.Get(t.Context(), client.ObjectKeyFromObject(replacement), current); err != nil || !reflect.DeepEqual(replacement, current) {
					t.Fatal("old index recovery adopted a replacement checkpoint")
				}
			}
			if len(native.tags) != 0 || len(native.templates) != 1 || native.boots != 1 || native.suspends != 1 {
				t.Fatal("orphan index cleanup stranded artifacts or replayed native execution")
			}
		})
	}
}

func TestCheckpointExportLegacyProtectionAndFailureBoundaries(t *testing.T) {
	for _, scenario := range []string{"backfill", "write rejected", "already deleting", "released acquisition", "bad release UID"} {
		t.Run(scenario, func(t *testing.T) {
			c, native, source, _ := suspendedSource(t)
			cp := exportReady(t, c, native, newCheckpoint(t, c, source, false))
			cm := checkpointIndex(t, c, cp)
			cm.Finalizers = []string{"other.example/retain"}
			if scenario == "released acquisition" || scenario == "bad release UID" {
				cm.Annotations = map[string]string{exportReleasedAnnotation: exportReleaseProof(cm)}
				if scenario == "bad release UID" {
					cm.Annotations[exportReleasedAnnotation] = "wrong-exact-UID-proof"
				}
			}
			if err := c.Update(t.Context(), cm); err != nil {
				t.Fatal(err)
			}
			if scenario == "already deleting" {
				if err := c.Delete(t.Context(), cm, client.Preconditions{UID: &cm.UID}); err != nil {
					t.Fatal(err)
				}
			}
			request := restoreRequest(t, c, source, cp)
			if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil {
				t.Fatal("unprotected or released index acquired a new imported native instance")
			}
			before := checkpointIndex(t, c, cp)
			_, record, err := driver(c, native).read(t.Context(), source.Key)
			if err != nil {
				t.Fatal(err)
			}
			catalog, artifact, err := driver(c, native).readCatalogReference(t.Context(), cp.Namespace, record.CheckpointCatalog)
			if err != nil {
				t.Fatal(err)
			}
			catalogBefore := catalog.DeepCopy()
			wrapped := c
			if scenario == "write rejected" {
				wrapped = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, inner client.WithWatch, object client.Object, options ...client.UpdateOption) error {
					if client.ObjectKeyFromObject(object) == client.ObjectKeyFromObject(cm) {
						return fmt.Errorf("protection CAS unavailable")
					}
					return inner.Update(ctx, object, options...)
				}})
			}
			result, err := checkpointController(wrapped, native).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
			if scenario == "backfill" {
				if err != nil || result.RequeueAfter <= 0 || !slices.Contains(checkpointIndex(t, c, cp).Finalizers, exportProtectionFinalizer) ||
					!slices.Contains(checkpointIndex(t, c, cp).Finalizers, "other.example/retain") || !reflect.DeepEqual(before.Data, checkpointIndex(t, c, cp).Data) {
					t.Fatal("legacy protection was not backfilled before retained-reference effects")
				}
			} else if err == nil || !reflect.DeepEqual(before, checkpointIndex(t, c, cp)) {
				t.Fatal("unsafe legacy/released index was mutated or did not fail closed")
			}
			currentCatalog, currentArtifact, err := driver(c, native).readCatalogReference(t.Context(), cp.Namespace, record.CheckpointCatalog)
			if err != nil || !reflect.DeepEqual(catalogBefore, currentCatalog) || !reflect.DeepEqual(artifact, currentArtifact) ||
				currentArtifact.Owners[workspaceArtifactOwner(request.Key)] || native.boots != 1 || len(native.actors) != 0 || len(native.tags) != 1 || len(native.templates) != 2 {
				t.Fatal("protection failure changed exact retained ownership or created imported compute")
			}
		})
	}
}

func TestCheckpointMissingIndexBeforeAcceptedReleaseFailsClosed(t *testing.T) {
	c, native, source, _ := suspendedSource(t)
	cp := exportReady(t, c, native, newCheckpoint(t, c, source, false))
	if err := c.Delete(t.Context(), cp, client.Preconditions{UID: &cp.UID}); err != nil {
		t.Fatal(err)
	}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}
	if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	cm := checkpointIndex(t, c, cp)
	cm.Finalizers = nil // Simulate loss outside the protected controller path.
	if err := c.Update(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), cm, client.Preconditions{UID: &cm.UID}); err != nil {
		t.Fatal(err)
	}
	if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err == nil {
		t.Fatal("early Deleting phase was mistaken for accepted catalog release")
	}
	current := &api.ExecutionWorkspaceCheckpoint{}
	if err := c.Get(t.Context(), req.NamespacedName, current); err != nil || !slices.Contains(current.Finalizers, checkpointFinalizer) || checkpointReleaseAccepted(current) {
		t.Fatal("missing cleanup evidence released the checkpoint finalizer")
	}
}

func TestCheckpointExportReleaseCASLossRecoversWithoutCatalog(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed=%v", committed), func(t *testing.T) {
			c, native, source, stopped := suspendedSource(t)
			cp := exportReady(t, c, native, newCheckpoint(t, c, source, false))
			allocationDeleted(t, c, native, source, stopped.Identity)
			cm := checkpointIndex(t, c, cp)
			cm.Finalizers = append(cm.Finalizers, "other.example/retain")
			if err := c.Update(t.Context(), cm); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(t.Context(), cp, client.Preconditions{UID: &cp.UID}); err != nil {
				t.Fatal(err)
			}
			failed := false
			wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, inner client.WithWatch, object client.Object, options ...client.UpdateOption) error {
				if item, ok := object.(*corev1.ConfigMap); ok && item.Name == cm.Name && item.Annotations[exportReleasedAnnotation] != "" && !failed {
					failed = true
					if committed {
						if err := inner.Update(ctx, object, options...); err != nil {
							return err
						}
					}
					return fmt.Errorf("index release response unavailable")
				}
				return inner.Update(ctx, object, options...)
			}})
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}
			for range 20 {
				_, err := checkpointController(wrapped, native).Reconcile(t.Context(), req)
				if failed {
					if err == nil {
						t.Fatal("release failure was not returned")
					}
					break
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if !failed || slices.Contains(checkpointIndex(t, c, cp).Finalizers, exportProtectionFinalizer) == committed {
				t.Fatal("release protection did not reflect the actual accepted CAS")
			}
			for range 5 {
				if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(cp), &api.ExecutionWorkspaceCheckpoint{}); !apierrors.IsNotFound(err) {
				t.Fatal("accepted index release could not finalize the exact checkpoint")
			}
			released := checkpointIndex(t, c, cp)
			if released.Annotations[exportReleasedAnnotation] != exportReleaseProof(released) || !reflect.DeepEqual(released.Finalizers, []string{"other.example/retain"}) {
				t.Fatal("release retry lost its receipt or another controller's finalizer")
			}
			var index checkpointExport
			if err := json.Unmarshal([]byte(released.Data[exportDataKey]), &index); err != nil {
				t.Fatal(err)
			}
			workspace := &api.ExecutionWorkspace{}
			if err := c.Get(t.Context(), clientKey(source.Key), workspace); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(t.Context(), workspace, client.Preconditions{UID: &workspace.UID}); err != nil {
				t.Fatal(err)
			}
			if _, err := checkpointController(c, native).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKey{Namespace: cp.Namespace, Name: "catalog/" + index.Artifact.Name}}); err != nil {
				t.Fatal(err)
			}
			if _, err := checkpointController(c, native).Reconcile(t.Context(), indexRequest(cm)); err != nil {
				t.Fatal("accepted release retried catalog access after collection", err)
			}
		})
	}
}

func TestCheckpointRetirementFreezesExactTransfersAndRejectsLateRestore(t *testing.T) {
	c, native, source, stopped := suspendedSource(t)
	cp := exportReady(t, c, native, newCheckpoint(t, c, source, false))
	allocationDeleted(t, c, native, source, stopped.Identity)
	existing := restoreRequest(t, c, source, cp)
	if err := c.Delete(t.Context(), cp, client.Preconditions{UID: &cp.UID}); err != nil {
		t.Fatal(err)
	}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}
	if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	lostFence := false
	wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, inner client.WithWatch, object client.Object, options ...client.UpdateOption) error {
		if item, ok := object.(*corev1.ConfigMap); ok && item.Labels[exportLabel] == string(cp.UID) {
			if err := inner.Update(ctx, object, options...); err != nil {
				return err
			}
			lostFence = true
			return fmt.Errorf("lost accepted transfer fence response")
		}
		return inner.Update(ctx, object, options...)
	}})
	if _, err := checkpointController(wrapped, native).Reconcile(t.Context(), req); err == nil || !lostFence {
		t.Fatal("fixture did not lose an accepted transfer fence")
	}
	current := &api.ExecutionWorkspaceCheckpoint{}
	if err := c.Get(t.Context(), req.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(current.Status.Conditions, "Ready")
	_, index, err := driver(c, native).readExport(t.Context(), current)
	if err != nil || condition == nil || condition.Status != metav1.ConditionFalse || current.Status.Phase != "Deleting" ||
		!index.Retiring || !index.allowsRetiringTransfer(existing) || len(index.Transfers) != 1 {
		t.Fatal("checkpoint did not durably withdraw Ready and freeze the exact existing transfer")
	}
	// Publish a late, independently identified restore after the snapshot.
	lateWorkspace := &api.ExecutionWorkspace{}
	if err := c.Get(t.Context(), clientKey(existing.Key), lateWorkspace); err != nil {
		t.Fatal(err)
	}
	lateWorkspace.Name, lateWorkspace.UID, lateWorkspace.ResourceVersion = "late-fork", "late-fork-uid", ""
	late := *existing.DeepCopy()
	late.Key.Name, late.Key.WorkspaceUID = lateWorkspace.Name, lateWorkspace.UID
	late.Runtime.PoolBinding.UID = "late-pool-uid"
	late.Revision, _ = sdk.WorkloadRevision(late)
	lateWorkspace.Spec.Workload = &late
	lateWorkspace.Annotations = nil
	if err := c.Create(t.Context(), lateWorkspace); err != nil {
		t.Fatal(err)
	}
	if _, err := driver(c, native).EnsureAllocation(t.Context(), late); err == nil || native.boots != 1 || len(native.actors) != 0 {
		t.Fatal("late restore joined the retirement fence or created native compute")
	}
	first := ready(t, c, native, existing)
	for range 12 {
		if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Get(t.Context(), req.NamespacedName, current); !apierrors.IsNotFound(err) {
		t.Fatal("late restore stranded public checkpoint finalization")
	}
	if _, err := driver(c, native).EnsureAllocation(t.Context(), late); err == nil || native.boots != 2 {
		t.Fatal("late retry acquired an already released checkpoint reference")
	}
	if out, err := driver(c, native).Observe(t.Context(), existing.Key); err != nil || out.Identity != first.Identity || out.State != sdk.AllocationReady {
		t.Fatal("withdrawn public Ready invalidated an independently acquired workspace owner")
	}
	retired(t, c, native, existing, first.Identity, false)
	allocationDeleted(t, c, native, existing, first.Identity)
	if len(native.tags) != 0 || len(native.templates) != 1 {
		t.Fatal("final exact transferred owner did not retire retained artifacts")
	}
}

func TestCheckpointLateRestoreBetweenReleaseScanAndLostResponseRetry(t *testing.T) {
	c, native, source, stopped := suspendedSource(t)
	cp := exportReady(t, c, native, newCheckpoint(t, c, source, false))
	allocationDeleted(t, c, native, source, stopped.Identity)
	if err := c.Delete(t.Context(), cp, client.Preconditions{UID: &cp.UID}); err != nil {
		t.Fatal(err)
	}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}
	for range 2 {
		if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
			t.Fatal(err)
		}
	}
	var late sdk.WorkloadRequest
	lost := false
	wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, inner client.WithWatch, object client.Object, options ...client.UpdateOption) error {
		cm, ok := object.(*corev1.ConfigMap)
		if ok && cm.Data[catalogDataKey] != "" && !lost {
			var artifact checkpointArtifact
			if err := json.Unmarshal([]byte(cm.Data[catalogDataKey]), &artifact); err != nil {
				t.Fatal(err)
			}
			if !artifact.Owners[publicArtifactOwner(cp)] {
				// Simulate a stale Core admission arriving after the initial
				// empty scan but before the public-owner release CAS commits.
				late = restoreRequest(t, c, source, cp)
				if err := inner.Update(ctx, object, options...); err != nil {
					return err
				}
				lost = true
				return fmt.Errorf("lost accepted public reference release response")
			}
		}
		return inner.Update(ctx, object, options...)
	}})
	if _, err := checkpointController(wrapped, native).Reconcile(t.Context(), req); err == nil || !lost {
		t.Fatal("fixture did not inject late admission during accepted release")
	}
	if _, err := driver(c, native).EnsureAllocation(t.Context(), late); err == nil {
		t.Fatal("late restore acquired the released public reference")
	}
	// Publish another exact admission before the release controller retries.
	workspace := &api.ExecutionWorkspace{}
	if err := c.Get(t.Context(), clientKey(late.Key), workspace); err != nil {
		t.Fatal(err)
	}
	workspace.Name, workspace.UID, workspace.ResourceVersion = "retry-fork", "retry-fork-uid", ""
	retry := *late.DeepCopy()
	retry.Key.Name, retry.Key.WorkspaceUID = workspace.Name, workspace.UID
	retry.Runtime.PoolBinding.UID = "retry-pool-uid"
	retry.Revision, _ = sdk.WorkloadRevision(retry)
	workspace.Spec.Workload = &retry
	workspace.Annotations = nil
	if err := c.Create(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	if _, err := driver(c, native).EnsureAllocation(t.Context(), retry); err == nil {
		t.Fatal("retry admission joined an already frozen retirement")
	}
	_, index, err := driver(c, native).readExport(t.Context(), cp)
	if err != nil || !index.Retiring || len(index.Transfers) != 0 {
		t.Fatal("accepted release retry reopened the immutable empty transfer fence")
	}
	for range 15 {
		if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(cp), &api.ExecutionWorkspaceCheckpoint{}); !apierrors.IsNotFound(err) ||
		native.boots != 1 || len(native.actors) != 0 || len(native.tags) != 0 || len(native.templates) != 1 {
		t.Fatal("late admissions stranded finalization, acquired native effects, or replayed source execution")
	}
}

func TestCheckpointFrozenTransferWithdrawalCancelsWaitWithoutReleasingOwnedData(t *testing.T) {
	for _, acquired := range []bool{false, true} {
		t.Run(fmt.Sprintf("acquired=%v", acquired), func(t *testing.T) {
			c, native, source, stopped := suspendedSource(t)
			cp := exportReady(t, c, native, newCheckpoint(t, c, source, false))
			allocationDeleted(t, c, native, source, stopped.Identity)
			fork := restoreRequest(t, c, source, cp)
			if acquired {
				ready(t, c, native, fork)
			}
			if err := c.Delete(t.Context(), cp, client.Preconditions{UID: &cp.UID}); err != nil {
				t.Fatal(err)
			}
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}
			for range 2 {
				if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
					t.Fatal(err)
				}
			}
			_, index, err := driver(c, native).readExport(t.Context(), cp)
			if err != nil || !index.allowsRetiringTransfer(fork) {
				t.Fatal("exact pending transfer was not frozen", err)
			}
			workspace := &api.ExecutionWorkspace{}
			if err := c.Get(t.Context(), clientKey(fork.Key), workspace); err != nil {
				t.Fatal(err)
			}
			workspace.Spec.CoreAdmission = nil
			if err := c.Update(t.Context(), workspace); err != nil {
				t.Fatal(err)
			}
			if _, err := driver(c, native).EnsureAllocation(t.Context(), fork); !errors.Is(err, sdk.ErrWorkspaceNotAdmitted) {
				t.Fatal("withdrawn workspace still authorized native startup", err)
			}
			for range 15 {
				if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.Get(t.Context(), req.NamespacedName, &api.ExecutionWorkspaceCheckpoint{}); !apierrors.IsNotFound(err) {
				t.Fatal("withdrawn unowned candidate stranded checkpoint cleanup")
			}
			_, artifact, err := driver(c, native).readCatalogReference(t.Context(), cp.Namespace, &index.Artifact)
			if err != nil || artifact.Owners[workspaceArtifactOwner(fork.Key)] != acquired || artifact.Owners[publicArtifactOwner(cp)] {
				t.Fatal("withdrawal changed independent workspace ownership or retained public ownership", err)
			}
			if acquired {
				if native.boots != 2 || len(native.actors) != 1 || len(native.tags) != 1 {
					t.Fatal("withdrawal released independently acquired native data")
				}
			} else if native.boots != 1 || len(native.actors) != 0 || len(native.tags) != 0 || len(native.templates) != 1 {
				t.Fatal("cancellation created compute or stranded unowned native data")
			}
		})
	}
}

func TestCheckpointStaleReconcileCannotOverwriteAcceptedRelease(t *testing.T) {
	c, native, source, stopped := suspendedSource(t)
	cp := exportReady(t, c, native, newCheckpoint(t, c, source, false))
	allocationDeleted(t, c, native, source, stopped.Identity)
	if err := c.Delete(t.Context(), cp, client.Preconditions{UID: &cp.UID}); err != nil {
		t.Fatal(err)
	}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}
	interleaved := false
	wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, inner client.WithWatch, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
		if err := inner.Get(ctx, key, object, options...); err != nil {
			return err
		}
		cm, ok := object.(*corev1.ConfigMap)
		if !ok || cm.Labels[exportLabel] != string(cp.UID) || interleaved {
			return nil
		}
		interleaved = true
		// Pause one public Reconcile with old CP/index snapshots while the
		// other accepts exact release and deletes the protected index.
		for range 15 {
			if _, err := checkpointController(c, native).Reconcile(ctx, req); err != nil {
				t.Fatal(err)
			}
			if apierrors.IsNotFound(inner.Get(ctx, key, &corev1.ConfigMap{})) {
				pending := &api.ExecutionWorkspaceCheckpoint{}
				if err := inner.Get(ctx, req.NamespacedName, pending); err != nil || !checkpointReleaseAccepted(pending) {
					t.Fatal("release was not accepted before stale reconciliation resumed", err)
				}
				return nil
			}
		}
		t.Fatal("interleaved reconciliation never accepted index release")
		return nil
	}})
	if _, err := checkpointController(wrapped, native).Reconcile(t.Context(), req); !apierrors.IsConflict(err) || !interleaved {
		t.Fatal("stale phase did not conflict before replacing accepted cleanup evidence", err)
	}
	current := &api.ExecutionWorkspaceCheckpoint{}
	if err := c.Get(t.Context(), req.NamespacedName, current); err != nil || !checkpointReleaseAccepted(current) {
		t.Fatal("stale reconciliation regressed accepted cleanup evidence", err)
	}
	for range 3 {
		if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Get(t.Context(), req.NamespacedName, &api.ExecutionWorkspaceCheckpoint{}); !apierrors.IsNotFound(err) ||
		native.boots != 1 || len(native.actors) != 0 || len(native.tags) != 0 || len(native.templates) != 1 {
		t.Fatal("stale retry stranded accepted cleanup or replayed native execution")
	}
}

func TestCheckpointIndexCreateAfterSourceCollectionRetiresOnlyMetadata(t *testing.T) {
	for _, orphan := range []bool{false, true} {
		t.Run(fmt.Sprintf("orphan=%v", orphan), func(t *testing.T) {
			c, native, source, stopped := suspendedSource(t)
			cp := newCheckpoint(t, c, source, false)
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}
			if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
				t.Fatal(err)
			}
			paused := false
			wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Create: func(ctx context.Context, inner client.WithWatch, object client.Object, options ...client.CreateOption) error {
				cm, ok := object.(*corev1.ConfigMap)
				if !ok || cm.Labels[exportLabel] != string(cp.UID) || paused {
					return inner.Create(ctx, object, options...)
				}
				paused = true
				var index checkpointExport
				if err := json.Unmarshal([]byte(cm.Data[exportDataKey]), &index); err != nil || index.AcquisitionIssued == nil || *index.AcquisitionIssued {
					t.Fatal("new export omitted the explicit never-issued acquisition proof", err)
				}
				// Finish exact source/catalog cleanup while the protected index
				// CREATE is still in flight and no index is yet visible to GC.
				allocationDeleted(t, c, native, source, stopped.Identity)
				workspace := &api.ExecutionWorkspace{}
				if err := c.Get(ctx, clientKey(source.Key), workspace); err != nil {
					t.Fatal(err)
				}
				if err := c.Delete(ctx, workspace, client.Preconditions{UID: &workspace.UID}); err != nil {
					t.Fatal(err)
				}
				catalogReq := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: cp.Namespace, Name: "catalog/" + index.Artifact.Name}}
				for range 3 {
					if _, err := checkpointController(c, native).Reconcile(ctx, catalogReq); err != nil {
						t.Fatal(err)
					}
				}
				if err := c.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: index.Artifact.Name}, &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
					t.Fatal("catalog did not collect before the delayed index CREATE", err)
				}
				return inner.Create(ctx, object, options...)
			}})
			if _, err := checkpointController(wrapped, native).Reconcile(t.Context(), req); err != nil || !paused {
				t.Fatal("fixture did not complete delayed protected creation", err)
			}
			current := &api.ExecutionWorkspaceCheckpoint{}
			if err := c.Get(t.Context(), req.NamespacedName, current); err != nil {
				t.Fatal(err)
			}
			cm := checkpointIndex(t, c, cp)
			if orphan {
				current.Finalizers = nil
				if err := c.Update(t.Context(), current); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.Delete(t.Context(), current, client.Preconditions{UID: &current.UID}); err != nil {
				t.Fatal(err)
			}
			for range 15 {
				cleanupReq := req
				if orphan {
					cleanupReq = indexRequest(cm)
				}
				if _, err := checkpointController(c, native).Reconcile(t.Context(), cleanupReq); err != nil {
					t.Fatal(err)
				}
			}
			if !apierrors.IsNotFound(c.Get(t.Context(), req.NamespacedName, &api.ExecutionWorkspaceCheckpoint{})) ||
				!apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(cm), &corev1.ConfigMap{})) ||
				native.boots != 1 || len(native.actors) != 0 || len(native.tags) != 0 || len(native.templates) != 1 {
				t.Fatal("never-acquired index cleanup stranded metadata or issued native effects")
			}
		})
	}
}

func TestCheckpointAcquisitionIntentCASAndLostAcquisitionRecoverExactSelection(t *testing.T) {
	for _, scenario := range []string{"rejected intent", "lost intent response", "lost acquisition response"} {
		t.Run(scenario, func(t *testing.T) {
			c, native, source, _ := suspendedSource(t)
			cp := newCheckpoint(t, c, source, false)
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}
			for range 2 {
				if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
					t.Fatal(err)
				}
			}
			cm := checkpointIndex(t, c, cp)
			index, err := exportRecord(cm)
			if err != nil || index.AcquisitionIssued == nil || *index.AcquisitionIssued {
				t.Fatal("fixture did not start with protected never-issued intent", err)
			}
			if scenario == "lost acquisition response" {
				if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
					t.Fatal(err)
				}
			}
			failed := false
			wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, inner client.WithWatch, object client.Object, options ...client.UpdateOption) error {
				item, ok := object.(*corev1.ConfigMap)
				if !ok || failed {
					return inner.Update(ctx, object, options...)
				}
				match := item.Name == cm.Name && scenario != "lost acquisition response"
				if scenario == "lost acquisition response" && item.Name == index.Artifact.Name {
					var artifact checkpointArtifact
					if err := json.Unmarshal([]byte(item.Data[catalogDataKey]), &artifact); err != nil {
						t.Fatal(err)
					}
					match = artifact.Owners[publicArtifactOwner(cp)]
				}
				if !match {
					return inner.Update(ctx, object, options...)
				}
				if scenario != "rejected intent" {
					if err := inner.Update(ctx, object, options...); err != nil {
						return err
					}
				}
				failed = true
				return fmt.Errorf("acquisition intent/reference response unavailable")
			}})
			if _, err := checkpointController(wrapped, native).Reconcile(t.Context(), req); !failed || (err == nil && scenario != "lost acquisition response") {
				t.Fatal("fixture did not lose the exact intent/acquisition CAS", err)
			}
			_, observed, err := driver(c, native).readExport(t.Context(), cp)
			if err != nil || observed.Artifact != index.Artifact || observed.AcquisitionIssued == nil || *observed.AcquisitionIssued != (scenario != "rejected intent") {
				t.Fatal("retry lost actual issued intent or changed the selected artifact", err)
			}
			_, artifact, err := driver(c, native).readCatalogReference(t.Context(), cp.Namespace, &index.Artifact)
			if err != nil || artifact.Owners[publicArtifactOwner(cp)] != (scenario == "lost acquisition response") {
				t.Fatal("acquisition happened before intent acceptance or its accepted owner was lost", err)
			}
			current := &api.ExecutionWorkspaceCheckpoint{}
			if err := c.Get(t.Context(), req.NamespacedName, current); err != nil || current.Status.Phase == "Ready" {
				t.Fatal("uncertain acquisition published Ready", err)
			}
			cp = exportReady(t, c, native, cp)
			_, final, err := driver(c, native).readExport(t.Context(), cp)
			if err != nil || final.Artifact != index.Artifact || final.AcquisitionIssued == nil || !*final.AcquisitionIssued || native.boots != 1 || len(native.actors) != 0 {
				t.Fatal("accepted retry reselected data or replayed native compute", err)
			}
		})
	}
}

func TestCheckpointLegacyOrIssuedAcquisitionMissingCatalogStaysClosed(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			c, native, source, _ := suspendedSource(t)
			cp := exportReady(t, c, native, newCheckpoint(t, c, source, false))
			cm := checkpointIndex(t, c, cp)
			index, err := exportRecord(cm)
			if err != nil {
				t.Fatal(err)
			}
			if legacy {
				index.AcquisitionIssued = nil
				if err := driver(c, native).saveExport(t.Context(), cm, index); err != nil {
					t.Fatal(err)
				}
			}
			catalog, _, err := driver(c, native).readCatalogReference(t.Context(), cp.Namespace, &index.Artifact)
			if err != nil {
				t.Fatal(err)
			}
			catalog.Finalizers = nil
			if err := c.Update(t.Context(), catalog); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(t.Context(), catalog, client.Preconditions{UID: &catalog.UID}); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(t.Context(), cp, client.Preconditions{UID: &cp.UID}); err != nil {
				t.Fatal(err)
			}
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}
			if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
				t.Fatal(err)
			}
			for range 3 {
				if _, err := checkpointController(c, native).Reconcile(t.Context(), req); !apierrors.IsNotFound(err) {
					t.Fatal("uncertain/issued catalog disappearance was treated as accepted cleanup", err)
				}
			}
			current := &api.ExecutionWorkspaceCheckpoint{}
			if err := c.Get(t.Context(), req.NamespacedName, current); err != nil || !slices.Contains(current.Finalizers, checkpointFinalizer) ||
				!slices.Contains(checkpointIndex(t, c, cp).Finalizers, exportProtectionFinalizer) || native.boots != 1 || len(native.tags) != 1 || len(native.templates) != 2 {
				t.Fatal("uncertain catalog loss released obligations or changed native data", err)
			}
		})
	}
}

func TestCheckpointNeverIssuedIndexCannotClaimReadyForImport(t *testing.T) {
	c, native, source, _ := suspendedSource(t)
	cp := newCheckpoint(t, c, source, false)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}
	for range 2 {
		if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Get(t.Context(), req.NamespacedName, cp); err != nil {
		t.Fatal(err)
	}
	_, index, err := driver(c, native).readExport(t.Context(), cp)
	if err != nil || index.AcquisitionIssued == nil || *index.AcquisitionIssued {
		t.Fatal("fixture did not preserve never-issued acquisition intent", err)
	}
	// Public status alone cannot substitute for the private acquisition fence.
	cp.Status.Phase = "Ready"
	meta.SetStatusCondition(&cp.Status.Conditions, metav1.Condition{Type: "Ready", Status: metav1.ConditionTrue, Reason: "DataRetained", ObservedGeneration: cp.Generation, LastTransitionTime: metav1.Now()})
	if err := c.Status().Update(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	fork := restoreRequest(t, c, source, cp)
	if _, err := driver(c, native).EnsureAllocation(t.Context(), fork); err == nil {
		t.Fatal("public Ready admitted an import before catalog acquisition intent")
	}
	_, artifact, err := driver(c, native).readCatalogReference(t.Context(), cp.Namespace, &index.Artifact)
	if err != nil || artifact.Owners[workspaceArtifactOwner(fork.Key)] || artifact.Owners[publicArtifactOwner(cp)] ||
		native.boots != 1 || len(native.actors) != 0 || len(native.tags) != 1 || len(native.templates) != 2 {
		t.Fatal("never-issued reference created imported ownership or native compute", err)
	}
}

func TestCheckpointUnacquirableCatalogPreservesNeverIssuedIntent(t *testing.T) {
	for _, scenario := range []string{"source released", "collecting", "collected", "deleting", "unprotected"} {
		t.Run(scenario, func(t *testing.T) {
			c, native, source, _ := suspendedSource(t)
			cp := newCheckpoint(t, c, source, false)
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}
			for range 2 {
				if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
					t.Fatal(err)
				}
			}
			_, index, err := driver(c, native).readExport(t.Context(), cp)
			if err != nil {
				t.Fatal(err)
			}
			catalog, artifact, err := driver(c, native).readCatalogReference(t.Context(), cp.Namespace, &index.Artifact)
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "source released", "collecting", "collected":
				artifact.Owners[workspaceArtifactOwner(source.Key)] = false
				artifact.Deleting = scenario != "source released"
				artifact.Collected = scenario == "collected"
				if err := driver(c, native).saveCatalog(t.Context(), catalog, artifact); err != nil {
					t.Fatal(err)
				}
			case "deleting":
				if err := c.Delete(t.Context(), catalog, client.Preconditions{UID: &catalog.UID}); err != nil {
					t.Fatal(err)
				}
			case "unprotected":
				catalog.Finalizers = nil
				if err := c.Update(t.Context(), catalog); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
				t.Fatal(err)
			}
			current := &api.ExecutionWorkspaceCheckpoint{}
			if err := c.Get(t.Context(), req.NamespacedName, current); err != nil {
				t.Fatal(err)
			}
			condition := meta.FindStatusCondition(current.Status.Conditions, "Ready")
			_, observed, err := driver(c, native).readExport(t.Context(), current)
			if err != nil || observed.AcquisitionIssued == nil || *observed.AcquisitionIssued || current.Status.Phase != "Failed" || condition == nil || condition.Reason != "ReferenceUnavailable" {
				t.Fatal("unacquirable catalog consumed the never-issued proof or advertised Ready", err)
			}
			// Finish the already-selected catalog's API deletion. No new index
			// reference was issued, so subsequent cleanup is metadata-only.
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(catalog), catalog); err != nil {
				t.Fatal(err)
			}
			catalog.Finalizers = nil
			if err := c.Update(t.Context(), catalog); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(t.Context(), catalog, client.Preconditions{UID: &catalog.UID}); err != nil && !apierrors.IsNotFound(err) {
				t.Fatal(err)
			}
			if err := c.Delete(t.Context(), current, client.Preconditions{UID: &current.UID}); err != nil {
				t.Fatal(err)
			}
			for range 12 {
				if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
					t.Fatal(err)
				}
			}
			if !apierrors.IsNotFound(c.Get(t.Context(), req.NamespacedName, &api.ExecutionWorkspaceCheckpoint{})) ||
				!apierrors.IsNotFound(c.Get(t.Context(), exportKey(cp.Namespace, cp.UID), &corev1.ConfigMap{})) ||
				native.boots != 1 || len(native.tags) != 1 || len(native.templates) != 2 {
				t.Fatal("never-issued metadata cleanup claimed or performed native collection")
			}
		})
	}
}

func TestCheckpointDelayedCreateDuringCatalogDeletionKeepsNeverIssuedProof(t *testing.T) {
	c, native, source, stopped := suspendedSource(t)
	cp := newCheckpoint(t, c, source, false)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}
	if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	injected := false
	wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Create: func(ctx context.Context, inner client.WithWatch, object client.Object, options ...client.CreateOption) error {
		cm, ok := object.(*corev1.ConfigMap)
		if !ok || cm.Labels[exportLabel] != string(cp.UID) || injected {
			return inner.Create(ctx, object, options...)
		}
		injected = true
		var index checkpointExport
		if err := json.Unmarshal([]byte(cm.Data[exportDataKey]), &index); err != nil {
			t.Fatal(err)
		}
		allocationDeleted(t, c, native, source, stopped.Identity)
		workspace := &api.ExecutionWorkspace{}
		if err := c.Get(ctx, clientKey(source.Key), workspace); err != nil {
			t.Fatal(err)
		}
		if err := c.Delete(ctx, workspace, client.Preconditions{UID: &workspace.UID}); err != nil {
			t.Fatal(err)
		}
		created := false
		gcClient := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{List: func(ctx context.Context, base client.WithWatch, list client.ObjectList, listOptions ...client.ListOption) error {
			if err := base.List(ctx, list, listOptions...); err != nil {
				return err
			}
			if _, ok := list.(*corev1.ConfigMapList); !ok || created {
				return nil
			}
			created = true
			// GC has already read Collected and an empty index inventory.
			// Land CREATE, then run an ordinary export retry before GC DELETE.
			if err := inner.Create(ctx, object, options...); err != nil {
				t.Fatal(err)
			}
			if _, err := checkpointController(c, native).Reconcile(ctx, req); err != nil {
				t.Fatal(err)
			}
			_, observed, err := driver(c, native).readExport(ctx, cp)
			if err != nil || observed.AcquisitionIssued == nil || *observed.AcquisitionIssued {
				t.Fatal("already-collected catalog consumed never-issued intent", err)
			}
			return nil
		}})
		catalogReq := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: cp.Namespace, Name: "catalog/" + index.Artifact.Name}}
		if _, err := checkpointController(gcClient, native).Reconcile(ctx, catalogReq); err != nil {
			t.Fatal(err)
		}
		if !created || !apierrors.IsNotFound(c.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: index.Artifact.Name}, &corev1.ConfigMap{})) {
			t.Fatal("fixture did not overlap delayed CREATE with catalog GC completion")
		}
		return nil
	}})
	if _, err := checkpointController(wrapped, native).Reconcile(t.Context(), req); err != nil && !apierrors.IsConflict(err) {
		t.Fatal(err)
	}
	if !injected {
		t.Fatal("fixture did not delay index creation")
	}
	current := &api.ExecutionWorkspaceCheckpoint{}
	if err := c.Get(t.Context(), req.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), current, client.Preconditions{UID: &current.UID}); err != nil {
		t.Fatal(err)
	}
	for range 15 {
		if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
			t.Fatal(err)
		}
	}
	if !apierrors.IsNotFound(c.Get(t.Context(), req.NamespacedName, &api.ExecutionWorkspaceCheckpoint{})) ||
		!apierrors.IsNotFound(c.Get(t.Context(), exportKey(cp.Namespace, cp.UID), &corev1.ConfigMap{})) ||
		native.boots != 1 || len(native.actors) != 0 || len(native.tags) != 0 || len(native.templates) != 1 {
		t.Fatal("catalog GC overlap stranded never-issued metadata or replayed native work")
	}
}
