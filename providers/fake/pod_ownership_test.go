// Copyright (c) 2026. MIT License - see LICENSE file for details.

package fake

import (
	"errors"
	"reflect"
	"testing"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestRuntimePodSurvivesWorkspaceForegroundDeletionUntilExactRetirement(t *testing.T) {
	c, request := runtimeFixture(t)
	ready, err := New(c).EnsureAllocation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	podKey := client.ObjectKey{Namespace: ready.Startup.Pod.Namespace, Name: ready.Startup.Pod.Name}
	pod := &corev1.Pod{}
	if err := c.Get(t.Context(), podKey, pod); err != nil {
		t.Fatal(err)
	}
	if len(pod.OwnerReferences) != 0 {
		t.Fatalf("live runtime has a foreground-collectible owner dependency: %+v", pod.OwnerReferences)
	}
	originalPod := pod.DeepCopy()
	workspace := workspaceFor(t, c, request.Key)
	workspace.Finalizers = []string{coreWorkspaceFinalizer, metav1.FinalizerDeleteDependents}
	workspace.Spec.Lifecycle.DeletionPolicy = deletionPolicy()
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), workspace, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil {
		t.Fatal(err)
	}
	workspace = workspaceFor(t, c, request.Key)
	if workspace.DeletionTimestamp.IsZero() || workspace.Spec.Retirement != nil {
		t.Fatal("foreground deletion without Core retirement was not established")
	}
	// GC can DELETE the Workspace-owned journal, but its finalizer preserves
	// the recovery record. The runtime has no owner for GC to follow.
	cm := &corev1.ConfigMap{}
	if err := c.Get(t.Context(), journalKey(request.Key), cm); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	reconciler := &FakeExecutionWorkspaceReconciler{Client: c}
	reconcile := func() {
		t.Helper()
		if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err != nil {
			t.Fatal(err)
		}
	}
	reconcile()
	if err := c.Get(t.Context(), podKey, pod); err != nil || !reflect.DeepEqual(originalPod, pod) {
		t.Fatalf("runtime changed before Core retirement: %v", err)
	}
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil || record.Operation != "ensure" {
		t.Fatalf("provider retired without Core authorization: %v", err)
	}
	stale := ready.Identity
	stale.InstanceID += "-foreign"
	if _, err := New(c).StopInstance(t.Context(), request.Key, stale); !errors.Is(err, sdk.ErrStaleIdentity) {
		t.Fatalf("stale identity stopped the runtime: %v", err)
	}
	if err := c.Get(t.Context(), podKey, pod); err != nil || !reflect.DeepEqual(originalPod, pod) {
		t.Fatalf("stale retirement changed the runtime: %v", err)
	}
	workspace = workspaceFor(t, c, request.Key)
	workspace.Spec.Retirement = &api.WorkloadRetirement{Sequence: request.Sequence, Identity: ready.Identity, Action: api.WorkloadRetirementDelete}
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		reconcile()
	}
	if err := c.Get(t.Context(), podKey, pod); !apierrors.IsNotFound(err) {
		t.Fatalf("exact retirement did not remove the Pod: %v", err)
	}
	deleted, err := New(c).Observe(t.Context(), request.Key)
	if err != nil || deleted.State != sdk.AllocationDeleted || deleted.Identity != ready.Identity || deleted.Startup != nil {
		t.Fatalf("exact cleanup lost its terminal fence: %+v %v", deleted, err)
	}
	if err := sdk.ValidateDeletedDisposition(deleted.Disposition, deletionPolicy()); err != nil {
		t.Fatal(err)
	}
}

func TestPriorWorkspaceOwnedPodFailsClosedWithoutMigration(t *testing.T) {
	for _, lostResponse := range []bool{false, true} {
		t.Run(map[bool]string{false: "journaled UID", true: "unresolved UID"}[lostResponse], func(t *testing.T) {
			c, request := runtimeFixture(t)
			c.loseCreate = lostResponse
			_, err := New(c).EnsureAllocation(t.Context(), request)
			if (err != nil) != lostResponse {
				t.Fatalf("unexpected create result: %v", err)
			}
			cm, record, err := New(c).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			pod := &corev1.Pod{}
			podKey := client.ObjectKey{Namespace: record.Pod.Namespace, Name: record.Pod.Name}
			if err := c.Get(t.Context(), podKey, pod); err != nil {
				t.Fatal(err)
			}
			controller := true
			pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: "ExecutionWorkspace", Name: request.Key.Name, UID: request.Key.WorkspaceUID, Controller: &controller}}
			if err := c.Update(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			priorPod := pod.DeepCopy()
			priorJournal := cm.DeepCopy()
			if _, err := New(c).Observe(t.Context(), request.Key); !errors.Is(err, sdk.ErrStaleIdentity) {
				t.Fatalf("prior owned Pod passed startup observation: %v", err)
			}
			if _, err := New(c).EnsureAllocation(t.Context(), request); !errors.Is(err, sdk.ErrStaleIdentity) {
				t.Fatalf("prior owned Pod passed ensure: %v", err)
			}
			if err := c.Get(t.Context(), podKey, pod); err != nil || !reflect.DeepEqual(priorPod, pod) {
				t.Fatalf("prior Pod was migrated: %v", err)
			}
			if err := c.Get(t.Context(), journalKey(request.Key), cm); err != nil || !reflect.DeepEqual(priorJournal, cm) {
				t.Fatalf("prior journal was migrated: %v", err)
			}
			identity := record.Observation.Identity
			if lostResponse {
				if _, err := New(c).StopInstance(t.Context(), request.Key, identity); !errors.Is(err, sdk.ErrStaleIdentity) {
					t.Fatalf("prior Pod with unresolved UID was adopted for cleanup: %v", err)
				}
				if err := c.Get(t.Context(), podKey, pod); err != nil || !reflect.DeepEqual(priorPod, pod) {
					t.Fatalf("unresolved prior Pod was deleted: %v", err)
				}
				return
			}
			// Startup rejection does not prevent cleanup of a previously committed UID.
			stale := identity
			stale.RequestRevision += "-foreign"
			if _, err := New(c).StopInstance(t.Context(), request.Key, stale); !errors.Is(err, sdk.ErrStaleIdentity) {
				t.Fatalf("stale identity stopped a prior Pod: %v", err)
			}
			if _, err := New(c).StopInstance(t.Context(), request.Key, identity); err != nil {
				t.Fatal(err)
			}
			stopped, err := New(c).StopInstance(t.Context(), request.Key, identity)
			if err != nil || stopped.State != sdk.AllocationStopped || stopped.Identity != identity {
				t.Fatalf("prior Pod did not terminate by exact UID: %+v %v", stopped, err)
			}
			deleted, err := New(c).DeleteAllocation(t.Context(), request.Key, identity, deletionPolicy())
			if err != nil || deleted.State != sdk.AllocationDeleted || deleted.Identity != identity {
				t.Fatalf("prior Pod cleanup lost its exact fence: %+v %v", deleted, err)
			}
		})
	}
}
