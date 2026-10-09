package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func suspendedSource(t *testing.T) (client.Client, *nativeFixture, sdk.WorkloadRequest, sdk.AllocationObservation) {
	t.Helper()
	c, native, request := fixture(t, true)
	first := ready(t, c, native, request)
	stopped := retired(t, c, native, request, first.Identity, true)
	w := &api.ExecutionWorkspace{}
	if err := c.Get(t.Context(), clientKey(request.Key), w); err != nil {
		t.Fatal(err)
	}
	w.Spec.DesiredState = api.ExecutionWorkspaceDesiredSuspended
	if err := c.Update(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	w.Status.State = api.ExecutionWorkspaceStateSuspended
	if err := c.Status().Update(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	return c, native, request, stopped
}
func newCheckpoint(t *testing.T, c client.Client, request sdk.WorkloadRequest, recover bool) *api.ExecutionWorkspaceCheckpoint {
	t.Helper()
	cp := &api.ExecutionWorkspaceCheckpoint{ObjectMeta: metav1.ObjectMeta{Namespace: request.Key.Namespace, Name: "backup", UID: "checkpoint-uid", Generation: 1}, Spec: api.ExecutionWorkspaceCheckpointSpec{WorkspaceRef: api.ObjectIdentityReference{Name: request.Key.Name, UID: request.Key.WorkspaceUID}, RecoverLastCheckpoint: recover}}
	if err := c.Create(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	return cp
}
func checkpointController(c client.Client, native *nativeFixture) *CheckpointReconciler {
	return &CheckpointReconciler{Client: c, Control: native, Config: Config{ActorDNSSuffix: "actors.local", DirectEgressEnabled: true}}
}
func exportReady(t *testing.T, c client.Client, native *nativeFixture, cp *api.ExecutionWorkspaceCheckpoint) *api.ExecutionWorkspaceCheckpoint {
	t.Helper()
	for range 8 {
		if _, err := checkpointController(c, native).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
			t.Fatal(err)
		}
		current := &api.ExecutionWorkspaceCheckpoint{}
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(cp), current); err != nil {
			t.Fatal(err)
		}
		if current.Status.Phase == "Ready" {
			data, err := json.Marshal(current.Status)
			if err != nil {
				t.Fatal(err)
			}
			if current.Status.Digest == "" || current.Status.ClassBinding == nil || current.Status.CreatedAt == nil || strings.Contains(string(data), "snapshot") || strings.Contains(string(data), "native/") || strings.Contains(string(data), "tag-") {
				t.Fatalf("public checkpoint leaked native data or lacked provenance: %s", data)
			}
			return current
		}
	}
	t.Fatal("checkpoint never became Ready")
	return nil
}
func restoreRequest(t *testing.T, c client.Client, source sdk.WorkloadRequest, cp *api.ExecutionWorkspaceCheckpoint) sdk.WorkloadRequest {
	t.Helper()
	w := &api.ExecutionWorkspace{}
	if err := c.Get(t.Context(), clientKey(source.Key), w); err != nil {
		t.Fatal(err)
	}
	w = w.DeepCopy()
	w.Name = "fork"
	w.UID = "fork-uid"
	w.ResourceVersion = ""
	w.Annotations = nil
	w.Spec.DesiredState = api.ExecutionWorkspaceDesiredReady
	w.Spec.Retirement = nil
	w.Spec.SessionRef = &api.ObjectIdentityReference{Name: "fork-session", UID: "fork-session-uid"}
	w.Status.Allocation = nil
	w.Status.State = api.ExecutionWorkspaceStatePending
	w.Status.ExternalID = ""
	w.Status.Endpoints = nil
	request := *source.DeepCopy()
	request.Key.Name, request.Key.WorkspaceUID = w.Name, w.UID
	if cp != nil {
		request.RestoreFrom = &api.WorkloadCheckpointReference{Name: cp.Name, UID: cp.UID, Digest: cp.Status.Digest}
	}
	request.Runtime.PoolBinding.UID = "fork-pool-uid"
	request.Runtime.Template.Spec.Containers[0].Env[0].Value = "fork-public-nonce"
	request.Revision, _ = sdk.WorkloadRevision(request)
	w.Spec.Workload = &request
	if err := c.Create(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	return request
}
func allocationDeleted(t *testing.T, c client.Client, native *nativeFixture, request sdk.WorkloadRequest, identity sdk.InstanceIdentity) {
	t.Helper()
	policy := api.ExecutionWorkspaceDeletionPolicy{ProviderResources: api.WorkspaceDeletionActionDelete, PersistentVolumes: api.WorkspaceDeletionActionDelete, Checkpoints: api.WorkspaceDeletionActionDelete}
	for range 30 {
		out, err := driver(c, native).DeleteAllocation(t.Context(), request.Key, identity, policy)
		if err != nil {
			t.Fatal(err)
		}
		if out.State == sdk.AllocationDeleted {
			return
		}
	}
	t.Fatal("allocation never deleted")
}
func checkpointDeleted(t *testing.T, c client.Client, native *nativeFixture, cp *api.ExecutionWorkspaceCheckpoint) {
	t.Helper()
	current := &api.ExecutionWorkspaceCheckpoint{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(cp), current); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), current, client.Preconditions{UID: &current.UID}); err != nil {
		t.Fatal(err)
	}
	for range 15 {
		if _, err := checkpointController(c, native).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(cp), &api.ExecutionWorkspaceCheckpoint{}); apierrors.IsNotFound(err) {
			return
		} else if err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("checkpoint finalizer never released")
}

func TestCheckpointExportSurvivesSourceDeletionAndTransfersBeforeColdBoot(t *testing.T) {
	c, native, source, stopped := suspendedSource(t)
	cp := exportReady(t, c, native, newCheckpoint(t, c, source, false))
	allocationDeleted(t, c, native, source, stopped.Identity)
	if len(native.tags) != 1 || len(native.templates) != 2 {
		t.Fatal("source deletion removed independently retained data or restore template")
	}
	request := restoreRequest(t, c, source, cp)
	first := ready(t, c, native, request)
	if first.Identity.InstanceID == stopped.Identity.InstanceID || native.boots != 2 {
		t.Fatal("checkpoint import did not cold boot a fresh Actor")
	}
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	_, artifact, err := driver(c, native).readCatalogReference(t.Context(), request.Key.Namespace, record.InheritedCatalog)
	if err != nil {
		t.Fatal(err)
	}
	if !artifact.Owners[workspaceArtifactOwner(request.Key)] {
		t.Fatal("cold boot occurred without durable workspace ownership")
	}
	checkpointDeleted(t, c, native, cp)
	if len(native.tags) != 1 || len(native.templates) != 3 {
		t.Fatal("checkpoint deletion removed artifacts inherited by the new workspace")
	}
	retired(t, c, native, request, first.Identity, false)
	allocationDeleted(t, c, native, request, first.Identity)
	if len(native.tags) != 0 || len(native.templates) != 1 {
		t.Fatal("last reference did not collect native Tag and exact restore templates")
	}
}

func TestCheckpointLostImportWriteRecoversAfterPublicReferenceDeletion(t *testing.T) {
	c, native, source, stopped := suspendedSource(t)
	cp := exportReady(t, c, native, newCheckpoint(t, c, source, false))
	request := restoreRequest(t, c, source, cp)
	lost := false
	wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		cm, ok := obj.(*corev1.ConfigMap)
		if ok && cm.Data[catalogDataKey] != "" && !lost {
			var artifact checkpointArtifact
			if json.Unmarshal([]byte(cm.Data[catalogDataKey]), &artifact) == nil && artifact.Owners[workspaceArtifactOwner(request.Key)] {
				if err := inner.Update(ctx, obj, opts...); err != nil {
					return err
				}
				lost = true
				return fmt.Errorf("lost owner acquisition response")
			}
		}
		return inner.Update(ctx, obj, opts...)
	}})
	if _, err := driver(wrapped, native).EnsureAllocation(t.Context(), request); err == nil || !lost {
		t.Fatal("owner acquisition response was not lost")
	}
	if native.boots != 1 {
		t.Fatal("uncertain import booted before durable intent")
	}
	allocationDeleted(t, c, native, source, stopped.Identity)
	checkpointDeleted(t, c, native, cp)
	first := ready(t, c, native, request)
	if native.boots != 2 {
		t.Fatal("durably acquired import was not recovered after checkpoint finalization")
	}
	retired(t, c, native, request, first.Identity, false)
	allocationDeleted(t, c, native, request, first.Identity)
}

func TestCheckpointImportRejectsChangedPublicOrNativeIdentityBeforeBoot(t *testing.T) {
	for _, change := range []string{"checkpointUID", "digest", "class", "privateTagUID", "fullMemory", "templateUID", "image", "workerPoolUID"} {
		t.Run(change, func(t *testing.T) {
			c, native, source, _ := suspendedSource(t)
			cp := exportReady(t, c, native, newCheckpoint(t, c, source, false))
			request := restoreRequest(t, c, source, cp)
			_, _, err := driver(c, native).readExport(t.Context(), cp)
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "checkpointUID":
				request.RestoreFrom.UID += "changed"
			case "digest":
				request.RestoreFrom.Digest = "sha256:" + strings.Repeat("f", 64)
			case "class":
				request.Runtime.ClassBinding.Generation++
			case "privateTagUID":
				for _, tag := range native.tags {
					tag.Metadata.Uid += "changed"
				}
			case "fullMemory":
				for _, tag := range native.tags {
					tag.Status.Snapshot.ContentScope = pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
				}
			case "templateUID":
				for key, template := range native.templates {
					if key != "native/base" {
						template.Metadata.Uid += "changed"
					}
				}
			case "image":
				request.Runtime.Template.Spec.Containers[0].Image = "fixture.invalid/changed@sha256:" + strings.Repeat("1", 64)
			case "workerPoolUID":
				pool := &unstructured.Unstructured{}
				pool.SetAPIVersion("ate.dev/v1alpha1")
				pool.SetKind("WorkerPool")
				if err := c.Get(t.Context(), types.NamespacedName{Namespace: "native-workers", Name: "native-workers"}, pool); err != nil {
					t.Fatal(err)
				}
				pool.SetUID(pool.GetUID() + "changed")
				if err := c.Update(t.Context(), pool); err != nil {
					t.Fatal(err)
				}
			}
			request.Revision, _ = sdk.WorkloadRevision(request)
			if err := admit(c)(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil {
				t.Fatal("changed checkpoint identity or restore layout accepted")
			}
			if native.boots != 1 || len(native.actors) != 0 {
				t.Fatal("invalid checkpoint import materialized an Actor")
			}
		})
	}
}

func TestCheckpointCatalogLossClosesImportAndSourceCleanup(t *testing.T) {
	c, native, source, stopped := suspendedSource(t)
	cp := exportReady(t, c, native, newCheckpoint(t, c, source, false))
	request := restoreRequest(t, c, source, cp)
	_, index, err := driver(c, native).readExport(t.Context(), cp)
	if err != nil {
		t.Fatal(err)
	}
	cm, _, err := driver(c, native).readCatalogReference(t.Context(), cp.Namespace, &index.Artifact)
	if err != nil {
		t.Fatal(err)
	}
	cm.Finalizers = nil
	if err := c.Update(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil {
		t.Fatal("missing private catalog admitted import")
	}
	policy := api.ExecutionWorkspaceDeletionPolicy{ProviderResources: api.WorkspaceDeletionActionDelete, PersistentVolumes: api.WorkspaceDeletionActionDelete, Checkpoints: api.WorkspaceDeletionActionDelete}
	if _, err := driver(c, native).DeleteAllocation(t.Context(), source.Key, stopped.Identity, policy); err == nil {
		t.Fatal("missing live catalog allowed source cleanup")
	}
	if len(native.tags) != 1 || len(native.templates) != 2 || native.boots != 1 {
		t.Fatal("catalog loss mutated retained artifacts")
	}
}

func TestCheckpointExportDoesNotInterruptWorkAndRequiresExplicitRecovery(t *testing.T) {
	for _, recover := range []bool{false, true} {
		t.Run(fmt.Sprintf("recover=%v", recover), func(t *testing.T) {
			c, native, source, _ := suspendedSource(t)
			w := &api.ExecutionWorkspace{}
			if err := c.Get(t.Context(), clientKey(source.Key), w); err != nil {
				t.Fatal(err)
			}
			w.Spec.DesiredState = api.ExecutionWorkspaceDesiredReady
			w.Spec.Attachment = &api.ExecutionWorkspaceAttachment{TaskRef: api.ObjectIdentityReference{Name: "uncertain-task", UID: "task-uid"}, Epoch: 1}
			if err := c.Update(t.Context(), w); err != nil {
				t.Fatal(err)
			}
			w.Status.State = api.ExecutionWorkspaceStateFailed
			if err := c.Status().Update(t.Context(), w); err != nil {
				t.Fatal(err)
			}
			cp := newCheckpoint(t, c, source, recover)
			if recover {
				exportReady(t, c, native, cp)
			} else {
				for range 4 {
					if _, err := checkpointController(c, native).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
						t.Fatal(err)
					}
				}
				if err := c.Get(t.Context(), client.ObjectKeyFromObject(cp), cp); err != nil {
					t.Fatal(err)
				}
				if cp.Status.Phase == "Ready" {
					t.Fatal("failed source exported without explicit recovery")
				}
			}
			if native.suspends != 1 || native.boots != 1 {
				t.Fatal("export interrupted or replayed source work")
			}
		})
	}
}

func TestCheckpointFinalizerWaitsForPublishedImportOwnership(t *testing.T) {
	c, native, source, _ := suspendedSource(t)
	cp := exportReady(t, c, native, newCheckpoint(t, c, source, false))
	request := restoreRequest(t, c, source, cp)
	if err := c.Delete(t.Context(), cp, client.Preconditions{UID: &cp.UID}); err != nil {
		t.Fatal(err)
	}
	if _, err := checkpointController(c, native).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	current := &api.ExecutionWorkspaceCheckpoint{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(cp), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != "Deleting" || len(current.Finalizers) == 0 {
		t.Fatal("checkpoint did not close new admission while preserving the published import")
	}
	if _, err := checkpointController(c, native).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	first := ready(t, c, native, request)
	for range 10 {
		if _, err := checkpointController(c, native).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(cp), current); apierrors.IsNotFound(err) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(cp), current); !apierrors.IsNotFound(err) {
		t.Fatal("checkpoint did not finalize after exact ownership transfer")
	}
	if _, err := driver(c, native).Observe(t.Context(), request.Key); err != nil {
		t.Fatal(err)
	}
	retired(t, c, native, request, first.Identity, false)
}

func TestNativeWorkerSelectorsDoNotUnionAcrossSharedPool(t *testing.T) {
	c, native, source := fixture(t, false)
	first := ready(t, c, native, source)
	secondRequest := restoreRequest(t, c, source, nil)
	second := ready(t, c, native, secondRequest)
	for _, item := range []struct {
		request  sdk.WorkloadRequest
		identity sdk.InstanceIdentity
		foreign  sdk.InstanceIdentity
	}{{source, first.Identity, second.Identity}, {secondRequest, second.Identity, first.Identity}} {
		_, record, err := driver(c, native).read(t.Context(), item.request.Key)
		if err != nil {
			t.Fatal(err)
		}
		selector := nativeNetworkPolicy(record).PodSelector.MatchLabels
		pod := &corev1.Pod{}
		if err := c.Get(t.Context(), types.NamespacedName{Namespace: record.Worker.Namespace, Name: record.Worker.Pod}, pod); err != nil {
			t.Fatal(err)
		}
		if selector[workerAllocationLabel] != item.identity.AllocationID || selector[workerInstanceLabel] != item.identity.InstanceID || pod.Labels[workerInstanceLabel] != item.identity.InstanceID || selector[workerInstanceLabel] == item.foreign.InstanceID || selector[workerPoolLabel] != "" {
			t.Fatal("worker egress selectors include another instance in the shared WorkerPool")
		}
	}
}

func TestNativeWorkerForeignLabelsAndCapacityBlockReadinessAndTeardown(t *testing.T) {
	for _, change := range []string{"label", "capacity"} {
		t.Run(change, func(t *testing.T) {
			c, native, request := fixture(t, true)
			first := ready(t, c, native, request)
			_, record, err := driver(c, native).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			pod := &corev1.Pod{}
			if err := c.Get(t.Context(), types.NamespacedName{Namespace: record.Worker.Namespace, Name: record.Worker.Pod}, pod); err != nil {
				t.Fatal(err)
			}
			if change == "label" {
				pod.Labels[workerInstanceLabel] = "foreign-instance"
				if err := c.Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			} else {
				native.workers[record.Worker.Name].Status.Capacity.Actors = 2
			}
			if _, err := driver(c, native).Observe(t.Context(), request.Key); err == nil {
				t.Fatal("changed worker ownership or capacity reported Ready")
			}
			if _, err := driver(c, native).SuspendInstance(t.Context(), request.Key, first.Identity); err == nil {
				t.Fatal("changed worker ownership or capacity allowed capture")
			}
			if _, err := driver(c, native).StopInstance(t.Context(), request.Key, first.Identity); err == nil {
				t.Fatal("changed worker ownership or capacity allowed teardown")
			}
			if native.suspends != 0 || native.workers[record.Worker.Name].Status.State != pb.WorkerState_WORKER_STATE_ACTIVE {
				t.Fatal("ownership rejection mutated native worker or Actor")
			}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
				t.Fatal("ownership rejection deleted the worker Pod")
			}
		})
	}
}
