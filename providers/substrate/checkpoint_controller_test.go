// Copyright (c) 2026. MIT License - see LICENSE file for details.

package substrate

import (
	"context"
	"reflect"
	"testing"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestCheckpointMissingSourceRetriesWithoutClaimingOwnership(t *testing.T) {
	for _, transient := range []bool{false, true} {
		name := "unrouted absent source"
		if transient {
			name = "routed source cache miss"
		}
		t.Run(name, func(t *testing.T) {
			c, native, request := fixture(t, true)
			checkpoint := newCheckpoint(t, c, request, false)
			if transient {
				checkpoint.Labels = map[string]string{api.ProviderControllerLabel: ControllerName, "workspace.orka.ai/provider-name": "substrate"}
				if err := c.Update(t.Context(), checkpoint); err != nil {
					t.Fatal(err)
				}
			} else {
				workspace := &api.ExecutionWorkspace{}
				if err := c.Get(t.Context(), clientKey(request.Key), workspace); err != nil {
					t.Fatal(err)
				}
				if err := c.Delete(t.Context(), workspace, client.Preconditions{UID: &workspace.UID}); err != nil {
					t.Fatal(err)
				}
			}
			before := checkpoint.DeepCopy()
			missed := false
			wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, inner client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*api.ExecutionWorkspace); transient && ok && key == clientKey(request.Key) && !missed {
					missed = true
					return apierrors.NewNotFound(api.GroupVersion.WithResource("executionworkspaces").GroupResource(), key.Name)
				}
				return inner.Get(ctx, key, obj, opts...)
			}})
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(checkpoint)}
			result, err := checkpointController(wrapped, native).Reconcile(t.Context(), req)
			if err != nil || result.RequeueAfter <= 0 || result.RequeueAfter > time.Minute {
				t.Fatalf("missing source needs a bounded read-only retry: result=%#v err=%v", result, err)
			}
			current := &api.ExecutionWorkspaceCheckpoint{}
			if err := c.Get(t.Context(), req.NamespacedName, current); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, current) {
				t.Fatal("missing source was claimed or its status was changed without ownership")
			}
			if transient {
				if _, err := checkpointController(wrapped, native).Reconcile(t.Context(), req); err != nil {
					t.Fatal(err)
				}
				if err := c.Get(t.Context(), req.NamespacedName, current); err != nil {
					t.Fatal(err)
				}
				if !missed || !reflect.DeepEqual(current.Status, before.Status) || !controllerutil.ContainsFinalizer(current, checkpointFinalizer) {
					t.Fatal("retry did not resolve the exact source before acquiring provider ownership")
				}
			}
			if native.boots != 0 || native.suspends != 0 || len(native.actors) != 0 || len(native.tags) != 0 || len(native.templates) != 1 {
				t.Fatal("missing-source resolution mutated the native backend")
			}
		})
	}
}

func TestCheckpointCatalogCleanupRequiresExactProviderOwner(t *testing.T) {
	for _, ownership := range []string{"matching active", "matching draining", "matching disabled", "matching deleting", "missing", "replacement same controller", "replacement foreign controller", "matching UID foreign controller"} {
		t.Run(ownership, func(t *testing.T) {
			c, native, request, _ := suspendedSource(t)
			d := driver(c, native)
			_, record, err := d.read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			before, artifact, err := d.readCatalogReference(t.Context(), request.Key.Namespace, record.CheckpointCatalog)
			if err != nil {
				t.Fatal(err)
			}
			before = before.DeepCopy()
			if len(native.tags) != 1 || native.tags[artifact.Checkpoint.Atespace+"/"+artifact.Checkpoint.Tag.Name] == nil || native.templates[artifact.Checkpoint.Atespace+"/"+artifact.Checkpoint.Template.Name] == nil {
				t.Fatal("fixture lacks the exact retained native artifacts")
			}
			workspace := &api.ExecutionWorkspace{}
			if err := c.Get(t.Context(), clientKey(request.Key), workspace); err != nil {
				t.Fatal(err)
			}
			// An orphaned workspace may be collected only while the exact
			// provider registration still identifies this controller as owner.
			if err := c.Delete(t.Context(), workspace, client.Preconditions{UID: &workspace.UID}); err != nil {
				t.Fatal(err)
			}
			provider := &api.ExecutionWorkspaceProvider{}
			if err := c.Get(t.Context(), types.NamespacedName{Name: artifact.ProviderBinding.Name}, provider); err != nil {
				t.Fatal(err)
			}
			mayCollect := true
			switch ownership {
			case "matching draining":
				provider.Spec.LifecycleState = api.ExecutionWorkspaceProviderDraining
			case "matching disabled":
				provider.Spec.LifecycleState = api.ExecutionWorkspaceProviderDisabled
			case "matching deleting":
				provider.Finalizers = []string{"example.test/cleanup"}
			case "missing":
				mayCollect = false
				if err := c.Delete(t.Context(), provider, client.Preconditions{UID: &provider.UID}); err != nil {
					t.Fatal(err)
				}
			case "replacement same controller", "replacement foreign controller":
				mayCollect = false
				if err := c.Delete(t.Context(), provider, client.Preconditions{UID: &provider.UID}); err != nil {
					t.Fatal(err)
				}
				provider = provider.DeepCopy()
				provider.ResourceVersion = ""
				provider.UID = "replacement-provider-uid"
				if ownership == "replacement foreign controller" {
					provider.Spec.ControllerName = "other.example.test"
				}
				if err := c.Create(t.Context(), provider); err != nil {
					t.Fatal(err)
				}
			case "matching UID foreign controller":
				mayCollect = false
				provider.Spec.ControllerName = "other.example.test"
			}
			if ownership != "missing" && ownership != "replacement same controller" && ownership != "replacement foreign controller" {
				if err := c.Update(t.Context(), provider); err != nil {
					t.Fatal(err)
				}
			}
			if ownership == "matching deleting" {
				if err := c.Delete(t.Context(), provider, client.Preconditions{UID: &provider.UID}); err != nil {
					t.Fatal(err)
				}
			}
			req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: before.Namespace, Name: "catalog/" + before.Name}}
			for range 8 {
				if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
					t.Fatal(err)
				}
			}
			current := &corev1.ConfigMap{}
			err = c.Get(t.Context(), client.ObjectKeyFromObject(before), current)
			if mayCollect {
				if !apierrors.IsNotFound(err) || len(native.tags) != 0 || native.templates[artifact.Checkpoint.Atespace+"/"+artifact.Checkpoint.Template.Name] != nil {
					t.Fatalf("exact provider could not collect orphaned checkpoint data: %v", err)
				}
			} else {
				if err != nil || !reflect.DeepEqual(before, current) || len(native.tags) != 1 || native.templates[artifact.Checkpoint.Atespace+"/"+artifact.Checkpoint.Template.Name] == nil {
					t.Fatalf("unproven provider owner mutated or collected retained data: %v", err)
				}
			}
			if native.templates["native/base"] == nil || native.boots != 1 || native.suspends != 1 {
				t.Fatal("catalog reconciliation changed unrelated infrastructure or replayed work")
			}
		})
	}
}
