package substrate

import (
	"reflect"
	"testing"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestCheckpointNewExportRequiresAvailableExactProvider(t *testing.T) {
	for _, scenario := range []string{"deleting", "disabled", "foreign controller", "replacement provider", "changed source UID", "missing source"} {
		t.Run(scenario, func(t *testing.T) {
			c, native, request, _ := suspendedSource(t)
			checkpoint := newCheckpoint(t, c, request, false)
			provider := &api.ExecutionWorkspaceProvider{}
			if err := c.Get(t.Context(), types.NamespacedName{Name: "substrate"}, provider); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "disabled":
				provider.Spec.LifecycleState = api.ExecutionWorkspaceProviderDisabled
			case "foreign controller":
				provider.Spec.ControllerName = "other.example.test"
			case "replacement provider":
				if err := c.Delete(t.Context(), provider, client.Preconditions{UID: &provider.UID}); err != nil {
					t.Fatal(err)
				}
				provider.ResourceVersion = ""
				provider.UID = "replacement-provider-uid"
				if err := c.Create(t.Context(), provider); err != nil {
					t.Fatal(err)
				}
			case "changed source UID":
				checkpoint.Spec.WorkspaceRef.UID = "other-workspace-uid"
				if err := c.Update(t.Context(), checkpoint); err != nil {
					t.Fatal(err)
				}
			case "missing source":
				workspace := &api.ExecutionWorkspace{}
				if err := c.Get(t.Context(), clientKey(request.Key), workspace); err != nil {
					t.Fatal(err)
				}
				if err := c.Delete(t.Context(), workspace, client.Preconditions{UID: &workspace.UID}); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.Update(t.Context(), provider); err != nil {
				t.Fatal(err)
			}
			if scenario != "disabled" {
				deleteCheckpointProvider(t, c, provider)
			}
			before := checkpoint.DeepCopy()
			journals := &corev1.ConfigMapList{}
			if err := c.List(t.Context(), journals); err != nil {
				t.Fatal(err)
			}
			for range 3 {
				result, err := checkpointController(c, native).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(checkpoint)})
				if err != nil {
					t.Fatal(err)
				}
				if scenario == "missing source" && result.RequeueAfter <= 0 {
					t.Fatal("missing source did not receive a bounded ownership-safe retry")
				}
			}
			current := &api.ExecutionWorkspaceCheckpoint{}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(checkpoint), current); err != nil {
				t.Fatal(err)
			}
			if controllerutil.ContainsFinalizer(current, checkpointFinalizer) || current.Status.Digest != "" || current.Status.ClassBinding != nil || current.Status.CreatedAt != nil {
				t.Fatal("unavailable or foreign provider established a new checkpoint obligation")
			}
			if scenario == "foreign controller" || scenario == "missing source" {
				if !reflect.DeepEqual(before, current) {
					t.Fatal("checkpoint ownership was claimed without a selected exact provider and source")
				}
			} else {
				wantReason := "SourceChanged"
				if scenario == "deleting" {
					wantReason = "Deleting"
				} else if scenario == "disabled" {
					wantReason = "Disabled"
				}
				condition := meta.FindStatusCondition(current.Status.Conditions, "Ready")
				if condition == nil || condition.Reason != wantReason {
					t.Fatal("checkpoint did not report the exact admission boundary")
				}
			}
			assertCheckpointJournalsUnchanged(t, c, journals)
			if native.boots != 1 || native.suspends != 1 || len(native.tags) != 1 || len(native.templates) != 2 {
				t.Fatal("new checkpoint admission changed native retained data or replayed source work")
			}
		})
	}
}

func TestCheckpointRecordedExportProgressesAndRetiresWithUnavailableProvider(t *testing.T) {
	for _, deleting := range []bool{true, false} {
		name := "deleting"
		if !deleting {
			name = "disabled"
		}
		t.Run(name, func(t *testing.T) {
			c, native, request, stopped := suspendedSource(t)
			checkpoint := newCheckpoint(t, c, request, false)
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(checkpoint)}
			// Record the exact selected artifact before registration withdrawal,
			// but leave its retained public reference acquisition unfinished.
			for range 2 {
				if _, err := checkpointController(c, native).Reconcile(t.Context(), req); err != nil {
					t.Fatal(err)
				}
			}
			_, index, err := driver(c, native).readExport(t.Context(), checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			_, artifact, err := driver(c, native).readCatalogReference(t.Context(), checkpoint.Namespace, &index.Artifact)
			if err != nil {
				t.Fatal(err)
			}
			if artifact.Owners[publicArtifactOwner(checkpoint)] {
				t.Fatal("fixture already completed export before provider withdrawal")
			}
			provider := &api.ExecutionWorkspaceProvider{}
			if err := c.Get(t.Context(), types.NamespacedName{Name: artifact.ProviderBinding.Name}, provider); err != nil {
				t.Fatal(err)
			}
			if deleting {
				deleteCheckpointProvider(t, c, provider)
			} else {
				provider.Spec.LifecycleState = api.ExecutionWorkspaceProviderDisabled
				if err := c.Update(t.Context(), provider); err != nil {
					t.Fatal(err)
				}
			}
			checkpoint = exportReady(t, c, native, checkpoint)
			_, artifact, err = driver(c, native).readCatalogReference(t.Context(), checkpoint.Namespace, &index.Artifact)
			if err != nil || !artifact.Owners[publicArtifactOwner(checkpoint)] {
				t.Fatal("recorded export could not complete its retained reference after withdrawal")
			}
			checkpointDeleted(t, c, native, checkpoint)
			if err := c.Get(t.Context(), exportKey(checkpoint.Namespace, checkpoint.UID), &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
				t.Fatal("checkpoint cleanup retained its export index")
			}
			if len(native.tags) != 1 || len(native.templates) != 2 {
				t.Fatal("checkpoint cleanup deleted data still owned by its source")
			}
			allocationDeleted(t, c, native, request, stopped.Identity)
			if len(native.tags) != 0 || len(native.templates) != 1 || native.boots != 1 || native.suspends != 1 {
				t.Fatal("last exact owner could not retire native data after provider withdrawal")
			}
		})
	}
}

func deleteCheckpointProvider(t *testing.T, c client.Client, provider *api.ExecutionWorkspaceProvider) {
	t.Helper()
	provider.Finalizers = append(provider.Finalizers, "example.test/proof-hold")
	if err := c.Update(t.Context(), provider); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), provider, client.Preconditions{UID: &provider.UID}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(provider), provider); err != nil {
		t.Fatal(err)
	}
	if provider.DeletionTimestamp.IsZero() {
		t.Fatal("fixture provider is not deleting")
	}
}

func assertCheckpointJournalsUnchanged(t *testing.T, c client.Client, before *corev1.ConfigMapList) {
	t.Helper()
	current := &corev1.ConfigMapList{}
	if err := c.List(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, current) {
		t.Fatal("new export admission changed an export index or retained-artifact catalog")
	}
}
