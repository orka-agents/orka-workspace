package substrate

import (
	"testing"

	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestGVisorActorTemplateUsesSandboxResourceLimits(t *testing.T) {
	c, native, request := fixture(t, false)
	request.Runtime.Template.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("1500m"), corev1.ResourceMemory: resource.MustParse("512Mi"),
	}
	request.Resources = *request.Runtime.Template.Spec.Containers[0].Resources.DeepCopy()
	request.Revision, _ = sdk.WorkloadRevision(request)
	if err := admit(c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	ready(t, c, native, request)
	for name, template := range native.templates {
		if name == "native/base" {
			continue
		}
		if template.GetSandboxConfig().GetSandboxClass() != pb.SandboxClass_SANDBOX_CLASS_GVISOR || template.Containers[0].Resources != nil {
			t.Fatal("gVisor template contains unsupported per-container limits")
		}
		limits := template.GetResources().GetLimits()
		if len(limits) != 2 || limits[0].Name != "cpu" || limits[0].Quantity != "1500m" || limits[1].Name != "memory" || limits[1].Quantity != "512Mi" {
			t.Fatalf("admitted sandbox limits were lost: %v", limits)
		}
	}
}

func TestCheckpointImportRejectsChangedSourceWorkerSpecification(t *testing.T) {
	for _, field := range []string{"workerImage", "resources", "template"} {
		t.Run(field, func(t *testing.T) {
			c, native, source, _ := suspendedSource(t)
			cp := exportReady(t, c, native, newCheckpoint(t, c, source, false))
			request := restoreRequest(t, c, source, cp)
			pool := poolObject()
			if err := c.Get(t.Context(), client.ObjectKey{Namespace: "native-workers", Name: "native-workers"}, pool); err != nil {
				t.Fatal(err)
			}
			var value any = "foreign-native-worker"
			if field == "resources" {
				value = map[string]any{"limits": map[string]any{"cpu": "2"}}
			} else if field == "template" {
				value = map[string]any{"labels": map[string]any{"foreign": "infrastructure"}}
			}
			if err := unstructured.SetNestedField(pool.Object, value, "spec", field); err != nil {
				t.Fatal(err)
			}
			if err := c.Update(t.Context(), pool); err != nil {
				t.Fatal(err)
			}
			if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil {
				t.Fatal("same-UID source infrastructure mutation admitted a checkpoint import")
			}
			if native.boots != 1 || len(native.actors) != 0 {
				t.Fatal("changed infrastructure materialized a fresh Actor")
			}
		})
	}
}

func TestLostJournalCatalogReferenceWaitsForNativeCollection(t *testing.T) {
	c, native, request, _ := suspendedSource(t)
	d := driver(c, native)
	_, record, err := d.read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	// Catalog ownership survived a lost journal write. Rediscovery must keep
	// following its release tombstone through every asynchronous deletion.
	record.CheckpointCatalog, record.InheritedCatalog = nil, nil
	completed := false
	for range 10 {
		complete, err := d.releaseRecordArtifacts(t.Context(), record)
		if err != nil {
			t.Fatal(err)
		}
		if complete {
			if len(native.tags) != 0 || len(native.templates) != 1 {
				t.Fatal("cleanup completed before native data and its restore template were collected")
			}
			completed = true
			break
		}
	}
	if !completed {
		t.Fatal("rediscovered catalog never completed collection")
	}
}
