// Copyright (c) 2026. MIT License - see LICENSE file for details.

package fake

import (
	"reflect"
	"strings"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestPodRejectsUnadvertisedRequiredFeaturesBeforeEffects(t *testing.T) {
	for _, feature := range []workspacev1alpha1.ExecutionWorkspaceFeature{
		workspacev1alpha1.WorkspaceFeatureNativeProcess, workspacev1alpha1.WorkspaceFeatureSuspend,
		workspacev1alpha1.WorkspaceFeatureExec, workspacev1alpha1.WorkspaceFeatureFiles,
		workspacev1alpha1.WorkspaceFeatureReset, workspacev1alpha1.WorkspaceFeatureCheckpoint,
		workspacev1alpha1.WorkspaceFeatureRestore, workspacev1alpha1.WorkspaceFeatureServicePorts,
		workspacev1alpha1.WorkspaceFeatureTLS, "future.feature",
	} {
		t.Run(string(feature), func(t *testing.T) {
			c, request := runtimeFixture(t)
			request.Runtime.RequiredFeatures = []workspacev1alpha1.ExecutionWorkspaceFeature{workspacev1alpha1.WorkspaceFeatureACPRuntime, feature}
			request.Revision, _ = workspaceprovider.WorkloadRevision(request)
			if err := publishRequest(t.Context(), c, request); err != nil {
				t.Fatal(err)
			}
			before := workspaceFor(t, c, request.Key)
			for range 2 {
				observed, err := New(c).EnsureAllocation(t.Context(), request)
				if err == nil || !strings.Contains(err.Error(), "required feature") || observed.Startup != nil {
					t.Fatalf("unsupported feature produced allocation: %+v, %v", observed, err)
				}
			}
			var journals corev1.ConfigMapList
			if err := c.List(t.Context(), &journals); err != nil {
				t.Fatal(err)
			}
			if c.creates != 0 || len(journals.Items) != 0 || !reflect.DeepEqual(before, workspaceFor(t, c, request.Key)) {
				t.Fatal("unsupported feature wrote allocation intent, recovery marker, or Pod")
			}
		})
	}
}

func TestPodAcceptsExactlyAdvertisedRequiredFeatures(t *testing.T) {
	c, request := runtimeFixture(t)
	request.Runtime.RequiredFeatures = []workspacev1alpha1.ExecutionWorkspaceFeature{workspacev1alpha1.WorkspaceFeatureACPRuntime, workspacev1alpha1.WorkspaceFeaturePools}
	request.Revision, _ = workspaceprovider.WorkloadRevision(request)
	if err := publishRequest(t.Context(), c, request); err != nil {
		t.Fatal(err)
	}
	observed, err := New(c).EnsureAllocation(t.Context(), request)
	if err != nil || observed.State != workspaceprovider.AllocationReady || observed.Startup == nil || observed.Startup.Pod == nil || observed.Startup.Process != nil || c.creates != 1 {
		t.Fatalf("supported Pod features did not produce exact Pod startup: %+v, %v", observed, err)
	}
	if err := workspaceprovider.ValidateStartup(request, observed); err != nil {
		t.Fatal(err)
	}
}

func TestPodRejectsCheckpointImportWithoutFeatureMarker(t *testing.T) {
	c, request := runtimeFixture(t)
	request.RestoreFrom = &workspacev1alpha1.WorkloadCheckpointReference{Name: "checkpoint", UID: "checkpoint-uid", Digest: "sha256:" + strings.Repeat("a", 64)}
	request.Revision, _ = workspaceprovider.WorkloadRevision(request)
	if err := publishRequest(t.Context(), c, request); err != nil {
		t.Fatal(err)
	}
	if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), "checkpoint import") || observed.Startup != nil {
		t.Fatalf("unmarked checkpoint import was ignored: %+v, %v", observed, err)
	}
	var journals corev1.ConfigMapList
	if err := c.List(t.Context(), &journals); err != nil || len(journals.Items) != 0 || c.creates != 0 {
		t.Fatalf("checkpoint import wrote allocation effects: %v", err)
	}
}

func TestLegacyUnadvertisedPodFeatureWithdrawsStartupButStillCleansUp(t *testing.T) {
	c, request := runtimeFixture(t)
	first, err := New(c).EnsureAllocation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	// Model a journal accepted by the older adapter, with the same exact Pod
	// lifetime and a valid frozen request that requires native process evidence.
	request.Runtime.RequiredFeatures = []workspacev1alpha1.ExecutionWorkspaceFeature{workspacev1alpha1.WorkspaceFeatureNativeProcess}
	request.Revision, _ = workspaceprovider.WorkloadRevision(request)
	if err := publishRequest(t.Context(), c, request); err != nil {
		t.Fatal(err)
	}
	cm, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	record.Request = request
	record.Observation.Identity.RequestRevision = request.Revision
	record.Observation.Startup.Identity = record.Observation.Identity
	if err := New(c).save(t.Context(), cm, record); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: first.Startup.Pod.Namespace, Name: first.Startup.Pod.Name}, pod); err != nil {
		t.Fatal(err)
	}
	pod.Annotations[podRequestAnnotation] = request.Revision
	if err := c.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	if observed, err := New(c).Observe(t.Context(), request.Key); err == nil || !strings.Contains(err.Error(), "required feature") || observed.Startup != nil {
		t.Fatalf("legacy native marker still produced Pod startup: %+v, %v", observed, err)
	}
	if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || observed.Startup != nil {
		t.Fatalf("legacy unsupported request still produced allocation: %+v, %v", observed, err)
	}
	id := record.Observation.Identity
	for range 2 {
		if _, err := New(c).StopInstance(t.Context(), request.Key, id); err != nil {
			t.Fatal(err)
		}
	}
	deleted, err := New(c).DeleteAllocation(t.Context(), request.Key, id, deletionPolicy())
	if err != nil || deleted.State != workspaceprovider.AllocationDeleted || deleted.Startup != nil || c.creates != 1 {
		t.Fatalf("unsupported legacy feature blocked exact cleanup or replayed compute: %+v, %v", deleted, err)
	}
}
