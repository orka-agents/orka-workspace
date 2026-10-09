// Copyright (c) 2026. MIT License - see LICENSE file for details.

package sandbox

import (
	"reflect"
	"strings"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestSandboxRejectsUnadvertisedRequiredFeaturesBeforeEffects(t *testing.T) {
	for _, feature := range []workspacev1alpha1.ExecutionWorkspaceFeature{
		workspacev1alpha1.WorkspaceFeatureNativeProcess, workspacev1alpha1.WorkspaceFeaturePools,
		workspacev1alpha1.WorkspaceFeatureExec, workspacev1alpha1.WorkspaceFeatureFiles,
		workspacev1alpha1.WorkspaceFeatureReset, workspacev1alpha1.WorkspaceFeatureCheckpoint,
		workspacev1alpha1.WorkspaceFeatureRestore, workspacev1alpha1.WorkspaceFeatureServicePorts,
		workspacev1alpha1.WorkspaceFeatureTLS, "future.feature",
	} {
		t.Run(string(feature), func(t *testing.T) {
			c, request := fixture(t, false)
			request.Runtime.RequiredFeatures = []workspacev1alpha1.ExecutionWorkspaceFeature{workspacev1alpha1.WorkspaceFeatureACPRuntime, feature}
			request.Revision, _ = workspaceprovider.WorkloadRevision(request)
			if err := admit(t, c)(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			before := &workspacev1alpha1.ExecutionWorkspace{}
			key := client.ObjectKey{Namespace: request.Key.Namespace, Name: request.Key.Name}
			if err := c.Get(t.Context(), key, before); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), "required feature") || observed.Startup != nil {
					t.Fatalf("unsupported feature produced allocation: %+v, %v", observed, err)
				}
			}
			requireNoSandboxAllocation(t, c, request)
			after := &workspacev1alpha1.ExecutionWorkspace{}
			if err := c.Get(t.Context(), key, after); err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("unsupported feature wrote recovery marker or status: %v", err)
			}
		})
	}
}

func TestSandboxAcceptsAdvertisedFeaturesWithRequiredDataProfile(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(map[bool]string{false: "ACP runtime", true: "data suspension"}[persistent], func(t *testing.T) {
			c, request := fixture(t, persistent)
			request.Runtime.RequiredFeatures = []workspacev1alpha1.ExecutionWorkspaceFeature{workspacev1alpha1.WorkspaceFeatureACPRuntime}
			if persistent {
				request.Runtime.RequiredFeatures = append(request.Runtime.RequiredFeatures, workspacev1alpha1.WorkspaceFeatureSuspend)
			}
			request.Revision, _ = workspaceprovider.WorkloadRevision(request)
			if err := admit(t, c)(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			observed := ready(t, c, request)
			if observed.Startup == nil || observed.Startup.Pod == nil || observed.Startup.Process != nil {
				t.Fatal("advertised features did not produce exact Pod startup")
			}
			if err := workspaceprovider.ValidateStartup(request, observed); err != nil {
				t.Fatal(err)
			}
		})
	}
	// Advertising suspension does not make an ephemeral profile durable.
	c, request := fixture(t, false)
	request.Runtime.RequiredFeatures = []workspacev1alpha1.ExecutionWorkspaceFeature{workspacev1alpha1.WorkspaceFeatureSuspend}
	request.Revision, _ = workspaceprovider.WorkloadRevision(request)
	if err := admit(t, c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := New(c).EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), "data-only") {
		t.Fatalf("required suspension accepted without durable profile: %v", err)
	}
	requireNoSandboxAllocation(t, c, request)
}

func TestSandboxRejectsCheckpointImportWithoutFeatureMarker(t *testing.T) {
	c, request := fixture(t, false)
	request.RestoreFrom = &workspacev1alpha1.WorkloadCheckpointReference{Name: "checkpoint", UID: "checkpoint-uid", Digest: "sha256:" + strings.Repeat("a", 64)}
	request.Revision, _ = workspaceprovider.WorkloadRevision(request)
	if err := admit(t, c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), "checkpoint import") || observed.Startup != nil {
		t.Fatalf("unmarked checkpoint import was ignored: %+v, %v", observed, err)
	}
	requireNoSandboxAllocation(t, c, request)
}

func TestLegacyUnadvertisedSandboxFeatureWithdrawsStartupButStillCleansUp(t *testing.T) {
	c, request := fixture(t, false)
	ready(t, c, request)
	// Preserve the original exact native resources while modeling an older
	// journal that accepted an unsupported native-process requirement.
	request.Runtime.RequiredFeatures = []workspacev1alpha1.ExecutionWorkspaceFeature{workspacev1alpha1.WorkspaceFeatureNativeProcess}
	request.Revision, _ = workspaceprovider.WorkloadRevision(request)
	if err := admit(t, c)(t.Context(), request); err != nil {
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
	if observed, err := New(c).Observe(t.Context(), request.Key); err == nil || !strings.Contains(err.Error(), "required feature") || observed.Startup != nil {
		t.Fatalf("legacy native marker still produced Pod startup: %+v, %v", observed, err)
	}
	if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || observed.Startup != nil {
		t.Fatalf("legacy unsupported request still produced allocation: %+v, %v", observed, err)
	}
	id := record.Observation.Identity
	driver := &simulatedLifecycle{New(c)}
	var stopped workspaceprovider.AllocationObservation
	for range 5 {
		stopped, err = driver.StopInstance(t.Context(), request.Key, id)
		if err != nil {
			t.Fatal(err)
		}
		if stopped.State == workspaceprovider.AllocationStopped {
			break
		}
	}
	if stopped.State != workspaceprovider.AllocationStopped || stopped.Startup != nil {
		t.Fatal("unsupported legacy feature blocked exact stop")
	}
	for range 10 {
		deleted, err := driver.DeleteAllocation(t.Context(), request.Key, id, cleanupPolicy())
		if err != nil {
			t.Fatal(err)
		}
		if deleted.State == workspaceprovider.AllocationDeleted {
			if deleted.Startup != nil {
				t.Fatal("deleted legacy allocation retained startup")
			}
			return
		}
	}
	t.Fatal("unsupported legacy feature blocked exact delete")
}
