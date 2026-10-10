// Copyright (c) 2026. MIT License - see LICENSE file for details.

package conformance

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	structuralcel "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"sigs.k8s.io/yaml"
)

// This independent lifecycle model requires the exact persisted request before
// checking lineage. It cannot satisfy the adversarial check by denying admission.
// Factory calls share the store, as provider restarts would.
type continuationStore struct {
	admitted       sdk.WorkloadRequest
	current        *sdk.WorkloadRequest
	observed       sdk.AllocationObservation
	history        map[int64]sdk.AllocationObservation
	forgedAttempts int
	ignoreLineage  bool
	deleted        bool
}

func (s *continuationStore) EnsureAllocation(_ context.Context, request sdk.WorkloadRequest) (sdk.AllocationObservation, error) {
	if err := request.Validate(); err != nil {
		return sdk.AllocationObservation{}, err
	}
	if s.current != nil {
		if request.Sequence < s.current.Sequence {
			old, ok := s.history[request.Sequence]
			if !ok || old.Identity.RequestRevision != request.Revision {
				return sdk.AllocationObservation{}, sdk.ErrRequestConflict
			}
			return old, nil
		}
		if request.Sequence == s.current.Sequence {
			if !reflect.DeepEqual(request, *s.current) {
				return sdk.AllocationObservation{}, sdk.ErrRequestConflict
			}
			return s.observed, nil
		}
	}
	if !reflect.DeepEqual(request, s.admitted) {
		return sdk.AllocationObservation{}, sdk.ErrWorkspaceNotAdmitted
	}
	validationRequest := copyRequest(request)
	if request.RetainedData != nil && s.observed.RetainedData != nil && *request.RetainedData != *s.observed.RetainedData {
		s.forgedAttempts++
		if s.ignoreLineage {
			// Negative control: a broken provider trusts the request's lineage
			// instead of comparing it to the stopped instance's durable proof.
			validationRequest.RetainedData = s.observed.RetainedData
			validationRequest.Revision, _ = sdk.WorkloadRevision(validationRequest)
		}
	}
	if err := sdk.ValidateWorkloadTransition(s.current, validationRequest, &s.observed, s.current != nil); err != nil {
		return sdk.AllocationObservation{}, err
	}
	if s.current != nil {
		s.history[s.current.Sequence] = s.observed
	}
	current := copyRequest(request)
	s.current = &current
	identity := sdk.InstanceIdentity{AllocationID: "allocation", InstanceID: "instance-" + strconv.FormatInt(request.Sequence, 10), RequestRevision: request.Revision}
	s.observed = sdk.AllocationObservation{Key: request.Key, Sequence: request.Sequence, Identity: identity, State: sdk.AllocationReady,
		Startup: &sdk.StartupEvidence{ContractVersion: sdk.LifecycleContractV1, Identity: identity, Endpoint: "http://fixture.invalid"}}
	return s.observed, nil
}

func (s *continuationStore) Observe(_ context.Context, key sdk.AllocationKey) (sdk.AllocationObservation, error) {
	if key != s.observed.Key {
		return sdk.AllocationObservation{}, sdk.ErrNotFound
	}
	return s.observed, nil
}

func (s *continuationStore) StopInstance(_ context.Context, key sdk.AllocationKey, identity sdk.InstanceIdentity) (sdk.AllocationObservation, error) {
	if key != s.observed.Key || identity != s.observed.Identity {
		return sdk.AllocationObservation{}, sdk.ErrStaleIdentity
	}
	s.observed.State = sdk.AllocationStopped
	s.observed.Startup = nil
	return s.observed, nil
}

func (s *continuationStore) SuspendInstance(ctx context.Context, key sdk.AllocationKey, identity sdk.InstanceIdentity) (sdk.AllocationObservation, error) {
	if _, err := s.StopInstance(ctx, key, identity); err != nil {
		return sdk.AllocationObservation{}, err
	}
	s.observed.RetainedData = &sdk.RetainedDataReference{ID: "retained-" + identity.InstanceID, SourceInstance: identity, ProofSHA256: "sha256:" + strings.Repeat("b", 64)}
	return s.observed, nil
}

func (s *continuationStore) DeleteAllocation(_ context.Context, key sdk.AllocationKey, identity sdk.InstanceIdentity, policy api.ExecutionWorkspaceDeletionPolicy) (sdk.AllocationObservation, error) {
	if key != s.observed.Key || identity != s.observed.Identity {
		return sdk.AllocationObservation{}, sdk.ErrStaleIdentity
	}
	if s.observed.State != sdk.AllocationStopped {
		return sdk.AllocationObservation{}, sdk.ErrInstanceRunning
	}
	s.deleted = true
	s.observed.State = sdk.AllocationDeleted
	s.observed.RetainedData = nil
	s.observed.Disposition = &api.ExecutionWorkspaceDisposition{Compute: api.DispositionDeleted, AccessCredentials: api.DispositionNotApplicable,
		EphemeralSecrets: api.DispositionNotApplicable, WorkspaceData: api.DispositionDeleted, PersistentVolumes: api.DispositionDeleted,
		Checkpoints: api.DispositionDeleted, ProviderResources: api.DispositionDeleted}
	if err := sdk.ValidateDeletedDisposition(s.observed.Disposition, policy); err != nil {
		return sdk.AllocationObservation{}, err
	}
	return s.observed, nil
}

func continuationFixture() *continuationStore {
	request := sdk.WorkloadRequest{Sequence: 1, Key: sdk.AllocationKey{Namespace: "test", Name: "fixture", WorkspaceUID: "workspace", ProviderUID: "provider"},
		Image: "fixture.invalid/runtime@sha256:" + strings.Repeat("a", 64)}
	request.Revision, _ = sdk.WorkloadRevision(request)
	return &continuationStore{admitted: request, history: map[int64]sdk.AllocationObservation{}}
}

// Validate the real shipped CEL admission rules, not a permissive fake callback.
func continuationAdmission(t *testing.T, store *continuationStore) AdmitRequest {
	t.Helper()
	data, err := os.ReadFile("../config/crd/bases/workspace.orka.ai_executionworkspaces.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.UnmarshalStrict(data, &crd); err != nil {
		t.Fatal(err)
	}
	var schemaProps apiextensions.JSONSchemaProps
	if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(crd.Spec.Versions[0].Schema.OpenAPIV3Schema, &schemaProps, nil); err != nil {
		t.Fatal(err)
	}
	schema, err := structuralschema.NewStructural(&schemaProps)
	if err != nil {
		t.Fatal(err)
	}
	validator := structuralcel.NewValidator(schema, true, celconfig.PerCallLimit)
	object := func(request sdk.WorkloadRequest) map[string]any {
		workspace := &api.ExecutionWorkspace{ObjectMeta: metav1.ObjectMeta{Namespace: request.Key.Namespace, Name: request.Key.Name, UID: request.Key.WorkspaceUID},
			Spec: api.ExecutionWorkspaceSpec{Mode: api.ExecutionWorkspaceModeInteractive, Slot: "default", DesiredState: api.ExecutionWorkspaceDesiredReady,
				ClassBinding:    api.ImmutableObjectBinding{Name: "class", UID: "class", Generation: 1, ProfileHash: "sha256:" + strings.Repeat("a", 64)},
				ProviderBinding: api.ImmutableObjectBinding{Name: "provider", UID: request.Key.ProviderUID, Generation: 1}, Workload: &request,
				Lifecycle: api.ExecutionWorkspaceLifecycle{DefaultOnDetach: api.WorkspaceOnDetachSuspend, AllowedOnDetach: []api.WorkspaceOnDetach{api.WorkspaceOnDetachSuspend, api.WorkspaceOnDetachDelete},
					DetachTimeout: metav1.Duration{Duration: time.Minute}, DeletionPolicy: api.ExecutionWorkspaceDeletionPolicy{ProviderResources: api.WorkspaceDeletionActionDelete, PersistentVolumes: api.WorkspaceDeletionActionDelete, Checkpoints: api.WorkspaceDeletionActionDelete}}}}
		value, err := runtime.DefaultUnstructuredConverter.ToUnstructured(workspace)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	return func(ctx context.Context, next sdk.WorkloadRequest) error {
		errs, _ := validator.Validate(ctx, field.NewPath("object"), schema, object(next), object(store.admitted), celconfig.RuntimeCELCostBudget)
		if len(errs) != 0 {
			return fmt.Errorf("admit workload: %s", errs.ToAggregate())
		}
		store.admitted = copyRequest(next)
		return nil
	}
}

func TestContinuationAdmissionEnforcesImmutableRequestSequence(t *testing.T) {
	store := continuationFixture()
	admit := continuationAdmission(t, store)
	first := store.admitted
	identity := sdk.InstanceIdentity{AllocationID: "allocation", InstanceID: "instance-1", RequestRevision: first.Revision}
	retained := &sdk.RetainedDataReference{ID: "retained", SourceInstance: identity, ProofSHA256: "sha256:" + strings.Repeat("b", 64)}
	next := nextRequest(first, identity, retained)
	if err := admit(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*sdk.WorkloadRequest){
		func(request *sdk.WorkloadRequest) { request.RetainedData.ID += "-foreign" },
		func(request *sdk.WorkloadRequest) { request.Sequence += 2 },
		func(request *sdk.WorkloadRequest) {
			request.PreviousInstance.RequestRevision += "-foreign"
			request.Sequence++
		},
	} {
		changed := copyRequest(next)
		mutate(&changed)
		changed.Revision, _ = sdk.WorkloadRevision(changed)
		if err := admit(t.Context(), changed); err == nil || !strings.Contains(err.Error(), "workload is immutable within a sequence") {
			t.Fatalf("shipped immutable-sequence rule did not reject mutation: %v", err)
		}
		if !reflect.DeepEqual(store.admitted, next) {
			t.Fatal("rejected admission changed the persisted request")
		}
	}
}

func TestSuspensionConformanceWithImmutableAdmission(t *testing.T) {
	store := continuationFixture()
	first := store.admitted
	if err := CheckSuspension(t.Context(), func() sdk.Lifecycle { return store }, first, continuationAdmission(t, store)); err != nil {
		t.Fatal(err)
	}
	if store.forgedAttempts != 1 || store.admitted.Sequence != first.Sequence+2 || store.current.Sequence != first.Sequence+1 ||
		!store.deleted || store.observed.Identity.RequestRevision != store.current.Revision {
		t.Fatalf("forged request was not admitted/rejected before exact predecessor cleanup: %+v", store)
	}
}

func TestSuspensionConformanceRejectsProviderThatIgnoresRetainedLineage(t *testing.T) {
	store := continuationFixture()
	store.ignoreLineage = true
	err := CheckSuspension(t.Context(), func() sdk.Lifecycle { return store }, store.admitted, continuationAdmission(t, store))
	if err == nil || !strings.Contains(err.Error(), "forged retained lineage was accepted") || store.forgedAttempts != 1 {
		t.Fatalf("provider bypassing lineage proof passed or failed only admission: attempts=%d err=%v", store.forgedAttempts, err)
	}
}
