// Copyright (c) 2026. MIT License - see LICENSE file for details.

package workspaceprovider

import (
	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	"strings"
	"testing"
)

func TestWorkloadReplacementRequiresExactTerminatedPredecessor(t *testing.T) {
	previous := workloadFixture()
	identity := InstanceIdentity{AllocationID: "allocation", InstanceID: "instance-one", RequestRevision: previous.Revision}
	stopped := AllocationObservation{Sequence: 1, Key: previous.Key, Identity: identity, State: AllocationStopped}
	next := *previous.DeepCopy()
	next.Sequence = 2
	next.PreviousInstance = &identity
	next.Args = []string{"new-public-bootstrap"}
	next.Revision, _ = WorkloadRevision(next)
	if err := ValidateWorkloadTransition(&previous, next, &stopped, false); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*WorkloadRequest, *AllocationObservation){
		"missing termination":         func(_ *WorkloadRequest, o *AllocationObservation) { o.State = AllocationPending },
		"still ready":                 func(_ *WorkloadRequest, o *AllocationObservation) { o.State = AllocationReady },
		"deleted allocation":          func(_ *WorkloadRequest, o *AllocationObservation) { o.State = AllocationDeleted },
		"stale sequence":              func(_ *WorkloadRequest, o *AllocationObservation) { o.Sequence++ },
		"foreign workspace":           func(_ *WorkloadRequest, o *AllocationObservation) { o.Key.WorkspaceUID = "foreign" },
		"foreign provider":            func(_ *WorkloadRequest, o *AllocationObservation) { o.Key.ProviderUID = "foreign" },
		"wrong previous instance":     func(n *WorkloadRequest, _ *AllocationObservation) { n.PreviousInstance.InstanceID = "foreign" },
		"sequence skip":               func(n *WorkloadRequest, _ *AllocationObservation) { n.Sequence++ },
		"same sequence changed":       func(n *WorkloadRequest, _ *AllocationObservation) { n.Sequence = 1; n.PreviousInstance = nil },
		"withdrawn evidence retained": func(_ *WorkloadRequest, o *AllocationObservation) { o.Startup = &StartupEvidence{} },
	} {
		t.Run(name, func(t *testing.T) {
			request := next.DeepCopy()
			observation := stopped.DeepCopy()
			mutate(request, observation)
			request.Revision, _ = WorkloadRevision(*request)
			if err := ValidateWorkloadTransition(&previous, *request, observation, false); err == nil {
				t.Fatal("accepted unsafe replacement")
			}
		})
	}
	if err := ValidateWorkloadTransition(nil, next, nil, false); err == nil {
		t.Fatal("replacement has no persisted predecessor")
	}
	if err := ValidateWorkloadTransition(&previous, next, nil, false); err == nil {
		t.Fatal("replacement has no termination evidence")
	}
	if err := ValidateWorkloadTransition(&previous, previous, &stopped, false); err != nil {
		t.Fatalf("same request retry must remain observable: %v", err)
	}
}

func TestColdResumeRequiresMatchingRetainedDataProof(t *testing.T) {
	previous := workloadFixture()
	identity := InstanceIdentity{AllocationID: "allocation", InstanceID: "instance-one", RequestRevision: previous.Revision}
	stopped := AllocationObservation{Sequence: 1, Key: previous.Key, Identity: identity, State: AllocationStopped}
	next := *previous.DeepCopy()
	next.Sequence = 2
	next.PreviousInstance = &identity
	next.Revision, _ = WorkloadRevision(next)
	if err := ValidateWorkloadTransition(&previous, next, &stopped, true); err == nil {
		t.Fatal("stopped alone authorized cold resume")
	}
	lineage := RetainedDataReference{ID: "workspace-owned-data", SourceInstance: identity, ProofSHA256: "sha256:" + strings.Repeat("b", 64)}
	stopped.RetainedData = &lineage
	next.RetainedData = lineage.DeepCopy()
	next.Revision, _ = WorkloadRevision(next)
	if err := ValidateWorkloadTransition(&previous, next, &stopped, true); err != nil {
		t.Fatal(err)
	}
	next.RetainedData.ProofSHA256 = "sha256:" + strings.Repeat("c", 64)
	next.Revision, _ = WorkloadRevision(next)
	if err := ValidateWorkloadTransition(&previous, next, &stopped, true); err == nil {
		t.Fatal("foreign lineage authorized cold resume")
	}
	next.RetainedData = nil
	next.Revision, _ = WorkloadRevision(next)
	if err := ValidateWorkloadTransition(&previous, next, &stopped, false); err == nil {
		t.Fatal("discarded retained lineage during replacement")
	}
}

func TestReplacementPreservesIndependentRestoreReference(t *testing.T) {
	previous := workloadFixture()
	previous.RestoreFrom = &workspacev1alpha1.WorkloadCheckpointReference{Name: "saved", UID: "checkpoint-uid", Digest: "sha256:" + strings.Repeat("d", 64)}
	previous.Revision, _ = WorkloadRevision(previous)
	identity := InstanceIdentity{AllocationID: "allocation", InstanceID: "instance", RequestRevision: previous.Revision}
	stopped := AllocationObservation{Sequence: 1, Key: previous.Key, Identity: identity, State: AllocationStopped}
	next := previous.DeepCopy()
	next.Sequence++
	next.PreviousInstance = &identity
	next.Revision, _ = WorkloadRevision(*next)
	if err := ValidateWorkloadTransition(&previous, *next, &stopped, false); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*WorkloadRequest){
		"removed":         func(r *WorkloadRequest) { r.RestoreFrom = nil },
		"replaced UID":    func(r *WorkloadRequest) { r.RestoreFrom.UID = "replacement" },
		"replaced digest": func(r *WorkloadRequest) { r.RestoreFrom.Digest = "sha256:" + strings.Repeat("e", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := next.DeepCopy()
			mutate(changed)
			changed.Revision, _ = WorkloadRevision(*changed)
			if err := ValidateWorkloadTransition(&previous, *changed, &stopped, false); err == nil {
				t.Fatal("restore provenance changed during replacement")
			}
		})
	}
}

func TestRetirementAuthorizationBindsSequenceInstanceAndAction(t *testing.T) {
	request := workloadFixture()
	identity := InstanceIdentity{AllocationID: "allocation", InstanceID: "instance", RequestRevision: request.Revision}
	observation := AllocationObservation{Sequence: request.Sequence, Key: request.Key, Identity: identity, State: AllocationReady}
	authorization := WorkloadRetirement{Sequence: request.Sequence, Identity: identity, Action: workspacev1alpha1.WorkloadRetirementDelete}
	if err := ValidateWorkloadRetirement(&request, &observation, &authorization, workspacev1alpha1.WorkloadRetirementDelete); err != nil {
		t.Fatal(err)
	}
	if err := ValidateWorkloadRetirement(&request, &observation, &authorization, workspacev1alpha1.WorkloadRetirementStop); err != nil {
		t.Fatal(err)
	}
	if err := ValidateWorkloadRetirement(&request, &observation, &authorization, workspacev1alpha1.WorkloadRetirementSuspend); err == nil {
		t.Fatal("deletion authorized data capture")
	}
	if err := ValidateWorkloadRetirement(&request, &observation, nil, workspacev1alpha1.WorkloadRetirementStop); err == nil {
		t.Fatal("absent core authorization allowed stop")
	}
	authorization.Identity.InstanceID = "foreign"
	if err := ValidateWorkloadRetirement(&request, &observation, &authorization, workspacev1alpha1.WorkloadRetirementDelete); err == nil {
		t.Fatal("foreign instance authorized deletion")
	}
	authorization.Identity = identity
	authorization.Sequence++
	if err := ValidateWorkloadRetirement(&request, &observation, &authorization, workspacev1alpha1.WorkloadRetirementDelete); err == nil {
		t.Fatal("foreign sequence authorized deletion")
	}
}
