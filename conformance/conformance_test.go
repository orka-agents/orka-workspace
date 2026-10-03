package conformance_test

import (
	"context"
	"strings"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	"github.com/orka-agents/orka-workspace/conformance"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
)

type noOp struct{}

func (noOp) EnsureAllocation(context.Context, workspaceprovider.WorkloadRequest) (workspaceprovider.AllocationObservation, error) {
	return workspaceprovider.AllocationObservation{}, nil
}
func (noOp) Observe(context.Context, workspaceprovider.AllocationKey) (workspaceprovider.AllocationObservation, error) {
	return workspaceprovider.AllocationObservation{}, workspaceprovider.ErrNotFound
}
func (noOp) StopInstance(context.Context, workspaceprovider.AllocationKey, workspaceprovider.InstanceIdentity) (workspaceprovider.AllocationObservation, error) {
	return workspaceprovider.AllocationObservation{}, nil
}
func (noOp) DeleteAllocation(context.Context, workspaceprovider.AllocationKey, workspaceprovider.InstanceIdentity, workspacev1alpha1.ExecutionWorkspaceDeletionPolicy) (workspaceprovider.AllocationObservation, error) {
	return workspaceprovider.AllocationObservation{}, nil
}

func TestRejectsProviderThatDoesNothing(t *testing.T) {
	request := workspaceprovider.WorkloadRequest{
		Key:   workspaceprovider.AllocationKey{Namespace: "test", Name: "test", WorkspaceUID: "workspace", ProviderUID: "provider"},
		Image: "registry.example/fixture@sha256:" + strings.Repeat("a", 64),
	}
	request.Revision, _ = workspaceprovider.WorkloadRevision(request)
	if err := conformance.Check(context.Background(), func() workspaceprovider.Lifecycle { return noOp{} }, request); err == nil {
		t.Fatal("a provider that does nothing passed lifecycle conformance")
	}
}
