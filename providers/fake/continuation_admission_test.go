// Copyright (c) 2026. MIT License - see LICENSE file for details.

package fake

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/orka-agents/orka-workspace/conformance"
	sdk "github.com/orka-agents/orka-workspace/sdk"
)

func TestSuspensionConformanceWithPersistedImmutableAdmission(t *testing.T) {
	c, request := fixture(t)
	if err := publishRequest(t.Context(), c, request); err != nil {
		t.Fatal(err)
	}
	admitted := []int64{}
	forgedAttempts := 0
	factory := func() sdk.Lifecycle {
		return &checkedLineageLifecycle{Lifecycle: New(c), t: t, forgedAttempts: &forgedAttempts}
	}
	admit := func(ctx context.Context, next sdk.WorkloadRequest) error {
		previous := workspaceFor(t, c, request.Key).Spec.Workload
		if next.Sequence == previous.Sequence {
			if !reflect.DeepEqual(next, *previous) {
				return sdk.ErrRequestConflict
			}
		} else if next.Sequence != previous.Sequence+1 || next.PreviousInstance == nil || next.PreviousInstance.RequestRevision != previous.Revision {
			return sdk.ErrRequestConflict
		}
		admitted = append(admitted, next.Sequence)
		return publishRequest(ctx, c, next)
	}
	if err := conformance.CheckSuspension(t.Context(), factory, request, admit); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(admitted, []int64{request.Sequence + 1, request.Sequence + 2}) || forgedAttempts != 1 {
		t.Fatalf("expected one valid resume followed by one admitted forgery: sequences=%v attempts=%d", admitted, forgedAttempts)
	}
	workspace := workspaceFor(t, c, request.Key)
	deleted, err := New(c).Observe(t.Context(), request.Key)
	if err != nil || deleted.State != sdk.AllocationDeleted || deleted.Sequence != request.Sequence+1 ||
		workspace.Spec.Workload.Sequence != deleted.Sequence+1 || workspace.Spec.Workload.PreviousInstance == nil || *workspace.Spec.Workload.PreviousInstance != deleted.Identity {
		t.Fatalf("forged successor admission blocked exact journal cleanup: %+v %v", deleted, err)
	}
}

type checkedLineageLifecycle struct {
	*Lifecycle
	t              *testing.T
	forgedAttempts *int
}

func (d *checkedLineageLifecycle) EnsureAllocation(ctx context.Context, request sdk.WorkloadRequest) (sdk.AllocationObservation, error) {
	before, readErr := d.Observe(ctx, request.Key)
	forged := readErr == nil && before.RetainedData != nil && request.RetainedData != nil && *request.RetainedData != *before.RetainedData
	if forged {
		(*d.forgedAttempts)++
		workspace := workspaceFor(d.t, d.client, request.Key)
		if !reflect.DeepEqual(*workspace.Spec.Workload, request) {
			d.t.Fatal("adversarial request is not exactly admitted")
		}
	}
	observed, err := d.Lifecycle.EnsureAllocation(ctx, request)
	if forged {
		if err == nil || errors.Is(err, sdk.ErrWorkspaceNotAdmitted) || !strings.Contains(err.Error(), "retained data does not match the exact stopped instance") {
			d.t.Fatalf("forged lineage did not reach the provider's lineage check: %v", err)
		}
		after, readErr := d.Observe(ctx, request.Key)
		if readErr != nil || !reflect.DeepEqual(before, after) {
			d.t.Fatalf("forged lineage changed the exact stopped observation: %v", readErr)
		}
	}
	return observed, err
}
