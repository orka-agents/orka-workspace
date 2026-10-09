package substrate

import (
	"context"
	"strings"
	"testing"

	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type suspensionReviewControl struct {
	*nativeFixture
	calls  int
	submit func(*pb.SuspendActorRequest) (*pb.SuspendActorResponse, error)
}

func (c *suspensionReviewControl) SuspendActor(_ context.Context, request *pb.SuspendActorRequest, _ ...grpc.CallOption) (*pb.SuspendActorResponse, error) {
	c.calls++
	return c.submit(request)
}

func TestSuspensionRejectionAndUnknownDeliveryRemainActionableAfterRestart(t *testing.T) {
	for _, code := range []codes.Code{codes.FailedPrecondition, codes.Unavailable} {
		t.Run(code.String(), func(t *testing.T) {
			c, native, request := fixture(t, true)
			first := ready(t, c, native, request)
			control := &suspensionReviewControl{nativeFixture: native, submit: func(*pb.SuspendActorRequest) (*pb.SuspendActorResponse, error) {
				return nil, status.Error(code, "backend rejected or lost the request")
			}}
			d := driver(c, native)
			d.control = control
			if _, err := d.SuspendInstance(t.Context(), request.Key, first.Identity); status.Code(err) != code {
				t.Fatalf("first backend error changed: %v", err)
			}
			for range 3 {
				// A new Lifecycle reads only the durable issued fence and exact
				// backend state, without remembering the earlier RPC error.
				d = driver(c, native)
				d.control = control
				observed, err := d.SuspendInstance(t.Context(), request.Key, first.Identity)
				if err == nil || !strings.Contains(err.Error(), "outcome is unresolved") || !strings.Contains(err.Error(), "refusing replay") {
					t.Fatalf("issued suspension silently became successful progress: %v", err)
				}
				if observed.State != sdk.AllocationPending || observed.Startup != nil || observed.RetainedData != nil {
					t.Fatal("unresolved suspension reported startup or retained data")
				}
			}
			_, record, err := d.read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			if !record.WorkerDrained || record.Pending == nil || !record.Pending.SuspendIssued || record.Checkpoint != nil || control.calls != 1 || native.suspends != 0 || native.boots != 1 || len(native.tags) != 0 {
				t.Fatal("failed suspension lost its exact fence or replayed an effect")
			}
			// Explicit exact-instance Stop remains available when retaining data
			// cannot be completed. It does not issue another suspension.
			stopped := retired(t, c, native, request, first.Identity, false)
			if stopped.State != sdk.AllocationStopped || stopped.Identity != first.Identity || stopped.RetainedData != nil || control.calls != 1 || native.suspends != 0 {
				t.Fatal("unresolved suspension blocked exact Stop or claimed retained data")
			}
		})
	}
}

func TestSuspensionLostResponseRecoversExactDataWithoutDuplicateRPC(t *testing.T) {
	c, native, request := fixture(t, true)
	first := ready(t, c, native, request)
	control := &suspensionReviewControl{nativeFixture: native, submit: func(request *pb.SuspendActorRequest) (*pb.SuspendActorResponse, error) {
		actor := native.actors[refKey(request.Actor)]
		actor.Metadata.Version++
		actor.Status.State = pb.ActorState_ACTOR_STATE_SUSPENDING
		return nil, status.Error(codes.Unavailable, "response lost while backend continues")
	}}
	d := driver(c, native)
	d.control = control
	if _, err := d.SuspendInstance(t.Context(), request.Key, first.Identity); status.Code(err) != codes.Unavailable {
		t.Fatalf("lost-response error changed: %v", err)
	}
	for range 2 {
		d = driver(c, native)
		d.control = control
		observed, err := d.SuspendInstance(t.Context(), request.Key, first.Identity)
		if err != nil || observed.State != sdk.AllocationPending || observed.RetainedData != nil || control.calls != 1 || len(native.tags) != 0 {
			t.Fatalf("known native SUSPENDING progress failed or replayed: %v", err)
		}
	}
	_, record, err := d.read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	// Complete the original outstanding backend operation after the response
	// was lost. The provider must recover by observation, without resubmitting.
	if _, err := native.SuspendActor(t.Context(), &pb.SuspendActorRequest{Actor: actorRef(record)}); err != nil {
		t.Fatal(err)
	}
	var stopped sdk.AllocationObservation
	for range 12 {
		d = driver(c, native)
		d.control = control
		stopped, err = d.SuspendInstance(t.Context(), request.Key, first.Identity)
		if err != nil {
			t.Fatal(err)
		}
		if stopped.State == sdk.AllocationStopped {
			break
		}
	}
	if stopped.State != sdk.AllocationStopped || stopped.Identity != first.Identity || stopped.Startup != nil || stopped.RetainedData == nil || !stopped.RetainedData.Valid() || stopped.RetainedData.SourceInstance != first.Identity || control.calls != 1 || native.suspends != 1 || native.deletes != 1 {
		t.Fatal("lost response did not recover exact Data retention without replay")
	}
}

func TestLegacyIssuedSuspensionUsesExactBackendStateWithoutReplay(t *testing.T) {
	for _, state := range []pb.ActorState{pb.ActorState_ACTOR_STATE_RUNNING, pb.ActorState_ACTOR_STATE_CRASHED, pb.ActorState_ACTOR_STATE_SUSPENDING} {
		t.Run(state.String(), func(t *testing.T) {
			c, native, request := fixture(t, true)
			first := ready(t, c, native, request)
			d := driver(c, native)
			cm, record, err := d.read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			actor := native.actors[metaKey(&pb.ResourceMetadata{Atespace: record.Atespace, Name: record.Actor.Name})]
			record.Operation = "suspend"
			record.Pending = &checkpointIntent{Name: "legacy-checkpoint", SuspendIssued: true, PriorSnapshotDigest: snapshotDigest(actor.Status.ExternalSnapshot)}
			record.Observation = pendingObservation(record)
			record.WorkerDrained = true
			actor.Status.State = state
			native.workers[record.Worker.Name].Status.State = pb.WorkerState_WORKER_STATE_DRAINING
			if err := d.save(t.Context(), cm, record); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				observed, err := driver(c, native).SuspendInstance(t.Context(), request.Key, first.Identity)
				if state == pb.ActorState_ACTOR_STATE_SUSPENDING {
					if err != nil {
						t.Fatalf("legacy accepted progress rejected: %v", err)
					}
				} else if err == nil || !strings.Contains(err.Error(), "outcome is unresolved") {
					t.Fatalf("legacy failed outcome silently accepted: %v", err)
				}
				if observed.State != sdk.AllocationPending || observed.Startup != nil || observed.RetainedData != nil || native.suspends != 0 || len(native.tags) != 0 || native.deletes != 0 {
					t.Fatal("legacy issued fence replayed a suspension or claimed retention")
				}
			}
		})
	}
}
