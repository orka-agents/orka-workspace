// Copyright (c) 2026. MIT License - see LICENSE file for details.

package substrate

import (
	"context"
	"strings"
	"testing"

	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ambiguousActorControl struct {
	*nativeFixture
	accepted bool
	creates  int
}

func (c *ambiguousActorControl) CreateActor(ctx context.Context, request *pb.CreateActorRequest, options ...grpc.CallOption) (*pb.Actor, error) {
	c.creates++
	if c.creates == 1 {
		if c.accepted {
			if _, err := c.nativeFixture.CreateActor(ctx, request, options...); err != nil {
				return nil, err
			}
			// The accepted lifetime disappeared before its UID could be observed.
			delete(c.actors, metaKey(request.Actor.Metadata))
		}
		return nil, status.Error(codes.Unavailable, "create response lost")
	}
	return c.nativeFixture.CreateActor(ctx, request, options...)
}

func TestAmbiguousActorCreateWithAbsentUnknownLifetimeNeverReplays(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		name := "unobserved acceptance"
		if accepted {
			name = "accepted lifetime disappeared"
		}
		t.Run(name, func(t *testing.T) {
			c, native, request := fixture(t, false)
			control := &ambiguousActorControl{nativeFixture: native, accepted: accepted}
			d := driver(c, native)
			d.control = control
			if _, err := d.EnsureAllocation(t.Context(), request); status.Code(err) != codes.Unavailable {
				t.Fatalf("fixture did not lose the create response: %v", err)
			}
			_, record, err := d.read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			if !record.Actor.CreateIssued || record.Actor.UID != "" {
				t.Fatal("create intent was not durably recorded before the uncertain call")
			}
			for range 3 {
				if _, err := d.EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), "creation was already issued") {
					t.Fatalf("absent uncertain Actor was not closed to replay: %v", err)
				}
			}
			if control.creates != 1 || native.boots != 0 || len(native.actors) != 0 {
				t.Fatalf("uncertain create was replayed: creates=%d boots=%d actors=%d", control.creates, native.boots, len(native.actors))
			}
		})
	}
}
