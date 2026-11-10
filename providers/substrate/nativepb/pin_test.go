package ateapipb

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestNativeProtocolIsPinnedUntouched(t *testing.T) {
	raw, err := os.ReadFile("ateapi.proto")
	if err != nil {
		t.Fatal(err)
	}
	pin, err := os.ReadFile("upstream.env")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	for _, line := range []string{
		"SUBSTRATE_UPSTREAM_COMMIT=b34fbee742906494b5eb4f1f36987176bf8770a4",
		"SUBSTRATE_UPSTREAM_PROTO_PATH=api/ateapi/v1/ateapi.proto",
		"SUBSTRATE_UPSTREAM_PROTO_SHA256=" + hex.EncodeToString(sum[:]),
		"SUBSTRATE_PROTOC_VERSION=36.0",
	} {
		if !strings.Contains(string(pin), line+"\n") {
			t.Fatalf("native control source drifted from the pinned provider: missing %q", line)
		}
	}
	if File_ateapi_proto.Package() != "substrate.ateapi.v1" {
		t.Fatalf("native control package is %q, want substrate.ateapi.v1", File_ateapi_proto.Package())
	}
	for _, message := range []string{"SuspendActorRequest", "ResumeActorRequest", "DeleteActorRequest"} {
		descriptor := File_ateapi_proto.Messages().ByName(protoreflect.Name(message))
		if descriptor == nil {
			t.Fatal("missing upstream lifecycle message")
		}
		for _, name := range []string{"actor_uid", "actor_version", "operation_id", "request_revision", "boot", "any_state"} {
			if descriptor.Fields().ByName(protoreflect.Name(name)) != nil {
				t.Fatalf("adapter manufactured or retained a native operation field %s.%s", message, name)
			}
		}
	}
}
