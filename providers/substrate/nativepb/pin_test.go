package ateapipb

import (
	"crypto/sha256"
	"encoding/hex"
	"google.golang.org/protobuf/reflect/protoreflect"
	"os"
	"strings"
	"testing"
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
	if !strings.Contains(string(pin), "SUBSTRATE_UPSTREAM_COMMIT=fa6d949685a6318940a9a0195c867c864009b820") || !strings.Contains(string(pin), "SUBSTRATE_UPSTREAM_PROTO_SHA256="+hex.EncodeToString(sum[:])) {
		t.Fatal("native control source drifted from the pinned provider")
	}
	for _, message := range []string{"SuspendActorRequest", "ResumeActorRequest", "DeleteActorRequest"} {
		descriptor := File_ateapi_proto.Messages().ByName(protoreflect.Name(message))
		if descriptor == nil {
			t.Fatal("missing upstream lifecycle message")
		}
		for _, name := range []string{"actor_uid", "actor_version", "operation_id", "request_revision"} {
			if descriptor.Fields().ByName(protoreflect.Name(name)) != nil {
				t.Fatalf("adapter manufactured native operation fence %s.%s", message, name)
			}
		}
	}
}
