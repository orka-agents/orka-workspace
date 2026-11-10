// Copyright (c) 2026. MIT License - see LICENSE file for details.

package substrate

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	corev1 "k8s.io/api/core/v1"
)

// coreSizedEnvironment fills the supervisor environment the way Orka core does
// for native runtimes (31 entries, or 32 with its e2e marker).
func coreSizedEnvironment(t *testing.T, container *corev1.Container, entries int) {
	t.Helper()
	if len(container.Env) > entries {
		t.Fatalf("fixture already has %d environment entries", len(container.Env))
	}
	for i := len(container.Env); i < entries; i++ {
		container.Env = append(container.Env, corev1.EnvVar{Name: fmt.Sprintf("ORKA_ACP_FIXTURE_%02d", i), Value: "value"})
	}
}

func TestNativeEgressTrustOnlyWhenHTTPSIsIntercepted(t *testing.T) {
	passthrough := &api.RuntimeEgressPolicy{Rules: []api.RuntimeEgressRule{
		{Protocol: api.RuntimeEgressHTTP, Host: "auth-proxy.orka-system.svc.cluster.local", Ports: []int32{8080}},
		{Protocol: api.RuntimeEgressTLSPassthrough, Host: "api.example.com", Ports: []int32{443}},
	}}
	for _, entries := range []int{31, 32} {
		_, _, request := fixture(t, false)
		request.Runtime.Egress = passthrough
		coreSizedEnvironment(t, &request.Runtime.Template.Spec.Containers[0], entries)
		record, err := newRecord(request)
		if err != nil {
			t.Fatal(err)
		}
		compiled, err := compileContainer(record)
		if err != nil {
			t.Fatalf("%d-entry core environment without interception was refused: %v", entries, err)
		}
		for _, env := range compiled.Env {
			if slices.Contains(egressTrustEnvironment, env.Name) {
				t.Fatalf("trust variable %s added without an HTTPS rule", env.Name)
			}
		}
		for _, mount := range compiled.VolumeMounts {
			if mount.Name == egressTrustVolumeName {
				t.Fatal("trust bundle mounted without an HTTPS rule")
			}
		}
	}

	// Interception needs the seven trust variables; a runtime that cannot also
	// hold them is refused with an explicit reason instead of losing trust.
	_, _, request := fixture(t, false)
	request.Runtime.Egress = &api.RuntimeEgressPolicy{Rules: append(slices.Clone(passthrough.Rules),
		api.RuntimeEgressRule{Protocol: api.RuntimeEgressHTTPS, Host: "registry.example.com", Ports: []int32{443}})}
	coreSizedEnvironment(t, &request.Runtime.Template.Spec.Containers[0], 31)
	record, err := newRecord(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compileContainer(record); err == nil || !strings.Contains(err.Error(), "HTTPS interception") {
		t.Fatalf("over-limit interception error = %v", err)
	}

	intercepted := request.Runtime.Egress
	_, _, request = fixture(t, false)
	request.Runtime.Egress = intercepted
	record, err = newRecord(request)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := compileContainer(record)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, env := range compiled.Env {
		names[env.Name] = true
	}
	for _, name := range egressTrustEnvironment {
		if !names[name] {
			t.Fatalf("intercepted HTTPS egress lacks trust variable %s", name)
		}
	}
	if !slices.ContainsFunc(compiled.VolumeMounts, func(mount *pb.VolumeMount) bool { return mount.Name == egressTrustVolumeName }) {
		t.Fatal("intercepted HTTPS egress lacks the trust bundle mount")
	}
}
