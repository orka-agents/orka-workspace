// Copyright (c) 2026. MIT License - see LICENSE file for details.

package substrate

import (
	"reflect"
	"strings"
	"testing"

	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
)

func TestNativeRejectsImplicitImageIdentityBeforeEffects(t *testing.T) {
	for _, tc := range []struct {
		name      string
		pod       *corev1.PodSecurityContext
		container *corev1.SecurityContext
	}{
		{name: "no security contexts"},
		{name: "empty security contexts", pod: &corev1.PodSecurityContext{}, container: &corev1.SecurityContext{}},
		{name: "pod user only", pod: &corev1.PodSecurityContext{RunAsUser: new(int64(0))}},
		{name: "pod group only", pod: &corev1.PodSecurityContext{RunAsGroup: new(int64(0))}},
		{name: "container user only", container: &corev1.SecurityContext{RunAsUser: new(int64(0))}},
		{name: "container group only", container: &corev1.SecurityContext{RunAsGroup: new(int64(0))}},
		{name: "non-root false is not an identity", container: &corev1.SecurityContext{RunAsNonRoot: new(false)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, native, request := fixture(t, false)
			request.Runtime.Template.Spec.SecurityContext = tc.pod
			request.Runtime.Template.Spec.Containers[0].SecurityContext = tc.container
			assertNativeRequestRejectedBeforeEffects(t, c, native, request, "explicit effective UID/GID 0")
		})
	}
}

func TestNativeExplicitRootIdentityAndUndeclaredPortsPreserveStartup(t *testing.T) {
	for _, tc := range []struct {
		name      string
		pod       *corev1.PodSecurityContext
		container *corev1.SecurityContext
	}{
		{name: "Pod inherited", pod: &corev1.PodSecurityContext{RunAsUser: new(int64(0)), RunAsGroup: new(int64(0))}},
		{name: "container explicit", container: &corev1.SecurityContext{RunAsUser: new(int64(0)), RunAsGroup: new(int64(0))}},
		{name: "Pod user and container group", pod: &corev1.PodSecurityContext{RunAsUser: new(int64(0))}, container: &corev1.SecurityContext{RunAsGroup: new(int64(0))}},
		{name: "container user and Pod group", pod: &corev1.PodSecurityContext{RunAsGroup: new(int64(0))}, container: &corev1.SecurityContext{RunAsUser: new(int64(0))}},
	} {
		for _, emptyPorts := range []bool{false, true} {
			t.Run(tc.name+"/"+map[bool]string{false: "nil Ports", true: "empty Ports"}[emptyPorts], func(t *testing.T) {
				c, native, request := fixture(t, false)
				request.Runtime.Template.Spec.SecurityContext = tc.pod
				container := &request.Runtime.Template.Spec.Containers[0]
				container.SecurityContext = tc.container
				container.Ports = nil
				if emptyPorts {
					container.Ports = []corev1.ContainerPort{}
				}
				container.Env = append(container.Env, corev1.EnvVar{Name: "ORKA_ACP_LISTEN_ADDRESS", Value: ":80"})
				request.Revision, _ = sdk.WorkloadRevision(request)
				before := request.DeepCopy()
				if err := admit(c)(t.Context(), request); err != nil {
					t.Fatal(err)
				}
				created := ready(t, c, native, request)
				observed, err := driver(c, native).Observe(t.Context(), request.Key)
				if err != nil || observed.State != sdk.AllocationReady || observed.Startup == nil || observed.Startup.Process == nil || created.Startup.Process == nil ||
					*observed.Startup.Process != *created.Startup.Process || observed.Identity != created.Identity || !strings.HasSuffix(observed.Startup.Endpoint, ":80") {
					t.Fatalf("undeclared port80 lost exact native startup: %+v, %v", observed, err)
				}
				if err := sdk.ValidateStartup(request, observed); err != nil {
					t.Fatal(err)
				}
				_, record, err := driver(c, native).read(t.Context(), request.Key)
				if err != nil || record.TemplateSpec.Containers[0].Readyz.HttpGet.Port != 80 || native.boots != 1 || !reflect.DeepEqual(&request, before) {
					t.Fatalf("native identity/listener compilation changed admitted intent or replayed: %v", err)
				}
			})
		}
	}
}
