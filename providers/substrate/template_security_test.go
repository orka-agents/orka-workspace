// Copyright (c) 2026. MIT License - see LICENSE file for details.

package substrate

import (
	"reflect"
	"testing"

	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func TestNativeSecurityPreservesOnlyAdmittedCapabilities(t *testing.T) {
	for _, capabilities := range []*corev1.Capabilities{
		nil,
		{Drop: []corev1.Capability{"ALL"}},
		{Drop: []corev1.Capability{"ALL"}, Add: []corev1.Capability{"KILL"}},
		{Drop: []corev1.Capability{"AUDIT_WRITE"}, Add: []corev1.Capability{"NET_BIND_SERVICE"}},
		{Drop: []corev1.Capability{"ALL"}, Add: []corev1.Capability{"CHOWN", "KILL", "SETGID", "SETUID"}},
	} {
		_, _, request := fixture(t, false)
		zero := int64(0)
		request.Runtime.Template.Spec.SecurityContext = &corev1.PodSecurityContext{RunAsUser: &zero, RunAsGroup: &zero, RunAsNonRoot: new(false)}
		request.Runtime.Template.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{
			RunAsUser: &zero, RunAsGroup: &zero, RunAsNonRoot: new(false),
			Privileged: new(false), ReadOnlyRootFilesystem: new(false), Capabilities: capabilities,
		}
		original := request.DeepCopy()
		record, err := newRecord(request)
		if err != nil {
			t.Fatal(err)
		}
		compiled, err := compileContainer(record)
		if err != nil {
			t.Fatal(err)
		}
		var want *pb.Capabilities
		if capabilities != nil {
			want = &pb.Capabilities{}
			for _, value := range capabilities.Add {
				want.Add = append(want.Add, string(value))
			}
			for _, value := range capabilities.Drop {
				want.Drop = append(want.Drop, string(value))
			}
		}
		if !reflect.DeepEqual(compiled.GetSecurityContext().GetCapabilities(), want) {
			t.Fatalf("compiler widened or discarded admitted capabilities: got %v, want %v", compiled.GetSecurityContext().GetCapabilities(), want)
		}
		if !reflect.DeepEqual(request.Runtime.Template, original.Runtime.Template) {
			t.Fatal("compiler rewrote admitted security settings")
		}
	}
}

func TestNativeRejectsUnsupportedSecurityBeforeEffects(t *testing.T) {
	for _, test := range []struct {
		name  string
		apply func(*corev1.PodSecurityContext, *corev1.SecurityContext)
	}{
		{name: "pod user", apply: func(p *corev1.PodSecurityContext, _ *corev1.SecurityContext) { p.RunAsUser = new(int64(1000)) }},
		{name: "pod group", apply: func(p *corev1.PodSecurityContext, _ *corev1.SecurityContext) { p.RunAsGroup = new(int64(1000)) }},
		{name: "pod non-root", apply: func(p *corev1.PodSecurityContext, _ *corev1.SecurityContext) { p.RunAsNonRoot = new(true) }},
		{name: "pod seccomp", apply: func(p *corev1.PodSecurityContext, _ *corev1.SecurityContext) {
			p.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}
		}},
		{name: "pod apparmor", apply: func(p *corev1.PodSecurityContext, _ *corev1.SecurityContext) {
			p.AppArmorProfile = &corev1.AppArmorProfile{Type: corev1.AppArmorProfileTypeRuntimeDefault}
		}},
		{name: "pod selinux", apply: func(p *corev1.PodSecurityContext, _ *corev1.SecurityContext) {
			p.SELinuxOptions = &corev1.SELinuxOptions{Type: "restricted"}
		}},
		{name: "pod windows", apply: func(p *corev1.PodSecurityContext, _ *corev1.SecurityContext) {
			p.WindowsOptions = &corev1.WindowsSecurityContextOptions{}
		}},
		{name: "fs group", apply: func(p *corev1.PodSecurityContext, _ *corev1.SecurityContext) { p.FSGroup = new(int64(1000)) }},
		{name: "fs group change", apply: func(p *corev1.PodSecurityContext, _ *corev1.SecurityContext) {
			p.FSGroupChangePolicy = new(corev1.FSGroupChangeOnRootMismatch)
		}},
		{name: "supplemental groups", apply: func(p *corev1.PodSecurityContext, _ *corev1.SecurityContext) { p.SupplementalGroups = []int64{1000} }},
		{name: "supplemental groups policy", apply: func(p *corev1.PodSecurityContext, _ *corev1.SecurityContext) {
			p.SupplementalGroupsPolicy = new(corev1.SupplementalGroupsPolicyStrict)
		}},
		{name: "pod sysctls", apply: func(p *corev1.PodSecurityContext, _ *corev1.SecurityContext) {
			p.Sysctls = []corev1.Sysctl{{Name: "net.ipv4.ip_unprivileged_port_start", Value: "0"}}
		}},
		{name: "selinux change", apply: func(p *corev1.PodSecurityContext, _ *corev1.SecurityContext) {
			p.SELinuxChangePolicy = new(corev1.SELinuxChangePolicyRecursive)
		}},
		{name: "container user", apply: func(_ *corev1.PodSecurityContext, c *corev1.SecurityContext) { c.RunAsUser = new(int64(1000)) }},
		{name: "container group", apply: func(_ *corev1.PodSecurityContext, c *corev1.SecurityContext) { c.RunAsGroup = new(int64(1000)) }},
		{name: "container non-root", apply: func(_ *corev1.PodSecurityContext, c *corev1.SecurityContext) { c.RunAsNonRoot = new(true) }},
		{name: "privileged", apply: func(_ *corev1.PodSecurityContext, c *corev1.SecurityContext) { c.Privileged = new(true) }},
		{name: "read-only root", apply: func(_ *corev1.PodSecurityContext, c *corev1.SecurityContext) { c.ReadOnlyRootFilesystem = new(true) }},
		{name: "no new privileges", apply: func(_ *corev1.PodSecurityContext, c *corev1.SecurityContext) { c.AllowPrivilegeEscalation = new(false) }},
		{name: "escalation declaration", apply: func(_ *corev1.PodSecurityContext, c *corev1.SecurityContext) { c.AllowPrivilegeEscalation = new(true) }},
		{name: "container seccomp", apply: func(_ *corev1.PodSecurityContext, c *corev1.SecurityContext) {
			c.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}
		}},
		{name: "container apparmor", apply: func(_ *corev1.PodSecurityContext, c *corev1.SecurityContext) {
			c.AppArmorProfile = &corev1.AppArmorProfile{Type: corev1.AppArmorProfileTypeRuntimeDefault}
		}},
		{name: "container selinux", apply: func(_ *corev1.PodSecurityContext, c *corev1.SecurityContext) {
			c.SELinuxOptions = &corev1.SELinuxOptions{Type: "restricted"}
		}},
		{name: "container windows", apply: func(_ *corev1.PodSecurityContext, c *corev1.SecurityContext) {
			c.WindowsOptions = &corev1.WindowsSecurityContextOptions{}
		}},
		{name: "proc mount", apply: func(_ *corev1.PodSecurityContext, c *corev1.SecurityContext) {
			c.ProcMount = new(corev1.DefaultProcMount)
		}},
		{name: "all capabilities add", apply: func(_ *corev1.PodSecurityContext, c *corev1.SecurityContext) {
			c.Capabilities = &corev1.Capabilities{Add: []corev1.Capability{"ALL"}}
		}},
		{name: "prefixed capability", apply: func(_ *corev1.PodSecurityContext, c *corev1.SecurityContext) {
			c.Capabilities = &corev1.Capabilities{Add: []corev1.Capability{"CAP_CHOWN"}}
		}},
		{name: "invalid capability", apply: func(_ *corev1.PodSecurityContext, c *corev1.SecurityContext) {
			c.Capabilities = &corev1.Capabilities{Drop: []corev1.Capability{"not_a_capability"}}
		}},
		{name: "duplicate capability", apply: func(_ *corev1.PodSecurityContext, c *corev1.SecurityContext) {
			c.Capabilities = &corev1.Capabilities{Add: []corev1.Capability{"CHOWN", "CHOWN"}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, native, request := fixture(t, false)
			podContext, containerContext := &corev1.PodSecurityContext{RunAsUser: new(int64(0)), RunAsGroup: new(int64(0))}, &corev1.SecurityContext{}
			test.apply(podContext, containerContext)
			request.Runtime.Template.Spec.SecurityContext = podContext
			request.Runtime.Template.Spec.Containers[0].SecurityContext = containerContext
			var err error
			request.Revision, err = sdk.WorkloadRevision(request)
			if err != nil {
				t.Fatal(err)
			}
			if err := admit(c)(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil {
				t.Fatal("unsupported security intent was accepted")
			}
			if native.boots != 0 || len(native.actors) != 0 || len(native.templates) != 1 || len(native.workers) != 0 {
				t.Fatal("unsupported security intent created native compute or a template")
			}
			if err := c.Get(t.Context(), journalKey(request.Key), &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
				t.Fatalf("unsupported security intent created an allocation journal: %v", err)
			}
		})
	}
}
