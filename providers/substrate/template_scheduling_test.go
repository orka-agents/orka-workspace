// Copyright (c) 2026. MIT License - see LICENSE file for details.

package substrate

import (
	"strings"
	"testing"

	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNativeRejectsAdmittedPodSchedulingBeforeEffects(t *testing.T) {
	for _, test := range []struct {
		name  string
		apply func(*corev1.PodSpec)
	}{
		{name: "node selector", apply: func(p *corev1.PodSpec) { p.NodeSelector = map[string]string{"kubernetes.io/os": "linux"} }},
		{name: "node name", apply: func(p *corev1.PodSpec) { p.NodeName = "admitted-node" }},
		{name: "node affinity", apply: func(p *corev1.PodSpec) {
			p.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"trusted"}}}}}}}}
		}},
		{name: "pod affinity", apply: func(p *corev1.PodSpec) {
			p.Affinity = &corev1.Affinity{PodAffinity: &corev1.PodAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{TopologyKey: "kubernetes.io/hostname", LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "trusted"}}}}}}
		}},
		{name: "pod anti-affinity", apply: func(p *corev1.PodSpec) {
			p.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{TopologyKey: "kubernetes.io/hostname", LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "untrusted"}}}}}}
		}},
		{name: "tolerations", apply: func(p *corev1.PodSpec) {
			p.Tolerations = []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}
		}},
		{name: "scheduler", apply: func(p *corev1.PodSpec) { p.SchedulerName = "admitted-scheduler" }},
		{name: "priority class", apply: func(p *corev1.PodSpec) { p.PriorityClassName = "admitted-priority" }},
		{name: "priority", apply: func(p *corev1.PodSpec) { p.Priority = new(int32(1000)) }},
		{name: "preemption", apply: func(p *corev1.PodSpec) { p.PreemptionPolicy = new(corev1.PreemptNever) }},
		{name: "runtime class", apply: func(p *corev1.PodSpec) { p.RuntimeClassName = new("admitted-runtime") }},
		{name: "runtime overhead", apply: func(p *corev1.PodSpec) {
			p.Overhead = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}
		}},
		{name: "topology spread", apply: func(p *corev1.PodSpec) {
			p.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{MaxSkew: 1, TopologyKey: "zone", WhenUnsatisfiable: corev1.DoNotSchedule, LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "admitted"}}}}
		}},
		{name: "scheduling gate", apply: func(p *corev1.PodSpec) {
			p.SchedulingGates = []corev1.PodSchedulingGate{{Name: "workspace.orka.ai/blocked"}}
		}},
		{name: "scheduling group", apply: func(p *corev1.PodSpec) {
			p.SchedulingGroup = &corev1.PodSchedulingGroup{PodGroupName: new("admitted-group")}
		}},
		{name: "pod resource claims", apply: func(p *corev1.PodSpec) {
			p.ResourceClaims = []corev1.PodResourceClaim{{Name: "admitted-device", ResourceClaimName: new("device")}}
		}},
		{name: "pod resource budget", apply: func(p *corev1.PodSpec) {
			p.Resources = &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}
		}},
		{name: "pod OS", apply: func(p *corev1.PodSpec) { p.OS = &corev1.PodOS{Name: corev1.Linux} }},
		{name: "eviction responder", apply: func(p *corev1.PodSpec) {
			p.EvictionResponders = []corev1.EvictionResponder{{Name: "workspace.orka.ai/evictor"}}
		}},
		{name: "container resource claims", apply: func(p *corev1.PodSpec) {
			p.Containers[0].Resources.Claims = []corev1.ResourceClaim{{Name: "admitted-device"}}
		}},
		{name: "host port", apply: func(p *corev1.PodSpec) {
			p.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 80, HostPort: 8080}}
		}},
		{name: "host IP", apply: func(p *corev1.PodSpec) {
			p.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 80, HostIP: "192.0.2.1"}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, native, request := fixture(t, false)
			test.apply(&request.Runtime.Template.Spec)
			request.Resources = *request.Runtime.Template.Spec.Containers[0].Resources.DeepCopy()
			var err error
			request.Revision, err = sdk.WorkloadRevision(request)
			if err != nil {
				t.Fatal(err)
			}
			if err := admit(c)(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), "unsupported") {
				t.Fatalf("admitted scheduling constraint was ignored: %v", err)
			}
			if native.boots != 0 || len(native.actors) != 0 || len(native.templates) != 1 || len(native.workers) != 0 {
				t.Fatal("unsupported scheduling created native compute or a template")
			}
			if err := c.Get(t.Context(), journalKey(request.Key), &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
				t.Fatalf("unsupported scheduling created an allocation journal: %v", err)
			}
		})
	}
}

func TestNativeRejectsKubernetesEnvironmentExpansionBeforeEffects(t *testing.T) {
	for _, value := range []string{"$(EARLIER)", "prefix/$(EARLIER)/suffix", "$(LATER)", "$(UNDEFINED)", "$$(EARLIER)", "$$", "$$literal"} {
		t.Run(value, func(t *testing.T) {
			c, native, request := fixture(t, false)
			container := &request.Runtime.Template.Spec.Containers[0]
			container.Env = append(container.Env, corev1.EnvVar{Name: "EARLIER", Value: "resolved"}, corev1.EnvVar{Name: "REFERENCE", Value: value}, corev1.EnvVar{Name: "LATER", Value: "resolved later"})
			var err error
			request.Revision, err = sdk.WorkloadRevision(request)
			if err != nil {
				t.Fatal(err)
			}
			if err := admit(c)(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), "environment expansion") {
				t.Fatalf("Kubernetes expansion was copied as a native literal: %v", err)
			}
			if native.boots != 0 || len(native.actors) != 0 || len(native.templates) != 1 || len(native.workers) != 0 {
				t.Fatal("unsupported environment expansion created native resources")
			}
			if err := c.Get(t.Context(), journalKey(request.Key), &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
				t.Fatalf("unsupported expansion created an allocation journal: %v", err)
			}
		})
	}
}

func TestNativePreservesResolvedEnvironmentLiterals(t *testing.T) {
	c, native, request := fixture(t, false)
	container := &request.Runtime.Template.Spec.Containers[0]
	container.Env = append(container.Env,
		corev1.EnvVar{Name: "EARLIER", Value: "resolved"},
		corev1.EnvVar{Name: "REFERENCE", Value: "prefix/resolved/suffix"},
		corev1.EnvVar{Name: "LITERAL", Value: "$literal/${unchanged}/$"},
	)
	var err error
	request.Revision, err = sdk.WorkloadRevision(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := admit(c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	ready(t, c, native, request)
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	env := record.TemplateSpec.Containers[0].Env
	want := container.Env[len(container.Env)-3:]
	for i, actual := range env[len(env)-3:] {
		if actual.Name != want[i].Name || actual.Value != want[i].Value {
			t.Fatal("compiler changed ordered resolved environment literals")
		}
	}
	if revision, err := sdk.WorkloadRevision(request); err != nil || revision != request.Revision {
		t.Fatal("provider rewrote admitted environment intent")
	}
}

func TestNativeRejectsKubernetesCommandExpansionBeforeEffects(t *testing.T) {
	for _, field := range []string{"command", "args"} {
		for _, value := range []string{"$(EARLIER)", "prefix/$(EARLIER)/suffix", "$(UNDEFINED)", "$$(EARLIER)", "$$"} {
			t.Run(field+"/"+value, func(t *testing.T) {
				c, native, request := fixture(t, false)
				container := &request.Runtime.Template.Spec.Containers[0]
				container.Env = append(container.Env, corev1.EnvVar{Name: "EARLIER", Value: "resolved"})
				if field == "command" {
					container.Command = []string{"/bin/sh", "-c", "printf '%s' " + value}
				} else {
					container.Args = []string{value}
				}
				request.Command, request.Args = container.Command, container.Args
				var err error
				request.Revision, err = sdk.WorkloadRevision(request)
				if err != nil {
					t.Fatal(err)
				}
				if err := admit(c)(t.Context(), request); err != nil {
					t.Fatal(err)
				}
				if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), "command/argument expansion") {
					t.Fatalf("Kubernetes command expansion was copied as a native literal: %v", err)
				}
				if native.boots != 0 || len(native.actors) != 0 || len(native.templates) != 1 || len(native.workers) != 0 {
					t.Fatal("unsupported command expansion created native resources")
				}
				if err := c.Get(t.Context(), journalKey(request.Key), &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
					t.Fatalf("unsupported expansion created an allocation journal: %v", err)
				}
			})
		}
	}
}
