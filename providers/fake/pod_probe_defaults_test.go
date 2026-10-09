package fake

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func probeAdmissionContainer(handler string) corev1.Container {
	probe := &corev1.Probe{}
	switch handler {
	case "HTTP":
		probe.HTTPGet = &corev1.HTTPGetAction{Port: intstr.FromInt32(8080)}
	case "GRPC":
		probe.GRPC = &corev1.GRPCAction{Port: 8080}
	case "Exec":
		probe.Exec = &corev1.ExecAction{Command: []string{"/health"}}
	}
	return corev1.Container{
		LivenessProbe: probe.DeepCopy(), ReadinessProbe: probe.DeepCopy(), StartupProbe: probe.DeepCopy(),
		Lifecycle: &corev1.Lifecycle{
			PostStart: &corev1.LifecycleHandler{HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromInt32(8080)}},
			PreStop:   &corev1.LifecycleHandler{HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromInt32(8080)}},
		},
	}
}

// Model the values persisted by the pinned Kubernetes Pod defaulting pass,
// independently of the provider comparison helpers.
func realizeProbeAdmission(container *corev1.Container, httpProtocol bool) {
	for _, probe := range []*corev1.Probe{container.LivenessProbe, container.ReadinessProbe, container.StartupProbe} {
		probe.TimeoutSeconds, probe.PeriodSeconds, probe.SuccessThreshold, probe.FailureThreshold = 1, 10, 1, 3
		if probe.GRPC != nil {
			probe.GRPC.Service = new(string(""))
		}
	}
	for _, action := range []*corev1.HTTPGetAction{
		container.LivenessProbe.HTTPGet, container.ReadinessProbe.HTTPGet, container.StartupProbe.HTTPGet,
		container.Lifecycle.PostStart.HTTPGet, container.Lifecycle.PreStop.HTTPGet,
	} {
		if action != nil {
			action.Path, action.Scheme = "/", corev1.URISchemeHTTP
			if httpProtocol {
				action.Protocol = new(corev1.HTTPProtocolHTTP1)
			}
		}
	}
}

func installProbeTemplate(container *corev1.Container, probes corev1.Container) {
	container.LivenessProbe = probes.LivenessProbe
	container.ReadinessProbe = probes.ReadinessProbe
	container.StartupProbe = probes.StartupProbe
	container.Lifecycle = probes.Lifecycle
}

func explicitProbeContainer() corev1.Container {
	container := probeAdmissionContainer("HTTP")
	container.ReadinessProbe.HTTPGet = nil
	container.ReadinessProbe.GRPC = &corev1.GRPCAction{Port: 8080, Service: new(string("ready"))}
	for _, probe := range []*corev1.Probe{container.LivenessProbe, container.ReadinessProbe, container.StartupProbe} {
		probe.TimeoutSeconds, probe.PeriodSeconds, probe.SuccessThreshold, probe.FailureThreshold = 2, 12, 1, 5
	}
	container.ReadinessProbe.SuccessThreshold = 2
	container.LivenessProbe.HTTPGet.Path = "/live"
	container.LivenessProbe.HTTPGet.Scheme = corev1.URISchemeHTTP
	container.LivenessProbe.HTTPGet.Protocol = new(corev1.HTTPProtocolHTTP2)
	container.StartupProbe.HTTPGet.Path = "/startup"
	container.StartupProbe.HTTPGet.Scheme = corev1.URISchemeHTTPS
	container.StartupProbe.HTTPGet.Protocol = new(corev1.HTTPProtocolHTTP1)
	container.Lifecycle.PostStart.HTTPGet.Path = "/after-start"
	container.Lifecycle.PostStart.HTTPGet.Scheme = corev1.URISchemeHTTP
	container.Lifecycle.PreStop.HTTPGet.Path = "/before-stop"
	container.Lifecycle.PreStop.HTTPGet.Scheme = corev1.URISchemeHTTPS
	return container
}

func probeDriftCases() []struct {
	name   string
	mutate func(*corev1.Container)
} {
	return []struct {
		name   string
		mutate func(*corev1.Container)
	}{
		{"timeout", func(c *corev1.Container) { c.LivenessProbe.TimeoutSeconds = 1 }},
		{"period", func(c *corev1.Container) { c.StartupProbe.PeriodSeconds = 10 }},
		{"success threshold", func(c *corev1.Container) { c.ReadinessProbe.SuccessThreshold = 1 }},
		{"failure threshold", func(c *corev1.Container) { c.LivenessProbe.FailureThreshold = 3 }},
		{"HTTP path", func(c *corev1.Container) { c.LivenessProbe.HTTPGet.Path = "/" }},
		{"HTTP scheme", func(c *corev1.Container) { c.StartupProbe.HTTPGet.Scheme = corev1.URISchemeHTTP }},
		{"HTTP protocol", func(c *corev1.Container) { c.LivenessProbe.HTTPGet.Protocol = new(corev1.HTTPProtocolHTTP1) }},
		{"GRPC service", func(c *corev1.Container) { c.ReadinessProbe.GRPC.Service = new(string("")) }},
		{"post-start action", func(c *corev1.Container) { c.Lifecycle.PostStart.HTTPGet.Path = "/" }},
		{"pre-stop action", func(c *corev1.Container) { c.Lifecycle.PreStop.HTTPGet.Scheme = corev1.URISchemeHTTP }},
		{"missing probe", func(c *corev1.Container) { c.LivenessProbe = nil }},
		{"changed handler", func(c *corev1.Container) {
			c.LivenessProbe.HTTPGet = nil
			c.LivenessProbe.Exec = &corev1.ExecAction{Command: []string{"/health"}}
		}},
	}
}

type probeAdmissionClient struct {
	client.Client
	protocolDefault bool
}

func (c *probeAdmissionClient) Create(ctx context.Context, object client.Object, options ...client.CreateOption) error {
	if pod, ok := object.(*corev1.Pod); ok {
		realizeProbeAdmission(&pod.Spec.Containers[0], c.protocolDefault)
	}
	return c.Client.Create(ctx, object, options...)
}

func TestProbeAdmissionDefaultsPreserveFakeStartup(t *testing.T) {
	for _, handler := range []string{"HTTP", "GRPC", "Exec"} {
		for _, protocolDefault := range []bool{false, true} {
			t.Run(handler+"/"+map[bool]string{false: "H2C disabled", true: "H2C enabled"}[protocolDefault], func(t *testing.T) {
				base, request := runtimeFixture(t)
				c := &probeAdmissionClient{Client: base, protocolDefault: protocolDefault}
				installProbeTemplate(&request.Runtime.Template.Spec.Containers[0], probeAdmissionContainer(handler))
				var err error
				request.Revision, err = sdk.WorkloadRevision(request)
				if err != nil {
					t.Fatal(err)
				}
				if err := publishRequest(t.Context(), c, request); err != nil {
					t.Fatal(err)
				}
				observed, err := New(c).EnsureAllocation(t.Context(), request)
				if err != nil || observed.State != sdk.AllocationReady || observed.Startup == nil || observed.Startup.Pod == nil {
					t.Fatalf("API-defaulted probes did not allow startup: observation=%+v error=%v", observed, err)
				}
				identity := *observed.Startup.Pod
				if observed, err := New(c).Observe(t.Context(), request.Key); err != nil || observed.Startup == nil || *observed.Startup.Pod != identity {
					t.Fatalf("API-defaulted probes lost the exact startup identity: observation=%+v error=%v", observed, err)
				}
				revision, err := sdk.WorkloadRevision(request)
				if err != nil || revision != request.Revision {
					t.Fatal("comparison changed the frozen request")
				}
			})
		}
	}
}

func TestProbeAdmissionPreservesExplicitFakeIntent(t *testing.T) {
	for _, test := range probeDriftCases() {
		t.Run(test.name, func(t *testing.T) {
			c, request := runtimeFixture(t)
			installProbeTemplate(&request.Runtime.Template.Spec.Containers[0], explicitProbeContainer())
			var err error
			request.Revision, err = sdk.WorkloadRevision(request)
			if err != nil {
				t.Fatal(err)
			}
			if err := publishRequest(t.Context(), c, request); err != nil {
				t.Fatal(err)
			}
			observed, err := New(c).EnsureAllocation(t.Context(), request)
			if err != nil || observed.Startup == nil {
				t.Fatalf("explicit probes lost startup: %v", err)
			}
			pod := &corev1.Pod{}
			if err := c.Get(t.Context(), types.NamespacedName{Namespace: observed.Startup.Pod.Namespace, Name: observed.Startup.Pod.Name}, pod); err != nil {
				t.Fatal(err)
			}
			test.mutate(&pod.Spec.Containers[0])
			if err := c.Update(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || observed.Startup != nil {
				t.Fatalf("changed explicit probes published startup: observation=%+v error=%v", observed, err)
			}
			if observed, err := New(c).Observe(t.Context(), request.Key); err == nil || observed.Startup != nil {
				t.Fatalf("changed explicit probes remained observable: observation=%+v error=%v", observed, err)
			}
		})
	}
}
