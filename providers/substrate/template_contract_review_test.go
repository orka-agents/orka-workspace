// Copyright (c) 2026. MIT License - see LICENSE file for details.

package substrate

import (
	"testing"

	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestNativeRejectsUnsupportedPodRuntimeBeforeEffects(t *testing.T) {
	for _, test := range []struct {
		name  string
		apply func(*corev1.PodSpec)
	}{
		{"termination grace", func(p *corev1.PodSpec) { p.TerminationGracePeriodSeconds = new(int64(120)) }},
		{"zero termination grace", func(p *corev1.PodSpec) { p.TerminationGracePeriodSeconds = new(int64(0)) }},
		{"host network", func(p *corev1.PodSpec) { p.HostNetwork = true }},
		{"host PID", func(p *corev1.PodSpec) { p.HostPID = true }},
		{"host IPC", func(p *corev1.PodSpec) { p.HostIPC = true }},
		{"shared process namespace", func(p *corev1.PodSpec) { p.ShareProcessNamespace = new(true) }},
		{"DNS configuration", func(p *corev1.PodSpec) { p.DNSConfig = &corev1.PodDNSConfig{Nameservers: []string{"192.0.2.53"}} }},
		{"cluster DNS", func(p *corev1.PodSpec) { p.DNSPolicy = corev1.DNSClusterFirst }},
		{"host DNS", func(p *corev1.PodSpec) { p.DNSPolicy = corev1.DNSDefault }},
		{"no DNS", func(p *corev1.PodSpec) { p.DNSPolicy = corev1.DNSNone }},
		{"service account", func(p *corev1.PodSpec) { p.ServiceAccountName = "runtime" }},
		{"default service account", func(p *corev1.PodSpec) { p.ServiceAccountName = "default" }},
		{"deprecated service account", func(p *corev1.PodSpec) { p.DeprecatedServiceAccount = "runtime" }},
		{"hostname", func(p *corev1.PodSpec) { p.Hostname = "admitted-host" }},
		{"subdomain", func(p *corev1.PodSpec) { p.Subdomain = "admitted-subdomain" }},
		{"hostname override", func(p *corev1.PodSpec) { p.HostnameOverride = new("admitted.example") }},
		{"FQDN hostname", func(p *corev1.PodSpec) { p.SetHostnameAsFQDN = new(true) }},
		{"host alias", func(p *corev1.PodSpec) {
			p.HostAliases = []corev1.HostAlias{{IP: "192.0.2.1", Hostnames: []string{"admitted"}}}
		}},
		{"user namespace", func(p *corev1.PodSpec) { p.HostUsers = new(false) }},
		{"service links", func(p *corev1.PodSpec) { p.EnableServiceLinks = new(true) }},
		{"readiness gate", func(p *corev1.PodSpec) {
			p.ReadinessGates = []corev1.PodReadinessGate{{ConditionType: "workspace.orka.ai/custom-ready"}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, native, request := fixture(t, false)
			test.apply(&request.Runtime.Template.Spec)
			assertNativeRequestRejectedBeforeEffects(t, c, native, request, "Pod settings are unsupported")
		})
	}
}

func TestNativeRejectsUnsupportedContainerRuntimeBeforeEffects(t *testing.T) {
	httpProbe := func() *corev1.Probe {
		return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/v2/health", Port: intstr.FromInt32(80)}}}
	}
	for _, test := range []struct {
		name     string
		apply    func(*corev1.Container)
		expected string
	}{
		{"readiness HTTP probe", func(c *corev1.Container) { c.ReadinessProbe = httpProbe() }, "probes are unsupported"},
		{"startup HTTP probe", func(c *corev1.Container) { c.StartupProbe = httpProbe() }, "probes are unsupported"},
		{"liveness HTTP probe", func(c *corev1.Container) { c.LivenessProbe = httpProbe() }, "probes are unsupported"},
		{"readiness exec probe", func(c *corev1.Container) {
			c.ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"/bin/true"}}}}
		}, "probes are unsupported"},
		{"lifecycle pre-stop", func(c *corev1.Container) {
			c.Lifecycle = &corev1.Lifecycle{PreStop: &corev1.LifecycleHandler{Sleep: &corev1.SleepAction{Seconds: 5}}}
		}, "container settings are unsupported"},
		{"lifecycle post-start", func(c *corev1.Container) {
			c.Lifecycle = &corev1.Lifecycle{PostStart: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"/bin/true"}}}}
		}, "container settings are unsupported"},
		{"stdin", func(c *corev1.Container) { c.Stdin = true }, "container settings are unsupported"},
		{"stdin once", func(c *corev1.Container) { c.StdinOnce = true }, "container settings are unsupported"},
		{"TTY", func(c *corev1.Container) { c.TTY = true }, "container settings are unsupported"},
		{"resize policy", func(c *corev1.Container) {
			c.ResizePolicy = []corev1.ContainerResizePolicy{{ResourceName: corev1.ResourceMemory, RestartPolicy: corev1.RestartContainer}}
		}, "container settings are unsupported"},
		{"container restart policy", func(c *corev1.Container) { c.RestartPolicy = new(corev1.ContainerRestartPolicyAlways) }, "container settings are unsupported"},
		{"container restart rule", func(c *corev1.Container) {
			c.RestartPolicyRules = []corev1.ContainerRestartRule{{Action: corev1.ContainerRestartRuleActionRestart, ExitCodes: &corev1.ContainerRestartRuleOnExitCodes{Operator: corev1.ContainerRestartRuleOnExitCodesOpIn, Values: []int32{1}}}}
		}, "container settings are unsupported"},
		{"volume device", func(c *corev1.Container) {
			c.VolumeDevices = []corev1.VolumeDevice{{Name: "block", DevicePath: "/dev/admitted"}}
		}, "container settings are unsupported"},
		{"termination message path", func(c *corev1.Container) { c.TerminationMessagePath = corev1.TerminationMessagePathDefault }, "container settings are unsupported"},
		{"termination message policy", func(c *corev1.Container) { c.TerminationMessagePolicy = corev1.TerminationMessageReadFile }, "container settings are unsupported"},
		{"always pull", func(c *corev1.Container) { c.ImagePullPolicy = corev1.PullAlways }, "image pull policy is unsupported"},
		{"never pull", func(c *corev1.Container) { c.ImagePullPolicy = corev1.PullNever }, "image pull policy is unsupported"},
		{"other TCP port", func(c *corev1.Container) {
			c.Ports = []corev1.ContainerPort{{ContainerPort: 8080, Protocol: corev1.ProtocolTCP}}
		}, "ports must use TCP 80"},
		{"UDP port", func(c *corev1.Container) {
			c.Ports = []corev1.ContainerPort{{ContainerPort: 80, Protocol: corev1.ProtocolUDP}}
		}, "ports must use TCP 80"},
		{"SCTP port", func(c *corev1.Container) {
			c.Ports = []corev1.ContainerPort{{ContainerPort: 80, Protocol: corev1.ProtocolSCTP}}
		}, "ports must use TCP 80"},
		{"other listener", func(c *corev1.Container) {
			c.Env = append(c.Env, corev1.EnvVar{Name: "ORKA_ACP_LISTEN_ADDRESS", Value: ":8080"})
		}, "listener must be literal :80"},
		{"empty listener", func(c *corev1.Container) { c.Env = append(c.Env, corev1.EnvVar{Name: "ORKA_ACP_LISTEN_ADDRESS"}) }, "listener must be literal :80"},
		{"downward listener", func(c *corev1.Container) {
			c.Env = append(c.Env, corev1.EnvVar{Name: "ORKA_ACP_LISTEN_ADDRESS", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}})
		}, "listener must be literal :80"},
		{"session directory", func(c *corev1.Container) {
			c.Env = append(c.Env, corev1.EnvVar{Name: "ORKA_ACP_SESSION_BASE_DIR", Value: "/custom-sessions"})
		}, "environment ORKA_ACP_SESSION_BASE_DIR is unsupported"},
		{"MCP broker", func(c *corev1.Container) {
			c.Env = append(c.Env, corev1.EnvVar{Name: "ORKA_ACP_MCP_BROKER_URL", Value: "https://broker.invalid"})
		}, "environment ORKA_ACP_MCP_BROKER_URL is unsupported"},
		{"Pod namespace", func(c *corev1.Container) {
			c.Env = append(c.Env, corev1.EnvVar{Name: "ORKA_ACP_POD_NAMESPACE", Value: "custom-namespace"})
		}, "environment ORKA_ACP_POD_NAMESPACE is unsupported"},
		{"downward API version", func(c *corev1.Container) {
			c.Env = append(c.Env, corev1.EnvVar{Name: "CUSTOM", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "unsupported/v1", FieldPath: "metadata.namespace"}}})
		}, "downward field API version is unsupported"},
		{"literal and downward field", func(c *corev1.Container) {
			c.Env = append(c.Env, corev1.EnvVar{Name: "CUSTOM", Value: "literal", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}})
		}, "environment must be literal"},
		{"multiple downward sources", func(c *corev1.Container) {
			c.Env = append(c.Env, corev1.EnvVar{Name: "CUSTOM", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}, ResourceFieldRef: &corev1.ResourceFieldSelector{Resource: "limits.cpu"}}})
		}, "environment must be literal"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, native, request := fixture(t, false)
			test.apply(&request.Runtime.Template.Spec.Containers[0])
			assertNativeRequestRejectedBeforeEffects(t, c, native, request, test.expected)
		})
	}
}

func TestNativeRejectsOtherBootstrapPortBeforeEffects(t *testing.T) {
	c, native, request := fixture(t, false)
	request.Runtime.BootstrapPort = 8080
	assertNativeRequestRejectedBeforeEffects(t, c, native, request, "requires bootstrap port 80")
}

func TestNativeNamespaceFieldUsesEffectiveRuntimeNamespace(t *testing.T) {
	for _, namespace := range []string{"", "runtime"} {
		t.Run("namespace="+namespace, func(t *testing.T) {
			c, native, request := fixture(t, false)
			request.Runtime.Template.Namespace = namespace
			container := &request.Runtime.Template.Spec.Containers[0]
			container.Env = append(container.Env, corev1.EnvVar{Name: "ADMITTED_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}})
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
			want := namespace
			if want == "" {
				want = request.Key.Namespace
			}
			for _, env := range record.TemplateSpec.Containers[0].Env {
				if env.Name == "ADMITTED_NAMESPACE" && env.Value == want {
					return
				}
			}
			t.Fatal("native downward namespace did not match effective runtime placement")
		})
	}
}

func TestNativeRejectsNonExactDurableMountsBeforeEffects(t *testing.T) {
	for _, test := range []struct {
		name  string
		apply func(*corev1.PodSpec)
	}{
		{"alternative mount", func(p *corev1.PodSpec) {
			p.Containers[0].VolumeMounts = append(p.Containers[0].VolumeMounts, corev1.VolumeMount{Name: durableVolumeName, MountPath: "/alternative"})
		}},
		{"duplicate mount", func(p *corev1.PodSpec) {
			p.Containers[0].VolumeMounts = append(p.Containers[0].VolumeMounts, p.Containers[0].VolumeMounts[0])
		}},
		{"read-only mount", func(p *corev1.PodSpec) { p.Containers[0].VolumeMounts[0].ReadOnly = true }},
		{"sub-path", func(p *corev1.PodSpec) { p.Containers[0].VolumeMounts[0].SubPath = "subdirectory" }},
		{"sub-path expression", func(p *corev1.PodSpec) { p.Containers[0].VolumeMounts[0].SubPathExpr = "$(SUBDIRECTORY)" }},
		{"propagation", func(p *corev1.PodSpec) {
			p.Containers[0].VolumeMounts[0].MountPropagation = new(corev1.MountPropagationBidirectional)
		}},
		{"recursive read-only", func(p *corev1.PodSpec) {
			p.Containers[0].VolumeMounts[0].RecursiveReadOnly = new(corev1.RecursiveReadOnlyDisabled)
		}},
		{"bind options", func(p *corev1.PodSpec) { p.Containers[0].VolumeMounts[0].BindMountOptions = []string{"rbind"} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, native, request := fixture(t, true)
			test.apply(&request.Runtime.Template.Spec)
			assertNativeRequestRejectedBeforeEffects(t, c, native, request, "requires admitted durable workspace mount")
		})
	}
}

func TestNativeRejectsIgnoredVolumeDeclarationsBeforeEffects(t *testing.T) {
	for _, test := range []struct {
		name    string
		volumes []corev1.Volume
	}{
		{"host path", []corev1.Volume{{Name: "unused", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/host-data"}}}}},
		{"persistent claim", []corev1.Volume{{Name: "unused", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "admitted"}}}}},
		{"other placeholder", []corev1.Volume{{Name: "unused"}}},
		{"durable source override", []corev1.Volume{{Name: durableVolumeName, VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/host-data"}}}}},
		{"duplicate durable placeholder", []corev1.Volume{{Name: durableVolumeName}, {Name: durableVolumeName}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, native, request := fixture(t, true)
			request.Runtime.Template.Spec.Volumes = test.volumes
			assertNativeRequestRejectedBeforeEffects(t, c, native, request, "volume")
		})
	}
}

func TestNativeSupportsMatchingRuntimeDefaultsAndExactDurableMount(t *testing.T) {
	c, native, request := fixture(t, true)
	pod := &request.Runtime.Template.Spec
	pod.EnableServiceLinks = new(false)
	pod.ShareProcessNamespace = new(false)
	pod.SetHostnameAsFQDN = new(false)
	pod.HostUsers = new(true)
	pod.Volumes = []corev1.Volume{{Name: durableVolumeName}}
	container := &pod.Containers[0]
	container.ImagePullPolicy = corev1.PullIfNotPresent
	container.Ports = []corev1.ContainerPort{{Name: "control", ContainerPort: 80}}
	container.Env = append(container.Env, corev1.EnvVar{Name: "ORKA_ACP_LISTEN_ADDRESS", Value: ":80"})
	var err error
	request.Revision, err = sdk.WorkloadRevision(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := admit(c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	observed := ready(t, c, native, request)
	if observed.Startup == nil || observed.Startup.Process == nil {
		t.Fatal("matching native intent did not produce sealed process evidence")
	}
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	compiled := record.TemplateSpec.Containers[0]
	if compiled.Readyz.GetHttpGet().GetPath() != "/v2/health" || compiled.Readyz.GetHttpGet().GetPort() != 80 || len(compiled.VolumeMounts) != 2 {
		t.Fatal("native health gate or exact durable/identity mounts changed")
	}
	if record.Request.Runtime.Template.Spec.Containers[0].Ports[0].Protocol != "" || record.Request.Runtime.Template.Spec.Containers[0].ReadinessProbe != nil {
		t.Fatal("compiler rewrote the admitted runtime defaults")
	}
	if revision, err := sdk.WorkloadRevision(request); err != nil || revision != request.Revision {
		t.Fatal("native compiler rewrote frozen supported intent")
	}
}
