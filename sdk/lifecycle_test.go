package workspaceprovider

import (
	"errors"
	"strings"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

func workloadFixture() WorkloadRequest {
	r := WorkloadRequest{Sequence: 1, Key: AllocationKey{Namespace: "runtime", Name: "workspace", WorkspaceUID: "workspace-uid", ProviderUID: "provider-uid"}, Image: "registry.example/runtime@sha256:" + strings.Repeat("a", 64)}
	r.Revision, _ = WorkloadRevision(r)
	return r
}

func TestStartupEvidenceBindsExactRequestAndInstance(t *testing.T) {
	request := workloadFixture()
	identity := InstanceIdentity{AllocationID: "allocation", InstanceID: "incarnation", RequestRevision: request.Revision}
	observation := AllocationObservation{Sequence: request.Sequence, Key: request.Key, Identity: identity, State: AllocationReady,
		Startup: &StartupEvidence{ContractVersion: LifecycleContractV1, Identity: identity, Endpoint: "http://runtime.example:8080"}}
	if err := ValidateStartup(request, observation); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*AllocationObservation){
		"other workspace":       func(o *AllocationObservation) { o.Key.WorkspaceUID = "other" },
		"other provider":        func(o *AllocationObservation) { o.Key.ProviderUID = "other" },
		"stale request":         func(o *AllocationObservation) { o.Identity.RequestRevision = "stale" },
		"replayed evidence":     func(o *AllocationObservation) { o.Startup.Identity.InstanceID = "retired" },
		"incompatible contract": func(o *AllocationObservation) { o.Startup.ContractVersion += ".unknown" },
		"stopped instance":      func(o *AllocationObservation) { o.State = AllocationStopped },
		"missing evidence":      func(o *AllocationObservation) { o.Startup = nil },
		"credential URL":        func(o *AllocationObservation) { o.Startup.Endpoint = "https://fixture-user@example.com" },
		"query URL":             func(o *AllocationObservation) { o.Startup.Endpoint = "https://example.com?credential=value" },
		"missing Pod UID":       func(o *AllocationObservation) { o.Startup.Pod = &PodReference{Namespace: "runtime", Name: "pod"} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			copy := observation
			evidence := *observation.Startup
			copy.Startup = &evidence
			mutate(&copy)
			if err := ValidateStartup(request, copy); err == nil {
				t.Fatal("accepted invalid startup evidence")
			}
		})
	}
}

func TestWorkloadRevisionCoversAllAllocationIntent(t *testing.T) {
	request := workloadFixture()
	request.Command = []string{"changed"}
	if err := request.Validate(); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("changed command: %v", err)
	}
	request.Revision, _ = WorkloadRevision(request)
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	request.ParametersRef = &workspacev1alpha1.TypedObjectReference{Group: "test.orka.ai", Kind: "Profile", Name: "profile"}
	request.Revision, _ = WorkloadRevision(request)
	if err := request.Validate(); err == nil {
		t.Fatal("accepted unbound mutable parameter reference")
	}
}

func TestRuntimeRequestRejectsPreBootstrapCredentialAccess(t *testing.T) {
	request := workloadFixture()
	no := false
	binding := workspacev1alpha1.ImmutableObjectBinding{Name: "binding", UID: "uid", Generation: 1, ProfileHash: "sha256:" + strings.Repeat("b", 64)}
	request.Runtime = &RuntimeWorkload{BootstrapPort: 8080, PoolBinding: binding, ClassBinding: binding, Protocol: "orka.harness.v2", ContainerName: "runtime", Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		AutomountServiceAccountToken: &no, Containers: []corev1.Container{{Name: "runtime", Image: request.Image}},
	}}}
	request.Revision, _ = WorkloadRevision(request)
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*corev1.PodSpec){
		"automatic token": func(p *corev1.PodSpec) { p.AutomountServiceAccountToken = nil },
		"secret environment": func(p *corev1.PodSpec) {
			p.Containers[0].Env = []corev1.EnvVar{{Name: "CREDENTIAL", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{}}}}
		},
		"environment import": func(p *corev1.PodSpec) { p.Containers[0].EnvFrom = []corev1.EnvFromSource{{}} },
		"secret volume": func(p *corev1.PodSpec) {
			p.Volumes = []corev1.Volume{{VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{}}}}
		},
		"mutable config volume": func(p *corev1.PodSpec) {
			p.Volumes = []corev1.Volume{{VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{}}}}
		},
		"mutable projected config": func(p *corev1.PodSpec) {
			p.Volumes = []corev1.Volume{{VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{ConfigMap: &corev1.ConfigMapProjection{}}}}}}}
		},
		"file environment": func(p *corev1.PodSpec) {
			p.Containers[0].Env = []corev1.EnvVar{{Name: "CREDENTIAL", ValueFrom: &corev1.EnvVarSource{FileKeyRef: &corev1.FileKeySelector{}}}}
		},
		"projected token": func(p *corev1.PodSpec) {
			p.Volumes = []corev1.Volume{{VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{}}}}}}}
		},
		"different image": func(p *corev1.PodSpec) { p.Containers[0].Image = "different" },
	} {
		t.Run(name, func(t *testing.T) {
			copy := request
			runtime := *request.Runtime
			copy.Runtime = &runtime
			copy.Runtime.Template = *request.Runtime.Template.DeepCopy()
			mutate(&copy.Runtime.Template.Spec)
			copy.Revision, _ = WorkloadRevision(copy)
			if err := copy.Validate(); err == nil {
				t.Fatal("accepted unbound runtime or credential access")
			}
		})
	}
}
