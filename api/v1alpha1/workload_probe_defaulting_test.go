// Copyright (c) 2026. MIT License - see LICENSE file for details.

package v1alpha1

import (
	"errors"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

func TestGRPCProbeRevisionSurvivesShippedCRDDefaults(t *testing.T) {
	data, err := os.ReadFile("../../config/crd/bases/workspace.orka.ai_executionworkspaces.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.UnmarshalStrict(data, &crd); err != nil {
		t.Fatal(err)
	}
	var internal apiextensions.JSONSchemaProps
	if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(crd.Spec.Versions[0].Schema.OpenAPIV3Schema, &internal, nil); err != nil {
		t.Fatal(err)
	}
	schema, err := structuralschema.NewStructural(&internal)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"runtime", "init", "ephemeral"} {
		t.Run(kind, func(t *testing.T) {
			request := validationRuntimeRequest(t)
			probe := &corev1.Probe{ProbeHandler: corev1.ProbeHandler{GRPC: &corev1.GRPCAction{Port: 8080}}}
			pod := &request.Runtime.Template.Spec
			switch kind {
			case "runtime":
				pod.Containers[0].LivenessProbe, pod.Containers[0].ReadinessProbe, pod.Containers[0].StartupProbe = probe.DeepCopy(), probe.DeepCopy(), probe.DeepCopy()
			case "init":
				pod.InitContainers = []corev1.Container{{Name: "init", Image: request.Image, LivenessProbe: probe.DeepCopy(), ReadinessProbe: probe.DeepCopy(), StartupProbe: probe.DeepCopy()}}
			case "ephemeral":
				pod.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug", Image: request.Image, LivenessProbe: probe.DeepCopy(), ReadinessProbe: probe.DeepCopy(), StartupProbe: probe.DeepCopy()}}}
			}
			setValidationRevision(t, &request)
			before := request.Revision
			workspace := &ExecutionWorkspace{
				TypeMeta:   metav1.TypeMeta{APIVersion: GroupVersion.String(), Kind: "ExecutionWorkspace"},
				ObjectMeta: metav1.ObjectMeta{Namespace: request.Key.Namespace, Name: request.Key.Name, UID: request.Key.WorkspaceUID},
				Spec: ExecutionWorkspaceSpec{Mode: ExecutionWorkspaceModeInteractive, ClassBinding: request.Runtime.ClassBinding,
					ProviderBinding: ImmutableObjectBinding{Name: "provider", UID: request.Key.ProviderUID, Generation: 1},
					Slot:            "default", DesiredState: ExecutionWorkspaceDesiredReady, Lifecycle: testLifecycle(), Workload: &request},
			}
			object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(workspace)
			if err != nil {
				t.Fatal(err)
			}
			// These are the API-server algorithms and the actual shipped schema,
			// rather than a hand-written model of the gRPC default.
			pruning.Prune(object, schema, true)
			defaulting.Default(object, schema)
			var persisted ExecutionWorkspace
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object, &persisted); err != nil {
				t.Fatal(err)
			}
			for _, probe := range grpcDefaultingProbes(&persisted.Spec.Workload.Runtime.Template.Spec, kind) {
				if probe.GRPC.Service == nil || *probe.GRPC.Service != "" {
					t.Fatal("shipped CRD did not persist the omitted gRPC service as empty")
				}
			}
			after, err := WorkloadRevision(*persisted.Spec.Workload)
			if err != nil || after != before || persisted.Spec.Workload.Revision != before {
				t.Fatalf("CRD defaults changed the revision: before=%s after=%s err=%v", before, after, err)
			}
			for _, probe := range grpcDefaultingProbes(pod, kind) {
				if probe.GRPC.Service != nil {
					t.Fatal("hashing or persistence mutated the caller's omitted service")
				}
			}
			if kind == "ephemeral" {
				if err := persisted.Spec.Workload.Validate(); err == nil {
					t.Fatal("hash canonicalization allowed an unsupported ephemeral container")
				}
			} else if err := persisted.Spec.Workload.Validate(); err != nil {
				t.Fatalf("defaulted public workload failed validation: %v", err)
			}
			for i, name := range []string{"liveness", "readiness", "startup"} {
				t.Run(name+" service drift", func(t *testing.T) {
					changed := persisted.Spec.Workload.DeepCopy()
					grpcDefaultingProbes(&changed.Runtime.Template.Spec, kind)[i].GRPC.Service = new("different-service")
					revision, err := WorkloadRevision(*changed)
					if err != nil || revision == before {
						t.Fatalf("explicit gRPC service drift was erased: revision=%s err=%v", revision, err)
					}
					if kind != "ephemeral" && !errors.Is(changed.Validate(), ErrRequestConflict) {
						t.Fatal("public validation accepted a changed service under the admitted revision")
					}
				})
			}
		})
	}
}

func grpcDefaultingProbes(pod *corev1.PodSpec, kind string) []*corev1.Probe {
	switch kind {
	case "init":
		return []*corev1.Probe{pod.InitContainers[0].LivenessProbe, pod.InitContainers[0].ReadinessProbe, pod.InitContainers[0].StartupProbe}
	case "ephemeral":
		return []*corev1.Probe{pod.EphemeralContainers[0].LivenessProbe, pod.EphemeralContainers[0].ReadinessProbe, pod.EphemeralContainers[0].StartupProbe}
	default:
		return []*corev1.Probe{pod.Containers[0].LivenessProbe, pod.Containers[0].ReadinessProbe, pod.Containers[0].StartupProbe}
	}
}
