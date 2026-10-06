// Copyright (c) 2026. MIT License - see LICENSE file for details.

package v1alpha1

import (
	"context"
	"os"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	structuralcel "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	crdvalidation "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation"
	"sigs.k8s.io/yaml"
)

func TestWorkloadCRDsPassAPIServerValidation(t *testing.T) {
	for _, name := range []string{"executionworkspaces", "executionworkspaceproviders"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile("../../config/crd/bases/workspace.orka.ai_" + name + ".yaml")
			if err != nil {
				t.Fatal(err)
			}
			var external apiextensionsv1.CustomResourceDefinition
			if err := yaml.UnmarshalStrict(data, &external); err != nil {
				t.Fatal(err)
			}
			var internal apiextensions.CustomResourceDefinition
			if err := apiextensionsv1.Convert_v1_CustomResourceDefinition_To_apiextensions_CustomResourceDefinition(&external, &internal, nil); err != nil {
				t.Fatal(err)
			}
			internal.Status.StoredVersions = []string{"v1alpha1"}
			if errs := crdvalidation.ValidateCustomResourceDefinition(context.Background(), &internal); len(errs) != 0 {
				t.Fatal(errs)
			}
		})
	}
}

func TestPersistedWorkloadSurvivesPruningAndRejectsMutation(t *testing.T) {
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
	validator := structuralcel.NewValidator(schema, true, celconfig.PerCallLimit)
	binding := ImmutableObjectBinding{Name: "class", UID: "class-uid", Generation: 1, ProfileHash: "sha256:" + strings.Repeat("a", 64)}
	no := false
	request := &WorkloadRequest{
		Sequence: 1,
		Key:      AllocationKey{Namespace: "tasks", Name: "workspace", WorkspaceUID: "workspace-uid", ProviderUID: "provider-uid"},
		Image:    "fixture.example/runtime@sha256:" + strings.Repeat("b", 64),
		Runtime: &RuntimeWorkload{BootstrapPort: 8080, PoolBinding: binding, ClassBinding: binding, Protocol: "orka.harness.v2", ContainerName: "runtime",
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Namespace: "runtimes", Labels: map[string]string{"fixture": "runtime"}, Annotations: map[string]string{"fixture.example/revision": "one"}},
				Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: &no, Containers: []corev1.Container{{Name: "runtime", Image: "fixture.example/runtime@sha256:" + strings.Repeat("b", 64), Ports: []corev1.ContainerPort{{ContainerPort: 8080}}}}, Volumes: []corev1.Volume{
					{Name: "azure", VolumeSource: corev1.VolumeSource{AzureDisk: &corev1.AzureDiskVolumeSource{DiskName: "disk", DataDiskURI: "uri"}}},
					{Name: "iscsi", VolumeSource: corev1.VolumeSource{ISCSI: &corev1.ISCSIVolumeSource{TargetPortal: "portal", IQN: "iqn"}}},
					{Name: "rbd", VolumeSource: corev1.VolumeSource{RBD: &corev1.RBDVolumeSource{CephMonitors: []string{"monitor"}, RBDImage: "image"}}},
					{Name: "scaleio", VolumeSource: corev1.VolumeSource{ScaleIO: &corev1.ScaleIOVolumeSource{Gateway: "gateway", System: "system", SecretRef: &corev1.LocalObjectReference{Name: "name"}}}},
				}}}},
	}
	request.Revision, _ = WorkloadRevision(*request)
	workspace := &ExecutionWorkspace{TypeMeta: metav1.TypeMeta{APIVersion: GroupVersion.String(), Kind: "ExecutionWorkspace"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "tasks", Name: "workspace", UID: "workspace-uid"},
		Spec:       ExecutionWorkspaceSpec{Mode: ExecutionWorkspaceModeInteractive, ClassBinding: binding, ProviderBinding: ImmutableObjectBinding{Name: "provider", UID: "provider-uid", Generation: 1}, Slot: "default", DesiredState: ExecutionWorkspaceDesiredReady, Lifecycle: testLifecycle(), Workload: request},
	}
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(workspace)
	if err != nil {
		t.Fatal(err)
	}
	pruning.Prune(object, schema, true)
	defaulting.Default(object, schema)
	var decoded ExecutionWorkspace
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := decoded.Spec.Workload.Validate(); err != nil {
		t.Fatalf("API pruning/defaulting changed frozen workload: %v", err)
	}
	validate := func(value, old any) field.ErrorList {
		errs, _ := validator.Validate(context.Background(), field.NewPath("object"), schema, value, old, celconfig.RuntimeCELCostBudget)
		return errs
	}
	if errs := validate(object, nil); len(errs) != 0 {
		t.Fatal(errs)
	}
	for name, mutate := range map[string]func(*ExecutionWorkspace){
		"template metadata":  func(w *ExecutionWorkspace) { w.Spec.Workload.Runtime.Template.Labels["fixture"] = "changed" },
		"template namespace": func(w *ExecutionWorkspace) { w.Spec.Workload.Runtime.Template.Namespace = "foreign" },
		"template image":     func(w *ExecutionWorkspace) { w.Spec.Workload.Runtime.Template.Spec.Containers[0].Image = "changed" },
		"removed workload":   func(w *ExecutionWorkspace) { w.Spec.Workload = nil },
		"skipped sequence": func(w *ExecutionWorkspace) {
			w.Spec.Workload.Sequence = 3
			w.Spec.Workload.PreviousInstance = &InstanceIdentity{AllocationID: "allocation", InstanceID: "previous", RequestRevision: request.Revision}
		},
	} {
		t.Run(name, func(t *testing.T) {
			modified := decoded.DeepCopy()
			mutate(modified)
			if modified.Spec.Workload != nil {
				modified.Spec.Workload.Revision, _ = WorkloadRevision(*modified.Spec.Workload)
			}
			changed, err := runtime.DefaultUnstructuredConverter.ToUnstructured(modified)
			if err != nil {
				t.Fatal(err)
			}
			if errs := validate(changed, object); len(errs) == 0 {
				t.Fatal("accepted immutable workload mutation")
			}
		})
	}
}
