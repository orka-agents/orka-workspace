// Copyright (c) 2026. MIT License - see LICENSE file for details.

package v1alpha1

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func validationRuntimeRequest(t *testing.T) WorkloadRequest {
	t.Helper()
	request := WorkloadRequest{
		Sequence: 1,
		Key:      AllocationKey{Namespace: "tasks", Name: "workspace", WorkspaceUID: "workspace-uid", ProviderUID: "provider-uid"},
		Image:    "registry.example/runtime@sha256:" + strings.Repeat("a", 64),
	}
	binding := ImmutableObjectBinding{Name: "binding", UID: "binding-uid", Generation: 1, ProfileHash: "sha256:" + strings.Repeat("b", 64)}
	request.Runtime = &RuntimeWorkload{
		BootstrapPort: 8080, PoolBinding: binding, ClassBinding: binding, Protocol: "orka.harness.v2", ContainerName: "runtime",
		Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			AutomountServiceAccountToken: new(false),
			Containers:                   []corev1.Container{{Name: "runtime", Image: request.Image}},
		}},
	}
	setValidationRevision(t, &request)
	return request
}

func setValidationRevision(t *testing.T, request *WorkloadRequest) {
	t.Helper()
	var err error
	request.Revision, err = WorkloadRevision(*request)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeRequestRejectsStorageCredentialReferences(t *testing.T) {
	secret := &corev1.LocalObjectReference{Name: "private-runtime-credentials"}
	for name, source := range map[string]corev1.VolumeSource{
		"iscsi":     {ISCSI: &corev1.ISCSIVolumeSource{SecretRef: secret}},
		"rbd":       {RBD: &corev1.RBDVolumeSource{SecretRef: secret}},
		"flex":      {FlexVolume: &corev1.FlexVolumeSource{SecretRef: secret}},
		"cinder":    {Cinder: &corev1.CinderVolumeSource{SecretRef: secret}},
		"cephfs":    {CephFS: &corev1.CephFSVolumeSource{SecretRef: secret}},
		"azurefile": {AzureFile: &corev1.AzureFileVolumeSource{SecretName: secret.Name}},
		"scaleio":   {ScaleIO: &corev1.ScaleIOVolumeSource{SecretRef: secret}},
		"storageos": {StorageOS: &corev1.StorageOSVolumeSource{SecretRef: secret}},
		"csi":       {CSI: &corev1.CSIVolumeSource{NodePublishSecretRef: secret}},
	} {
		t.Run(name, func(t *testing.T) {
			request := validationRuntimeRequest(t)
			request.Runtime.Template.Spec.Volumes = []corev1.Volume{{Name: "storage", VolumeSource: source}}
			setValidationRevision(t, &request)
			if err := request.Validate(); err == nil {
				t.Fatal("accepted storage credential reference before bootstrap")
			}
			if revision, err := WorkloadRevision(request); err != nil || revision != request.Revision {
				t.Fatalf("validation mutated frozen request: revision=%q err=%v", revision, err)
			}
		})
	}
	for name, source := range map[string]corev1.VolumeSource{
		"scratch":  {EmptyDir: &corev1.EmptyDirVolumeSource{}},
		"claim":    {PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}},
		"iscsi":    {ISCSI: &corev1.ISCSIVolumeSource{TargetPortal: "portal", IQN: "iqn"}},
		"rbd":      {RBD: &corev1.RBDVolumeSource{CephMonitors: []string{"monitor"}, RBDImage: "image"}},
		"csi":      {CSI: &corev1.CSIVolumeSource{Driver: "storage.example"}},
		"metadata": {Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{DownwardAPI: &corev1.DownwardAPIProjection{}}}}},
	} {
		t.Run("public/"+name, func(t *testing.T) {
			request := validationRuntimeRequest(t)
			request.Runtime.Template.Spec.Volumes = []corev1.Volume{{Name: "storage", VolumeSource: source}}
			setValidationRevision(t, &request)
			if err := request.Validate(); err != nil {
				t.Fatalf("rejected storage without credential references: %v", err)
			}
		})
	}
}

func TestRuntimeRequestPinsEveryMaterializedContainerImage(t *testing.T) {
	for _, kind := range []string{"init", "regular"} {
		for name, image := range map[string]string{
			"tag":      "registry.example/helper:latest",
			"empty":    "",
			"short":    "registry.example/helper@sha256:abcd",
			"nonhex":   "registry.example/helper@sha256:" + strings.Repeat("z", 64),
			"no image": "@sha256:" + strings.Repeat("a", 64),
			"pinned":   "registry.example/helper@sha256:" + strings.Repeat("c", 64),
		} {
			t.Run(kind+"/"+name, func(t *testing.T) {
				request := validationRuntimeRequest(t)
				helper := corev1.Container{Name: "helper", Image: image}
				if kind == "init" {
					request.Runtime.Template.Spec.InitContainers = []corev1.Container{helper}
				} else {
					request.Runtime.Template.Spec.Containers = append(request.Runtime.Template.Spec.Containers, helper)
				}
				setValidationRevision(t, &request)
				err := request.Validate()
				if name == "pinned" && err != nil {
					t.Fatalf("rejected digest-pinned helper: %v", err)
				}
				if name != "pinned" && err == nil {
					t.Fatal("accepted unpinned materialized container")
				}
			})
		}
	}
}

func TestRuntimeStartupRequiresExactMaterializationIdentity(t *testing.T) {
	request := validationRuntimeRequest(t)
	identity := InstanceIdentity{AllocationID: "allocation", InstanceID: "instance", RequestRevision: request.Revision}
	pod := &PodReference{Namespace: "runtimes", Name: "runtime", UID: "pod-uid"}
	process := &NativeProcessEvidence{
		Namespace: "runtimes", Name: "actor", UID: "actor-uid", Version: 1,
		Worker: *pod, ChallengeSHA256: "sha256:" + strings.Repeat("d", 64),
	}
	for _, tc := range []struct {
		name    string
		pod     *PodReference
		process *NativeProcessEvidence
		valid   bool
	}{
		{name: "missing"},
		{name: "pod", pod: pod, valid: true},
		{name: "native process", process: process, valid: true},
		{name: "ambiguous", pod: pod, process: process},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observation := AllocationObservation{
				Sequence: request.Sequence, Key: request.Key, Identity: identity, State: AllocationReady,
				Startup: &StartupEvidence{ContractVersion: LifecycleContractV1, Identity: identity, Endpoint: "http://runtime.example:8080", Pod: tc.pod, Process: tc.process},
			}
			err := ValidateStartup(request, observation)
			if tc.valid && err != nil {
				t.Fatalf("rejected exact materialization evidence: %v", err)
			}
			if !tc.valid && err == nil {
				t.Fatal("accepted missing or ambiguous runtime materialization identity")
			}
		})
	}
	request.Runtime = nil
	setValidationRevision(t, &request)
	identity.RequestRevision = request.Revision
	observation := AllocationObservation{
		Sequence: request.Sequence, Key: request.Key, Identity: identity, State: AllocationReady,
		Startup: &StartupEvidence{ContractVersion: LifecycleContractV1, Identity: identity, Endpoint: "http://runtime.example:8080"},
	}
	if err := ValidateStartup(request, observation); err != nil {
		t.Fatalf("changed endpoint-only lifecycle evidence without a runtime template: %v", err)
	}
}
