// Copyright (c) 2026. MIT License - see LICENSE file for details.

package v1alpha1

import (
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestRuntimeRequestRejectsNonMetadataProjectedSources(t *testing.T) {
	metadata := &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{
		Path: "uid", FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"},
	}}}
	certificate := &corev1.PodCertificateProjection{
		SignerName: "operator.example/runtime", KeyType: "ED25519", CredentialBundlePath: "identity.pem",
	}
	trust := &corev1.ClusterTrustBundleProjection{Name: new("operator-trust"), Path: "trust.pem"}
	for _, test := range []struct {
		name    string
		sources []corev1.VolumeProjection
	}{
		{"Secret", []corev1.VolumeProjection{{Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "private-credentials"}}}}},
		{"ConfigMap", []corev1.VolumeProjection{{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "mutable-config"}}}}},
		{"ServiceAccountToken", []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"}}}},
		{"PodCertificate", []corev1.VolumeProjection{{PodCertificate: certificate}}},
		{"ClusterTrustBundle", []corev1.VolumeProjection{{ClusterTrustBundle: trust}}},
		{"unknown source", []corev1.VolumeProjection{{}}},
		{"mixed metadata and certificate entry", []corev1.VolumeProjection{{DownwardAPI: metadata, PodCertificate: certificate}}},
		{"mixed metadata and trust entry", []corev1.VolumeProjection{{DownwardAPI: metadata, ClusterTrustBundle: trust}}},
		{"mixed metadata and Secret entry", []corev1.VolumeProjection{{DownwardAPI: metadata, Secret: &corev1.SecretProjection{}}}},
		{"mixed metadata and ConfigMap entry", []corev1.VolumeProjection{{DownwardAPI: metadata, ConfigMap: &corev1.ConfigMapProjection{}}}},
		{"mixed metadata and token entry", []corev1.VolumeProjection{{DownwardAPI: metadata, ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"}}}},
		{"mixed sources", []corev1.VolumeProjection{{DownwardAPI: metadata}, {PodCertificate: certificate}}},
		{"unsafe first source", []corev1.VolumeProjection{{ClusterTrustBundle: trust}, {DownwardAPI: metadata}}},
	} {
		for _, mount := range []string{"unmounted", "runtime", "init", "sidecar"} {
			t.Run(test.name+"/"+mount, func(t *testing.T) {
				request := validationRuntimeRequest(t)
				request.Runtime.Template.Spec.Volumes = []corev1.Volume{{
					Name: "projected", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: test.sources}},
				}}
				containerMount := corev1.VolumeMount{Name: "projected", MountPath: "/projected", ReadOnly: true}
				switch mount {
				case "runtime":
					request.Runtime.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{containerMount}
				case "init":
					request.Runtime.Template.Spec.InitContainers = []corev1.Container{{Name: "init", Image: request.Image, VolumeMounts: []corev1.VolumeMount{containerMount}}}
				case "sidecar":
					request.Runtime.Template.Spec.Containers = append(request.Runtime.Template.Spec.Containers, corev1.Container{Name: "sidecar", Image: request.Image, VolumeMounts: []corev1.VolumeMount{containerMount}})
				}
				setValidationRevision(t, &request)
				if err := request.Validate(); err == nil || !strings.Contains(err.Error(), "allow only downward API metadata") {
					t.Fatalf("public allocation intent accepted nonmetadata projection: %v", err)
				}
				if revision, err := WorkloadRevision(request); err != nil || revision != request.Revision {
					t.Fatal("projection validation mutated frozen allocation intent")
				}
			})
		}
	}
}

func TestRuntimeRequestRejectsUnknownProjectedSourceAfterDecoding(t *testing.T) {
	request := validationRuntimeRequest(t)
	var source corev1.VolumeProjection
	if err := json.Unmarshal([]byte(`{"futureCredential":{"name":"private-credential"}}`), &source); err != nil {
		t.Fatal(err)
	}
	request.Runtime.Template.Spec.Volumes = []corev1.Volume{{
		Name: "unknown", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{source}}},
	}}
	setValidationRevision(t, &request)
	if err := request.Validate(); err == nil {
		t.Fatal("unknown projected source became an accepted empty projection")
	}
}

func TestRuntimeRequestPreservesDownwardMetadataProjections(t *testing.T) {
	request := validationRuntimeRequest(t)
	for _, field := range []string{"metadata.name", "metadata.namespace", "metadata.uid"} {
		request.Runtime.Template.Spec.Volumes = append(request.Runtime.Template.Spec.Volumes, corev1.Volume{
			Name: strings.TrimPrefix(field, "metadata."),
			VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{
				DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{
					Path: "metadata", FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: field},
				}}},
			}}}},
		})
	}
	setValidationRevision(t, &request)
	if err := request.Validate(); err != nil {
		t.Fatalf("rejected public downward metadata: %v", err)
	}
	if revision, err := WorkloadRevision(request); err != nil || revision != request.Revision {
		t.Fatal("metadata validation changed the admitted revision")
	}
}

func TestRuntimeRequestRejectsMutableEnvironmentInEveryContainer(t *testing.T) {
	for _, source := range []struct {
		name  string
		value *corev1.EnvVarSource
	}{
		{"Secret", &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "private"}, Key: "token"}}},
		{"ConfigMap", &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "mutable"}, Key: "config"}}},
	} {
		for _, kind := range []string{"runtime", "init", "sidecar"} {
			t.Run(source.name+"/"+kind, func(t *testing.T) {
				request := validationRuntimeRequest(t)
				env := []corev1.EnvVar{{Name: "VALUE", ValueFrom: source.value}}
				switch kind {
				case "runtime":
					request.Runtime.Template.Spec.Containers[0].Env = env
				case "init":
					request.Runtime.Template.Spec.InitContainers = []corev1.Container{{Name: "init", Image: request.Image, Env: env}}
				case "sidecar":
					request.Runtime.Template.Spec.Containers = append(request.Runtime.Template.Spec.Containers, corev1.Container{Name: "sidecar", Image: request.Image, Env: env})
				}
				setValidationRevision(t, &request)
				if err := request.Validate(); err == nil || !strings.Contains(err.Error(), "mutable configuration or credentials") {
					t.Fatalf("accepted mutable environment in %s: %v", kind, err)
				}
			})
		}
	}
}
