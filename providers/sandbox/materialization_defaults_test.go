package sandbox

import (
	"testing"

	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func materializedPod(t *testing.T, c client.Client, request sdk.WorkloadRequest) *corev1.Pod {
	t.Helper()
	observed, err := (&simulatedLifecycle{New(c)}).EnsureAllocation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Startup != nil {
		t.Fatal("startup was published before Pod materialization")
	}
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: record.Namespace, Name: record.Claim.Name}, pod); err != nil {
		t.Fatal(err)
	}
	return pod
}

func TestDownwardAPIVolumeDefaultsPreserveStartupAttestation(t *testing.T) {
	for _, projected := range []bool{false, true} {
		name := "downwardAPI"
		if projected {
			name = "projected"
		}
		t.Run(name, func(t *testing.T) {
			c, request := fixture(t, false)
			items := []corev1.DownwardAPIVolumeFile{{Path: "name", FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}}
			volume := corev1.Volume{Name: "metadata"}
			if projected {
				volume.Projected = &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{DownwardAPI: &corev1.DownwardAPIProjection{Items: items}}}}
			} else {
				volume.DownwardAPI = &corev1.DownwardAPIVolumeSource{Items: items}
			}
			request.Runtime.Template.Spec.Volumes = []corev1.Volume{volume}
			request.Runtime.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: volume.Name, MountPath: "/runtime-metadata", ReadOnly: true}}
			var err error
			request.Revision, err = sdk.WorkloadRevision(request)
			if err != nil {
				t.Fatal(err)
			}
			if err := admit(t, c)(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			pod := materializedPod(t, c, request)
			mode := int32(0644)
			if projected {
				pod.Spec.Volumes[0].Projected.DefaultMode = &mode
				pod.Spec.Volumes[0].Projected.Sources[0].DownwardAPI.Items[0].FieldRef.APIVersion = "v1"
			} else {
				pod.Spec.Volumes[0].DownwardAPI.DefaultMode = &mode
				pod.Spec.Volumes[0].DownwardAPI.Items[0].FieldRef.APIVersion = "v1"
			}
			if err := c.Update(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			observed := ready(t, c, request)
			if observed.Startup == nil || observed.Startup.Pod.UID != pod.UID {
				t.Fatal("API-defaulted Pod did not retain its exact startup identity")
			}
			if projected {
				*pod.Spec.Volumes[0].Projected.DefaultMode = 0400
			} else {
				*pod.Spec.Volumes[0].DownwardAPI.DefaultMode = 0400
			}
			if err := c.Update(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			if observed, err := New(c).Observe(t.Context(), request.Key); err == nil || observed.Startup != nil {
				t.Fatalf("changed volume mode retained startup: observation=%+v error=%v", observed, err)
			}
		})
	}
}

func TestAdmissionInjectedImagePullSecretsWithdrawStartup(t *testing.T) {
	c, request := fixture(t, false)
	pod := materializedPod(t, c, request)
	pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "admission-pull-credentials"}}
	if err := c.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || observed.Startup != nil {
		t.Fatalf("injected pull credentials were accepted: observation=%+v error=%v", observed, err)
	}
	if observed, err := New(c).Observe(t.Context(), request.Key); err == nil || observed.Startup != nil {
		t.Fatalf("injected pull credentials remained observable: observation=%+v error=%v", observed, err)
	}
}
