package substrate

import (
	"strings"
	"testing"

	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestNativeRejectsAdmittedEmptyDirQuotaBeforeCompute(t *testing.T) {
	for _, test := range []struct {
		name, quota string
		suspend     bool
	}{
		{name: "scratch quota", quota: "512Mi"},
		{name: "durable mount quota", quota: "4Gi", suspend: true},
		{name: "invalid negative quota", quota: "-1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, native, request := fixture(t, test.suspend)
			name := "scratch"
			if test.suspend {
				name = durableVolumeName
			} else {
				request.Runtime.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: name, MountPath: "/tmp"}}
			}
			quota := resource.MustParse(test.quota)
			request.Runtime.Template.Spec.Volumes = []corev1.Volume{{Name: name, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &quota}}}}
			request.Revision, _ = sdk.WorkloadRevision(request)
			if err := admit(c)(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), "native emptyDir quota") {
				t.Fatalf("unsupported admitted quota was not rejected: %v", err)
			}
			if native.boots != 0 || len(native.actors) != 0 || len(native.templates) != 1 || len(native.workers) != 0 {
				t.Fatal("unsupported quota created native compute or a derived template")
			}
			pools := &unstructured.UnstructuredList{}
			pools.SetAPIVersion("ate.dev/v1alpha1")
			pools.SetKind("WorkerPoolList")
			if err := c.List(t.Context(), pools); err != nil || len(pools.Items) != 1 {
				t.Fatalf("unsupported quota created a private worker pool: count=%d err=%v", len(pools.Items), err)
			}
			if revision, err := sdk.WorkloadRevision(request); err != nil || revision != request.Revision {
				t.Fatal("provider rewrote the admitted workload")
			}
		})
	}
}

func TestNativeRejectsEmptyDirWithoutQuotaBeforeCompute(t *testing.T) {
	for _, medium := range []corev1.StorageMedium{corev1.StorageMediumDefault, corev1.StorageMediumMemory} {
		for _, explicitZero := range []bool{false, true} {
			c, native, request := fixture(t, false)
			emptyDir := &corev1.EmptyDirVolumeSource{Medium: medium}
			if explicitZero {
				zero := resource.MustParse("0")
				emptyDir.SizeLimit = &zero
			}
			request.Runtime.Template.Spec.Volumes = []corev1.Volume{{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: emptyDir}}}
			request.Runtime.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "scratch", MountPath: "/tmp"}}
			request.Revision, _ = sdk.WorkloadRevision(request)
			if err := admit(c)(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), "native emptyDir volume") {
				t.Fatalf("emptyDir was accepted without a native volume primitive: %v", err)
			}
			if native.boots != 0 || len(native.actors) != 0 || len(native.templates) != 1 || len(native.workers) != 0 {
				t.Fatal("unsupported emptyDir created native compute or a derived template")
			}
		}
	}
}
