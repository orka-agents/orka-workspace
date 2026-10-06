package workspaceprovider

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestPatchStatusRejectsStaleSnapshotWithoutOverwritingCurrentStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "runtime", Name: "instance"}, Status: corev1.PodStatus{Phase: corev1.PodPending}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(pod).WithObjects(pod).Build()
	before := &corev1.Pod{}
	key := client.ObjectKeyFromObject(pod)
	if err := c.Get(t.Context(), key, before); err != nil {
		t.Fatal(err)
	}
	first, stale := before.DeepCopy(), before.DeepCopy()
	first.Status.Phase = corev1.PodRunning
	stale.Status.Phase = corev1.PodSucceeded
	if err := PatchStatus(t.Context(), c, before, first); err != nil {
		t.Fatal(err)
	}
	if err := PatchStatus(t.Context(), c, before, stale); !apierrors.IsConflict(err) {
		t.Fatalf("stale status patch did not conflict: %v", err)
	}
	current := &corev1.Pod{}
	if err := c.Get(t.Context(), key, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != corev1.PodRunning {
		t.Fatalf("stale controller overwrote current status: %s", current.Status.Phase)
	}
	retry := current.DeepCopy()
	retry.Status.Phase = corev1.PodSucceeded
	if err := PatchStatus(t.Context(), c, current, retry); err != nil {
		t.Fatalf("fresh status snapshot could not retry: %v", err)
	}
}
