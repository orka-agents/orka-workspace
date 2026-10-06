package fake

import (
	"reflect"
	"strings"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func storageRequest(t *testing.T, request workspaceprovider.WorkloadRequest, source corev1.VolumeSource, mount bool) workspaceprovider.WorkloadRequest {
	t.Helper()
	request.Runtime = request.Runtime.DeepCopy()
	request.Runtime.Template.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: source}}
	if mount {
		request.Runtime.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "data", MountPath: "/data"}}
	}
	var err error
	request.Revision, err = workspaceprovider.WorkloadRevision(request)
	if err != nil {
		t.Fatal(err)
	}
	// These templates are valid public requests for providers that implement
	// their storage lifecycle, rather than failures of shared validation.
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	return request
}

func TestPodRejectsUnsupportedStorageBeforeAllocationEffects(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source corev1.VolumeSource
		mount  bool
	}{
		{"PVC mounted", corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "shared-data"}}, true},
		{"PVC unmounted", corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "shared-data"}}, false},
		{"generic ephemeral claim", corev1.VolumeSource{Ephemeral: &corev1.EphemeralVolumeSource{VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}}}}}}, true},
		{"NFS", corev1.VolumeSource{NFS: &corev1.NFSVolumeSource{Server: "storage.example", Path: "/shared"}}, true},
		{"HostPath", corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/persistent-data"}}, true},
		{"CSI", corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{Driver: "storage.example"}}, true},
		{"RBD", corev1.VolumeSource{RBD: &corev1.RBDVolumeSource{CephMonitors: []string{"storage.example"}, RBDImage: "shared"}}, true},
		{"mixed ephemeral and persistent", corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}, NFS: &corev1.NFSVolumeSource{Server: "storage.example", Path: "/shared"}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, request := runtimeFixture(t)
			request = storageRequest(t, request, tc.source, tc.mount)
			if err := publishRequest(t.Context(), c, request); err != nil {
				t.Fatal(err)
			}
			before := workspaceFor(t, c, request.Key)
			for range 2 {
				observed, err := New(c).EnsureAllocation(t.Context(), request)
				if err == nil || !strings.Contains(err.Error(), "unsupported storage lifecycle") || observed.Startup != nil {
					t.Fatalf("unsupported storage request accepted: %v", err)
				}
			}
			if c.creates != 0 {
				t.Fatal("unsupported storage created a Pod")
			}
			var journals corev1.ConfigMapList
			if err := c.List(t.Context(), &journals); err != nil {
				t.Fatal(err)
			}
			if len(journals.Items) != 0 {
				t.Fatal("unsupported storage wrote a journal")
			}
			if !reflect.DeepEqual(before, workspaceFor(t, c, request.Key)) {
				t.Fatal("unsupported storage wrote a recovery marker or status")
			}
		})
	}
}

func TestPodSupportsEphemeralAndPublicMetadataVolumes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source corev1.VolumeSource
	}{
		{"EmptyDir", corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{"DownwardAPI", corev1.VolumeSource{DownwardAPI: &corev1.DownwardAPIVolumeSource{Items: []corev1.DownwardAPIVolumeFile{{Path: "name", FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.name"}}}}}},
		{"credential-free Projected", corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{Path: "name", FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.name"}}}}}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, request := runtimeFixture(t)
			request = storageRequest(t, request, tc.source, true)
			if err := publishRequest(t.Context(), c, request); err != nil {
				t.Fatal(err)
			}
			observed, err := New(c).EnsureAllocation(t.Context(), request)
			if err != nil || observed.State != workspaceprovider.AllocationReady || observed.Startup == nil || c.creates != 1 {
				t.Fatalf("supported volume rejected: %v", err)
			}
		})
	}
}

// Seed a request already materialized by an older fake adapter. New allocation
// validation must not prevent retirement of its recorded exact Pod UID.
func legacyStorageAllocation(t *testing.T) (*podClient, workspaceprovider.WorkloadRequest, workspaceprovider.InstanceIdentity) {
	t.Helper()
	c, request := runtimeFixture(t)
	allocated, err := New(c).EnsureAllocation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	request = storageRequest(t, request, corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "shared-data"}}, true)
	if err := publishRequest(t.Context(), c, request); err != nil {
		t.Fatal(err)
	}
	cm, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	record.Request = request
	record.Observation.Identity.RequestRevision = request.Revision
	record.Observation.Startup.Identity = record.Observation.Identity
	if err := New(c).save(t.Context(), cm, record); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: allocated.Startup.Pod.Namespace, Name: allocated.Startup.Pod.Name}, pod); err != nil {
		t.Fatal(err)
	}
	pod.Spec = *request.Runtime.Template.Spec.DeepCopy()
	pod.Annotations[podRequestAnnotation] = request.Revision
	if err := c.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	return c, request, record.Observation.Identity
}

func TestLegacyPersistentStorageStopsExactPodWithoutClaimingStorageDeletion(t *testing.T) {
	c, request, identity := legacyStorageAllocation(t)
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: request.Runtime.Template.Namespace, Name: "shared-data", UID: "shared-pvc-uid"}}
	if err := c.Create(t.Context(), pvc); err != nil {
		t.Fatal(err)
	}
	before := pvc.DeepCopy()
	for range 2 {
		if _, err := New(c).StopInstance(t.Context(), request.Key, identity); err != nil {
			t.Fatal(err)
		}
	}
	stopped, err := New(c).Observe(t.Context(), request.Key)
	if err != nil || stopped.State != workspaceprovider.AllocationStopped || stopped.Identity != identity || stopped.Startup != nil {
		t.Fatalf("legacy exact Pod cleanup failed: %v", err)
	}
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: record.Pod.Namespace, Name: record.Pod.Name}, pod); !apierrors.IsNotFound(err) {
		t.Fatalf("exact Pod remains: %v", err)
	}
	recordBefore := *record
	policy := deletionPolicy()
	policy.PersistentVolumes = workspacev1alpha1.WorkspaceDeletionActionDelete
	for range 2 {
		observed, err := New(c).DeleteAllocation(t.Context(), request.Key, identity, policy)
		if err == nil || !strings.Contains(err.Error(), "unsupported storage lifecycle") || observed.State == workspaceprovider.AllocationDeleted || observed.Disposition != nil {
			t.Fatalf("legacy storage deletion was falsely confirmed: %v", err)
		}
	}
	_, record, err = New(c).read(t.Context(), request.Key)
	if err != nil || !reflect.DeepEqual(recordBefore, *record) {
		t.Fatalf("legacy stopped journal changed: %v", err)
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(pvc), pvc); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, pvc) {
		t.Fatal("shared PVC was mutated")
	}
}

func TestLegacyUnsupportedStorageDeletionClaimsCannotBeReplayed(t *testing.T) {
	c, request, identity := legacyStorageAllocation(t)
	cm, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	policy := deletionPolicy()
	policy.PersistentVolumes = workspacev1alpha1.WorkspaceDeletionActionDelete
	record.Operation = "delete"
	record.DeletionPolicy = &policy
	record.Observation.State = workspaceprovider.AllocationDeleted
	record.Observation.Startup = nil
	record.Observation.Disposition = fakeDeletedDisposition(policy)
	if err := New(c).save(t.Context(), cm, record); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []struct {
		name   string
		invoke func() (workspaceprovider.AllocationObservation, error)
	}{
		{"Observe", func() (workspaceprovider.AllocationObservation, error) {
			return New(c).Observe(t.Context(), request.Key)
		}},
		{"Stop", func() (workspaceprovider.AllocationObservation, error) {
			return New(c).StopInstance(t.Context(), request.Key, identity)
		}},
		{"Delete", func() (workspaceprovider.AllocationObservation, error) {
			return New(c).DeleteAllocation(t.Context(), request.Key, identity, policy)
		}},
	} {
		t.Run(operation.name, func(t *testing.T) {
			observed, err := operation.invoke()
			if err == nil || !strings.Contains(err.Error(), "unsupported storage lifecycle") || observed.Disposition != nil {
				t.Fatalf("legacy false disposition replayed: %v", err)
			}
		})
	}
}
