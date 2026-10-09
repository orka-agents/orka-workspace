package sandbox

import (
	"reflect"
	"strings"
	"testing"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func sandboxVolumeRequest(t *testing.T, c client.Client, request sdk.WorkloadRequest, source corev1.VolumeSource) sdk.WorkloadRequest {
	t.Helper()
	request.Runtime.Template.Spec.Volumes = append(request.Runtime.Template.Spec.Volumes, corev1.Volume{Name: "extra-storage", VolumeSource: source})
	request.Runtime.Template.Spec.Containers[0].VolumeMounts = append(request.Runtime.Template.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "extra-storage", MountPath: "/extra-storage"})
	var err error
	request.Revision, err = sdk.WorkloadRevision(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := admit(t, c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	return request
}

func TestSandboxRejectsUnmanagedStorageBeforeAllocation(t *testing.T) {
	for name, source := range map[string]corev1.VolumeSource{
		"hostPath":                 {HostPath: &corev1.HostPathVolumeSource{Path: "/operator-data"}},
		"pre-existing PVC":         {PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "shared-data"}},
		"NFS":                      {NFS: &corev1.NFSVolumeSource{Server: "data.example.test", Path: "/shared"}},
		"inline CSI":               {CSI: &corev1.CSIVolumeSource{Driver: "storage.example.test"}},
		"generic ephemeral PVC":    {Ephemeral: &corev1.EphemeralVolumeSource{VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{}}},
		"image volume":             {Image: &corev1.ImageVolumeSource{Reference: "fixture.invalid/data@sha256:" + strings.Repeat("1", 64)}},
		"mixed volume sources":     {EmptyDir: &corev1.EmptyDirVolumeSource{}, HostPath: &corev1.HostPathVolumeSource{Path: "/operator-data"}},
		"mutable trust projection": {Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{ClusterTrustBundle: &corev1.ClusterTrustBundleProjection{Name: new("operator-trust"), Path: "trust.pem"}}}}},
	} {
		t.Run(name, func(t *testing.T) {
			c, request := fixture(t, false)
			request = sandboxVolumeRequest(t, c, request, source)
			before := &api.ExecutionWorkspace{}
			if err := c.Get(t.Context(), client.ObjectKey{Namespace: request.Key.Namespace, Name: request.Key.Name}, before); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || observed.Startup != nil {
					t.Fatalf("unmanaged storage reached allocation: %+v, %v", observed, err)
				}
			}
			requireNoSandboxAllocation(t, c, request)
			after := &api.ExecutionWorkspace{}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(before), after); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("unmanaged storage wrote admission markers or status")
			}
		})
	}
}

func TestSandboxSupportedEphemeralVolumesKeepManagedDurableLifecycle(t *testing.T) {
	c, request := fixture(t, true)
	items := []corev1.DownwardAPIVolumeFile{{Path: "uid", FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}}
	request.Runtime.Template.Spec.Volumes = []corev1.Volume{
		{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "metadata", VolumeSource: corev1.VolumeSource{DownwardAPI: &corev1.DownwardAPIVolumeSource{Items: items}}},
		{Name: "projected-metadata", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{DownwardAPI: &corev1.DownwardAPIProjection{Items: items}}}}}},
	}
	for _, volume := range request.Runtime.Template.Spec.Volumes {
		request.Runtime.Template.Spec.Containers[0].VolumeMounts = append(request.Runtime.Template.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: volume.Name, MountPath: "/" + volume.Name})
	}
	var err error
	request.Revision, err = sdk.WorkloadRevision(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := admit(t, c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	first := ready(t, c, request)
	if first.Startup == nil || len(first.Startup.PersistentVolumes) != 1 {
		t.Fatal("supported ephemeral volumes lost managed storage evidence")
	}
	if stopped := suspended(t, c, request, first); stopped.RetainedData == nil {
		t.Fatal("supported ephemeral volumes prevented managed data suspension")
	}
}

// Reconstruct a journal and matching native objects written by the prior
// implementation, which admitted arbitrary storage. Cleanup must keep the
// exact ownership fence without adopting the referenced storage.
func legacySandboxStorage(t *testing.T, source corev1.VolumeSource) (client.Client, sdk.WorkloadRequest, sdk.InstanceIdentity) {
	t.Helper()
	c, request := fixture(t, false)
	first := ready(t, c, request)
	request = sandboxVolumeRequest(t, c, request, source)
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
	template := &extv1beta1.SandboxTemplate{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: record.Namespace, Name: record.Template.Name}, template); err != nil {
		t.Fatal(err)
	}
	template.Spec = desiredTemplate(record).Spec
	if err := c.Update(t.Context(), template); err != nil {
		t.Fatal(err)
	}
	sb := &sandboxv1beta1.Sandbox{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: record.Namespace, Name: record.Sandbox.Name}, sb); err != nil {
		t.Fatal(err)
	}
	sb.Spec.PodTemplate.Spec = *request.Runtime.Template.Spec.DeepCopy()
	if err := c.Update(t.Context(), sb); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: first.Startup.Pod.Namespace, Name: first.Startup.Pod.Name}, pod); err != nil {
		t.Fatal(err)
	}
	pod.Spec = *request.Runtime.Template.Spec.DeepCopy()
	if err := c.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	return c, request, record.Observation.Identity
}

func TestLegacySandboxUnmanagedStorageCanDrainWithoutClaimingDataDeletion(t *testing.T) {
	for name, source := range map[string]corev1.VolumeSource{
		"PVC":      {PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "shared-data"}},
		"hostPath": {HostPath: &corev1.HostPathVolumeSource{Path: "/operator-data"}},
		"NFS":      {NFS: &corev1.NFSVolumeSource{Server: "data.example.test", Path: "/shared"}},
	} {
		t.Run(name, func(t *testing.T) {
			c, request, identity := legacySandboxStorage(t, source)
			shared := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: request.Runtime.Template.Namespace, Name: "shared-data", UID: "operator-owned-pvc"}}
			if err := c.Create(t.Context(), shared); err != nil {
				t.Fatal(err)
			}
			before := shared.DeepCopy()
			if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || observed.Startup != nil {
				t.Fatalf("legacy unmanaged storage was re-admitted: %+v, %v", observed, err)
			}
			if observed, err := New(c).Observe(t.Context(), request.Key); err == nil || observed.Startup != nil {
				t.Fatalf("legacy unmanaged storage retained startup: %+v, %v", observed, err)
			}
			stopped := false
			for range 5 {
				observed, err := (&simulatedLifecycle{New(c)}).StopInstance(t.Context(), request.Key, identity)
				if err != nil {
					t.Fatal(err)
				}
				if observed.State == sdk.AllocationStopped {
					stopped = true
					break
				}
			}
			if !stopped {
				t.Fatal("legacy exact Pod did not stop")
			}
			var blocked error
			for range 8 {
				observed, err := (&simulatedLifecycle{New(c)}).DeleteAllocation(t.Context(), request.Key, identity, cleanupPolicy())
				if observed.State == sdk.AllocationDeleted || observed.Disposition != nil {
					t.Fatal("legacy cleanup falsely confirmed storage disposition")
				}
				if err != nil {
					blocked = err
					break
				}
			}
			if blocked == nil || !strings.Contains(blocked.Error(), "unmanaged storage prevents data disposition") {
				t.Fatalf("legacy cleanup failed without an actionable storage error: %v", blocked)
			}
			_, record, err := New(c).read(t.Context(), request.Key)
			if err != nil || record.Observation.State != sdk.AllocationStopped || record.Observation.Identity != identity || record.Observation.Disposition != nil || record.Observation.Startup != nil {
				t.Fatalf("legacy stopped recovery evidence was lost: %v", err)
			}
			for _, entry := range []struct {
				name   string
				object client.Object
			}{{record.Pod.Name, &corev1.Pod{}}, {record.Claim.Name, &extv1beta1.SandboxClaim{}}, {record.Sandbox.Name, &sandboxv1beta1.Sandbox{}}, {record.Template.Name, &extv1beta1.SandboxTemplate{}}, {record.WarmPool.Name, &extv1beta1.SandboxWarmPool{}}, {record.Anchor.Name, &corev1.ConfigMap{}}} {
				if err := c.Get(t.Context(), client.ObjectKey{Namespace: record.Namespace, Name: entry.name}, entry.object); !apierrors.IsNotFound(err) {
					t.Fatalf("legacy compute cleanup left %T: %v", entry.object, err)
				}
			}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(shared), shared); err != nil || !reflect.DeepEqual(before, shared) {
				t.Fatalf("legacy cleanup changed external storage: %v", err)
			}
			if observed, err := New(c).Observe(t.Context(), request.Key); err != nil || observed.State != sdk.AllocationStopped || observed.Disposition != nil {
				t.Fatalf("legacy drain lost its read-only stopped state: %+v, %v", observed, err)
			}
		})
	}
}

func TestLegacySandboxUnsupportedStorageTerminalClaimsCannotReplay(t *testing.T) {
	c, request, identity := legacySandboxStorage(t, corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "shared-data"}})
	cm, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	policy := cleanupPolicy()
	record.Operation = "delete"
	record.DeletionPolicy = &policy
	record.Observation.State = sdk.AllocationDeleted
	record.Observation.Startup = nil
	record.Observation.Disposition = deletedDisposition(false)
	if err := New(c).save(t.Context(), cm, record); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []struct {
		name   string
		invoke func() (sdk.AllocationObservation, error)
	}{
		{"Observe", func() (sdk.AllocationObservation, error) { return New(c).Observe(t.Context(), request.Key) }},
		{"Stop", func() (sdk.AllocationObservation, error) {
			return New(c).StopInstance(t.Context(), request.Key, identity)
		}},
		{"Delete", func() (sdk.AllocationObservation, error) {
			return New(c).DeleteAllocation(t.Context(), request.Key, identity, policy)
		}},
	} {
		t.Run(operation.name, func(t *testing.T) {
			if observed, err := operation.invoke(); err == nil || observed.Disposition != nil {
				t.Fatalf("legacy false data disposition replayed: %+v, %v", observed, err)
			}
		})
	}
}

func TestSandboxTemplateBehaviorPolicyDriftWithdrawsStartup(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		for _, field := range []string{"network", "environment", "volume claims"} {
			t.Run(map[bool]string{false: "ephemeral", true: "durable"}[persistent]+"/"+field, func(t *testing.T) {
				c, request := fixture(t, persistent)
				first := ready(t, c, request)
				_, record, err := New(c).read(t.Context(), request.Key)
				if err != nil {
					t.Fatal(err)
				}
				template := &extv1beta1.SandboxTemplate{}
				if err := c.Get(t.Context(), types.NamespacedName{Namespace: record.Namespace, Name: record.Template.Name}, template); err != nil {
					t.Fatal(err)
				}
				switch field {
				case "network":
					template.Spec.NetworkPolicyManagement = extv1beta1.NetworkPolicyManagementManaged
				case "environment":
					template.Spec.EnvVarsInjectionPolicy = extv1beta1.EnvVarsInjectionPolicyOverrides
				case "volume claims":
					template.Spec.VolumeClaimTemplatesPolicy = extv1beta1.VolumeClaimTemplatesPolicyOverrides
				}
				if err := c.Update(t.Context(), template); err != nil {
					t.Fatal(err)
				}
				if observed, err := New(c).Observe(t.Context(), request.Key); err == nil || observed.Startup != nil {
					t.Fatalf("template behavior drift retained startup: %+v, %v", observed, err)
				}
				if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || observed.Startup != nil {
					t.Fatalf("template behavior drift was re-admitted: %+v, %v", observed, err)
				}
				if persistent {
					if _, err := New(c).SuspendInstance(t.Context(), request.Key, first.Identity); err == nil {
						t.Fatal("template behavior drift allowed retained-data suspension")
					}
					sb := &sandboxv1beta1.Sandbox{}
					if err := c.Get(t.Context(), client.ObjectKey{Namespace: record.Namespace, Name: record.Sandbox.Name}, sb); err != nil || sb.UID != record.Sandbox.UID || sb.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeRunning {
						t.Fatalf("rejected suspension changed the exact Sandbox: %v", err)
					}
					_, after, err := New(c).read(t.Context(), request.Key)
					if err != nil || after.Operation != "ensure" || after.Observation.State != sdk.AllocationReady {
						t.Fatalf("rejected suspension changed lifecycle intent: %v", err)
					}
				}
				for range 5 {
					observed, err := (&simulatedLifecycle{New(c)}).StopInstance(t.Context(), request.Key, first.Identity)
					if err != nil {
						t.Fatal(err)
					}
					if observed.State == sdk.AllocationStopped {
						return
					}
				}
				t.Fatal("template behavior drift blocked exact stop")
			})
		}
	}
}
