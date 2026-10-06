package sandbox

import (
	"context"
	"errors"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func suspended(t *testing.T, c client.Client, request workspaceprovider.WorkloadRequest, first workspaceprovider.AllocationObservation) workspaceprovider.AllocationObservation {
	t.Helper()
	for range 5 {
		observed, err := (&simulatedLifecycle{New(c)}).SuspendInstance(t.Context(), request.Key, first.Identity)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == workspaceprovider.AllocationStopped {
			return observed
		}
	}
	t.Fatal("Sandbox never suspended")
	return workspaceprovider.AllocationObservation{}
}

func continuation(request workspaceprovider.WorkloadRequest, retired workspaceprovider.AllocationObservation) workspaceprovider.WorkloadRequest {
	next := *request.DeepCopy()
	next.Sequence++
	next.PreviousInstance = &retired.Identity
	next.RetainedData = retired.RetainedData
	next.Revision, _ = workspaceprovider.WorkloadRevision(next)
	return next
}

func TestColdResumeRejectsLostStorageAndLivePod(t *testing.T) {
	for _, mutation := range []string{"PVCUID", "PVUID", "SandboxUID", "livePod", "missingMount"} {
		t.Run(mutation, func(t *testing.T) {
			c, request := fixture(t, true)
			first := ready(t, c, request)
			retired := suspended(t, c, request, first)
			next := continuation(request, retired)
			_, record, err := New(c).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "PVCUID":
				pvc := &corev1.PersistentVolumeClaim{}
				if err := c.Get(t.Context(), types.NamespacedName{Namespace: record.Namespace, Name: record.Storage.ClaimName}, pvc); err != nil {
					t.Fatal(err)
				}
				pvc.UID = "replacement"
				if err := c.Update(t.Context(), pvc); err != nil {
					t.Fatal(err)
				}
			case "PVUID":
				pv := &corev1.PersistentVolume{}
				if err := c.Get(t.Context(), types.NamespacedName{Name: record.Storage.VolumeName}, pv); err != nil {
					t.Fatal(err)
				}
				pv.UID = "replacement"
				if err := c.Update(t.Context(), pv); err != nil {
					t.Fatal(err)
				}
			case "SandboxUID":
				sb, err := New(c).sandbox(t.Context(), record)
				if err != nil {
					t.Fatal(err)
				}
				sb.UID = "replacement"
				if err := c.Update(t.Context(), sb); err != nil {
					t.Fatal(err)
				}
			case "livePod":
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: record.Namespace, Name: record.Pod.Name, UID: record.Pod.UID}}
				if err := c.Create(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			case "missingMount":
				next.Runtime.Template.Spec.Containers[0].VolumeMounts = nil
				next.Revision, _ = workspaceprovider.WorkloadRevision(next)
			}
			if err := admit(t, c)(t.Context(), next); err != nil {
				t.Fatal(err)
			}
			if _, err := New(c).EnsureAllocation(t.Context(), next); err == nil {
				t.Fatal("unsafe cold resume was accepted")
			}
			_, after, err := New(c).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			if after.Request.Sequence != 1 || after.Observation.RetainedData == nil {
				t.Fatal("rejected resume consumed the last retained lineage")
			}
		})
	}
}

type resumeRaceClient struct {
	client.Client
	duringTemplateUpdate func() error
	done                 bool
}

func (c *resumeRaceClient) Update(ctx context.Context, object client.Object, options ...client.UpdateOption) error {
	_, isTemplate := object.(*extv1beta1.SandboxTemplate)
	if !c.done && isTemplate {
		c.done = true
		if err := c.duringTemplateUpdate(); err != nil {
			return err
		}
	}
	return c.Client.Update(ctx, object, options...)
}

func TestResumeCannotRestartAfterConcurrentStop(t *testing.T) {
	c, request := fixture(t, true)
	first := ready(t, c, request)
	retired := suspended(t, c, request, first)
	next := continuation(request, retired)
	next.Runtime.Template.Spec.Containers[0].Env = append(next.Runtime.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "PUBLIC_NONCE", Value: "next"})
	next.Revision, _ = workspaceprovider.WorkloadRevision(next)
	if err := admit(t, c)(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	race := &resumeRaceClient{Client: c}
	race.duringTemplateUpdate = func() error {
		_, record, err := New(c).read(t.Context(), request.Key)
		if err != nil {
			return err
		}
		for range 3 {
			observed, err := (&simulatedLifecycle{New(c)}).StopInstance(t.Context(), request.Key, record.Observation.Identity)
			if err != nil {
				return err
			}
			if observed.State == workspaceprovider.AllocationStopped {
				return nil
			}
		}
		return errors.New("concurrent stop did not settle")
	}
	if _, err := New(race).EnsureAllocation(t.Context(), next); err == nil {
		t.Fatal("stale resume survived concurrent retirement")
	}
	if !race.done {
		t.Fatal("race hook never ran")
	}
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := New(c).sandbox(t.Context(), record)
	if err != nil {
		t.Fatal(err)
	}
	if record.Observation.State != workspaceprovider.AllocationStopped || sb.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeSuspended {
		t.Fatal("resume restarted retired compute")
	}
}

func TestDeleteBeforeReadinessStillWaitsForBackingPV(t *testing.T) {
	c, request := fixture(t, true)
	first, err := (&simulatedLifecycle{New(c)}).EnsureAllocation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	var stopped workspaceprovider.AllocationObservation
	for range 4 {
		stopped, err = (&simulatedLifecycle{New(c)}).StopInstance(t.Context(), request.Key, first.Identity)
		if err != nil {
			t.Fatal(err)
		}
		if stopped.State == workspaceprovider.AllocationStopped {
			break
		}
	}
	policy := workspacev1alpha1.ExecutionWorkspaceDeletionPolicy{ProviderResources: workspacev1alpha1.WorkspaceDeletionActionDelete, PersistentVolumes: workspacev1alpha1.WorkspaceDeletionActionDelete, Checkpoints: workspacev1alpha1.WorkspaceDeletionActionDelete}
	observed, err := New(c).DeleteAllocation(t.Context(), request.Key, stopped.Identity, policy)
	if err != nil {
		t.Fatal(err)
	}
	if observed.State == workspaceprovider.AllocationDeleted {
		t.Fatal("deletion completed before downstream native cleanup")
	}
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.Storage == nil || record.Storage.VolumeUID == "" {
		t.Fatal("cleanup failed to pin storage created before readiness")
	}
	for _, object := range []client.Object{&sandboxv1beta1.Sandbox{ObjectMeta: metav1.ObjectMeta{Namespace: record.Namespace, Name: record.Sandbox.Name}}, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: record.Namespace, Name: record.Storage.ClaimName}}} {
		if err := c.Delete(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	observed, err = New(c).DeleteAllocation(t.Context(), request.Key, stopped.Identity, policy)
	if err != nil {
		t.Fatal(err)
	}
	if observed.State == workspaceprovider.AllocationDeleted {
		t.Fatal("data disposition claimed deletion while the PV remained")
	}
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: record.Storage.VolumeName}}
	if err := c.Delete(t.Context(), pv); err != nil {
		t.Fatal(err)
	}
	for range 6 {
		observed, err = New(c).DeleteAllocation(t.Context(), request.Key, stopped.Identity, policy)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == workspaceprovider.AllocationDeleted {
			return
		}
	}
	t.Fatal("cleanup did not finish after the PV disappeared")
}

func TestReplacementAfterStopBeforeNativeMaterialization(t *testing.T) {
	c, request := fixture(t, false)
	first, err := New(c).EnsureAllocation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	var stopped workspaceprovider.AllocationObservation
	for range 4 {
		stopped, err = New(c).StopInstance(t.Context(), request.Key, first.Identity)
		if err != nil {
			t.Fatal(err)
		}
		if stopped.State == workspaceprovider.AllocationStopped {
			break
		}
	}
	next := continuation(request, stopped)
	if err := admit(t, c)(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	for range 10 {
		observed, err := (&simulatedLifecycle{New(c)}).EnsureAllocation(t.Context(), next)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == workspaceprovider.AllocationReady {
			if observed.Sequence != 2 || observed.Identity.InstanceID == first.Identity.InstanceID {
				t.Fatal("replacement reused the retired instance")
			}
			return
		}
	}
	t.Fatal("replacement never became ready")
}
