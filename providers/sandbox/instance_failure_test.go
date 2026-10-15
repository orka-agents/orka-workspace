package sandbox

import (
	"strings"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// agent-sandbox replaces a deleted Pod, and restartPolicy Never makes a
// terminated Pod or a restarted process permanent. Each loses the exact
// runtime, so the workspace reports Failed instead of a stale identity, and
// Core's Stop retires it without capturing data.
func TestRuntimePodLossFailsAndRetiresByStop(t *testing.T) {
	for _, loss := range []string{"deleted", "terminated", "restarted"} {
		t.Run(loss, func(t *testing.T) {
			c, request := fixture(t, true)
			first := ready(t, c, request)
			podKey := types.NamespacedName{Namespace: first.Startup.Pod.Namespace, Name: first.Startup.Pod.Name}
			pod := &corev1.Pod{}
			if err := c.Get(t.Context(), podKey, pod); err != nil {
				t.Fatal(err)
			}
			switch loss {
			case "deleted":
				if err := c.Delete(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
				// The upstream controller recreates the Pod under the same name.
				if err := nativeTick(t.Context(), c); err != nil {
					t.Fatal(err)
				}
				if err := c.Get(t.Context(), podKey, pod); err != nil || pod.UID == first.Startup.Pod.UID {
					t.Fatalf("upstream replacement was not simulated: %v", err)
				}
			case "terminated":
				pod.Status.Phase = corev1.PodFailed
			case "restarted":
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "supervisor", RestartCount: 1}}
			}
			if loss != "deleted" {
				if err := c.Status().Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			}

			key := types.NamespacedName{Namespace: request.Key.Namespace, Name: request.Key.Name}
			r := &ExecutionWorkspaceReconciler{Client: c}
			workspace := &workspacev1alpha1.ExecutionWorkspace{}
			for range 2 {
				if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
					t.Fatal(err)
				}
				if err := c.Get(t.Context(), key, workspace); err != nil {
					t.Fatal(err)
				}
				provisioned := workspaceprovider.FindCondition(workspace.Status.Conditions, string(workspacev1alpha1.ConditionWorkspaceProvisioned))
				if workspace.Status.State != workspacev1alpha1.ExecutionWorkspaceStateFailed || workspace.Status.Allocation == nil || workspace.Status.Allocation.Startup != nil ||
					provisioned == nil || provisioned.Reason != instanceFailedReason || !strings.Contains(provisioned.Message, "uncertain work is never replayed") {
					t.Fatalf("runtime Pod loss was not reported as a terminal failure: %#v", workspace.Status)
				}
			}
			if observed, err := New(c).EnsureAllocation(t.Context(), request); err != nil || observed.Startup != nil {
				t.Fatalf("a failed instance became startable again: %#v, %v", observed, err)
			}

			workspace.Spec.Retirement = &workspacev1alpha1.WorkloadRetirement{Sequence: request.Sequence, Identity: first.Identity, Action: workspacev1alpha1.WorkloadRetirementStop}
			workspace.Generation++
			if err := c.Update(t.Context(), workspace); err != nil {
				t.Fatal(err)
			}
			for range 3 {
				if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
					t.Fatal(err)
				}
				if err := nativeTick(t.Context(), c); err != nil {
					t.Fatal(err)
				}
			}
			_, record, err := New(c).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			if record.Operation != "stop" || record.Observation.State != workspaceprovider.AllocationStopped || record.Observation.RetainedData != nil {
				t.Fatalf("Stop did not retire the failed instance: %s %#v", record.Operation, record.Observation)
			}
			if err := c.Get(t.Context(), podKey, &corev1.Pod{}); err == nil {
				t.Fatal("a runtime Pod survived retirement of the failed instance")
			}
			if err := c.Get(t.Context(), key, workspace); err != nil {
				t.Fatal(err)
			}
			if workspace.Status.State != workspacev1alpha1.ExecutionWorkspaceStateFailed {
				t.Fatalf("retirement cleared the terminal failure: %s", workspace.Status.State)
			}
		})
	}
}

// A replacement that agent-sandbox did not produce is never deleted on the
// failed instance's behalf.
func TestFailedStopNeverDeletesAForeignSameNamePod(t *testing.T) {
	c, request := fixture(t, false)
	first := ready(t, c, request)
	podKey := types.NamespacedName{Namespace: first.Startup.Pod.Namespace, Name: first.Startup.Pod.Name}
	pod := &corev1.Pod{}
	if err := c.Get(t.Context(), podKey, pod); err != nil {
		t.Fatal(err)
	}
	pod.UID = "foreign"
	pod.OwnerReferences = nil
	if err := c.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	if _, err := New(c).Observe(t.Context(), request.Key); err == nil {
		t.Fatal("a replaced exact Pod was observed as live")
	}
	if _, err := New(c).recordFailure(t.Context(), request.Key, &runtimeLostError{identity: first.Identity, reason: "runtime Pod was replaced"}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		observed, err := New(c).StopInstance(t.Context(), request.Key, first.Identity)
		if err == nil && observed.State == workspaceprovider.AllocationStopped {
			t.Fatal("a foreign same-name Pod counted as termination")
		}
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), pod); err != nil || pod.UID != "foreign" || pod.DeletionTimestamp != nil {
		t.Fatalf("foreign Pod was deleted: %v", err)
	}
}
