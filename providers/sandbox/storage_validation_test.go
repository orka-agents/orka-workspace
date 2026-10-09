package sandbox

import (
	"errors"
	"strings"
	"testing"
	"time"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func updateWorkspacePolicy(t *testing.T, c client.Client, request workspaceprovider.WorkloadRequest, change func(*workspacev1alpha1.ExecutionWorkspaceSpec)) {
	t.Helper()
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: request.Key.Namespace, Name: request.Key.Name}, workspace); err != nil {
		t.Fatal(err)
	}
	change(&workspace.Spec)
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
}

func requireNoSandboxAllocation(t *testing.T, c client.Client, request workspaceprovider.WorkloadRequest) {
	t.Helper()
	if _, _, err := New(c).read(t.Context(), request.Key); !errors.Is(err, workspaceprovider.ErrNotFound) {
		t.Fatalf("rejected policy created allocation intent: %v", err)
	}
	for _, list := range []client.ObjectList{&extv1beta1.SandboxClaimList{}, &extv1beta1.SandboxTemplateList{}, &extv1beta1.SandboxWarmPoolList{}, &sandboxv1beta1.SandboxList{}, &corev1.PodList{}, &corev1.PersistentVolumeClaimList{}, &corev1.ConfigMapList{}} {
		if err := c.List(t.Context(), list); err != nil {
			t.Fatal(err)
		}
		objects, err := apimeta.ExtractList(list)
		if err != nil || len(objects) != 0 {
			t.Fatalf("rejected policy created %T: %v, count %d", list, err, len(objects))
		}
	}
}

func TestSandboxRejectsRetainBeforeAllocation(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		for _, category := range []string{"provider resources", "persistent volumes", "checkpoints"} {
			t.Run(map[bool]string{false: "ephemeral", true: "durable"}[persistent]+"/"+category, func(t *testing.T) {
				c, request := fixture(t, persistent)
				updateWorkspacePolicy(t, c, request, func(spec *workspacev1alpha1.ExecutionWorkspaceSpec) {
					switch category {
					case "provider resources":
						spec.Lifecycle.DeletionPolicy.ProviderResources = workspacev1alpha1.WorkspaceDeletionActionRetain
					case "persistent volumes":
						spec.Lifecycle.DeletionPolicy.PersistentVolumes = workspacev1alpha1.WorkspaceDeletionActionRetain
					case "checkpoints":
						spec.Lifecycle.DeletionPolicy.Checkpoints = workspacev1alpha1.WorkspaceDeletionActionRetain
					}
				})
				if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), "all-Delete") || observed.Startup != nil {
					t.Fatalf("unsupported Retain policy reached allocation: %+v, %v", observed, err)
				}
				requireNoSandboxAllocation(t, c, request)
			})
		}
	}
}

func TestSandboxSuspensionRequiresOwningWorkspacePolicyBeforeAllocation(t *testing.T) {
	for _, failure := range []string{"service mode", "no session", "session without name", "session without UID", "Suspend disallowed", "no expiry", "zero idle expiry", "negative idle expiry", "zero lifetime", "negative lifetime", "lifetime shorter than idle", "count cap without lifetime"} {
		t.Run(failure, func(t *testing.T) {
			c, request := fixture(t, true)
			if failure == "count cap without lifetime" {
				request = cappedSandboxRequest(t, c, request, 1)
			}
			updateWorkspacePolicy(t, c, request, func(spec *workspacev1alpha1.ExecutionWorkspaceSpec) {
				switch failure {
				case "service mode":
					spec.Mode = workspacev1alpha1.ExecutionWorkspaceModeService
				case "no session":
					spec.SessionRef = nil
				case "session without name":
					spec.SessionRef.Name = ""
				case "session without UID":
					spec.SessionRef.UID = ""
				case "Suspend disallowed":
					spec.Lifecycle.AllowedOnDetach = []workspacev1alpha1.WorkspaceOnDetach{workspacev1alpha1.WorkspaceOnDetachDelete}
				case "no expiry":
					spec.Lifecycle.MaxLifetime = nil
				case "zero idle expiry", "negative idle expiry":
					value := time.Duration(0)
					if failure == "negative idle expiry" {
						value = -time.Minute
					}
					spec.Lifecycle.IdleTimeout = &metav1.Duration{Duration: value}
				case "zero lifetime", "negative lifetime":
					value := time.Duration(0)
					if failure == "negative lifetime" {
						value = -time.Minute
					}
					spec.Lifecycle.MaxLifetime = &metav1.Duration{Duration: value}
				case "lifetime shorter than idle":
					spec.Lifecycle.IdleTimeout = &metav1.Duration{Duration: 2 * time.Hour}
				case "count cap without lifetime":
					spec.Lifecycle.IdleTimeout = spec.Lifecycle.MaxLifetime
					spec.Lifecycle.MaxLifetime = nil
				}
			})
			if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || observed.Startup != nil {
				t.Fatalf("unsupported suspension lifecycle reached allocation: %+v, %v", observed, err)
			}
			requireNoSandboxAllocation(t, c, request)
		})
	}
}

func TestSandboxSuspensionAcceptsPositiveIdleExpiryWithoutCountCap(t *testing.T) {
	c, request := fixture(t, true)
	updateWorkspacePolicy(t, c, request, func(spec *workspacev1alpha1.ExecutionWorkspaceSpec) {
		spec.Lifecycle.IdleTimeout = spec.Lifecycle.MaxLifetime
		spec.Lifecycle.MaxLifetime = nil
	})
	first := ready(t, c, request)
	if retired := suspended(t, c, request, first); retired.RetainedData == nil {
		t.Fatal("positive idle expiry did not permit data-only suspension")
	}
}

func TestSandboxRechecksSuspensionPolicyBeforeNativeRetirement(t *testing.T) {
	c, request := fixture(t, true)
	first := ready(t, c, request)
	updateWorkspacePolicy(t, c, request, func(spec *workspacev1alpha1.ExecutionWorkspaceSpec) {
		spec.Lifecycle.MaxLifetime = nil
	})
	if _, err := New(c).SuspendInstance(t.Context(), request.Key, first.Identity); err == nil {
		t.Fatal("unbounded concrete workspace suspended its native allocation")
	}
	requireSandboxRunning(t, c, request)
	if observed, err := New(c).Observe(t.Context(), request.Key); err == nil || observed.Startup != nil {
		t.Fatalf("invalid concrete lifecycle retained startup evidence: %+v, %v", observed, err)
	}
	for range 4 {
		observed, err := (&simulatedLifecycle{New(c)}).StopInstance(t.Context(), request.Key, first.Identity)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == workspaceprovider.AllocationStopped {
			deleteSandboxAllocation(t, c, request, first.Identity)
			return
		}
	}
	t.Fatal("invalid suspension policy blocked exact authorized cleanup")
}

func TestSandboxRechecksSuspensionPolicyBeforeNativeResume(t *testing.T) {
	c, request := fixture(t, true)
	first := ready(t, c, request)
	retired := suspended(t, c, request, first)
	next := continuation(request, retired)
	if err := admit(t, c)(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	updateWorkspacePolicy(t, c, request, func(spec *workspacev1alpha1.ExecutionWorkspaceSpec) {
		spec.SessionRef = nil
	})
	if _, err := New(c).EnsureAllocation(t.Context(), next); err == nil {
		t.Fatal("non-session concrete workspace resumed retained storage")
	}
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := New(c).sandbox(t.Context(), record)
	if err != nil || sb.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeSuspended || record.Request.Sequence != request.Sequence || record.Observation.RetainedData == nil {
		t.Fatalf("rejected resume changed native mode or retained lineage: %v", err)
	}
}

func TestSandboxMalformedDurableJournalFailsClosedWithoutPanic(t *testing.T) {
	for _, capacity := range []string{"invalid", "", "0", "-1Gi"} {
		t.Run(capacity, func(t *testing.T) {
			c, request := fixture(t, true)
			first := ready(t, c, request)
			cm, record, err := New(c).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			record.Volume.Capacity = capacity
			if err := New(c).save(t.Context(), cm, record); err != nil {
				t.Fatal(err)
			}
			corruptData, corruptVersion := cm.Data[journalDataKey], cm.ResourceVersion
			policy := workspacev1alpha1.ExecutionWorkspaceDeletionPolicy{ProviderResources: workspacev1alpha1.WorkspaceDeletionActionDelete, PersistentVolumes: workspacev1alpha1.WorkspaceDeletionActionDelete, Checkpoints: workspacev1alpha1.WorkspaceDeletionActionDelete}
			for _, operation := range []struct {
				name string
				call func() (workspaceprovider.AllocationObservation, error)
			}{
				{"observe", func() (workspaceprovider.AllocationObservation, error) {
					return New(c).Observe(t.Context(), request.Key)
				}},
				{"ensure", func() (workspaceprovider.AllocationObservation, error) {
					return New(c).EnsureAllocation(t.Context(), request)
				}},
				{"suspend", func() (workspaceprovider.AllocationObservation, error) {
					return New(c).SuspendInstance(t.Context(), request.Key, first.Identity)
				}},
				{"stop", func() (workspaceprovider.AllocationObservation, error) {
					return New(c).StopInstance(t.Context(), request.Key, first.Identity)
				}},
				{"delete", func() (workspaceprovider.AllocationObservation, error) {
					return New(c).DeleteAllocation(t.Context(), request.Key, first.Identity, policy)
				}},
			} {
				t.Run(operation.name, func(t *testing.T) {
					if observed, err := operation.call(); err == nil || observed.Startup != nil {
						t.Fatalf("corrupt capacity was trusted: %+v, %v", observed, err)
					}
				})
			}
			stored := &corev1.ConfigMap{}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(cm), stored); err != nil || stored.Data[journalDataKey] != corruptData || stored.ResourceVersion != corruptVersion {
				t.Fatalf("failed operations changed the corrupt recovery journal: %v", err)
			}
			sb := &sandboxv1beta1.Sandbox{}
			if err := c.Get(t.Context(), types.NamespacedName{Namespace: record.Namespace, Name: record.Sandbox.Name}, sb); err != nil || sb.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeRunning {
				t.Fatalf("corrupt capacity caused a native mutation: %v", err)
			}
			pod := &corev1.Pod{}
			if err := c.Get(t.Context(), types.NamespacedName{Namespace: first.Startup.Pod.Namespace, Name: first.Startup.Pod.Name}, pod); err != nil || pod.UID != first.Startup.Pod.UID {
				t.Fatalf("corrupt capacity retired the exact runtime: %v", err)
			}
		})
	}
}
