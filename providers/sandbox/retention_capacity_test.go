package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	"github.com/orka-agents/orka-workspace/conformance"
	profilev1alpha1 "github.com/orka-agents/orka-workspace/providers/sandbox/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func cappedSandboxRequest(t *testing.T, c client.Client, request workspaceprovider.WorkloadRequest, limit int32) workspaceprovider.WorkloadRequest {
	t.Helper()
	profile := &profilev1alpha1.SandboxWorkspaceProfile{}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: request.Key.Namespace, Name: request.ParametersRef.Name}, profile); err != nil {
		t.Fatal(err)
	}
	profile.Spec.Retention = &profilev1alpha1.RetentionPolicy{MaxSuspendedWorkspaces: &limit}
	if err := c.Update(t.Context(), profile); err != nil {
		t.Fatal(err)
	}
	raw := &unstructured.Unstructured{}
	raw.SetGroupVersionKind(profilev1alpha1.GroupVersion.WithKind("SandboxWorkspaceProfile"))
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(profile), raw); err != nil {
		t.Fatal(err)
	}
	hash, err := workspaceprovider.ParametersProfileHash(raw)
	if err != nil {
		t.Fatal(err)
	}
	request.ParametersBinding.ProfileHash = hash
	request.Revision, _ = workspaceprovider.WorkloadRevision(request)
	if err := admit(t, c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	return request
}

func anotherSandboxRequest(t *testing.T, c client.Client, request workspaceprovider.WorkloadRequest, name, class string) workspaceprovider.WorkloadRequest {
	t.Helper()
	next := *request.DeepCopy()
	next.Key.Name = name
	next.Key.WorkspaceUID = types.UID(name)
	if class != "" {
		next.Runtime.ClassBinding.Name = class
		next.Runtime.ClassBinding.UID = types.UID(class)
	}
	next.Revision, _ = workspaceprovider.WorkloadRevision(next)
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: request.Key.Namespace, Name: request.Key.Name}, workspace); err != nil {
		t.Fatal(err)
	}
	workspace.ObjectMeta = metav1.ObjectMeta{Namespace: next.Key.Namespace, Name: name, UID: next.Key.WorkspaceUID, Generation: 1}
	workspace.Spec.ClassBinding = next.Runtime.ClassBinding
	workspace.Spec.CoreAdmission.ClassBinding = next.Runtime.ClassBinding
	workspace.Spec.Workload = &next
	workspace.Status.Allocation = nil
	if err := c.Create(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	return next
}

func requireSandboxRunning(t *testing.T, c client.Client, request workspaceprovider.WorkloadRequest) {
	t.Helper()
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := New(c).sandbox(t.Context(), record)
	if err != nil {
		t.Fatal(err)
	}
	if sb.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeRunning || record.Operation != "ensure" || record.Observation.RetainedData != nil {
		t.Fatal("capacity rejection changed native mode or retained lineage")
	}
}

func TestSandboxSuspendedCapacityZeroAndPendingOccupancy(t *testing.T) {
	for _, limit := range []int32{0, 1} {
		t.Run(string(rune('0'+limit)), func(t *testing.T) {
			c, request := fixture(t, true)
			request = cappedSandboxRequest(t, c, request, limit)
			first := ready(t, c, request)
			if limit == 0 {
				if _, err := New(c).SuspendInstance(t.Context(), request.Key, first.Identity); err == nil {
					t.Fatal("zero cap admitted suspension")
				}
				requireSandboxRunning(t, c, request)
				return
			}
			other := anotherSandboxRequest(t, c, request, "other", "")
			second := ready(t, c, other)
			pending, err := New(c).SuspendInstance(t.Context(), request.Key, first.Identity)
			if err != nil || pending.State != workspaceprovider.AllocationPending {
				t.Fatalf("first suspension did not reserve before completion: %v", err)
			}
			if _, err := New(c).SuspendInstance(t.Context(), other.Key, second.Identity); err == nil {
				t.Fatal("pending suspension did not consume the class cap")
			}
			requireSandboxRunning(t, c, other)
			independent := anotherSandboxRequest(t, c, request, "independent", "another-class")
			third := ready(t, c, independent)
			if _, err := New(c).SuspendInstance(t.Context(), independent.Key, third.Identity); err != nil {
				t.Fatalf("sharing a profile incorrectly shared another class's cap: %v", err)
			}
		})
	}
}

func TestSandboxResumeReleasesCapacityWithoutDeletingStorage(t *testing.T) {
	c, request := fixture(t, true)
	request = cappedSandboxRequest(t, c, request, 1)
	first := ready(t, c, request)
	retired := suspended(t, c, request, first)
	_, before, _ := New(c).read(t.Context(), request.Key)
	other := anotherSandboxRequest(t, c, request, "other", "")
	second := ready(t, c, other)
	if _, err := New(c).SuspendInstance(t.Context(), other.Key, second.Identity); err == nil {
		t.Fatal("occupied slot admitted another suspension")
	}
	next := continuation(request, retired)
	if err := admit(t, c)(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	resumed := ready(t, c, next)
	_, after, _ := New(c).read(t.Context(), next.Key)
	if resumed.Identity.InstanceID == first.Identity.InstanceID || *after.Storage != *before.Storage {
		t.Fatal("resume did not preserve storage with a new runtime instance")
	}
	if _, err := New(c).SuspendInstance(t.Context(), other.Key, second.Identity); err != nil {
		t.Fatalf("running resumed workspace still consumed suspended capacity: %v", err)
	}
}

func TestSandboxCountCapDoesNotBlockOrdinaryReplacement(t *testing.T) {
	c, request := fixture(t, true)
	profile := &profilev1alpha1.SandboxWorkspaceProfile{}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: request.Key.Namespace, Name: request.ParametersRef.Name}, profile); err != nil {
		t.Fatal(err)
	}
	profile.Spec.Suspend = nil
	if err := c.Update(t.Context(), profile); err != nil {
		t.Fatal(err)
	}
	request.Runtime.Template.Spec.Containers[0].VolumeMounts = nil
	request.Runtime.Template.Spec.Containers[0].Env = nil
	request = cappedSandboxRequest(t, c, request, 0)
	if err := conformance.CheckReplacement(t.Context(), func() workspaceprovider.Lifecycle {
		return &simulatedLifecycle{New(c)}
	}, request, admit(t, c)); err != nil {
		t.Fatal(err)
	}
}

func TestSandboxRetentionFreezesProfileAndBackfillsRunningLegacyJournal(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "frozen profile", true: "running legacy journal"}[legacy], func(t *testing.T) {
			c, request := fixture(t, true)
			request = cappedSandboxRequest(t, c, request, 1)
			first := ready(t, c, request)
			other := anotherSandboxRequest(t, c, request, "other", "")
			second := ready(t, c, other)
			if legacy {
				cm, record, err := New(c).read(t.Context(), request.Key)
				if err != nil {
					t.Fatal(err)
				}
				record.RetentionResolved, record.MaxSuspended = false, nil
				if err := New(c).save(t.Context(), cm, record); err != nil {
					t.Fatal(err)
				}
			} else {
				profile := &profilev1alpha1.SandboxWorkspaceProfile{ObjectMeta: metav1.ObjectMeta{Namespace: request.Key.Namespace, Name: request.ParametersRef.Name}}
				if err := c.Delete(t.Context(), profile); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := New(c).SuspendInstance(t.Context(), request.Key, first.Identity); err != nil {
				t.Fatalf("bound profile retention could not be used: %v", err)
			}
			if _, err := New(c).SuspendInstance(t.Context(), other.Key, second.Identity); err == nil {
				t.Fatal("profile retention was not preserved")
			}
		})
	}
}

type retentionReadBarrier struct {
	client.Client
	mu    sync.Mutex
	reads int
	ready chan struct{}
}

func (c *retentionReadBarrier) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	err := c.Client.Get(ctx, key, object, options...)
	if err != nil || !strings.HasPrefix(key.Name, "sandbox-retention-") {
		return err
	}
	c.mu.Lock()
	c.reads++
	wait := c.reads <= 2
	if c.reads == 2 {
		close(c.ready)
	}
	c.mu.Unlock()
	if wait {
		select {
		case <-c.ready:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func TestSandboxSuspendedCapacityCASAcrossIndependentDrivers(t *testing.T) {
	c, request := fixture(t, true)
	request = cappedSandboxRequest(t, c, request, 1)
	first := ready(t, c, request)
	retired := suspended(t, c, request, first)
	next := continuation(request, retired)
	if err := admit(t, c)(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	first = ready(t, c, next) // Leave an empty ledger with a real resourceVersion.
	other := anotherSandboxRequest(t, c, request, "other", "")
	second := ready(t, c, other)
	barrier := &retentionReadBarrier{Client: c, ready: make(chan struct{})}
	errors := make(chan error, 2)
	for _, attempt := range []struct {
		request workspaceprovider.WorkloadRequest
		id      workspaceprovider.InstanceIdentity
	}{{next, first.Identity}, {other, second.Identity}} {
		go func() {
			_, err := New(barrier).SuspendInstance(t.Context(), attempt.request.Key, attempt.id)
			errors <- err
		}()
	}
	successes := 0
	for range 2 {
		if <-errors == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent drivers admitted %d suspensions with one slot", successes)
	}
	var sandboxes sandboxv1beta1.SandboxList
	if err := c.List(t.Context(), &sandboxes); err != nil {
		t.Fatal(err)
	}
	suspendedCount := 0
	for _, sb := range sandboxes.Items {
		if sb.Spec.OperatingMode == sandboxv1beta1.SandboxOperatingModeSuspended {
			suspendedCount++
		}
	}
	if suspendedCount != 1 {
		t.Fatalf("native suspension escaped the CAS bound: %d", suspendedCount)
	}
}

type retentionReleaseRace struct {
	client.Client
	duringRelease func() error
	done          bool
}

func (c *retentionReleaseRace) Update(ctx context.Context, object client.Object, options ...client.UpdateOption) error {
	cm, ok := object.(*corev1.ConfigMap)
	if ok && !c.done && strings.HasPrefix(cm.Name, "sandbox-retention-") {
		var ledger retentionLedger
		if json.Unmarshal([]byte(cm.Data[retentionDataKey]), &ledger) == nil && len(ledger.Reservations) == 0 {
			c.done = true
			if err := c.duringRelease(); err != nil {
				return err
			}
		}
	}
	return c.Client.Update(ctx, object, options...)
}

func TestSandboxDelayedResumeReleasePreservesNewSuspension(t *testing.T) {
	c, request := fixture(t, true)
	request = cappedSandboxRequest(t, c, request, 1)
	first := ready(t, c, request)
	retired := suspended(t, c, request, first)
	next := continuation(request, retired)
	if err := admit(t, c)(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	other := anotherSandboxRequest(t, c, request, "other", "")
	second := ready(t, c, other)
	race := &retentionReleaseRace{Client: c}
	race.duringRelease = func() error {
		_, current, err := New(c).read(t.Context(), next.Key)
		if err != nil {
			return err
		}
		_, err = New(c).SuspendInstance(t.Context(), next.Key, current.Observation.Identity)
		return err
	}
	for range 5 {
		if _, err := (&simulatedLifecycle{New(race)}).EnsureAllocation(t.Context(), next); err != nil {
			t.Fatal(err)
		}
		if race.done {
			break
		}
	}
	if !race.done {
		t.Fatal("resume never attempted to release its prior slot")
	}
	if _, err := New(c).SuspendInstance(t.Context(), other.Key, second.Identity); err == nil {
		t.Fatal("delayed prior-sequence release removed the new suspension's slot")
	}
	if _, err := New(c).Observe(t.Context(), next.Key); err != nil {
		t.Fatalf("new suspension lost its exact reservation: %v", err)
	}
}

func deleteSandboxAllocation(t *testing.T, c client.Client, request workspaceprovider.WorkloadRequest, identity workspaceprovider.InstanceIdentity) {
	t.Helper()
	policy := workspacev1alpha1.ExecutionWorkspaceDeletionPolicy{ProviderResources: workspacev1alpha1.WorkspaceDeletionActionDelete, PersistentVolumes: workspacev1alpha1.WorkspaceDeletionActionDelete, Checkpoints: workspacev1alpha1.WorkspaceDeletionActionDelete}
	for range 10 {
		observed, err := (&simulatedLifecycle{New(c)}).DeleteAllocation(t.Context(), request.Key, identity, policy)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == workspaceprovider.AllocationDeleted {
			return
		}
	}
	t.Fatal("exact all-Delete cleanup never completed")
}

func TestSandboxDeletedSuspendRetryDoesNotResolveMissingLegacyProfile(t *testing.T) {
	c, request := fixture(t, true)
	request = cappedSandboxRequest(t, c, request, 1)
	first := ready(t, c, request)
	suspended(t, c, request, first)
	deleteSandboxAllocation(t, c, request, first.Identity)
	cm, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	record.RetentionResolved, record.MaxSuspended = false, nil
	if err := New(c).save(t.Context(), cm, record); err != nil {
		t.Fatal(err)
	}
	profile := &profilev1alpha1.SandboxWorkspaceProfile{ObjectMeta: metav1.ObjectMeta{Namespace: request.Key.Namespace, Name: request.ParametersRef.Name}}
	if err := c.Delete(t.Context(), profile); err != nil {
		t.Fatal(err)
	}
	observed, err := New(c).SuspendInstance(t.Context(), request.Key, first.Identity)
	if err != nil || observed.State != workspaceprovider.AllocationDeleted {
		t.Fatalf("terminal retry resolved a removed profile: state=%s err=%v", observed.State, err)
	}
}

func TestSandboxMissingAndLegacyOccupancyFailClosedUntilCleanup(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing ledger", true: "unreserved legacy journal"}[legacy], func(t *testing.T) {
			c, request := fixture(t, true)
			request = cappedSandboxRequest(t, c, request, 1)
			first := ready(t, c, request)
			retired := suspended(t, c, request, first)
			cm, record, err := New(c).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			ledgerCM := &corev1.ConfigMap{}
			if err := c.Get(t.Context(), retentionKey(record), ledgerCM); err != nil {
				t.Fatal(err)
			}
			if legacy {
				record.RetentionResolved, record.MaxSuspended, record.RetentionReservation = false, nil, nil
				if err := New(c).save(t.Context(), cm, record); err != nil {
					t.Fatal(err)
				}
				var ledger retentionLedger
				if err := json.Unmarshal([]byte(ledgerCM.Data[retentionDataKey]), &ledger); err != nil {
					t.Fatal(err)
				}
				ledger.Reservations = map[string]retentionOccupant{}
				if err := setRetention(ledgerCM, &ledger); err != nil {
					t.Fatal(err)
				}
				if err := c.Update(t.Context(), ledgerCM); err != nil {
					t.Fatal(err)
				}
			} else if err := c.Delete(t.Context(), ledgerCM); err != nil {
				t.Fatal(err)
			}
			if _, err := New(c).Observe(t.Context(), request.Key); err == nil {
				t.Fatal("unreserved retained lineage was published")
			}
			if _, err := New(c).SuspendInstance(t.Context(), request.Key, first.Identity); err == nil {
				t.Fatal("unreserved suspension was accepted on retry")
			}
			next := continuation(request, retired)
			if err := admit(t, c)(t.Context(), next); err != nil {
				t.Fatal(err)
			}
			if _, err := New(c).EnsureAllocation(t.Context(), next); err == nil {
				t.Fatal("unreserved retained data was resumed")
			}
			other := anotherSandboxRequest(t, c, request, "other", "")
			second := ready(t, c, other)
			if _, err := New(c).SuspendInstance(t.Context(), other.Key, second.Identity); err == nil {
				t.Fatal("existing unreserved occupancy was ignored for a new suspension")
			}
			requireSandboxRunning(t, c, other)
			if _, err := New(c).StopInstance(t.Context(), request.Key, first.Identity); err != nil {
				t.Fatalf("lost occupancy record prevented exact stop: %v", err)
			}
			deleteSandboxAllocation(t, c, request, first.Identity)
			if _, err := New(c).SuspendInstance(t.Context(), other.Key, second.Identity); err != nil {
				t.Fatalf("drained old occupancy still blocked its class: %v", err)
			}
		})
	}
}

type lostRetentionResponse struct {
	client.Client
	done bool
}

func (c *lostRetentionResponse) Update(ctx context.Context, object client.Object, options ...client.UpdateOption) error {
	cm, ok := object.(*corev1.ConfigMap)
	if ok && !c.done && strings.HasPrefix(cm.Name, "sandbox-retention-") {
		c.done = true
		if err := c.Client.Update(ctx, object, options...); err != nil {
			return err
		}
		return errors.New("reservation response lost")
	}
	return c.Client.Update(ctx, object, options...)
}

func TestSandboxReservationResponseLossKeepsCapacityAndCanRetry(t *testing.T) {
	c, request := fixture(t, true)
	request = cappedSandboxRequest(t, c, request, 1)
	first := ready(t, c, request)
	other := anotherSandboxRequest(t, c, request, "other", "")
	second := ready(t, c, other)
	lost := &lostRetentionResponse{Client: c}
	if _, err := New(lost).SuspendInstance(t.Context(), request.Key, first.Identity); err == nil || !lost.done {
		t.Fatal("reservation response was not lost")
	}
	requireSandboxRunning(t, c, request)
	if _, err := New(c).SuspendInstance(t.Context(), other.Key, second.Identity); err == nil {
		t.Fatal("uncertain reservation was reused by another workspace")
	}
	if _, err := New(c).SuspendInstance(t.Context(), request.Key, first.Identity); err != nil {
		t.Fatalf("retry could not recover its exact committed reservation: %v", err)
	}
}

func TestSandboxCleanupReleasesReservationWhoseIntentResponseWasLost(t *testing.T) {
	c, request := fixture(t, true)
	request = cappedSandboxRequest(t, c, request, 1)
	first := ready(t, c, request)
	other := anotherSandboxRequest(t, c, request, "other", "")
	second := ready(t, c, other)
	if _, err := New(&lostRetentionResponse{Client: c}).SuspendInstance(t.Context(), request.Key, first.Identity); err == nil {
		t.Fatal("reservation response was not lost")
	}
	stopped := false
	for range 4 {
		observed, err := (&simulatedLifecycle{New(c)}).StopInstance(t.Context(), request.Key, first.Identity)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == workspaceprovider.AllocationStopped {
			stopped = true
			break
		}
	}
	if !stopped {
		t.Fatal("exact stop did not complete")
	}
	deleteSandboxAllocation(t, c, request, first.Identity)
	if _, err := New(c).SuspendInstance(t.Context(), other.Key, second.Identity); err != nil {
		t.Fatalf("completed deletion leaked an uncertain reservation: %v", err)
	}
}
