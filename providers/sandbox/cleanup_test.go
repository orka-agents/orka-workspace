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

type cleanupDeleteClient struct {
	client.Client
	deletes     int
	beforeClaim func(context.Context, *client.DeleteOptions) error
}

func (c *cleanupDeleteClient) Delete(ctx context.Context, object client.Object, options ...client.DeleteOption) error {
	c.deletes++
	deleteOptions := &client.DeleteOptions{}
	for _, option := range options {
		option.ApplyToDelete(deleteOptions)
	}
	if _, ok := object.(*extv1beta1.SandboxClaim); ok && c.beforeClaim != nil {
		if err := c.beforeClaim(ctx, deleteOptions); err != nil {
			return err
		}
	}
	return c.Client.Delete(ctx, object, options...)
}

func cleanupFixture(t *testing.T) (client.Client, workspaceprovider.WorkloadRequest, *journalRecord) {
	t.Helper()
	c, request := fixture(t, false)
	first := ready(t, c, request)
	for range 5 {
		observed, err := (&simulatedLifecycle{New(c)}).StopInstance(t.Context(), request.Key, first.Identity)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == workspaceprovider.AllocationStopped {
			_, record, err := New(c).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			return c, request, record
		}
	}
	t.Fatal("allocation never stopped")
	return nil, workspaceprovider.WorkloadRequest{}, nil
}

func cleanupPolicy() workspacev1alpha1.ExecutionWorkspaceDeletionPolicy {
	return workspacev1alpha1.ExecutionWorkspaceDeletionPolicy{ProviderResources: workspacev1alpha1.WorkspaceDeletionActionDelete, PersistentVolumes: workspacev1alpha1.WorkspaceDeletionActionDelete, Checkpoints: workspacev1alpha1.WorkspaceDeletionActionDelete}
}

func TestDeleteAllocationRejectsReplacementBeforeAnyCascade(t *testing.T) {
	for _, kind := range []string{"anchor", "template", "warm pool", "claim", "sandbox"} {
		t.Run(kind, func(t *testing.T) {
			c, request, record := cleanupFixture(t)
			var ref objectReference
			var object client.Object
			switch kind {
			case "anchor":
				ref, object = record.Anchor, &corev1.ConfigMap{}
			case "template":
				ref, object = record.Template, &extv1beta1.SandboxTemplate{}
			case "warm pool":
				ref, object = record.WarmPool, &extv1beta1.SandboxWarmPool{}
			case "claim":
				ref, object = record.Claim, &extv1beta1.SandboxClaim{}
			case "sandbox":
				ref, object = record.Sandbox, &sandboxv1beta1.Sandbox{}
			}
			if err := c.Get(t.Context(), types.NamespacedName{Namespace: record.Namespace, Name: ref.Name}, object); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(t.Context(), object); err != nil {
				t.Fatal(err)
			}
			object.SetResourceVersion("")
			object.SetUID("replacement")
			if err := c.Create(t.Context(), object); err != nil {
				t.Fatal(err)
			}
			tracked := &cleanupDeleteClient{Client: c}
			if _, err := New(tracked).DeleteAllocation(t.Context(), request.Key, record.Observation.Identity, cleanupPolicy()); !errors.Is(err, workspaceprovider.ErrStaleIdentity) {
				t.Fatalf("replacement was accepted: %v", err)
			}
			if tracked.deletes != 0 {
				t.Fatalf("deleted %d native objects before verifying replacement", tracked.deletes)
			}
			current := &extv1beta1.SandboxClaim{}
			if err := c.Get(t.Context(), types.NamespacedName{Namespace: record.Namespace, Name: record.Claim.Name}, current); err != nil || current.DeletionTimestamp != nil {
				t.Fatalf("claim was cascaded before identity validation: %v", err)
			}
		})
	}
}

func TestDeleteAllocationRechecksCurrentSuspensionAndPodAbsence(t *testing.T) {
	for _, mutation := range []string{"running producer", "stale suspension", "foreign Pod"} {
		t.Run(mutation, func(t *testing.T) {
			c, request, record := cleanupFixture(t)
			sb := &sandboxv1beta1.Sandbox{}
			if err := c.Get(t.Context(), types.NamespacedName{Namespace: record.Namespace, Name: record.Sandbox.Name}, sb); err != nil {
				t.Fatal(err)
			}
			want := workspaceprovider.ErrInstanceRunning
			switch mutation {
			case "running producer":
				sb.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeRunning
				if err := c.Update(t.Context(), sb); err != nil {
					t.Fatal(err)
				}
			case "stale suspension":
				sb.Status.Conditions[0].ObservedGeneration = sb.Generation - 1
				if err := c.Status().Update(t.Context(), sb); err != nil {
					t.Fatal(err)
				}
			case "foreign Pod":
				want = workspaceprovider.ErrStaleIdentity
				controller := true
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: record.Pod.Namespace, Name: record.Pod.Name, UID: "replacement-Pod", OwnerReferences: []metav1.OwnerReference{{APIVersion: sandboxv1beta1.GroupVersion.String(), Kind: "Sandbox", Name: sb.Name, UID: sb.UID, Controller: &controller}}}}
				if err := c.Create(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			}
			tracked := &cleanupDeleteClient{Client: c}
			if _, err := New(tracked).DeleteAllocation(t.Context(), request.Key, record.Observation.Identity, cleanupPolicy()); !errors.Is(err, want) {
				t.Fatalf("invalid stop proof was accepted: %v", err)
			}
			if tracked.deletes != 0 {
				t.Fatalf("deleted %d native objects with invalid stop proof", tracked.deletes)
			}
		})
	}
}

func TestDeleteAllocationPreservesReplacementAppearingAtClaimDeletion(t *testing.T) {
	c, request, record := cleanupFixture(t)
	tracked := &cleanupDeleteClient{Client: c}
	tracked.beforeClaim = func(ctx context.Context, options *client.DeleteOptions) error {
		tracked.beforeClaim = nil
		sb := &sandboxv1beta1.Sandbox{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: record.Namespace, Name: record.Sandbox.Name}, sb); err != nil {
			return err
		}
		if err := c.Delete(ctx, sb); err != nil {
			return err
		}
		sb.ResourceVersion = ""
		sb.UID = "replacement-Sandbox"
		sb.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeRunning
		if err := c.Create(ctx, sb); err != nil {
			return err
		}
		// Simulate the parent cascade at the backend seam. Orphan propagation
		// preserves a replacement whose UID the retired journal never authorized.
		if options.PropagationPolicy == nil || *options.PropagationPolicy != metav1.DeletePropagationOrphan {
			return c.Delete(ctx, sb)
		}
		if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != record.Claim.UID || options.Preconditions.ResourceVersion == nil {
			t.Fatal("claim deletion omitted its exact UID and observed version")
		}
		return nil
	}
	if _, err := New(tracked).DeleteAllocation(t.Context(), request.Key, record.Observation.Identity, cleanupPolicy()); err != nil {
		t.Fatal(err)
	}
	replacement := &sandboxv1beta1.Sandbox{}
	key := types.NamespacedName{Namespace: record.Namespace, Name: record.Sandbox.Name}
	if err := c.Get(t.Context(), key, replacement); err != nil || replacement.UID != "replacement-Sandbox" || replacement.DeletionTimestamp != nil {
		t.Fatalf("claim cascade deleted the replacement: %v", err)
	}
	deletes := tracked.deletes
	if _, err := New(tracked).DeleteAllocation(t.Context(), request.Key, record.Observation.Identity, cleanupPolicy()); !errors.Is(err, workspaceprovider.ErrStaleIdentity) {
		t.Fatalf("replacement accepted on cleanup retry: %v", err)
	}
	if tracked.deletes != deletes {
		t.Fatal("cleanup retry deleted a replacement descendant")
	}
}
