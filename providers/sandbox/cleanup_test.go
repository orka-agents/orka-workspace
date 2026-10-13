package sandbox

import (
	"context"
	"errors"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type cleanupDeleteClient struct {
	client.Client
	deletes       int
	beforeClaim   func(context.Context, *client.DeleteOptions) error
	beforeSandbox func(context.Context, *sandboxv1beta1.Sandbox, *client.DeleteOptions) error
	beforePVC     func(context.Context, *corev1.PersistentVolumeClaim, *client.DeleteOptions) error
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
	if sb, ok := object.(*sandboxv1beta1.Sandbox); ok && c.beforeSandbox != nil {
		if err := c.beforeSandbox(ctx, sb, deleteOptions); err != nil {
			return err
		}
	}
	if pvc, ok := object.(*corev1.PersistentVolumeClaim); ok && c.beforePVC != nil {
		if err := c.beforePVC(ctx, pvc, deleteOptions); err != nil {
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

func TestDeleteAllocationPreservesPVCReplacedAtSandboxDeletion(t *testing.T) {
	c, request := fixture(t, true)
	first := ready(t, c, request)
	suspended(t, c, request, first)
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	tracked := &cleanupDeleteClient{Client: c}
	replaced := false
	tracked.beforeSandbox = func(ctx context.Context, sb *sandboxv1beta1.Sandbox, options *client.DeleteOptions) error {
		tracked.beforeSandbox = nil
		if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != record.Sandbox.UID || options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != sb.ResourceVersion {
			t.Fatal("Sandbox deletion omitted its exact UID and observed version")
		}
		pvc := &corev1.PersistentVolumeClaim{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: record.Namespace, Name: record.Storage.ClaimName}, pvc); err != nil {
			return err
		}
		// Model PVC protection and its release only after the exact Pod is gone.
		pvc.Finalizers = []string{"kubernetes.io/pvc-protection"}
		if err := c.Update(ctx, pvc); err != nil {
			return err
		}
		if err := c.Delete(ctx, pvc); err != nil {
			return err
		}
		if err := c.Get(ctx, client.ObjectKeyFromObject(pvc), pvc); err != nil {
			return err
		}
		if pvc.DeletionTimestamp == nil {
			t.Fatal("PVC protection did not delay deletion")
		}
		var pods corev1.PodList
		if err := c.List(ctx, &pods, client.InNamespace(record.Namespace)); err != nil {
			return err
		}
		for _, pod := range pods.Items {
			for _, volume := range pod.Spec.Volumes {
				if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == pvc.Name {
					t.Fatal("PVC protection cannot be released while a Pod uses the claim")
				}
			}
		}
		pvc.Finalizers = nil
		if err := c.Update(ctx, pvc); err != nil {
			return err
		}
		pvc.ResourceVersion = ""
		pvc.UID = "replacement-PVC"
		pvc.Spec.VolumeName = "replacement-PV"
		pvc.DeletionTimestamp = nil
		pvc.DeletionGracePeriodSeconds = nil
		pvc.Finalizers = []string{"kubernetes.io/pvc-protection"}
		if err := controllerutil.SetControllerReference(sb, pvc, c.Scheme()); err != nil {
			return err
		}
		if err := c.Create(ctx, pvc); err != nil {
			return err
		}
		owner := metav1.GetControllerOf(pvc)
		if owner == nil || owner.BlockOwnerDeletion == nil || !*owner.BlockOwnerDeletion {
			t.Fatal("replacement PVC must have the upstream blocking Sandbox owner")
		}
		replaced = true
		return nil
	}
	for range 4 {
		observed, err := New(tracked).DeleteAllocation(t.Context(), request.Key, first.Identity, cleanupPolicy())
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == workspaceprovider.AllocationDeleted {
			t.Fatal("replacement storage was accepted as deleted")
		}
		if replaced {
			break
		}
	}
	if !replaced {
		t.Fatal("cleanup never reached Sandbox deletion")
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: record.Namespace, Name: record.Storage.ClaimName}, pvc); err != nil || pvc.UID != "replacement-PVC" || pvc.DeletionTimestamp != nil {
		t.Fatalf("Sandbox cascade deleted the replacement PVC: %v", err)
	}
	deletes := tracked.deletes
	if _, err := New(tracked).DeleteAllocation(t.Context(), request.Key, first.Identity, cleanupPolicy()); !errors.Is(err, workspaceprovider.ErrStaleIdentity) {
		t.Fatalf("replacement PVC accepted on cleanup retry: %v", err)
	}
	if tracked.deletes != deletes {
		t.Fatal("cleanup retry deleted a replacement descendant")
	}
	_, after, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if *after.Storage != *record.Storage || after.Observation.State == workspaceprovider.AllocationDeleted {
		t.Fatal("cleanup changed its recorded storage authority or claimed terminal deletion")
	}
	// The native fixture must also honor orphaning when it runs after cleanup.
	if err := nativeTick(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(pvc), pvc); err != nil || pvc.DeletionTimestamp != nil {
		t.Fatalf("native GC collected the orphaned replacement PVC: %v", err)
	}
	if metav1.GetControllerOf(pvc) != nil {
		t.Fatal("Sandbox orphan deletion left a dangling controller reference")
	}
}

func TestDeleteAllocationPreservesSandboxReplacedAtSandboxDeletion(t *testing.T) {
	c, request, record := cleanupFixture(t)
	tracked := &cleanupDeleteClient{Client: c}
	replaced := false
	tracked.beforeSandbox = func(ctx context.Context, sb *sandboxv1beta1.Sandbox, options *client.DeleteOptions) error {
		tracked.beforeSandbox = nil
		if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != record.Sandbox.UID || options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != sb.ResourceVersion {
			t.Fatal("Sandbox deletion omitted its exact UID and observed version")
		}
		if err := c.Delete(ctx, sb, client.PropagationPolicy(metav1.DeletePropagationOrphan)); err != nil {
			return err
		}
		sb.UID = "replacement-Sandbox"
		sb.ResourceVersion = ""
		sb.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeRunning
		if err := c.Create(ctx, sb); err != nil {
			return err
		}
		replaced = true
		return nil
	}
	for range 4 {
		_, err := New(tracked).DeleteAllocation(t.Context(), request.Key, record.Observation.Identity, cleanupPolicy())
		if replaced {
			if !errors.Is(err, workspaceprovider.ErrStaleIdentity) {
				t.Fatalf("replacement Sandbox accepted at delete boundary: %v", err)
			}
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !replaced {
		t.Fatal("cleanup never reached Sandbox deletion")
	}
	sb := &sandboxv1beta1.Sandbox{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: record.Namespace, Name: record.Sandbox.Name}, sb); err != nil || sb.UID != "replacement-Sandbox" || sb.DeletionTimestamp != nil {
		t.Fatalf("cleanup deleted the replacement Sandbox: %v", err)
	}
	if err := nativeTick(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(sb), sb); err != nil || sb.DeletionTimestamp != nil {
		t.Fatalf("native GC deleted the orphaned replacement Sandbox: %v", err)
	}
}

func TestDeleteAllocationWaitsForExactPVAbsence(t *testing.T) {
	for _, name := range []string{"original PV remains", "replacement PV"} {
		t.Run(name, func(t *testing.T) {
			replacement := name == "replacement PV"
			c, request := fixture(t, true)
			first := ready(t, c, request)
			suspended(t, c, request, first)
			_, record, err := New(c).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			tracked := &cleanupDeleteClient{Client: c}
			tracked.beforeSandbox = func(_ context.Context, sb *sandboxv1beta1.Sandbox, options *client.DeleteOptions) error {
				if options.PropagationPolicy == nil || *options.PropagationPolicy != metav1.DeletePropagationOrphan || options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != record.Sandbox.UID || options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != sb.ResourceVersion {
					t.Fatal("Sandbox cleanup omitted orphan propagation or its exact identity fence")
				}
				return nil
			}
			for range 8 {
				observed, err := New(tracked).DeleteAllocation(t.Context(), request.Key, first.Identity, cleanupPolicy())
				if err != nil {
					t.Fatal(err)
				}
				if observed.State == workspaceprovider.AllocationDeleted {
					t.Fatal("deletion completed while the exact PV remained")
				}
			}
			if err := c.Get(t.Context(), client.ObjectKey{Namespace: record.Namespace, Name: record.Storage.ClaimName}, &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
				t.Fatalf("journaled PVC did not delete: %v", err)
			}
			pv := &corev1.PersistentVolume{}
			if err := c.Get(t.Context(), client.ObjectKey{Name: record.Storage.VolumeName}, pv); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(t.Context(), pv); err != nil {
				t.Fatal(err)
			}
			if replacement {
				pv.UID = "replacement-PV"
				pv.ResourceVersion = ""
				if err := c.Create(t.Context(), pv); err != nil {
					t.Fatal(err)
				}
				deletes := tracked.deletes
				if _, err := New(tracked).DeleteAllocation(t.Context(), request.Key, first.Identity, cleanupPolicy()); !errors.Is(err, workspaceprovider.ErrStaleIdentity) {
					t.Fatalf("replacement PV accepted as absence proof: %v", err)
				}
				if tracked.deletes != deletes {
					t.Fatal("cleanup deleted objects after observing a replacement PV")
				}
				if err := c.Get(t.Context(), client.ObjectKeyFromObject(pv), pv); err != nil || pv.UID != "replacement-PV" || pv.DeletionTimestamp != nil {
					t.Fatalf("cleanup deleted the replacement PV: %v", err)
				}
				return
			}
			for range 6 {
				observed, err := New(tracked).DeleteAllocation(t.Context(), request.Key, first.Identity, cleanupPolicy())
				if err != nil {
					t.Fatal(err)
				}
				if observed.State == workspaceprovider.AllocationDeleted {
					return
				}
			}
			t.Fatal("cleanup did not complete after exact PV absence")
		})
	}
}

func TestDeleteAllocationPreservesPVCReplacedAtPVCDeletion(t *testing.T) {
	c, request := fixture(t, true)
	first := ready(t, c, request)
	suspended(t, c, request, first)
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	tracked := &cleanupDeleteClient{Client: c}
	replaced := false
	tracked.beforePVC = func(ctx context.Context, pvc *corev1.PersistentVolumeClaim, options *client.DeleteOptions) error {
		tracked.beforePVC = nil
		if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != record.Storage.ClaimUID || options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != pvc.ResourceVersion {
			t.Fatal("PVC deletion omitted its exact UID and observed version")
		}
		if err := c.Delete(ctx, pvc); err != nil {
			return err
		}
		pvc.UID = "replacement-PVC"
		pvc.ResourceVersion = ""
		if err := c.Create(ctx, pvc); err != nil {
			return err
		}
		replaced = true
		return nil
	}
	for range 6 {
		_, err := New(tracked).DeleteAllocation(t.Context(), request.Key, first.Identity, cleanupPolicy())
		if replaced {
			if !errors.Is(err, workspaceprovider.ErrStaleIdentity) {
				t.Fatalf("replacement PVC accepted at delete boundary: %v", err)
			}
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !replaced {
		t.Fatal("cleanup never reached PVC deletion")
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: record.Namespace, Name: record.Storage.ClaimName}, pvc); err != nil || pvc.UID != "replacement-PVC" || pvc.DeletionTimestamp != nil {
		t.Fatalf("cleanup deleted the replacement PVC: %v", err)
	}
}
