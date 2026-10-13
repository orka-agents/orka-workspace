package sandbox

import (
	"context"
	"errors"
	"fmt"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type rejectedNativeCreateClient struct {
	client.Client
	kind      string
	rejection error
	rejected  bool
}

func (c *rejectedNativeCreateClient) Create(ctx context.Context, object client.Object, options ...client.CreateOption) error {
	kind := fmt.Sprintf("%T", object)
	if cm, ok := object.(*corev1.ConfigMap); ok {
		if cm.Immutable == nil || !*cm.Immutable {
			return c.Client.Create(ctx, object, options...)
		}
		kind = "anchor"
	}
	if !c.rejected && kind == c.kind {
		c.rejected = true
		return c.rejection
	}
	return c.Client.Create(ctx, object, options...)
}

func rejectedReference(record *journalRecord, kind string) objectReference {
	switch kind {
	case "anchor":
		return record.Anchor
	case "*v1beta1.SandboxTemplate":
		return record.Template
	case "*v1beta1.SandboxWarmPool":
		return record.WarmPool
	default:
		return record.Claim
	}
}

func TestDefinitiveNativeCreateRejectionCanRetry(t *testing.T) {
	resource := schema.GroupResource{Resource: "native"}
	for _, rejection := range []struct {
		name string
		err  error
	}{
		{"invalid", apierrors.NewInvalid(schema.GroupKind{Kind: "Native"}, "rejected", nil)},
		{"forbidden", apierrors.NewForbidden(resource, "rejected", errors.New("denied"))},
		{"badRequest", apierrors.NewBadRequest("rejected")},
		{"unauthorized", apierrors.NewUnauthorized("rejected")},
		{"notFound", apierrors.NewNotFound(resource, "rejected")},
		{"rateLimited", apierrors.NewTooManyRequests("rejected", 1)},
	} {
		for _, kind := range []string{"anchor", "*v1beta1.SandboxTemplate", "*v1beta1.SandboxWarmPool", "*v1beta1.SandboxClaim"} {
			t.Run(rejection.name+"/"+kind, func(t *testing.T) {
				c, request := fixture(t, false)
				faulty := &rejectedNativeCreateClient{Client: c, kind: kind, rejection: rejection.err}
				if _, err := New(faulty).EnsureAllocation(t.Context(), request); err == nil || !faulty.rejected {
					t.Fatalf("creation was not rejected: %v", err)
				}
				_, record, err := New(c).read(t.Context(), request.Key)
				if err != nil {
					t.Fatal(err)
				}
				if ref := rejectedReference(record, kind); ref.CreateIssued || ref.UID != "" {
					t.Fatalf("definitive rejection left ambiguous intent: %#v", ref)
				}
				observed := ready(t, c, request)
				if observed.Identity != record.Observation.Identity {
					t.Fatal("retry replaced the committed allocation identity")
				}
			})
		}
	}
}

func TestAmbiguousNativeCreateCannotRecreateOrProveTermination(t *testing.T) {
	for _, kind := range []string{"anchor", "*v1beta1.SandboxTemplate", "*v1beta1.SandboxWarmPool", "*v1beta1.SandboxClaim"} {
		t.Run(kind, func(t *testing.T) {
			c, request := fixture(t, false)
			faulty := &rejectedNativeCreateClient{Client: c, kind: kind, rejection: errors.New("connection ended without a reply")}
			if _, err := New(faulty).EnsureAllocation(t.Context(), request); err == nil {
				t.Fatal("expected an unknown creation outcome")
			}
			_, record, err := New(c).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			if !rejectedReference(record, kind).CreateIssued {
				t.Fatal("unknown creation outcome lost its recovery obligation")
			}
			if _, err := New(c).EnsureAllocation(t.Context(), request); err == nil {
				t.Fatal("ambiguous creation was retried as a fresh native create")
			}
			if _, err := New(c).StopInstance(t.Context(), request.Key, record.Observation.Identity); err == nil {
				t.Fatal("unknown creation outcome was treated as terminated")
			}
		})
	}
}

type exactPodDeleteClient struct {
	client.Client
	uid     types.UID
	deleted bool
}

func (c *exactPodDeleteClient) Delete(ctx context.Context, object client.Object, options ...client.DeleteOption) error {
	if _, ok := object.(*corev1.Pod); ok {
		deleteOptions := &client.DeleteOptions{}
		for _, option := range options {
			option.ApplyToDelete(deleteOptions)
		}
		preconditions := deleteOptions.Preconditions
		if preconditions == nil || preconditions.UID == nil || *preconditions.UID != c.uid || preconditions.ResourceVersion == nil || *preconditions.ResourceVersion != object.GetResourceVersion() {
			return errors.New("Pod deletion did not fence its exact UID and observed version")
		}
		c.deleted = true
	}
	return c.Client.Delete(ctx, object, options...)
}

func completeSuspensionCondition(t *testing.T, c client.Client, record *journalRecord) {
	t.Helper()
	sb := &sandboxv1beta1.Sandbox{}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: record.Namespace, Name: record.Sandbox.Name}, sb); err != nil {
		t.Fatal(err)
	}
	sb.Status.Conditions = []metav1.Condition{{Type: string(sandboxv1beta1.SandboxConditionSuspended), Status: metav1.ConditionTrue, ObservedGeneration: sb.Generation, Reason: sandboxv1beta1.SandboxReasonSuspendedPodTerminated}}
	if err := c.Status().Update(t.Context(), sb); err != nil {
		t.Fatal(err)
	}
}

func TestStopExactPodAfterOwnershipMetadataLossWaitsForAbsence(t *testing.T) {
	c, request := fixture(t, true)
	first := ready(t, c, request)
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: record.Pod.Namespace, Name: record.Pod.Name}, pod); err != nil {
		t.Fatal(err)
	}
	pod.OwnerReferences = nil
	pod.Finalizers = []string{"test.orka.ai/hold"}
	if err := c.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	if _, err := New(c).SuspendInstance(t.Context(), request.Key, first.Identity); !errors.Is(err, workspaceprovider.ErrStaleIdentity) {
		t.Fatalf("ownership loss relaxed data-suspension attestation: %v", err)
	}
	guarded := &exactPodDeleteClient{Client: c, uid: record.Pod.UID}
	for range 3 {
		observed, err := New(guarded).StopInstance(t.Context(), request.Key, first.Identity)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State != workspaceprovider.AllocationPending || observed.Startup != nil || observed.RetainedData != nil {
			t.Fatal("Pod still present but retirement reported terminal or retained state")
		}
	}
	if !guarded.deleted {
		t.Fatal("known exact Pod was not retired after owner-reference loss")
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), pod); err != nil || pod.DeletionTimestamp == nil {
		t.Fatalf("deleting Pod was not preserved by its finalizer: %v", err)
	}
	pod.Finalizers = nil
	if err := c.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	completeSuspensionCondition(t, c, record)
	observed, err := New(c).StopInstance(t.Context(), request.Key, first.Identity)
	if err != nil || observed.State != workspaceprovider.AllocationStopped || observed.RetainedData != nil {
		t.Fatalf("exact termination did not settle: %#v, %v", observed, err)
	}
	for _, object := range []client.Object{&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: record.Namespace, Name: record.Storage.ClaimName}}, &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: record.Storage.VolumeName}}} {
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(object), object); err != nil {
			t.Fatalf("stop destroyed durable data: %v", err)
		}
	}
}

func TestStopRefusesReplacementPodUID(t *testing.T) {
	c, request := fixture(t, false)
	first := ready(t, c, request)
	pod := &corev1.Pod{}
	key := types.NamespacedName{Namespace: first.Startup.Pod.Namespace, Name: first.Startup.Pod.Name}
	if err := c.Get(t.Context(), key, pod); err != nil {
		t.Fatal(err)
	}
	pod.UID = "replacement"
	pod.OwnerReferences = nil
	if err := c.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	if _, err := New(c).StopInstance(t.Context(), request.Key, first.Identity); !errors.Is(err, workspaceprovider.ErrStaleIdentity) {
		t.Fatalf("replacement UID was accepted: %v", err)
	}
	if err := c.Get(t.Context(), key, pod); err != nil || pod.DeletionTimestamp != nil {
		t.Fatalf("foreign replacement Pod was deleted: %v", err)
	}
}

func TestPendingSuspensionCanEscalateToDeleteWithoutRetainedClaim(t *testing.T) {
	c, request := fixture(t, true)
	first := ready(t, c, request)
	observed, err := New(c).SuspendInstance(t.Context(), request.Key, first.Identity)
	if err != nil || observed.State != workspaceprovider.AllocationPending {
		t.Fatalf("suspension did not remain pending: %#v, %v", observed, err)
	}
	for range 5 {
		observed, err = (&simulatedLifecycle{New(c)}).StopInstance(t.Context(), request.Key, first.Identity)
		if err != nil {
			t.Fatal(err)
		}
		if observed.RetainedData != nil {
			t.Fatal("cancelled suspension published a retained-data claim")
		}
		if observed.State == workspaceprovider.AllocationStopped {
			break
		}
	}
	if observed.State != workspaceprovider.AllocationStopped {
		t.Fatal("pending suspension prevented terminal stop")
	}
	policy := workspacev1alpha1.ExecutionWorkspaceDeletionPolicy{ProviderResources: workspacev1alpha1.WorkspaceDeletionActionDelete, PersistentVolumes: workspacev1alpha1.WorkspaceDeletionActionDelete, Checkpoints: workspacev1alpha1.WorkspaceDeletionActionDelete}
	// Claim, Sandbox, and PVC now retire separately before PV absence closes cleanup.
	for range 8 {
		observed, err = (&simulatedLifecycle{New(c)}).DeleteAllocation(t.Context(), request.Key, first.Identity, policy)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == workspaceprovider.AllocationDeleted {
			return
		}
	}
	t.Fatal("cancelled suspension prevented terminal data cleanup")
}

func TestOwnerMetadataLossDoesNotAuthorizeAnotherOwnedPod(t *testing.T) {
	c, request := fixture(t, false)
	first := ready(t, c, request)
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: record.Pod.Namespace, Name: record.Pod.Name}, pod); err != nil {
		t.Fatal(err)
	}
	unexpected := pod.DeepCopy()
	unexpected.Name += "-foreign"
	unexpected.UID = "foreign"
	unexpected.ResourceVersion = ""
	if err := c.Create(t.Context(), unexpected); err != nil {
		t.Fatal(err)
	}
	pod.OwnerReferences = nil
	if err := c.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	if _, err := New(c).StopInstance(t.Context(), request.Key, first.Identity); !errors.Is(err, workspaceprovider.ErrStaleIdentity) {
		t.Fatalf("unexpected owned process was accepted: %v", err)
	}
	sb, err := New(c).sandbox(t.Context(), record)
	if err != nil || sb.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeRunning {
		t.Fatalf("foreign process caused a native stop: %v", err)
	}
}
