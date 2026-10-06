package fake

import (
	"errors"
	"reflect"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	fakev1alpha1 "github.com/orka-agents/orka-workspace/providers/fake/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func workspaceFor(t *testing.T, c client.Client, key workspaceprovider.AllocationKey) *workspacev1alpha1.ExecutionWorkspace {
	t.Helper()
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: key.Namespace, Name: key.Name}, workspace); err != nil {
		t.Fatal(err)
	}
	return workspace
}

func TestAdmissionSignalsMustBothMatchCurrentGeneration(t *testing.T) {
	for _, signal := range []string{"missing-marker", "wrong-binding", "old-marker", "missing-condition", "old-condition", "condition-false", "disabled-provider"} {
		t.Run(signal, func(t *testing.T) {
			c, request := fixture(t)
			workspace := workspaceFor(t, c, request.Key)
			switch signal {
			case "missing-marker":
				workspace.Spec.CoreAdmission = nil
			case "wrong-binding":
				workspace.Spec.CoreAdmission.ProviderBinding.UID = "foreign"
			case "old-marker":
				workspace.Spec.CoreAdmission.AdmittedGeneration = 0
			case "missing-condition":
				workspace.Status.Conditions = nil
			case "old-condition":
				workspace.Status.Conditions[0].ObservedGeneration = 0
			case "condition-false":
				workspace.Status.Conditions[0].Status = metav1.ConditionFalse
			case "disabled-provider":
				provider := &workspacev1alpha1.ExecutionWorkspaceProvider{}
				if err := c.Get(t.Context(), types.NamespacedName{Name: workspace.Spec.ProviderBinding.Name}, provider); err != nil {
					t.Fatal(err)
				}
				provider.Spec.LifecycleState = workspacev1alpha1.ExecutionWorkspaceProviderDisabled
				if err := c.Update(t.Context(), provider); err != nil {
					t.Fatal(err)
				}
			}
			status := workspace.Status.DeepCopy()
			if err := c.Update(t.Context(), workspace); err != nil {
				t.Fatal(err)
			}
			workspace.Status = *status
			if err := c.Status().Update(t.Context(), workspace); err != nil {
				t.Fatal(err)
			}
			if _, err := New(c).EnsureAllocation(t.Context(), request); !errors.Is(err, workspaceprovider.ErrWorkspaceNotAdmitted) {
				t.Fatalf("unadmitted workspace progressed: %v", err)
			}
			journals := &corev1.ConfigMapList{}
			if err := c.List(t.Context(), journals); err != nil {
				t.Fatal(err)
			}
			if len(journals.Items) > 0 {
				t.Fatal("unadmitted workspace wrote intent")
			}
		})
	}
}

func TestReconcileUsesLifecycleAndAllowsRevocationAndDeletionWhenAdmissionIsStale(t *testing.T) {
	c, request := fixture(t)
	workspace := workspaceFor(t, c, request.Key)
	class := &workspacev1alpha1.ExecutionWorkspaceClass{ObjectMeta: metav1.ObjectMeta{Namespace: workspace.Namespace, Name: workspace.Spec.ClassBinding.Name, UID: workspace.Spec.ClassBinding.UID}}
	if err := c.Create(t.Context(), class); err != nil {
		t.Fatal(err)
	}
	workspace.Spec.Workload = &request
	workspace.Spec.Lifecycle.DeletionPolicy = deletionPolicy()
	workspace.Spec.Attachment = &workspacev1alpha1.ExecutionWorkspaceAttachment{Epoch: 1}
	workspace.Spec.AttachmentEpoch = 1
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	r := &FakeExecutionWorkspaceReconciler{Client: c}
	reconcileRequest := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}
	if _, err := r.Reconcile(t.Context(), reconcileRequest); err != nil {
		t.Fatal(err)
	}
	observed, err := New(c).Observe(t.Context(), request.Key)
	if err != nil || observed.State != workspaceprovider.AllocationReady {
		t.Fatalf("reconciler bypassed durable lifecycle: %v", err)
	}
	workspace = workspaceFor(t, c, request.Key)
	if workspace.Status.AttachedEpoch != 1 {
		t.Fatalf("attachment epoch = %d", workspace.Status.AttachedEpoch)
	}
	workspace.Spec.Attachment = nil
	workspace.Generation++
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), reconcileRequest); err != nil {
		t.Fatal(err)
	}
	workspace = workspaceFor(t, c, request.Key)
	if workspace.Status.AttachedEpoch != 0 {
		t.Fatal("stale admission blocked revocation")
	}
	workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredDeleted
	workspace.Spec.Retirement = &workspacev1alpha1.WorkloadRetirement{Sequence: request.Sequence, Identity: observed.Identity, Action: workspacev1alpha1.WorkloadRetirementDelete}
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), reconcileRequest); err != nil {
		t.Fatal(err)
	}
	workspace = workspaceFor(t, c, request.Key)
	if workspace.Status.State != workspacev1alpha1.ExecutionWorkspaceStateDeleted {
		t.Fatalf("cleanup state = %q", workspace.Status.State)
	}
	observed, err = New(c).Observe(t.Context(), request.Key)
	if err != nil || observed.State != workspaceprovider.AllocationDeleted {
		t.Fatalf("cleanup bypassed lifecycle: %v", err)
	}
	condition := workspaceprovider.FindCondition(workspace.Status.Conditions, string(workspacev1alpha1.ConditionWorkspaceAdmitted))
	if condition == nil || condition.ObservedGeneration != 1 {
		t.Fatal("provider rewrote core admission")
	}
}

func TestProviderPreservesCoreOwnedConditions(t *testing.T) {
	c, _ := fixture(t)
	provider := &workspacev1alpha1.ExecutionWorkspaceProvider{}
	key := types.NamespacedName{Name: "fake"}
	if err := c.Get(t.Context(), key, provider); err != nil {
		t.Fatal(err)
	}
	provider.Spec.ParametersRef = workspacev1alpha1.TypedObjectReference{Group: fakev1alpha1.GroupVersion.Group, Kind: "FakeProviderConfig", Name: "fake"}
	if err := c.Update(t.Context(), provider); err != nil {
		t.Fatal(err)
	}
	conditions := []metav1.Condition{
		{Type: string(workspacev1alpha1.ConditionProviderCompatible), Status: metav1.ConditionFalse, Reason: "CorePending", ObservedGeneration: 1, LastTransitionTime: metav1.Now()},
		{Type: string(workspacev1alpha1.ConditionProviderReady), Status: metav1.ConditionFalse, Reason: "CorePending", ObservedGeneration: 1, LastTransitionTime: metav1.Now()},
		{Type: "HeartbeatFresh", Status: metav1.ConditionFalse, Reason: "CorePending", ObservedGeneration: 1, LastTransitionTime: metav1.Now()},
	}
	provider.Status.Conditions = conditions
	if err := c.Status().Update(t.Context(), provider); err != nil {
		t.Fatal(err)
	}
	// Re-read the canonical timestamp precision stored by the API client.
	if err := c.Get(t.Context(), key, provider); err != nil {
		t.Fatal(err)
	}
	conditions = provider.Status.Conditions
	config := &fakev1alpha1.FakeProviderConfig{ObjectMeta: metav1.ObjectMeta{Name: "fake", UID: "config-uid"}}
	if err := c.Create(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	r := &FakeExecutionWorkspaceProviderReconciler{Client: c}
	for _, configured := range []bool{true, false} {
		if !configured {
			if err := c.Delete(t.Context(), config); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(t.Context(), key, provider); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(provider.Status.Conditions, conditions) {
			t.Fatal("provider rewrote core-owned conditions")
		}
		for _, feature := range provider.Status.SupportedFeatures {
			if feature != workspacev1alpha1.WorkspaceFeaturePools && feature != workspacev1alpha1.WorkspaceFeatureACPRuntime {
				t.Fatalf("unsupported capability advertised: %q", feature)
			}
		}
	}
}
