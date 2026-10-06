package sandbox

import (
	"context"
	"errors"
	"strings"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type serviceAccountReadFailure struct{ client.Client }

func (c serviceAccountReadFailure) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if _, ok := object.(*corev1.ServiceAccount); ok {
		return errors.New("ServiceAccount API unavailable")
	}
	return c.Client.Get(ctx, key, object, options...)
}

func setPullCredentials(t *testing.T, c client.Client, namespace, name string) {
	t.Helper()
	account := &corev1.ServiceAccount{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: namespace, Name: name}, account); err != nil {
		t.Fatal(err)
	}
	account.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "registry-credentials"}}
	if err := c.Update(t.Context(), account); err != nil {
		t.Fatal(err)
	}
}

func TestServiceAccountCredentialsRejectedBeforeAllocation(t *testing.T) {
	for _, test := range []string{"default", "selected", "deprecated-alias", "namespace-fallback", "missing", "unavailable", "durable"} {
		t.Run(test, func(t *testing.T) {
			c, request := fixture(t, test == "durable")
			namespace, name := request.Runtime.Template.Namespace, "default"
			switch test {
			case "selected", "deprecated-alias":
				name = "selected"
				if test == "selected" {
					request.Runtime.Template.Spec.ServiceAccountName = name
				} else {
					request.Runtime.Template.Spec.DeprecatedServiceAccount = name
				}
				if err := c.Create(t.Context(), &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}); err != nil {
					t.Fatal(err)
				}
			case "namespace-fallback":
				namespace = request.Key.Namespace
				request.Runtime.Template.Namespace = ""
				if err := c.Create(t.Context(), &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}); err != nil {
					t.Fatal(err)
				}
			case "missing":
				if err := c.Delete(t.Context(), &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}); err != nil {
					t.Fatal(err)
				}
			case "unavailable":
				c = serviceAccountReadFailure{c}
			}
			if test != "missing" && test != "unavailable" {
				setPullCredentials(t, c, namespace, name)
			}
			request.Revision, _ = workspaceprovider.WorkloadRevision(request)
			if err := admit(t, c)(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), "ServiceAccount") || observed.Startup != nil {
				t.Fatalf("unsafe ServiceAccount accepted: %#v, %v", observed, err)
			}
			requireNoSandboxAllocation(t, c, request)
		})
	}
}

func TestCleanSelectedServiceAccountPreservesFrozenIntent(t *testing.T) {
	c, request := fixture(t, false)
	setPullCredentials(t, c, request.Runtime.Template.Namespace, "default")
	request.Runtime.Template.Spec.ServiceAccountName = "clean-selected"
	request.Runtime.Template.Spec.DeprecatedServiceAccount = "default"
	if err := c.Create(t.Context(), &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: request.Runtime.Template.Namespace, Name: "clean-selected"}}); err != nil {
		t.Fatal(err)
	}
	request.Revision, _ = workspaceprovider.WorkloadRevision(request)
	if err := admit(t, c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	ready(t, c, request)
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil || record.Request.Revision != request.Revision || record.Request.Runtime.Template.Spec.ServiceAccountName != "clean-selected" || record.Request.Runtime.Template.Spec.DeprecatedServiceAccount != "default" {
		t.Fatalf("frozen account intent changed: %v", err)
	}
}

type pullSecretAdmission struct{ client.Client }

func (c pullSecretAdmission) Create(ctx context.Context, object client.Object, options ...client.CreateOption) error {
	if pod, ok := object.(*corev1.Pod); ok {
		pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "registry-credentials"}}
	}
	return c.Client.Create(ctx, object, options...)
}

func TestAdmissionInjectedPullCredentialsNeverPublishStartup(t *testing.T) {
	c, request := fixture(t, false)
	d := &simulatedLifecycle{New(pullSecretAdmission{c})}
	var err error
	for range 5 {
		var observed workspaceprovider.AllocationObservation
		observed, err = d.EnsureAllocation(t.Context(), request)
		if observed.Startup != nil {
			t.Fatal("admission-mutated Pod reported Ready")
		}
		if err != nil {
			break
		}
	}
	if err == nil {
		t.Fatal("admission-mutated Pod was never rejected")
	}
	if observed, err := d.Observe(t.Context(), request.Key); err == nil || observed.Startup != nil {
		t.Fatalf("admission-mutated Pod recovered Ready: %#v, %v", observed, err)
	}
	_, record, err := d.read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	var stopped workspaceprovider.AllocationObservation
	for range 5 {
		stopped, err = d.StopInstance(t.Context(), request.Key, record.Observation.Identity)
		if err != nil {
			t.Fatal(err)
		}
		if stopped.State == workspaceprovider.AllocationStopped {
			break
		}
	}
	if stopped.State != workspaceprovider.AllocationStopped {
		t.Fatal("admission-mutated Pod lost exact cleanup")
	}
}

func TestInjectedPodPullCredentialsBlockReadyAndPreserveExactCleanup(t *testing.T) {
	c, request := fixture(t, false)
	first := ready(t, c, request)
	pod := &corev1.Pod{}
	podKey := client.ObjectKey{Namespace: first.Startup.Pod.Namespace, Name: first.Startup.Pod.Name}
	if err := c.Get(t.Context(), podKey, pod); err != nil {
		t.Fatal(err)
	}
	pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "registry-credentials"}}
	if err := c.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	if actual, err := New(c).Observe(t.Context(), request.Key); err == nil || actual.Startup != nil {
		t.Fatalf("mutated Pod reported Ready: %#v, %v", actual, err)
	}
	if actual, err := New(c).EnsureAllocation(t.Context(), request); err == nil || actual.Startup != nil {
		t.Fatalf("mutated Pod recovered Ready: %#v, %v", actual, err)
	}
	setPullCredentials(t, c, request.Runtime.Template.Namespace, "default")
	d := &simulatedLifecycle{New(serviceAccountReadFailure{c})}
	var stopped workspaceprovider.AllocationObservation
	var err error
	for range 5 {
		stopped, err = d.StopInstance(t.Context(), request.Key, first.Identity)
		if err != nil {
			t.Fatal(err)
		}
		if stopped.State == workspaceprovider.AllocationStopped {
			break
		}
	}
	if stopped.State != workspaceprovider.AllocationStopped {
		t.Fatal("exact cleanup depended on ServiceAccount availability")
	}
	policy := workspacev1alpha1.ExecutionWorkspaceDeletionPolicy{ProviderResources: workspacev1alpha1.WorkspaceDeletionActionDelete, PersistentVolumes: workspacev1alpha1.WorkspaceDeletionActionDelete, Checkpoints: workspacev1alpha1.WorkspaceDeletionActionDelete}
	var deleted workspaceprovider.AllocationObservation
	for range 8 {
		deleted, err = d.DeleteAllocation(t.Context(), request.Key, first.Identity, policy)
		if err != nil {
			t.Fatal(err)
		}
		if deleted.State == workspaceprovider.AllocationDeleted {
			break
		}
	}
	if deleted.State != workspaceprovider.AllocationDeleted {
		t.Fatal("exact deletion depended on ServiceAccount availability")
	}
}
