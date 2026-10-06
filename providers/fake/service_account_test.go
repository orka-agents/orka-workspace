package fake

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
	for _, test := range []string{"default", "selected", "deprecated-alias", "runtime-namespace", "namespace-fallback", "missing", "unavailable"} {
		t.Run(test, func(t *testing.T) {
			base, request := runtimeFixture(t)
			var c client.Client = base
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
			case "runtime-namespace":
				namespace = "runtime-only"
				request.Runtime.Template.Namespace = namespace
				if err := c.Create(t.Context(), &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}); err != nil {
					t.Fatal(err)
				}
			case "namespace-fallback":
				request.Runtime.Template.Namespace = ""
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
			if err := publishRequest(t.Context(), c, request); err != nil {
				t.Fatal(err)
			}
			if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), "ServiceAccount") || observed.Startup != nil {
				t.Fatalf("unsafe ServiceAccount accepted: %#v, %v", observed, err)
			}
			journals := &corev1.ConfigMapList{}
			pods := &corev1.PodList{}
			if err := c.List(t.Context(), journals); err != nil {
				t.Fatal(err)
			}
			if err := c.List(t.Context(), pods); err != nil {
				t.Fatal(err)
			}
			if len(journals.Items) != 0 || len(pods.Items) != 0 || base.creates != 0 {
				t.Fatal("ServiceAccount rejection followed allocation effects")
			}
		})
	}
}

func TestCleanSelectedServiceAccountPreservesFrozenIntent(t *testing.T) {
	c, request := runtimeFixture(t)
	setPullCredentials(t, c, request.Runtime.Template.Namespace, "default")
	request.Runtime.Template.Spec.ServiceAccountName = "clean-selected"
	request.Runtime.Template.Spec.DeprecatedServiceAccount = "default"
	if err := c.Create(t.Context(), &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: request.Runtime.Template.Namespace, Name: "clean-selected"}}); err != nil {
		t.Fatal(err)
	}
	request.Revision, _ = workspaceprovider.WorkloadRevision(request)
	if err := publishRequest(t.Context(), c, request); err != nil {
		t.Fatal(err)
	}
	observed, err := New(c).EnsureAllocation(t.Context(), request)
	if err != nil || observed.State != workspaceprovider.AllocationReady || observed.Startup == nil {
		t.Fatalf("clean selected account rejected: %#v, %v", observed, err)
	}
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
	c, request := runtimeFixture(t)
	d := New(pullSecretAdmission{c})
	if observed, err := d.EnsureAllocation(t.Context(), request); err == nil || observed.Startup != nil {
		t.Fatalf("admission-mutated Pod reported Ready: %#v, %v", observed, err)
	}
	if observed, err := d.Observe(t.Context(), request.Key); err == nil || observed.Startup != nil {
		t.Fatalf("admission-mutated Pod recovered Ready: %#v, %v", observed, err)
	}
	_, record, err := d.read(t.Context(), request.Key)
	if err != nil || record.Pod == nil || record.Pod.UID == "" {
		t.Fatalf("known creation response lost exact cleanup identity: %v", err)
	}
	for range 2 {
		if _, err := d.StopInstance(t.Context(), request.Key, record.Observation.Identity); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.DeleteAllocation(t.Context(), request.Key, record.Observation.Identity, deletionPolicy()); err != nil {
		t.Fatal(err)
	}
}

func TestInjectedPodPullCredentialsBlockReadyAndPreserveExactCleanup(t *testing.T) {
	c, request := runtimeFixture(t)
	observed, err := New(c).EnsureAllocation(t.Context(), request)
	if err != nil || observed.Startup == nil || observed.Startup.Pod == nil {
		t.Fatalf("clean initial allocation failed: %#v, %v", observed, err)
	}
	pod := &corev1.Pod{}
	podKey := client.ObjectKey{Namespace: observed.Startup.Pod.Namespace, Name: observed.Startup.Pod.Name}
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
	var stopped workspaceprovider.AllocationObservation
	for range 3 {
		stopped, err = New(serviceAccountReadFailure{c}).StopInstance(t.Context(), request.Key, observed.Identity)
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
	deleted, err := New(serviceAccountReadFailure{c}).DeleteAllocation(t.Context(), request.Key, observed.Identity, policy)
	if err != nil || deleted.State != workspaceprovider.AllocationDeleted {
		t.Fatalf("exact deletion failed: %#v, %v", deleted, err)
	}
}
