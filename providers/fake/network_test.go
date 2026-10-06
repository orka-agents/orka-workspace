// Copyright (c) 2026. MIT License - see LICENSE file for details.

package fake

import (
	"context"
	"errors"
	"testing"

	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func fakeNetworkFixture(t *testing.T) (*podClient, workspaceprovider.WorkloadRequest, *networkingv1.NetworkPolicy) {
	t.Helper()
	c, request := runtimeFixture(t)
	if err := networkingv1.AddToScheme(c.Scheme()); err != nil {
		t.Fatal(err)
	}
	request.Runtime.NetworkPolicy = &networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{MatchLabels: request.Runtime.Template.Labels},
		PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
		Egress: []networkingv1.NetworkPolicyEgressRule{{
			To:    []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "core"}}}},
			Ports: []networkingv1.NetworkPolicyPort{{Port: new(intstr.FromInt32(443))}},
		}},
	}
	var err error
	request.Revision, err = workspaceprovider.WorkloadRevision(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := publishRequest(t.Context(), c, request); err != nil {
		t.Fatal(err)
	}
	policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: request.Key.Namespace, Name: "core-network"}, Spec: *request.Runtime.NetworkPolicy.DeepCopy()}
	return c, request, policy
}

func TestFakeNetworkIsolationRequiredBeforeAllocation(t *testing.T) {
	for _, failure := range []string{"missing", "wrong namespace", "wrong selector", "missing ingress isolation", "unadmitted ingress", "unadmitted egress", "unknown direction", "host network"} {
		t.Run(failure, func(t *testing.T) {
			c, request, policy := fakeNetworkFixture(t)
			switch failure {
			case "wrong namespace":
				policy.Namespace = "foreign"
			case "wrong selector":
				policy.Spec.PodSelector.MatchLabels = map[string]string{"role": "foreign"}
			case "missing ingress isolation":
				policy.Spec.PolicyTypes = []networkingv1.PolicyType{networkingv1.PolicyTypeEgress}
			case "unadmitted ingress":
				policy.Spec.Ingress = []networkingv1.NetworkPolicyIngressRule{{}}
			case "unadmitted egress":
				policy.Spec.Egress = []networkingv1.NetworkPolicyEgressRule{{}}
			case "unknown direction":
				policy.Spec.PolicyTypes = append(policy.Spec.PolicyTypes, "Unknown")
			case "host network":
				request.Runtime.Template.Spec.HostNetwork = true
				request.Revision, _ = workspaceprovider.WorkloadRevision(request)
				if err := publishRequest(t.Context(), c, request); err != nil {
					t.Fatal(err)
				}
			}
			if failure != "missing" {
				if err := c.Create(t.Context(), policy); err != nil {
					t.Fatal(err)
				}
			}
			for range 2 {
				if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || observed.Startup != nil {
					t.Fatalf("unverified network admitted: %+v, %v", observed, err)
				}
			}
			journals := &corev1.ConfigMapList{}
			if err := c.List(t.Context(), journals); err != nil {
				t.Fatal(err)
			}
			if c.creates != 0 || len(journals.Items) != 0 {
				t.Fatalf("network rejection created %d Pods and %d journals", c.creates, len(journals.Items))
			}
		})
	}
}

type fakeNetworkUnavailable struct{ client.Client }

func (c fakeNetworkUnavailable) List(ctx context.Context, list client.ObjectList, options ...client.ListOption) error {
	if _, ok := list.(*networkingv1.NetworkPolicyList); ok {
		return errors.New("NetworkPolicy API unavailable")
	}
	return c.Client.List(ctx, list, options...)
}

func TestFakeNetworkInventoryFailureHasNoAllocationEffects(t *testing.T) {
	c, request, policy := fakeNetworkFixture(t)
	if err := c.Create(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	if observed, err := New(fakeNetworkUnavailable{c}).EnsureAllocation(t.Context(), request); err == nil || observed.Startup != nil {
		t.Fatalf("unreadable policy inventory admitted: %+v, %v", observed, err)
	}
	journals := &corev1.ConfigMapList{}
	if err := c.List(t.Context(), journals); err != nil {
		t.Fatal(err)
	}
	if c.creates != 0 || len(journals.Items) != 0 {
		t.Fatal("unreadable policy inventory created an allocation")
	}
}

func TestFakeNetworkDriftWithdrawsStartupWithoutBlockingExactCleanup(t *testing.T) {
	c, request, policy := fakeNetworkFixture(t)
	policy.Spec = workspaceprovider.NormalizedNetworkPolicySpec(policy.Spec)
	if err := c.Create(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	ready, err := New(c).EnsureAllocation(t.Context(), request)
	if err != nil || ready.Startup == nil {
		t.Fatalf("defaulted network rejected: %+v, %v", ready, err)
	}
	if revision, err := workspaceprovider.WorkloadRevision(request); err != nil || revision != request.Revision || request.Runtime.NetworkPolicy.Egress[0].Ports[0].Protocol != nil {
		t.Fatal("network normalization rewrote frozen intent")
	}
	// The provider's realized instance label can select a policy that the frozen
	// template labels did not select before creation.
	extra := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: policy.Namespace, Name: "additional-grant"}, Spec: networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{podInstanceLabel: ready.Identity.InstanceID}},
		PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress}, Egress: []networkingv1.NetworkPolicyEgressRule{{}},
	}}
	if err := c.Create(t.Context(), extra); err != nil {
		t.Fatal(err)
	}
	for _, lifecycle := range []*Lifecycle{New(c), New(fakeNetworkUnavailable{c})} {
		if observed, err := lifecycle.Observe(t.Context(), request.Key); err == nil || observed.Startup != nil {
			t.Fatalf("network drift or read failure retained startup: %+v, %v", observed, err)
		}
	}
	d := New(fakeNetworkUnavailable{c})
	if _, err := d.StopInstance(t.Context(), request.Key, ready.Identity); err != nil {
		t.Fatal(err)
	}
	stopped, err := d.StopInstance(t.Context(), request.Key, ready.Identity)
	if err != nil || stopped.State != workspaceprovider.AllocationStopped {
		t.Fatalf("network drift blocked exact stop: %+v, %v", stopped, err)
	}
	deleted, err := d.DeleteAllocation(t.Context(), request.Key, ready.Identity, deletionPolicy())
	if err != nil || deleted.State != workspaceprovider.AllocationDeleted {
		t.Fatalf("network drift blocked exact deletion: %+v, %v", deleted, err)
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(extra), &networkingv1.NetworkPolicy{}); err != nil {
		t.Fatalf("cleanup changed operator policy: %v", err)
	}
}

func TestFakeEgressOnlyPolicyPreservesUnrestrictedIngress(t *testing.T) {
	c, request, policy := fakeNetworkFixture(t)
	request.Runtime.NetworkPolicy.PolicyTypes = []networkingv1.PolicyType{networkingv1.PolicyTypeEgress}
	request.Revision, _ = workspaceprovider.WorkloadRevision(request)
	if err := publishRequest(t.Context(), c, request); err != nil {
		t.Fatal(err)
	}
	policy.Spec.Ingress = []networkingv1.NetworkPolicyIngressRule{{}}
	if err := c.Create(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	if observed, err := New(c).EnsureAllocation(t.Context(), request); err != nil || observed.Startup == nil {
		t.Fatalf("unrestricted admitted ingress rejected: %+v, %v", observed, err)
	}
}
