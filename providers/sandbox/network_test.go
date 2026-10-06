// Copyright (c) 2026. MIT License - see LICENSE file for details.

package sandbox

import (
	"context"
	"errors"
	"strings"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func networkRequest(t *testing.T, c client.Client, request workspaceprovider.WorkloadRequest, spec networkingv1.NetworkPolicySpec) workspaceprovider.WorkloadRequest {
	t.Helper()
	request.Runtime.NetworkPolicy = spec.DeepCopy()
	var err error
	request.Revision, err = workspaceprovider.WorkloadRevision(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := admit(t, c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	return request
}

func denyNetwork() networkingv1.NetworkPolicySpec {
	return networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"orka.ai/pool": "pool"}}, PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress}}
}

func createPolicy(t *testing.T, c client.Client, namespace, name string, spec networkingv1.NetworkPolicySpec) *networkingv1.NetworkPolicy {
	t.Helper()
	policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}, Spec: spec}
	if err := c.Create(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	return policy
}

func TestNetworkPolicyPortProtocolDefaultsWithoutChangingIntent(t *testing.T) {
	for _, direction := range []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress} {
		t.Run(string(direction), func(t *testing.T) {
			c, request := fixture(t, false)
			admitted := denyNetwork()
			port := networkingv1.NetworkPolicyPort{Port: new(intstr.FromInt32(443))}
			if direction == networkingv1.PolicyTypeIngress {
				admitted.Ingress = []networkingv1.NetworkPolicyIngressRule{{Ports: []networkingv1.NetworkPolicyPort{port}}}
			} else {
				admitted.Egress = []networkingv1.NetworkPolicyEgressRule{{Ports: []networkingv1.NetworkPolicyPort{port}}}
			}
			request = networkRequest(t, c, request, admitted)
			actual := workspaceprovider.NormalizedNetworkPolicySpec(admitted)
			policy := createPolicy(t, c, request.Runtime.Template.Namespace, "core-policy", actual)
			ready(t, c, request)
			if observed, err := New(c).Observe(t.Context(), request.Key); err != nil || observed.Startup == nil {
				t.Fatalf("API-defaulted TCP policy rejected: %+v, %v", observed, err)
			}
			var frozen *corev1.Protocol
			if direction == networkingv1.PolicyTypeIngress {
				frozen = request.Runtime.NetworkPolicy.Ingress[0].Ports[0].Protocol
				policy.Spec.Ingress[0].Ports[0].Protocol = new(corev1.ProtocolUDP)
			} else {
				frozen = request.Runtime.NetworkPolicy.Egress[0].Ports[0].Protocol
				policy.Spec.Egress[0].Ports[0].Protocol = new(corev1.ProtocolUDP)
			}
			if revision, err := workspaceprovider.WorkloadRevision(request); frozen != nil || err != nil || revision != request.Revision {
				t.Fatal("normalization rewrote the frozen workload")
			}
			if err := c.Update(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			if observed, err := New(c).Observe(t.Context(), request.Key); err == nil || observed.Startup != nil {
				t.Fatalf("changed UDP permissions accepted: %+v, %v", observed, err)
			}
		})
	}
}

func TestNetworkPolicyRequiredBeforeNativeAllocation(t *testing.T) {
	for _, failure := range []string{"missing", "wrong namespace", "wrong selector", "admitted selector mismatch", "unknown policy type", "unadmitted ingress", "unadmitted egress", "missing egress isolation", "terminating", "host network"} {
		t.Run(failure, func(t *testing.T) {
			c, request := fixture(t, false)
			admitted := denyNetwork()
			if failure == "admitted selector mismatch" {
				admitted.PodSelector.MatchLabels["orka.ai/pool"] = "foreign"
			}
			if failure == "unknown policy type" {
				admitted.PolicyTypes = []networkingv1.PolicyType{"Unsupported"}
			}
			if failure == "host network" {
				request.Runtime.Template.Spec.HostNetwork = true
			}
			request = networkRequest(t, c, request, admitted)
			actual := denyNetwork()
			namespace := request.Runtime.Template.Namespace
			switch failure {
			case "wrong namespace":
				namespace = request.Key.Namespace
			case "wrong selector":
				actual.PodSelector.MatchLabels["orka.ai/pool"] = "foreign"
			case "unadmitted ingress":
				actual.Ingress = []networkingv1.NetworkPolicyIngressRule{{}}
			case "unadmitted egress":
				actual.Egress = []networkingv1.NetworkPolicyEgressRule{{}}
			case "missing egress isolation":
				actual.PolicyTypes = nil // Default ingress isolation only.
			}
			if failure != "missing" {
				policy := createPolicy(t, c, namespace, "core-policy", actual)
				if failure == "terminating" {
					policy.Finalizers = []string{"example.test/cleanup"}
					if err := c.Update(t.Context(), policy); err != nil {
						t.Fatal(err)
					}
					if err := c.Delete(t.Context(), policy); err != nil {
						t.Fatal(err)
					}
				}
			}
			if observed, err := New(c).EnsureAllocation(t.Context(), request); err == nil || observed.Startup != nil {
				t.Fatalf("unsafe allocation accepted: %+v, %v", observed, err)
			}
			if _, _, err := New(c).read(t.Context(), request.Key); !errors.Is(err, workspaceprovider.ErrNotFound) {
				t.Fatalf("network failure created allocation intent: %v", err)
			}
			claims := &extv1beta1.SandboxClaimList{}
			if err := c.List(t.Context(), claims); err != nil || len(claims.Items) != 0 {
				t.Fatalf("network failure created a native claim: %v", err)
			}
		})
	}
}

func TestNetworkPolicyDefaultsAndAdditiveRules(t *testing.T) {
	for _, mode := range []string{"explicit deny both", "default ingress", "default ingress and egress", "explicit egress only", "split core rules", "narrower applied rules"} {
		t.Run(mode, func(t *testing.T) {
			c, request := fixture(t, false)
			admitted := denyNetwork()
			switch mode {
			case "default ingress":
				admitted.PolicyTypes = nil
			case "default ingress and egress":
				admitted.PolicyTypes = nil
				admitted.Egress = []networkingv1.NetworkPolicyEgressRule{{To: []networkingv1.NetworkPolicyPeer{{IPBlock: &networkingv1.IPBlock{CIDR: "192.0.2.0/24"}}}}}
			case "explicit egress only":
				admitted.PolicyTypes = []networkingv1.PolicyType{networkingv1.PolicyTypeEgress}
			case "split core rules", "narrower applied rules":
				admitted.Ingress = []networkingv1.NetworkPolicyIngressRule{{From: []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "controller"}}}}}}
				admitted.Egress = []networkingv1.NetworkPolicyEgressRule{{To: []networkingv1.NetworkPolicyPeer{{IPBlock: &networkingv1.IPBlock{CIDR: "192.0.2.0/24"}}}}}
			}
			request = networkRequest(t, c, request, admitted)
			namespace := request.Runtime.Template.Namespace
			if mode == "split core rules" {
				createPolicy(t, c, namespace, "core-deny", denyNetwork())
				for _, direction := range []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress} {
					allow := *admitted.DeepCopy()
					allow.PolicyTypes = []networkingv1.PolicyType{direction}
					if direction == networkingv1.PolicyTypeIngress {
						allow.Egress = nil
					} else {
						allow.Ingress = nil
					}
					createPolicy(t, c, namespace, "core-"+strings.ToLower(string(direction)), allow)
				}
			} else if mode == "narrower applied rules" {
				// Core's admitted rules are a permission envelope. Its publisher
				// can include rules for other placements bearing the pool label.
				createPolicy(t, c, namespace, "core-deny", denyNetwork())
			} else {
				createPolicy(t, c, namespace, "core-policy", admitted)
			}
			// Broader policies for other Pods or namespaces do not affect this Pod.
			unrelated := denyNetwork()
			unrelated.PodSelector.MatchLabels["orka.ai/pool"] = "other"
			unrelated.Ingress = []networkingv1.NetworkPolicyIngressRule{{}}
			unrelated.Egress = []networkingv1.NetworkPolicyEgressRule{{}}
			createPolicy(t, c, namespace, "other-pod", unrelated)
			unrelated.PodSelector = admitted.PodSelector
			createPolicy(t, c, request.Key.Namespace, "other-namespace", unrelated)
			first := ready(t, c, request)
			if observed, err := New(c).Observe(t.Context(), request.Key); err != nil || observed.Startup == nil || observed.Identity != first.Identity {
				t.Fatalf("applied network rules did not remain ready: %+v, %v", observed, err)
			}
		})
	}
}

func TestUnisolatedNetworkDirectionAllowsAdditionalRestrictions(t *testing.T) {
	for _, required := range []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress} {
		for _, permission := range []string{"deny all", "allow all", "specific CIDR"} {
			t.Run(string(required)+"/"+permission, func(t *testing.T) {
				c, request := fixture(t, false)
				admitted := denyNetwork()
				admitted.PolicyTypes = []networkingv1.PolicyType{required}
				request = networkRequest(t, c, request, admitted)
				createPolicy(t, c, request.Runtime.Template.Namespace, "core-policy", admitted)
				extra := denyNetwork()
				if required == networkingv1.PolicyTypeIngress {
					extra.PolicyTypes = []networkingv1.PolicyType{networkingv1.PolicyTypeEgress}
					if permission != "deny all" {
						extra.Egress = []networkingv1.NetworkPolicyEgressRule{{}}
						if permission == "specific CIDR" {
							extra.Egress[0].To = []networkingv1.NetworkPolicyPeer{{IPBlock: &networkingv1.IPBlock{CIDR: "192.0.2.0/24"}}}
						}
					}
				} else {
					extra.PolicyTypes = []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}
					if permission != "deny all" {
						extra.Ingress = []networkingv1.NetworkPolicyIngressRule{{}}
						if permission == "specific CIDR" {
							extra.Ingress[0].From = []networkingv1.NetworkPolicyPeer{{IPBlock: &networkingv1.IPBlock{CIDR: "192.0.2.0/24"}}}
						}
					}
				}
				createPolicy(t, c, request.Runtime.Template.Namespace, "additional-isolation", extra)
				first := ready(t, c, request)
				pod := &corev1.Pod{}
				if err := c.Get(t.Context(), types.NamespacedName{Namespace: first.Startup.Pod.Namespace, Name: first.Startup.Pod.Name}, pod); err != nil {
					t.Fatal(err)
				}
				// Upstream labels can select another restriction after allocation.
				extra.PodSelector = metav1.LabelSelector{MatchLabels: map[string]string{sandboxv1beta1.SandboxTemplateRefHashLabel: pod.Labels[sandboxv1beta1.SandboxTemplateRefHashLabel]}}
				createPolicy(t, c, pod.Namespace, "realized-pod-isolation", extra)
				if observed, err := New(c).Observe(t.Context(), request.Key); err != nil || observed.Startup == nil || observed.Identity != first.Identity {
					t.Fatalf("additional restriction exceeded an unrestricted direction: %+v, %v", observed, err)
				}
				// The required direction's empty envelope still forbids grants.
				violation := *admitted.DeepCopy()
				if required == networkingv1.PolicyTypeIngress {
					violation.Ingress = []networkingv1.NetworkPolicyIngressRule{{}}
				} else {
					violation.Egress = []networkingv1.NetworkPolicyEgressRule{{}}
				}
				createPolicy(t, c, pod.Namespace, "unadmitted-required-direction", violation)
				if observed, err := New(c).Observe(t.Context(), request.Key); err == nil || observed.Startup != nil {
					t.Fatalf("required direction's permission envelope was bypassed: %+v, %v", observed, err)
				}
			})
		}
	}
}

func TestRealizedPodNetworkPolicyDriftWithdrawsStartup(t *testing.T) {
	for _, drift := range []string{"deleted policy", "upstream label selects ingress", "upstream label selects egress"} {
		t.Run(drift, func(t *testing.T) {
			c, request := fixture(t, false)
			request = networkRequest(t, c, request, denyNetwork())
			policy := createPolicy(t, c, request.Runtime.Template.Namespace, "core-policy", denyNetwork())
			first := ready(t, c, request)
			key := types.NamespacedName{Namespace: request.Key.Namespace, Name: request.Key.Name}
			r := &ExecutionWorkspaceReconciler{Client: c}
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			if drift == "deleted policy" {
				if err := c.Delete(t.Context(), policy); err != nil {
					t.Fatal(err)
				}
			} else {
				pod := &corev1.Pod{}
				if err := c.Get(t.Context(), types.NamespacedName{Namespace: first.Startup.Pod.Namespace, Name: first.Startup.Pod.Name}, pod); err != nil {
					t.Fatal(err)
				}
				label := sandboxv1beta1.SandboxTemplateRefHashLabel
				extra := networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{label: pod.Labels[label]}}}
				if drift == "upstream label selects ingress" {
					extra.Ingress = []networkingv1.NetworkPolicyIngressRule{{}}
				} else {
					extra.PolicyTypes = []networkingv1.PolicyType{networkingv1.PolicyTypeEgress}
					extra.Egress = []networkingv1.NetworkPolicyEgressRule{{}}
				}
				createPolicy(t, c, pod.Namespace, "foreign-allow", extra)
			}
			if observed, err := New(c).Observe(t.Context(), request.Key); err == nil || observed.Startup != nil {
				t.Fatalf("network drift kept startup evidence: %+v, %v", observed, err)
			}
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err == nil {
				t.Fatal("reconcile ignored network drift")
			}
			workspace := &workspacev1alpha1.ExecutionWorkspace{}
			if err := c.Get(t.Context(), key, workspace); err != nil {
				t.Fatal(err)
			}
			if workspace.Status.Allocation == nil || workspace.Status.Allocation.Startup != nil || workspace.Status.Allocation.Identity != first.Identity || workspace.Status.Allocation.State != workspaceprovider.AllocationPending {
				t.Fatal("reconcile retained startup or lost its exact retirement fence")
			}
		})
	}
}

type unreadableNetworkClient struct{ client.Client }

func (c *unreadableNetworkClient) List(ctx context.Context, objects client.ObjectList, options ...client.ListOption) error {
	if _, ok := objects.(*networkingv1.NetworkPolicyList); ok {
		return apierrors.NewForbidden(schema.GroupResource{Group: "networking.k8s.io", Resource: "networkpolicies"}, "", errors.New("denied"))
	}
	return c.Client.List(ctx, objects, options...)
}

func TestUnreadableNetworkPolicyFailsClosed(t *testing.T) {
	c, request := fixture(t, false)
	request = networkRequest(t, c, request, denyNetwork())
	createPolicy(t, c, request.Runtime.Template.Namespace, "core-policy", denyNetwork())
	if _, err := New(&unreadableNetworkClient{c}).EnsureAllocation(t.Context(), request); !apierrors.IsForbidden(err) {
		t.Fatalf("unreadable network allowed allocation: %v", err)
	}
	first := ready(t, c, request)
	if observed, err := New(&unreadableNetworkClient{c}).Observe(t.Context(), request.Key); !apierrors.IsForbidden(err) || observed.Startup != nil {
		t.Fatalf("unreadable network retained startup: %+v, %v", observed, err)
	}
	// A network read failure must not prevent exact authorized retirement.
	key := types.NamespacedName{Namespace: request.Key.Namespace, Name: request.Key.Name}
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(t.Context(), key, workspace); err != nil {
		t.Fatal(err)
	}
	workspace.Spec.Retirement = &workspacev1alpha1.WorkloadRetirement{Sequence: request.Sequence, Identity: first.Identity, Action: workspacev1alpha1.WorkloadRetirementStop}
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	r := &ExecutionWorkspaceReconciler{Client: &unreadableNetworkClient{c}}
	for range 3 {
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
		if err := nativeTick(t.Context(), c); err != nil {
			t.Fatal(err)
		}
	}
	if observed, err := New(c).Observe(t.Context(), request.Key); err != nil || observed.State != workspaceprovider.AllocationStopped || observed.Identity != first.Identity {
		t.Fatalf("network read failure blocked exact retirement: %+v, %v", observed, err)
	}
}

func TestColdResumeRequiresAppliedNetworkBeforeNativeResume(t *testing.T) {
	c, request := fixture(t, true)
	request = networkRequest(t, c, request, denyNetwork())
	policy := createPolicy(t, c, request.Runtime.Template.Namespace, "core-policy", denyNetwork())
	first := ready(t, c, request)
	retired := suspended(t, c, request, first)
	next := continuation(request, retired)
	if err := admit(t, c)(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	if _, err := New(c).EnsureAllocation(t.Context(), next); err == nil {
		t.Fatal("cold resume started without admitted network isolation")
	}
	_, record, err := New(c).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := New(c).sandbox(t.Context(), record)
	if err != nil {
		t.Fatal(err)
	}
	if record.Request.Sequence != request.Sequence || record.Operation != "suspend" || sb.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeSuspended {
		t.Fatal("network failure changed the retained allocation or resumed its Sandbox")
	}
	createPolicy(t, c, request.Runtime.Template.Namespace, "core-policy", denyNetwork())
	if resumed := ready(t, c, next); resumed.Startup.Pod.UID == first.Startup.Pod.UID {
		t.Fatal("restored network did not cold boot a new Pod")
	}
}
