// Copyright (c) 2026. MIT License - see LICENSE file for details.

package substrate

import (
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	sdk "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func nativeUnionFixture(t *testing.T) (client.Client, *nativeFixture, sdk.WorkloadRequest) {
	t.Helper()
	c, native, request := fixture(t, false)
	pool := poolObject()
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "native-workers", Name: "native-workers"}, pool); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedStringMap(pool.Object, map[string]string{"operator": "native"}, "spec", "template", "labels"); err != nil {
		t.Fatal(err)
	}
	if err := c.Update(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	request.Runtime.NetworkPolicy.Egress = []networkingv1.NetworkPolicyEgressRule{{
		To: []networkingv1.NetworkPolicyPeer{
			{IPBlock: &networkingv1.IPBlock{CIDR: "192.0.2.0/24"}},
			{IPBlock: &networkingv1.IPBlock{CIDR: "198.51.100.0/24"}},
		},
		Ports: []networkingv1.NetworkPolicyPort{
			{Port: new(intstr.FromInt32(443))},
			{Port: new(intstr.FromInt32(8443)), EndPort: new(int32(8450))},
		},
	}}
	publishNativeUnionRequest(t, c, &request)
	return c, native, request
}

func publishNativeUnionRequest(t *testing.T, c client.Client, request *sdk.WorkloadRequest) {
	t.Helper()
	var err error
	request.Revision, err = sdk.WorkloadRevision(*request)
	if err != nil {
		t.Fatal(err)
	}
	if err := admit(c)(t.Context(), *request); err != nil {
		t.Fatal(err)
	}
}

func boundedNativeGrant() networkingv1.NetworkPolicySpec {
	return networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"operator": "native"}},
		PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
		Egress: []networkingv1.NetworkPolicyEgressRule{{
			To:    []networkingv1.NetworkPolicyPeer{{IPBlock: &networkingv1.IPBlock{CIDR: "192.0.2.0/24"}}},
			Ports: []networkingv1.NetworkPolicyPort{{Port: new(intstr.FromInt32(8445))}},
		}},
	}
}

func TestNativeSelectingPolicyUnionBeforeResume(t *testing.T) {
	for _, test := range []struct {
		name    string
		change  func(*networkingv1.NetworkPolicySpec)
		allowed bool
	}{
		{"bounded peer and port subset", func(*networkingv1.NetworkPolicySpec) {}, true},
		{"deny-all restriction", func(p *networkingv1.NetworkPolicySpec) { p.Egress = nil }, true},
		{"nonselecting broader grant", func(p *networkingv1.NetworkPolicySpec) {
			p.PodSelector.MatchLabels["operator"] = "other"
			p.Egress = []networkingv1.NetworkPolicyEgressRule{{}}
		}, true},
		{"operator ingress", func(p *networkingv1.NetworkPolicySpec) {
			p.PolicyTypes = []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}
			p.Ingress = []networkingv1.NetworkPolicyIngressRule{{}}
			p.Egress = nil
		}, true},
		{"inactive egress", func(p *networkingv1.NetworkPolicySpec) {
			p.PolicyTypes = []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}
			p.Egress = []networkingv1.NetworkPolicyEgressRule{{}}
		}, true},
		{"inherited-label broader grant", func(p *networkingv1.NetworkPolicySpec) {
			p.Egress = []networkingv1.NetworkPolicyEgressRule{{}}
		}, false},
		{"namespace-wide broader grant", func(p *networkingv1.NetworkPolicySpec) {
			p.PodSelector = metav1.LabelSelector{}
			p.Egress = []networkingv1.NetworkPolicyEgressRule{{}}
		}, false},
		{"default active egress broader grant", func(p *networkingv1.NetworkPolicySpec) {
			p.PolicyTypes = nil
			p.Egress = []networkingv1.NetworkPolicyEgressRule{{}}
		}, false},
		{"expression-selected broader grant", func(p *networkingv1.NetworkPolicySpec) {
			p.PodSelector = metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "operator", Operator: metav1.LabelSelectorOpIn, Values: []string{"native"}}}}
			p.Egress = []networkingv1.NetworkPolicyEgressRule{{}}
		}, false},
		{"broader peer", func(p *networkingv1.NetworkPolicySpec) {
			p.Egress[0].To[0].IPBlock.CIDR = "0.0.0.0/0"
		}, false},
		{"unbounded ports", func(p *networkingv1.NetworkPolicySpec) { p.Egress[0].Ports = nil }, false},
		{"different protocol", func(p *networkingv1.NetworkPolicySpec) { p.Egress[0].Ports[0].Protocol = new(corev1.ProtocolUDP) }, false},
		{"unsupported direction", func(p *networkingv1.NetworkPolicySpec) { p.PolicyTypes = []networkingv1.PolicyType{"Unknown"} }, false},
		{"nonselecting unsupported direction", func(p *networkingv1.NetworkPolicySpec) {
			p.PodSelector.MatchLabels["operator"] = "other"
			p.PolicyTypes = []networkingv1.PolicyType{"Unknown"}
		}, true},
		{"invalid selector", func(p *networkingv1.NetworkPolicySpec) {
			p.PodSelector = metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "operator", Operator: "Unknown"}}}
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, native, request := nativeUnionFixture(t)
			spec := boundedNativeGrant()
			test.change(&spec)
			policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "native-workers", Name: "operator-grant"}, Spec: spec}
			if err := c.Create(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			original := request.DeepCopy()
			if test.allowed {
				first := ready(t, c, native, request)
				if first.Startup == nil || native.boots != 1 {
					t.Fatal("bounded policies did not allow the exact native startup")
				}
				if observed, err := driver(c, native).Observe(t.Context(), request.Key); err != nil || observed.Startup == nil || observed.Startup.Process.Worker != first.Startup.Process.Worker {
					t.Fatalf("bounded policies lost exact startup evidence: %+v, %v", observed, err)
				}
			} else {
				observed, err := driver(c, native).EnsureAllocation(t.Context(), request)
				if err == nil || observed.Startup != nil || observed.State == sdk.AllocationReady {
					t.Fatalf("unadmitted effective egress reached startup: %+v, %v", observed, err)
				}
				if native.boots != 0 {
					t.Fatal("effective egress was checked after native execution")
				}
				if observed, _ := driver(c, native).Observe(t.Context(), request.Key); observed.Startup != nil || observed.State == sdk.AllocationReady {
					t.Fatal("blocked allocation published startup through Observe")
				}
			}
			if !reflect.DeepEqual(request, *original) {
				t.Fatal("network verification changed frozen intent")
			}
		})
	}
}

func TestNativeRealizedWorkerLabelsWithdrawUnadmittedStartup(t *testing.T) {
	for _, afterAdmissionLabel := range []bool{false, true} {
		t.Run(map[bool]string{false: "inherited label", true: "realized extra label"}[afterAdmissionLabel], func(t *testing.T) {
			c, native, request := nativeUnionFixture(t)
			first := ready(t, c, native, request)
			label := "operator"
			value := "native"
			if afterAdmissionLabel {
				label, value = "admission-network", "extra"
			}
			policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: first.Startup.Process.Worker.Namespace, Name: "late-grant"}, Spec: networkingv1.NetworkPolicySpec{
				PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{label: value}},
				PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
				Egress:      []networkingv1.NetworkPolicyEgressRule{{}},
			}}
			if err := c.Create(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			if afterAdmissionLabel {
				if observed, err := driver(c, native).Observe(t.Context(), request.Key); err != nil || observed.Startup == nil {
					t.Fatalf("nonselecting grant affected startup: %+v, %v", observed, err)
				}
				pod := &corev1.Pod{}
				if err := c.Get(t.Context(), client.ObjectKey{Namespace: first.Startup.Process.Worker.Namespace, Name: first.Startup.Process.Worker.Name}, pod); err != nil {
					t.Fatal(err)
				}
				pod.Labels[label] = value
				if err := c.Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			}
			journalBefore := &corev1.ConfigMap{}
			if err := c.Get(t.Context(), journalKey(request.Key), journalBefore); err != nil {
				t.Fatal(err)
			}
			if observed, err := driver(c, native).Observe(t.Context(), request.Key); err == nil || observed.Startup != nil || observed.State == sdk.AllocationReady {
				t.Fatalf("effective egress drift retained observable startup: %+v, %v", observed, err)
			}
			journalAfter := &corev1.ConfigMap{}
			if err := c.Get(t.Context(), journalKey(request.Key), journalAfter); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(journalBefore, journalAfter) {
				t.Fatal("read-only Observe mutated the allocation journal")
			}
			if observed, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil || observed.Startup != nil || observed.State == sdk.AllocationReady {
				t.Fatalf("effective egress drift retained ensured startup: %+v, %v", observed, err)
			}
			if native.boots != 1 {
				t.Fatal("verification replayed native execution")
			}
		})
	}
}

func TestNativePolicyUnionKeepsRulesAndNamespacesSeparate(t *testing.T) {
	for _, test := range []string{"cross-rule grant", "worker-relative peer"} {
		t.Run(test, func(t *testing.T) {
			c, native, request := nativeUnionFixture(t)
			extra := boundedNativeGrant()
			if test == "cross-rule grant" {
				request.Runtime.NetworkPolicy.Egress = []networkingv1.NetworkPolicyEgressRule{
					{To: []networkingv1.NetworkPolicyPeer{{IPBlock: &networkingv1.IPBlock{CIDR: "192.0.2.0/24"}}}, Ports: []networkingv1.NetworkPolicyPort{{Port: new(intstr.FromInt32(443))}}},
					{To: []networkingv1.NetworkPolicyPeer{{IPBlock: &networkingv1.IPBlock{CIDR: "198.51.100.0/24"}}}, Ports: []networkingv1.NetworkPolicyPort{{Port: new(intstr.FromInt32(8445))}}},
				}
			} else {
				peer := networkingv1.NetworkPolicyPeer{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "peer"}}}
				request.Runtime.NetworkPolicy.Egress[0].To = []networkingv1.NetworkPolicyPeer{peer}
				extra.Egress[0].To = []networkingv1.NetworkPolicyPeer{peer}
			}
			publishNativeUnionRequest(t, c, &request)
			if err := c.Create(t.Context(), &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "native-workers", Name: "cross-grant"}, Spec: extra}); err != nil {
				t.Fatal(err)
			}
			if observed, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil || observed.Startup != nil || native.boots != 0 {
				t.Fatalf("cross-rule or cross-namespace permission reached startup: %+v, %v", observed, err)
			}
		})
	}
}

func TestNativePolicyInventoryFailureBlocksResumeAndReady(t *testing.T) {
	for _, running := range []bool{false, true} {
		t.Run(map[bool]string{false: "before Resume", true: "after Ready"}[running], func(t *testing.T) {
			c, native, request := nativeUnionFixture(t)
			if running {
				ready(t, c, native, request)
			}
			reads := 0
			wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{List: func(ctx context.Context, inner client.WithWatch, list client.ObjectList, options ...client.ListOption) error {
				if _, ok := list.(*networkingv1.NetworkPolicyList); ok {
					reads++
					return apierrors.NewForbidden(schema.GroupResource{Group: "networking.k8s.io", Resource: "networkpolicies"}, "", errors.New("inventory denied"))
				}
				return inner.List(ctx, list, options...)
			}})
			observed, err := driver(wrapped, native).EnsureAllocation(t.Context(), request)
			if err == nil || !strings.Contains(err.Error(), "inventory denied") || observed.Startup != nil || reads == 0 {
				t.Fatalf("unreadable policy inventory allowed startup: %+v, %v", observed, err)
			}
			if running {
				if observed, err := driver(wrapped, native).Observe(t.Context(), request.Key); err == nil || observed.Startup != nil {
					t.Fatalf("unreadable policy inventory retained Ready: %+v, %v", observed, err)
				}
			}
			wantBoots := 0
			if running {
				wantBoots = 1
			}
			if native.boots != wantBoots {
				t.Fatal("policy inventory failure reached or replayed native execution")
			}
		})
	}
}

func TestNativeWorkerPolicyPeersUseEffectiveNamespaces(t *testing.T) {
	for _, namespace := range []string{"runtime", "native-workers", ""} {
		t.Run("template namespace="+namespace, func(t *testing.T) {
			c, native, request := nativeUnionFixture(t)
			request.Runtime.Template.Namespace = namespace
			extra := boundedNativeGrant()
			if namespace != "" {
				peer := networkingv1.NetworkPolicyPeer{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "peer"}}}
				request.Runtime.NetworkPolicy.Egress[0].To = []networkingv1.NetworkPolicyPeer{peer}
				if namespace != "native-workers" {
					peer.NamespaceSelector = &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelMetadataName: namespace}}
				}
				extra.Egress[0].To = []networkingv1.NetworkPolicyPeer{peer}
			}
			publishNativeUnionRequest(t, c, &request)
			if err := c.Create(t.Context(), &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "native-workers", Name: "bounded-peer"}, Spec: extra}); err != nil {
				t.Fatal(err)
			}
			first := ready(t, c, native, request)
			if observed, err := driver(c, native).Observe(t.Context(), request.Key); err != nil || observed.Startup == nil || observed.Startup.Process.Worker != first.Startup.Process.Worker {
				t.Fatalf("bounded namespace-aware policy lost startup: %+v, %v", observed, err)
			}
		})
	}
}

func TestNativePolicyDriftDoesNotBlockExactWorkerRetirementRecovery(t *testing.T) {
	for _, denyInventory := range []bool{false, true} {
		t.Run(map[bool]string{false: "broader selecting policy", true: "unreadable inventory"}[denyInventory], func(t *testing.T) {
			c, native, request := nativeUnionFixture(t)
			first := ready(t, c, native, request)
			cm, record, err := driver(c, native).read(t.Context(), request.Key)
			if err != nil {
				t.Fatal(err)
			}
			original := *record.Worker
			// A legacy/uncertain journal must recover the exact running assignment
			// during Stop even though it cannot provide startup evidence.
			record.Worker = nil
			if err := driver(c, native).save(t.Context(), cm, record); err != nil {
				t.Fatal(err)
			}
			policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: original.Namespace, Name: "operator-wide-egress"}, Spec: networkingv1.NetworkPolicySpec{
				PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"operator": "native"}},
				PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
				Egress:      []networkingv1.NetworkPolicyEgressRule{{}},
			}}
			if err := c.Create(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			reads := 0
			wrapped := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{List: func(ctx context.Context, inner client.WithWatch, list client.ObjectList, options ...client.ListOption) error {
				if _, ok := list.(*networkingv1.NetworkPolicyList); ok {
					reads++
					if denyInventory {
						return errors.New("inventory denied during retirement")
					}
				}
				return inner.List(ctx, list, options...)
			}})
			stopped := retired(t, wrapped, native, request, first.Identity, false)
			if stopped.Startup != nil || native.boots != 1 || len(native.actors) != 0 {
				t.Fatal("exact retirement retained startup or replayed execution")
			}
			_, recovered, err := driver(c, native).read(t.Context(), request.Key)
			if err != nil || recovered.Worker == nil || *recovered.Worker != original {
				t.Fatalf("retirement lost exact original worker identity: %+v, %v", recovered, err)
			}
			allocationDeleted(t, wrapped, native, request, first.Identity)
			if reads != 0 {
				t.Fatal("exact cleanup depended on a startup policy inventory")
			}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(policy), &networkingv1.NetworkPolicy{}); err != nil {
				t.Fatal("cleanup removed an operator-owned policy")
			}
		})
	}
}

func TestShippedSubstrateRBACAllowsPolicyInventory(t *testing.T) {
	file, err := os.Open("config/rbac.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder := yaml.NewYAMLOrJSONDecoder(file, 4096)
	for {
		var role rbacv1.ClusterRole
		err := decoder.Decode(&role)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if role.Kind != "ClusterRole" || role.Name != "orka-workspace-substrate" {
			continue
		}
		for _, rule := range role.Rules {
			if slices.Contains(rule.APIGroups, "networking.k8s.io") && slices.Contains(rule.Resources, "networkpolicies") && slices.Contains(rule.Verbs, "list") && len(rule.ResourceNames) == 0 {
				return
			}
		}
		t.Fatal("shipped provider role cannot inventory additive native NetworkPolicies")
	}
	t.Fatal("shipped provider ClusterRole is missing")
}
