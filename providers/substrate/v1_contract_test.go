// Copyright (c) 2026. MIT License - see LICENSE file for details.

package substrate

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestNativeEgressCompilesExplicitHostAllowlist(t *testing.T) {
	egress := &api.RuntimeEgressPolicy{Rules: []api.RuntimeEgressRule{
		{Protocol: api.RuntimeEgressHTTP, Host: "auth-proxy.orka-system.svc.cluster.local", Ports: []int32{8080}},
		{Protocol: api.RuntimeEgressHTTPS, Host: "*.example.com", Ports: []int32{8443, 443}},
		{Protocol: api.RuntimeEgressTLSPassthrough, Host: "git.example.org", Ports: []int32{443}},
	}}
	policy, err := compileEgressPolicy("native", egress)
	if err != nil {
		t.Fatal(err)
	}
	want := &pb.EgressPolicy{Metadata: &pb.ResourceMetadata{Atespace: "native", Name: "default"}, Rules: []*pb.EgressRule{
		{Http: &pb.HttpRule{Hostnames: []string{"auth-proxy.orka-system.svc.cluster.local"}, Ports: &pb.Ports{Numbers: []int32{8080}}}},
		{Https: &pb.HttpsRule{Hostnames: []string{"*.example.com"}, Ports: &pb.Ports{Numbers: []int32{443, 8443}}}},
		{TlsPassthrough: &pb.TlsPassthroughRule{Hostnames: []string{"git.example.org"}, Ports: &pb.Ports{Numbers: []int32{443}}}},
	}}
	if !proto.Equal(policy, want) {
		t.Fatalf("compiled egress differs:\n got %v\nwant %v", policy, want)
	}
	for _, invalid := range []*api.RuntimeEgressPolicy{nil, {Rules: []api.RuntimeEgressRule{{Protocol: api.RuntimeEgressHTTPS, Host: "*", Ports: []int32{443}}}}} {
		if _, err := compileEgressPolicy("native", invalid); err == nil {
			t.Fatal("invalid egress compiled")
		}
	}
	empty, err := compileEgressPolicy("native", &api.RuntimeEgressPolicy{})
	if err != nil || len(empty.Rules) != 0 {
		t.Fatal("empty allowlist did not compile to deny-all")
	}
}

// holdBeforeBoot leaves a created, suspended Actor without executing it.
func holdBeforeBoot(t *testing.T, c client.Client, native *nativeFixture, request sdk.WorkloadRequest) (*corev1.ConfigMap, *journalRecord) {
	t.Helper()
	native.beforeResume = func(context.Context, *pb.Actor) error {
		return status.Error(codes.ResourceExhausted, "hold before boot")
	}
	for range 4 {
		_, _ = driver(c, native).EnsureAllocation(t.Context(), request)
	}
	cm, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if record.Actor.UID == "" || record.BootRequested || native.boots != 0 {
		t.Fatal("fixture did not hold a created Actor before boot")
	}
	return cm, record
}

func TestNativeEgressIsAppliedBeforeResumeAndDeniesAllWhenCompilationFails(t *testing.T) {
	c, native, request := fixture(t, false)
	cm, record := holdBeforeBoot(t, c, native, request)
	key := record.Atespace + "/" + record.Actor.Name
	applied := native.egress[key]
	want, err := compileEgressPolicy(record.Atespace, request.Runtime.Egress)
	if err != nil {
		t.Fatal(err)
	}
	if applied == nil || !proto.Equal(&pb.EgressPolicy{Rules: applied.Rules}, &pb.EgressPolicy{Rules: want.Rules}) || record.EgressPolicy.UID != applied.Metadata.Uid || record.EgressDenyAll {
		t.Fatal("admitted allowlist was not applied and recorded before Resume")
	}
	// Drift is repaired under the observed policy's metadata precondition.
	applied.Rules = append(applied.Rules, &pb.EgressRule{TlsPassthrough: &pb.TlsPassthroughRule{Hostnames: []string{"*"}, Ports: &pb.Ports{All: &pb.AllPorts{}}}})
	if err := driver(c, native).ensureEgressPolicy(t.Context(), cm, record); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(&pb.EgressPolicy{Rules: native.egress[key].Rules}, &pb.EgressPolicy{Rules: want.Rules}) {
		t.Fatal("egress drift survived re-application")
	}
	// A compilation failure installs deny-all and refuses to boot.
	broken := *record
	broken.Request = *record.Request.DeepCopy()
	broken.Request.Runtime.Egress.Rules[0].Host = "*"
	err = driver(c, native).ensureEgressPolicy(t.Context(), cm, &broken)
	if err == nil || !strings.Contains(err.Error(), "denies all egress") {
		t.Fatalf("egress compilation failure was not fail-closed: %v", err)
	}
	if len(native.egress[key].Rules) != 0 || !broken.EgressDenyAll || native.boots != 0 {
		t.Fatal("egress compilation failure did not install deny-all before any boot")
	}
	if err := driver(c, native).verifyEgressPolicy(t.Context(), &broken); err == nil {
		t.Fatal("deny-all fallback was reported as the admitted allowlist")
	}
}

func TestNativeRejectsLegacyNetworkPolicyShapeBeforeEffects(t *testing.T) {
	for _, shape := range []string{"networkPolicy", "missing"} {
		t.Run(shape, func(t *testing.T) {
			c, native, request := fixture(t, false)
			request.Runtime.Egress = nil
			if shape == "networkPolicy" {
				request.Runtime.NetworkPolicy = &networkingv1.NetworkPolicySpec{PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress}}
			}
			assertNativeRequestRejectedBeforeEffects(t, c, native, request, "host-name egress allowlist")
		})
	}
}

func TestNativeTemplatePinsDataOnlyGoldenDisabledAcrossServerDefaults(t *testing.T) {
	c, native, request := fixture(t, true)
	ready(t, c, native, request)
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	compiled := record.TemplateSpec
	config := compiled.GetSnapshotConfig()
	if config.GetPreferredFidelity() != pb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES || config.GetGoldenSnapshotConfig().GetMode() != pb.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED || config.GetStorageLocation() != "gs://native/snapshots" || compiled.GetGoldenEgressPolicy() != nil {
		t.Fatalf("compiled snapshot policy is not data-only with golden snapshots disabled: %v", config)
	}
	stored := native.templates[record.Atespace+"/"+record.Template.Name]
	defaulted := cloneTemplate(compiled)
	applyNativeTemplateDefaults(defaulted)
	if !proto.Equal(defaulted, compiled) {
		t.Fatal("server defaulting changes the compiled template")
	}
	if hash, err := templateDigest(stored); err != nil || hash != record.Template.Digest {
		t.Fatal("stored template digest does not survive server defaulting")
	}
	if limits := compiled.GetResources().GetLimits(); limits.GetCpuMillicores() != 1000 || limits.GetMemoryBytes() != 512<<20 {
		t.Fatalf("operator template size was not inherited: %v", limits)
	}
	var trust *pb.Volume
	for _, volume := range compiled.Volumes {
		if volume.Name == egressTrustVolumeName {
			trust = volume
		}
	}
	if trust == nil || !slices.Equal(trust.GetSystemInfo().GetDataSources()[0].GetTrustBundle().GetNames(), []string{"egress-mitm.ate.dev", "system-roots.ate.dev"}) {
		t.Fatal("egress gateway trust bundle is not projected")
	}
	env := map[string]string{}
	for _, variable := range compiled.Containers[0].Env {
		env[variable.Name] = variable.Value
	}
	for _, name := range egressTrustEnvironment {
		if !strings.HasPrefix(env[name], egressTrustMountPath) {
			t.Fatalf("TLS trust variable %s does not use the projected bundle", name)
		}
	}
	pool := poolObject()
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: record.Placement.Namespace, Name: record.RuntimePool.Name}, pool); err != nil {
		t.Fatal(err)
	}
	limits, _, _ := unstructured.NestedStringMap(pool.Object, "spec", "template", "resources", "limits")
	classes, _, _ := unstructured.NestedSlice(pool.Object, "spec", "sandboxClasses")
	if limits["cpu"] != "1" || limits["memory"] != "576Mi" || len(classes) != 1 || classes[0].(map[string]any)["name"] != "gvisor" || pool.GetAPIVersion() != "ate.dev/v1" {
		t.Fatalf("private worker is not a gVisor pool sized for exactly one Actor plus the gVisor reservation: %v %v", limits, classes)
	}
}

func TestNativeTemplateRequiresExactPositiveActorSize(t *testing.T) {
	for _, change := range []string{"noSize", "subMillicore", "fractionalBytes"} {
		t.Run(change, func(t *testing.T) {
			c, native, request := fixture(t, false)
			container := &request.Runtime.Template.Spec.Containers[0]
			expected := "native Actor requires positive CPU and memory limits"
			switch change {
			case "noSize":
				native.templates["native/base"].Resources = nil
			case "subMillicore":
				container.Resources.Limits = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1500u")}
				expected = "whole number of millicores"
			case "fractionalBytes":
				container.Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1.5")}
				expected = "whole number of bytes"
			}
			request.Resources = *container.Resources.DeepCopy()
			assertNativeRequestRejectedBeforeEffects(t, c, native, request, expected)
		})
	}
}

func TestNativeOperatorWorkerPoolMustBeV1GvisorWithoutProviderOwnedFields(t *testing.T) {
	for _, change := range []string{"legacySandboxClass", "microvm", "twoClasses", "resources"} {
		t.Run(change, func(t *testing.T) {
			c, native, request := fixture(t, false)
			pool := poolObject()
			if err := c.Get(t.Context(), client.ObjectKey{Namespace: "native-workers", Name: "native-workers"}, pool); err != nil {
				t.Fatal(err)
			}
			spec := pool.Object["spec"].(map[string]any)
			expected := "gVisor"
			switch change {
			case "legacySandboxClass":
				spec["sandboxClass"] = "gvisor"
				expected = "removed v1alpha1 sandboxClass"
			case "microvm":
				spec["sandboxClasses"] = []any{map[string]any{"name": "microvm"}}
			case "twoClasses":
				spec["sandboxClasses"] = []any{map[string]any{"name": "gvisor"}, map[string]any{"name": "microvm"}}
			case "resources":
				spec["template"] = map[string]any{"resources": map[string]any{"limits": map[string]any{"memory": "8Gi"}}}
				expected = "provider-owned"
			}
			if err := c.Update(t.Context(), pool); err != nil {
				t.Fatal(err)
			}
			assertNativeRequestRejectedBeforeEffects(t, c, native, request, expected)
		})
	}
}

func TestFreshActorHoldingAnySnapshotIsNeverResumed(t *testing.T) {
	c, native, request := fixture(t, false)
	native.failAfter = "actor"
	if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil {
		t.Fatal("expected lost Actor creation response")
	}
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	// A golden or otherwise seeded snapshot would make Resume restore state
	// instead of cold booting.
	actor := native.actors[record.Atespace+"/"+record.Actor.Name]
	actor.Status.Snapshots = []*pb.Snapshot{{Uuid: "golden", Generation: 1, Owner: pb.SnapshotOwner_SNAPSHOT_OWNER_TAG, Fidelity: pb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, ActorTemplateUid: record.Template.UID}}
	actor.Status.LastAssignedGeneration = 1
	for range 3 {
		if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), "cold boot cannot be proven") {
			t.Fatalf("seeded fresh Actor was not rejected: %v", err)
		}
	}
	if native.boots != 0 {
		t.Fatal("seeded fresh Actor was resumed")
	}
}

func TestPrivateWorkerWithForeignAssignmentIsNeverResumed(t *testing.T) {
	c, native, request := fixture(t, false)
	native.foreignAssignment = true
	for range 4 {
		if _, err := driver(c, native).EnsureAllocation(t.Context(), request); err != nil && !strings.Contains(err.Error(), "another Actor") {
			t.Fatal(err)
		}
	}
	_, record, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if native.boots != 0 || record.BootRequested || record.Worker != nil {
		t.Fatal("shared private worker was selected for Resume")
	}
}

func TestSuspensionRejectsSnapshotsThatAreNotDataOnly(t *testing.T) {
	for _, fidelity := range []pb.SnapshotFidelity{pb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, pb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY} {
		t.Run(fidelity.String(), func(t *testing.T) {
			c, native, request := fixture(t, true)
			first := ready(t, c, native, request)
			native.suspendFidelity = fidelity
			var lastErr error
			for range 4 {
				observed, err := driver(c, native).SuspendInstance(t.Context(), request.Key, first.Identity)
				lastErr = err
				if observed.State == sdk.AllocationStopped || observed.RetainedData != nil {
					t.Fatal("non-data snapshot was retained")
				}
			}
			if lastErr == nil || !strings.Contains(lastErr.Error(), "data-only") || len(native.tags) != 0 || native.suspends != 1 {
				t.Fatalf("non-data snapshot was tagged or the suspension replayed: %v", lastErr)
			}
		})
	}
}

func TestLegacyV010ProviderStateFailsClosedWithClearCondition(t *testing.T) {
	c, native, request := fixture(t, false)
	ready(t, c, native, request)
	cm, _, err := driver(c, native).read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	cm.Data[journalDataKey] = strings.Replace(cm.Data[journalDataKey], `"version":"`+journalVersion+`"`, `"version":"`+legacyJournalVersion+`"`, 1)
	if err := c.Update(t.Context(), cm); err != nil {
		t.Fatal(err)
	}
	if _, _, err := driver(c, native).read(t.Context(), request.Key); !errors.Is(err, errLegacyNativeState) {
		t.Fatalf("legacy journal was not rejected: %v", err)
	}
	boots := native.boots
	r := &ExecutionWorkspaceReconciler{Client: c, Control: native, Config: Config{RouterEndpoint: fixtureRouter}}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: clientKey(request.Key)}); !errors.Is(err, errLegacyNativeState) {
		t.Fatalf("reconcile adopted legacy state: %v", err)
	}
	workspace := bindingWorkspace(t, c, request.Key)
	provisioned := sdk.FindCondition(workspace.Status.Conditions, string(api.ConditionWorkspaceProvisioned))
	if provisioned == nil || provisioned.Reason != legacyNativeStateReason || !strings.Contains(provisioned.Message, "reinstall Agent Substrate v1.0.0") || provisioned.Status != metav1.ConditionUnknown {
		t.Fatalf("legacy state condition is not explicit: %+v", provisioned)
	}
	if sdk.ConditionIsTrue(workspace.Status.Conditions, string(api.ConditionWorkspaceDataPlaneReady)) || native.boots != boots || native.deletes != 0 {
		t.Fatal("legacy state kept readiness or mutated native resources")
	}

	catalog := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: request.Key.Namespace, Name: "substrate-data-legacy", Labels: map[string]string{catalogLabel: legacyCatalogVersion}}, Data: map[string]string{catalogDataKey: "{}"}}
	if err := c.Create(t.Context(), catalog); err != nil {
		t.Fatal(err)
	}
	if _, err := driver(c, native).listCatalogs(t.Context(), request.Key.Namespace); !errors.Is(err, errLegacyNativeState) {
		t.Fatalf("legacy checkpoint catalog was ignored: %v", err)
	}
	if _, _, err := driver(c, native).readCatalog(t.Context(), client.ObjectKeyFromObject(catalog)); !errors.Is(err, errLegacyNativeState) {
		t.Fatalf("legacy checkpoint catalog was read: %v", err)
	}
}
