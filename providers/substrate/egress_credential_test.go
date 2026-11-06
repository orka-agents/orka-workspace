// Copyright (c) 2026. MIT License - see LICENSE file for details.

package substrate

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func credentialEgress() *api.RuntimeEgressPolicy {
	return &api.RuntimeEgressPolicy{Rules: []api.RuntimeEgressRule{
		{Protocol: api.RuntimeEgressHTTP, Host: "auth-proxy.orka-system.svc.cluster.local", Ports: []int32{8080}, ReplaceHeaders: []api.RuntimeEgressCredentialHeader{
			{Header: "Authorization", Prefix: "Bearer ", SecretKeyRef: api.RuntimeEgressSecretKeyReference{Namespace: "orka-runtimes", Name: "pool-provider-e1-gabc", Key: "token"}},
		}},
		{Protocol: api.RuntimeEgressHTTPS, Host: "api.example.com", Ports: []int32{443}, ReplaceHeaders: []api.RuntimeEgressCredentialHeader{
			{Header: "x-api-key", SecretKeyRef: api.RuntimeEgressSecretKeyReference{Name: "example-api", Key: "api.key"}},
		}},
		{Protocol: api.RuntimeEgressTLSPassthrough, Host: "git.example.org", Ports: []int32{443}},
	}}
}

func credentialConfig() Config {
	return Config{RouterEndpoint: fixtureRouter, EgressCredentialProvider: KubernetesSecretsCredentialProvider, EgressCredentialNamespaces: []string{"orka-runtimes"}}
}

func TestNativeEgressCompilesCredentialHeadersToSecretReferences(t *testing.T) {
	policy, err := compileEgressPolicy("native", "tenant", credentialEgress(), credentialConfig())
	if err != nil {
		t.Fatal(err)
	}
	want := &pb.EgressPolicy{Metadata: &pb.ResourceMetadata{Atespace: "native", Name: "default"}, Rules: []*pb.EgressRule{
		{Http: &pb.HttpRule{Hostnames: []string{"auth-proxy.orka-system.svc.cluster.local"}, Ports: &pb.Ports{Numbers: []int32{8080}}, Effects: &pb.HttpRuleEffects{ReplaceHeaders: []*pb.CredentialHeader{
			{Header: "Authorization", Prefix: "Bearer ", CredentialUri: "ate-secret://k8s.io/default/orka-runtimes/pool-provider-e1-gabc/token"},
		}}}},
		{Https: &pb.HttpsRule{Hostnames: []string{"api.example.com"}, Ports: &pb.Ports{Numbers: []int32{443}}, Effects: &pb.HttpRuleEffects{ReplaceHeaders: []*pb.CredentialHeader{
			{Header: "x-api-key", CredentialUri: "ate-secret://k8s.io/default/tenant/example-api/api.key"},
		}}}},
		{TlsPassthrough: &pb.TlsPassthroughRule{Hostnames: []string{"git.example.org"}, Ports: &pb.Ports{Numbers: []int32{443}}}},
	}}
	if !proto.Equal(policy, want) {
		t.Fatalf("compiled credential egress differs:\n got %v\nwant %v", policy, want)
	}
}

func TestNativeEgressCredentialsFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config Config
		want   string
	}{
		{"no credential provider", Config{}, "not configured"},
		{"unlisted namespace", Config{EgressCredentialProvider: KubernetesSecretsCredentialProvider}, "not permitted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := compileEgressPolicy("native", "tenant", credentialEgress(), tc.config); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("credential egress compiled: %v", err)
			}
		})
	}
	for _, config := range []Config{
		{EgressCredentialProvider: "vault.example.com"},
		{EgressCredentialNamespaces: []string{"orka-runtimes"}},
		{EgressCredentialProvider: KubernetesSecretsCredentialProvider, EgressCredentialNamespaces: []string{"Bad"}},
	} {
		if err := config.ValidateEgressCredentials(); err == nil {
			t.Fatalf("accepted invalid credential configuration %+v", config)
		}
	}
}

func credentialRequest(t *testing.T, request sdk.WorkloadRequest) sdk.WorkloadRequest {
	t.Helper()
	request.Runtime.Egress = credentialEgress()
	request.Runtime.RequiredFeatures = append(request.Runtime.RequiredFeatures, api.WorkspaceFeatureEgressCredentialInjection)
	var err error
	if request.Revision, err = sdk.WorkloadRevision(request); err != nil {
		t.Fatal(err)
	}
	return request
}

func TestNativeRejectsCredentialInjectionWithoutConfiguredProviderBeforeEffects(t *testing.T) {
	c, native, request := fixture(t, false)
	// The default driver declares no gateway credential provider.
	assertNativeRequestRejectedBeforeEffects(t, c, native, credentialRequest(t, request), "not configured")
}

func TestNativeAppliesCredentialEffectsBeforeResume(t *testing.T) {
	c, native, request := fixture(t, false)
	request = credentialRequest(t, request)
	if err := admit(c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	d := New(c, native, credentialConfig())
	d.challengeClient = native
	for range 12 {
		observed, err := d.EnsureAllocation(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == sdk.AllocationReady {
			break
		}
	}
	_, record, err := d.read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	want, err := compileEgressPolicy(record.Atespace, request.Key.Namespace, request.Runtime.Egress, credentialConfig())
	if err != nil {
		t.Fatal(err)
	}
	applied := native.egress[record.Atespace+"/"+record.Actor.Name]
	if applied == nil || !proto.Equal(&pb.EgressPolicy{Rules: applied.Rules}, &pb.EgressPolicy{Rules: want.Rules}) || record.EgressDenyAll || native.boots == 0 {
		t.Fatal("credential effects were not applied before the Actor resumed")
	}
	if err := d.verifyEgressPolicy(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	// Losing the credential provider later fails verification and a
	// re-application installs deny-all rather than a rule without its credential.
	unconfigured := New(c, native, Config{RouterEndpoint: fixtureRouter})
	unconfigured.challengeClient = native
	if err := unconfigured.verifyEgressPolicy(t.Context(), record); err == nil {
		t.Fatal("verification passed without a configured credential provider")
	}
	// Re-ensuring the running allocation after the configuration is withdrawn
	// refuses it and replaces the credential-bearing policy with deny-all.
	if _, err := unconfigured.EnsureAllocation(t.Context(), request); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("withdrawn credential configuration was not refused: %v", err)
	}
	if rules := native.egress[record.Atespace+"/"+record.Actor.Name].GetRules(); len(rules) != 0 {
		t.Fatalf("withdrawn credential configuration left gateway effects applied: %v", rules)
	}
	if _, revoked, err := d.read(t.Context(), request.Key); err != nil || !revoked.EgressDenyAll {
		t.Fatalf("revocation was not journaled: %v", err)
	}
}

// Observation of a ready Actor fails once its credential configuration is
// withdrawn, so the reconciler must revoke on that failure rather than rely on
// EnsureAllocation, which a failed observation never reaches.
func TestReconcilerRevokesWithdrawnCredentialInjectionWhenObservationFails(t *testing.T) {
	c, native, request := fixture(t, false)
	request = credentialRequest(t, request)
	if err := admit(c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	d := New(c, native, credentialConfig())
	d.challengeClient = native
	for range 12 {
		observed, err := d.EnsureAllocation(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == sdk.AllocationReady {
			break
		}
	}
	_, record, err := d.read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	actor := record.Atespace + "/" + record.Actor.Name
	if len(native.egress[actor].GetRules()) == 0 || record.EgressDenyAll {
		t.Fatal("fixture did not apply credential effects")
	}
	workspace := bindingWorkspace(t, c, request.Key)
	withdrawn := &ExecutionWorkspaceReconciler{Client: c, Control: native, Config: Config{RouterEndpoint: fixtureRouter}}
	_, _ = withdrawn.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)})
	if rules := native.egress[actor].GetRules(); len(rules) != 0 {
		t.Fatalf("reconciliation after withdrawal left gateway credential effects: %v", rules)
	}
	if _, revoked, err := d.read(t.Context(), request.Key); err != nil || !revoked.EgressDenyAll {
		t.Fatalf("revocation was not journaled: %v", err)
	}
}

// An Actor journaled before a header became reserved no longer compiles even
// though the provider is still configured; reconciliation must still replace
// its credential-bearing gateway policy with deny-all.
func TestReconcilerRevokesCredentialsInvalidatedByReservedHeaders(t *testing.T) {
	c, native, request := fixture(t, false)
	request = credentialRequest(t, request)
	if err := admit(c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	d := New(c, native, credentialConfig())
	d.challengeClient = native
	for range 12 {
		observed, err := d.EnsureAllocation(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == sdk.AllocationReady {
			break
		}
	}
	cm, record, err := d.read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	actor := record.Atespace + "/" + record.Actor.Name
	if len(native.egress[actor].GetRules()) == 0 {
		t.Fatal("fixture did not apply credential effects")
	}
	record.Request.Runtime.Egress.Rules[1].ReplaceHeaders[0].Header = "X-Forwarded-Port"
	if err := d.save(t.Context(), cm, record); err != nil {
		t.Fatal(err)
	}
	r := &ExecutionWorkspaceReconciler{Client: c, Control: native, Config: credentialConfig()}
	_, _ = r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(bindingWorkspace(t, c, request.Key))})
	if rules := native.egress[actor].GetRules(); len(rules) != 0 {
		t.Fatalf("a now-reserved credential header kept gateway effects applied: %v", rules)
	}
}

// A journal that fails admission while still compiling - here, credential
// rules without the required feature - must be revoked to deny-all, never
// re-applied.
func TestRevocationNeverReappliesCredentialRules(t *testing.T) {
	c, native, request := fixture(t, false)
	request = credentialRequest(t, request)
	if err := admit(c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	d := New(c, native, credentialConfig())
	d.challengeClient = native
	for range 12 {
		observed, err := d.EnsureAllocation(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == sdk.AllocationReady {
			break
		}
	}
	cm, record, err := d.read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	actor := record.Atespace + "/" + record.Actor.Name
	record.Request.Runtime.RequiredFeatures = slices.DeleteFunc(record.Request.Runtime.RequiredFeatures, func(feature api.ExecutionWorkspaceFeature) bool {
		return feature == api.WorkspaceFeatureEgressCredentialInjection
	})
	if _, compileErr := compileEgressPolicy(record.Atespace, request.Key.Namespace, record.Request.Runtime.Egress, credentialConfig()); compileErr != nil {
		t.Fatalf("fixture must still compile: %v", compileErr)
	}
	if err := d.save(t.Context(), cm, record); err != nil {
		t.Fatal(err)
	}
	if err := d.revokeWithdrawnEgressCredentials(t.Context(), request.Key); err == nil {
		t.Fatal("revocation reported success without denying egress")
	}
	if rules := native.egress[actor].GetRules(); len(rules) != 0 {
		t.Fatalf("revocation re-applied credential rules: %v", rules)
	}
	if _, revoked, err := d.readForEgressRevocation(t.Context(), request.Key); err != nil || !revoked.EgressDenyAll {
		t.Fatalf("revocation was not journaled: %v", err)
	}
}

// A running workspace whose Core admission is stale or withdrawn takes the
// reconciler's not-admitted early return. That path authorizes no allocation
// or startup, but withdrawn credential configuration must still replace the
// Actor's credential-bearing gateway policy with deny-all.
func TestReconcilerRevokesWithdrawnCredentialInjectionWithoutAdmission(t *testing.T) {
	c, native, request := fixture(t, false)
	request = credentialRequest(t, request)
	if err := admit(c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	d := New(c, native, credentialConfig())
	d.challengeClient = native
	for range 12 {
		observed, err := d.EnsureAllocation(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == sdk.AllocationReady {
			break
		}
	}
	_, record, err := d.read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	actor := record.Atespace + "/" + record.Actor.Name
	if len(native.egress[actor].GetRules()) == 0 || record.EgressDenyAll {
		t.Fatal("fixture did not apply credential effects")
	}
	workspace := bindingWorkspace(t, c, request.Key)
	if workspace.Spec.DesiredState != api.ExecutionWorkspaceDesiredReady || workspace.Spec.Retirement != nil || workspaceHasMaintenanceIntent(workspace) {
		t.Fatal("fixture must have no maintenance intent")
	}
	sdk.SetCondition(&workspace.Status.Conditions, metav1.Condition{Type: string(api.ConditionWorkspaceAdmitted), Status: metav1.ConditionFalse, Reason: string(api.ReasonProgressing), ObservedGeneration: workspace.Generation})
	if err := c.Status().Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	if workspaceCurrentlyAdmittedByCore(bindingWorkspace(t, c, request.Key)) {
		t.Fatal("fixture must not be admitted")
	}
	before := bindingWorkspace(t, c, request.Key)
	boots := native.boots
	withdrawn := &ExecutionWorkspaceReconciler{Client: c, Control: native, Config: Config{RouterEndpoint: fixtureRouter}}
	if _, err := withdrawn.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("revocation without admission was not reported: %v", err)
	}
	if rules := native.egress[actor].GetRules(); len(rules) != 0 {
		t.Fatalf("not-admitted reconciliation left gateway credential effects: %v", rules)
	}
	if _, revoked, err := d.readForEgressRevocation(t.Context(), request.Key); err != nil || !revoked.EgressDenyAll {
		t.Fatalf("revocation was not journaled: %v", err)
	}
	if native.boots != boots || !reflect.DeepEqual(bindingWorkspace(t, c, request.Key).Status, before.Status) {
		t.Fatal("not-admitted reconciliation started the Actor or published lifecycle status")
	}
	// Once revoked, the not-admitted path is idempotent and quiet.
	if _, err := withdrawn.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err != nil {
		t.Fatalf("repeat revocation failed: %v", err)
	}
}

// readyCredentialAllocation runs a credential-injecting allocation to Ready
// and returns its driver and the Actor's egress policy key.
func readyCredentialAllocation(t *testing.T) (client.Client, *nativeFixture, sdk.WorkloadRequest, *Lifecycle, string) {
	t.Helper()
	c, native, request := fixture(t, false)
	request = credentialRequest(t, request)
	if err := admit(c)(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	d := New(c, native, credentialConfig())
	d.challengeClient = native
	for range 12 {
		observed, err := d.EnsureAllocation(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == sdk.AllocationReady {
			break
		}
	}
	_, record, err := d.read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	actor := record.Atespace + "/" + record.Actor.Name
	if len(native.egress[actor].GetRules()) == 0 || record.EgressDenyAll {
		t.Fatal("fixture did not apply credential effects")
	}
	return c, native, request, d, actor
}

// timeOutBoot records the boot-timeout failure exactly as EnsureAllocation
// does: the Actor keeps running and its operation stays ensure.
func timeOutBoot(t *testing.T, d *Lifecycle, key sdk.AllocationKey) {
	t.Helper()
	cm, record, err := d.read(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.fail(t.Context(), cm, record, "native allocation produced no process evidence within 6m0s"); err != nil {
		t.Fatal(err)
	}
}

// A timed-out Actor stays running with its gateway policy, and observing a
// failed record verifies no egress. Under a Disabled provider - maintenance,
// so neither the not-admitted nor the observation-error hook runs, and the
// failure branch never reaches EnsureAllocation - withdrawn credential
// configuration must still replace the policy with deny-all.
func TestDisabledReconcilerRevokesWithdrawnCredentialsOfTimedOutActor(t *testing.T) {
	c, native, request, d, actor := readyCredentialAllocation(t)
	timeOutBoot(t, d, request.Key)
	workspace := bindingWorkspace(t, c, request.Key)
	provider := &api.ExecutionWorkspaceProvider{}
	if err := c.Get(t.Context(), client.ObjectKey{Name: workspace.Spec.ProviderBinding.Name}, provider); err != nil {
		t.Fatal(err)
	}
	provider.Spec.LifecycleState = api.ExecutionWorkspaceProviderDisabled
	if err := c.Update(t.Context(), provider); err != nil {
		t.Fatal(err)
	}
	withdrawn := &ExecutionWorkspaceReconciler{Client: c, Control: native, Config: Config{RouterEndpoint: fixtureRouter}}
	if _, err := withdrawn.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("revocation of the timed-out Actor was not reported: %v", err)
	}
	if rules := native.egress[actor].GetRules(); len(rules) != 0 {
		t.Fatalf("a timed-out Actor kept gateway credential effects: %v", rules)
	}
	_, record, err := d.readForEgressRevocation(t.Context(), request.Key)
	if err != nil || !record.EgressDenyAll || record.Operation != "ensure" {
		t.Fatalf("revocation was not journaled or changed the failed operation: %v", err)
	}
	if live, err := d.actor(t.Context(), record); err != nil || live == nil {
		t.Fatalf("fixture Actor must still exist: %v", err)
	}
	if got := bindingWorkspace(t, c, request.Key).Status.State; got != api.ExecutionWorkspaceStateFailed {
		t.Fatalf("workspace state = %q, want Failed", got)
	}
	// Once revoked, the failed instance reconciles quietly.
	if _, err := withdrawn.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err != nil {
		t.Fatalf("repeat reconciliation failed: %v", err)
	}
}

// Authorized cleanup of a failed instance proceeds under withdrawn
// credential configuration; the revocation never blocks its stop.
func TestFailedInstanceCleanupProceedsUnderWithdrawnCredentials(t *testing.T) {
	c, native, request, d, _ := readyCredentialAllocation(t)
	timeOutBoot(t, d, request.Key)
	_, record, err := d.read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	workspace := bindingWorkspace(t, c, request.Key)
	workspace.Spec.Retirement = &api.WorkloadRetirement{Sequence: request.Sequence, Identity: record.Observation.Identity, Action: api.WorkloadRetirementStop}
	status := workspace.Status.DeepCopy()
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	workspace.Status = *status
	if err := c.Status().Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	withdrawn := &ExecutionWorkspaceReconciler{Client: c, Control: native, Config: Config{RouterEndpoint: fixtureRouter}}
	for range 6 {
		if _, err := withdrawn.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err != nil {
			t.Fatalf("withdrawn credentials blocked authorized cleanup: %v", err)
		}
	}
	if _, record, err = d.read(t.Context(), request.Key); err != nil || record.Operation != "stop" || record.Observation.State != sdk.AllocationStopped {
		t.Fatalf("authorized cleanup did not stop the failed instance: %v, %v", err, record.Observation.State)
	}
}

// A healthy instance's authorized Stop can be accepted while its Actor keeps
// running. A retiring record's observation verifies no egress, so credential
// configuration withdrawn after the Stop was issued must still replace the
// policy with deny-all while that Actor exists, without re-applying compiled
// rules, and the Stop must still complete.
func TestStalledRetirementRevokesWithdrawnCredentials(t *testing.T) {
	c, native, request, d, actor := readyCredentialAllocation(t)
	_, record, err := d.read(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	workspace := bindingWorkspace(t, c, request.Key)
	workspace.Spec.Retirement = &api.WorkloadRetirement{Sequence: request.Sequence, Identity: record.Observation.Identity, Action: api.WorkloadRetirementStop}
	status := workspace.Status.DeepCopy()
	if err := c.Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	workspace.Status = *status
	if err := c.Status().Update(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	native.stallDelete = true
	configured := &ExecutionWorkspaceReconciler{Client: c, Control: native, Config: credentialConfig()}
	for range 3 {
		_, _ = configured.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)})
	}
	if _, record, err = d.read(t.Context(), request.Key); err != nil || record.Operation != "stop" || len(native.egress[actor].GetRules()) == 0 {
		t.Fatalf("fixture did not leave a stalled stop with credential effects: %v, %+v", err, record)
	}

	withdrawn := &ExecutionWorkspaceReconciler{Client: c, Control: native, Config: Config{RouterEndpoint: fixtureRouter}}
	for range 3 {
		_, _ = withdrawn.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)})
	}
	if rules := native.egress[actor].GetRules(); len(rules) != 0 {
		t.Fatalf("an Actor whose termination stalled kept gateway credential effects: %v", rules)
	}
	_, record, err = d.readForEgressRevocation(t.Context(), request.Key)
	if err != nil || !record.EgressDenyAll || record.Operation != "stop" {
		t.Fatalf("revocation of the retiring Actor was not journaled: %v, %+v", err, record)
	}
	if live, err := d.actor(t.Context(), record); err != nil || live == nil {
		t.Fatalf("fixture Actor must still exist: %v", err)
	}
	// Once the stall clears, the stop completes under deny-all.
	native.stallDelete = false
	for range 6 {
		if _, err := withdrawn.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err != nil {
			t.Fatalf("cleanup after a revoked stall failed: %v", err)
		}
	}
	if _, record, err = d.read(t.Context(), request.Key); err != nil || record.Observation.State != sdk.AllocationStopped {
		t.Fatalf("stalled stop never completed: %v, %v", err, record.Observation.State)
	}
}
