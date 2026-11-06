// Copyright (c) 2026. MIT License - see LICENSE file for details.

package substrate

import (
	"strings"
	"testing"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	sdk "github.com/orka-agents/orka-workspace/sdk"
	"google.golang.org/protobuf/proto"
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
