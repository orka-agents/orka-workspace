// Copyright (c) 2026. MIT License - see LICENSE file for details.

package substrate

import (
	"slices"
	"testing"

	api "github.com/orka-agents/orka-workspace/api/v1alpha1"
	profile "github.com/orka-agents/orka-workspace/providers/substrate/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestProviderAdvertisesNativeProcessOnlyWithPinnedReadyConfiguration(t *testing.T) {
	c, _, _ := fixture(t, false)
	provider := &api.ExecutionWorkspaceProvider{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: "substrate"}, provider); err != nil {
		t.Fatal(err)
	}
	config := &profile.SubstrateProviderConfig{ObjectMeta: metav1.ObjectMeta{Name: "substrate", UID: "config-uid"}}
	if err := c.Create(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	provider.Spec.ParametersRef = api.TypedObjectReference{Group: profile.GroupVersion.Group, Kind: "SubstrateProviderConfig", Name: config.Name}
	if err := c.Update(t.Context(), provider); err != nil {
		t.Fatal(err)
	}
	r := &ProviderReconciler{Client: c, Config: Config{APIEndpoint: "native-control:443", CAFile: "/installation/ca.crt", BearerTokenFile: "/installation/token", ActorDNSSuffix: "actors.local", DirectEgressEnabled: true}}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(provider)}
	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), req.NamespacedName, provider); err != nil {
		t.Fatal(err)
	}
	if provider.Status.ObservedGeneration != provider.Generation || provider.Status.PinnedParametersUID != string(config.UID) || provider.Status.Backend == nil || provider.Status.Backend.Version != UpstreamCommit || !slices.Contains(provider.Status.SupportedFeatures, api.WorkspaceFeatureNativeProcess) || !slices.Contains(provider.Status.SupportedFeatures, api.WorkspaceFeatureACPRuntime) {
		t.Fatal("configured provider did not publish its exact native-process capability")
	}
	// Without the installation's confinement prerequisite, the native-process
	// filesystem capability must not choose a weaker Core workload layout.
	r.Config.DirectEgressEnabled = false
	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), req.NamespacedName, provider); err != nil {
		t.Fatal(err)
	}
	if provider.Status.ObservedGeneration != 0 || len(provider.Status.SupportedFeatures) != 0 {
		t.Fatal("unconfigured provider retained its native-process advertisement")
	}
	r.Config.DirectEgressEnabled = true
	if err := c.Delete(t.Context(), config, client.Preconditions{UID: &config.UID}); err != nil {
		t.Fatal(err)
	}
	config.ResourceVersion = ""
	config.UID = "replacement-config-uid"
	if err := c.Create(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), req.NamespacedName, provider); err != nil {
		t.Fatal(err)
	}
	if provider.Status.PinnedParametersUID != "config-uid" || len(provider.Status.SupportedFeatures) != 0 {
		t.Fatal("same-name configuration replacement adopted or advertised native intent")
	}
}
