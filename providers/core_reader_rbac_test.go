package providers_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

func TestProviderBundlesShipScopedCoreReaderRoles(t *testing.T) {
	const (
		readerFile   = "core-reader-rbac.yaml"
		configLabel  = "workspace.orka.ai/aggregate-to-provider-config-reader"
		profileLabel = "workspace.orka.ai/aggregate-to-parameter-reader"
	)
	for _, provider := range []string{"sandbox", "substrate"} {
		t.Run(provider, func(t *testing.T) {
			directory := filepath.Join(provider, "config")
			data, err := os.ReadFile(filepath.Join(directory, "kustomization.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			var bundle struct {
				Resources []string `json:"resources"`
			}
			if err := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096).Decode(&bundle); err != nil {
				t.Fatal(err)
			}
			if !slices.Contains(bundle.Resources, readerFile) {
				t.Fatal("provider installation does not include Core reader roles")
			}
			data, err = os.ReadFile(filepath.Join(directory, readerFile))
			if err != nil {
				t.Fatal(err)
			}
			wanted := map[string]struct{ label, resource string }{
				"orka-workspace-" + provider + "-provider-config-reader": {configLabel, provider + "providerconfigs"},
				"orka-workspace-" + provider + "-profile-reader":         {profileLabel, provider + "workspaceprofiles"},
			}
			seen := map[string]bool{}
			decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
			for {
				var role rbacv1.ClusterRole
				if err := decoder.Decode(&role); errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				want, found := wanted[role.Name]
				if !found || seen[role.Name] || role.Kind != "ClusterRole" || role.APIVersion != rbacv1.SchemeGroupVersion.String() || role.Namespace != "" || role.AggregationRule != nil {
					t.Fatalf("unexpected or duplicate Core reader role: %s/%s", role.Kind, role.Name)
				}
				seen[role.Name] = true
				if role.Labels[want.label] != "true" || role.Labels[configLabel] != "" && role.Labels[profileLabel] != "" {
					t.Fatalf("reader role %s must contribute only to its intended aggregate", role.Name)
				}
				if len(role.Rules) != 1 {
					t.Fatalf("reader role %s has %d rules, want one", role.Name, len(role.Rules))
				}
				rule := role.Rules[0]
				if !slices.Equal(rule.APIGroups, []string{provider + ".workspace.orka.ai"}) ||
					!slices.Equal(rule.Resources, []string{want.resource}) || len(rule.ResourceNames) != 0 || len(rule.NonResourceURLs) != 0 {
					t.Fatalf("reader role %s grants unexpected resources: %+v", role.Name, rule)
				}
				if len(rule.Verbs) != 3 || !slices.Contains(rule.Verbs, "get") || !slices.Contains(rule.Verbs, "list") || !slices.Contains(rule.Verbs, "watch") {
					t.Fatalf("reader role %s must be strictly read-only: %v", role.Name, rule.Verbs)
				}
			}
			if len(seen) != len(wanted) {
				t.Fatalf("bundle includes %d Core reader roles, want %d", len(seen), len(wanted))
			}
			data, err = os.ReadFile(filepath.Join(directory, "rbac.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			decoder = yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
			for {
				var role rbacv1.ClusterRole
				if err := decoder.Decode(&role); errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				if role.Kind == "ClusterRole" && (role.Labels[configLabel] == "true" || role.Labels[profileLabel] == "true") {
					t.Fatalf("provider control role %s must not contribute its write permissions to Core readers", role.Name)
				}
			}
		})
	}
}
