package workspaceprovider

import (
	"encoding/json"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"os"
	"path/filepath"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
)

func TestClassProfileHashMatchesOrkaBaseline(t *testing.T) {
	t.Parallel()
	path := filepath.Join("..", "testdata", "baseline", "orka-1cd16c88b", "class-profile-hashes.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var captured struct {
		Baseline string `json:"baseline"`
		Fixtures []struct {
			Name     string                                    `json:"name"`
			Class    workspacev1alpha1.ExecutionWorkspaceClass `json:"class"`
			Resolved []json.RawMessage                         `json:"resolved"`
			Hash     string                                    `json:"hash"`
		} `json:"fixtures"`
	}
	if err := json.Unmarshal(data, &captured); err != nil {
		t.Fatal(err)
	}
	if captured.Baseline != "1cd16c88b43410e0ea46463f8a39b473d8e5250e" || len(captured.Fixtures) != 6 {
		t.Fatal("unexpected baseline or missing class-profile fixtures")
	}
	for _, fixture := range captured.Fixtures {
		t.Run(fixture.Name, func(t *testing.T) {
			// Keep the original resolved struct field order. Decoding these
			// into maps would test different bytes from the baseline resolver.
			resolved := make([]any, len(fixture.Resolved))
			for i := range fixture.Resolved {
				resolved[i] = fixture.Resolved[i]
			}
			got, err := ClassProfileHash(fixture.Class.Spec, resolved...)
			if err != nil {
				t.Fatal(err)
			}
			if got != fixture.Hash {
				t.Fatalf("ClassProfileHash = %s, want baseline %s", got, fixture.Hash)
			}
		})
	}
}

func TestParametersProfileHashPinsAPISpecOnly(t *testing.T) {
	parameters := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "fixture.workspace.orka.ai/v1alpha1", "kind": "Profile", "metadata": map[string]any{"name": "profile", "uid": "one"}, "spec": map[string]any{"capacity": "1Gi"}}}
	original, err := ParametersProfileHash(parameters)
	if err != nil {
		t.Fatal(err)
	}
	parameters.Object["status"] = map[string]any{"ready": true}
	parameters.SetResourceVersion("2")
	same, err := ParametersProfileHash(parameters)
	if err != nil || same != original {
		t.Fatal("observed metadata changed parameter content binding")
	}
	parameters.Object["spec"].(map[string]any)["capacity"] = "2Gi"
	changed, err := ParametersProfileHash(parameters)
	if err != nil || changed == original {
		t.Fatal("parameter content drift did not change binding")
	}
}
