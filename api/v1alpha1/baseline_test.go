package v1alpha1

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// These digests were captured from Orka commit
// 1cd16c88b43410e0ea46463f8a39b473d8e5250e before extraction.
func TestCRDsMatchOrkaBaseline(t *testing.T) {
	t.Parallel()
	expected := map[string]string{
		"workspace.orka.ai_executionworkspacecheckpoints.yaml": "2acb6a87a4139303876ed3c6fa3457223b5a5017a818ef40dcc2c9338bb1b5c1",
		"workspace.orka.ai_executionworkspaceclasses.yaml":     "33fba2aaeadb729280e4f9db640fbbd94dd11eb4feffb780f30da26ce323a43f",
		"workspace.orka.ai_executionworkspacepools.yaml":       "3cf8b4a10616aca5ce18b2be3e61d55998fb28bd8bab398ded89e2e75a38128c",
		"workspace.orka.ai_executionworkspaceproviders.yaml":   "6daf6cba48b9c1316601db755caad56f38936bdaa1f020610851011d104ec465",
		"workspace.orka.ai_executionworkspaces.yaml":           "a4469095009cac540308611b808f4a908c62cdc66df8b9cf0783d78b6f9e6a7f",
	}
	baseline := filepath.Join("..", "..", "testdata", "baseline", "orka-1cd16c88b", "crds")
	generated := filepath.Join("..", "..", "config", "crd", "bases")
	for _, dir := range []string{baseline, generated} {
		paths, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		if len(paths) != len(expected) {
			t.Fatalf("%s contains %d CRDs, want %d", dir, len(paths), len(expected))
		}
	}
	for name, digest := range expected {
		t.Run(name, func(t *testing.T) {
			want, err := os.ReadFile(filepath.Join(baseline, name))
			if err != nil {
				t.Fatal(err)
			}
			if got := fmt.Sprintf("%x", sha256.Sum256(want)); got != digest {
				t.Fatalf("baseline fixture changed: sha256=%s, want %s", got, digest)
			}
			got, err := os.ReadFile(filepath.Join(generated, name))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatal("generated CRD differs from the Orka baseline")
			}
		})
	}
}

func TestDeepCopyMatchesOrkaBaseline(t *testing.T) {
	t.Parallel()
	baseline, err := os.ReadFile(filepath.Join("..", "..", "testdata", "baseline", "orka-1cd16c88b", "zz_generated.deepcopy.go.txt"))
	if err != nil {
		t.Fatal(err)
	}
	const digest = "6c422e562f26a7dc8eb60ea5cf64fa3e2ac8c8dbf43ad8df8df42a755d0da4dd"
	if got := fmt.Sprintf("%x", sha256.Sum256(baseline)); got != digest {
		t.Fatalf("baseline deepcopy fixture changed: sha256=%s, want %s", got, digest)
	}
	generated, err := os.ReadFile("zz_generated.deepcopy.go")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(generated, baseline) {
		t.Fatal("generated deepcopy differs from the Orka baseline")
	}
}
