package v1alpha1

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"reflect"
	"slices"
	"strings"

	"os"
	"path/filepath"
	"sigs.k8s.io/yaml"
	"testing"
)

// These digests were captured from Orka commit
// 1cd16c88b43410e0ea46463f8a39b473d8e5250e before extraction.
func TestCRDsPreserveOrkaBaseline(t *testing.T) {
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
			if name != "workspace.orka.ai_executionworkspaces.yaml" && name != "workspace.orka.ai_executionworkspaceproviders.yaml" {
				if !bytes.Equal(got, want) {
					t.Fatal("unchanged CRD differs from the Orka baseline")
				}
				return
			}
			var before, after map[string]any
			if err := yaml.Unmarshal(want, &before); err != nil {
				t.Fatal(err)
			}
			if err := yaml.Unmarshal(got, &after); err != nil {
				t.Fatal(err)
			}
			original := schemaProperties(t, before)
			current := schemaProperties(t, after)
			// Phase 2 adds the handoff and adapter identity without rewriting
			// any previously stored field, required property or validation.
			spec := current["spec"].(map[string]any)
			props := spec["properties"].(map[string]any)
			delete(props, "workload")
			delete(props, "retirement")
			delete(props, "serviceAccountRef")
			status := current["status"].(map[string]any)["properties"].(map[string]any)
			delete(status, "allocation")
			rules := spec["x-kubernetes-validations"].([]any)
			preserved := rules[:0]
			for _, rule := range rules {
				expression := rule.(map[string]any)["rule"].(string)
				if !strings.Contains(expression, ".workload") && !strings.Contains(expression, ".retirement") && !strings.Contains(expression, ".serviceAccountRef") {
					preserved = append(preserved, rule)
				}
			}
			spec["x-kubernetes-validations"] = preserved
			if !reflect.DeepEqual(original, current) || !reflect.DeepEqual(before, after) {
				t.Fatal("Phase 2 changed a baseline schema field outside the additive handoff")
			}
		})
	}
}

func TestDeepCopyPreservesOrkaBaseline(t *testing.T) {
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
	previous := deepcopyFunctions(t, baseline, false)
	current := deepcopyFunctions(t, generated, true)
	for name, body := range previous {
		if current[name] != body {
			t.Fatalf("legacy deepcopy behavior changed in %s", name)
		}
	}
}

func schemaProperties(t *testing.T, crd map[string]any) map[string]any {
	t.Helper()
	version := crd["spec"].(map[string]any)["versions"].([]any)[0].(map[string]any)
	return version["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any)["properties"].(map[string]any)
}

func deepcopyFunctions(t *testing.T, source []byte, stripAdditions bool) map[string]string {
	t.Helper()
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "deepcopy.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	functions := map[string]string{}
	additions := map[string][]string{
		"ExecutionWorkspaceSpec":         {"Workload", "Retirement"},
		"ExecutionWorkspaceStatus":       {"Allocation"},
		"ExecutionWorkspaceProviderSpec": {"ServiceAccountRef"},
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil {
			continue
		}
		receiver := fn.Recv.List[0].Type.(*ast.StarExpr).X.(*ast.Ident).Name
		if stripAdditions && fn.Name.Name == "DeepCopyInto" {
			kept := fn.Body.List[:0]
			for _, statement := range fn.Body.List {
				conditional, ok := statement.(*ast.IfStmt)
				if ok {
					comparison, ok := conditional.Cond.(*ast.BinaryExpr)
					if ok {
						field, ok := comparison.X.(*ast.SelectorExpr)
						if ok && slices.Contains(additions[receiver], field.Sel.Name) {
							continue
						}
					}
				}
				kept = append(kept, statement)
			}
			fn.Body.List = kept
		}
		var buf bytes.Buffer
		if err := format.Node(&buf, token.NewFileSet(), fn); err != nil {
			t.Fatal(err)
		}
		functions[receiver+"."+fn.Name.Name] = buf.String()
	}
	return functions
}
