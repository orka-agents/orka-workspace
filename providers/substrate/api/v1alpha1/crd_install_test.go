package v1alpha1

import (
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// TestGeneratedCRDsInstallIntoARealAPIServer installs the generated CRDs into
// envtest's kube-apiserver, so CEL rule cost and behaviour are checked by the
// API server itself, not only by the in-process schema validator. It needs
// envtest assets (KUBEBUILDER_ASSETS), which CI provides.
func TestGeneratedCRDsInstallIntoARealAPIServer(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("KUBEBUILDER_ASSETS is required in CI")
		}
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd")}, ErrorIfCRDPathMissing: true}
	config, err := env.Start()
	if err != nil {
		t.Fatalf("the generated CRDs were rejected by the API server: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	c, err := client.New(config, client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	profile := func(name string, shared map[string]any) *unstructured.Unstructured {
		spec := map[string]any{"templateRef": map[string]any{"name": "template"}, "suspend": map[string]any{"mode": "DataOnly"}}
		if shared != nil {
			spec["placement"] = map[string]any{"sharedWorkers": shared}
		}
		object := &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"namespace": "default", "name": name}, "spec": spec}}
		object.SetGroupVersionKind(schema.GroupVersionKind{Group: GroupVersion.Group, Version: GroupVersion.Version, Kind: "SubstrateWorkspaceProfile"})
		return object
	}
	for name, shared := range map[string]map[string]any{
		"private":       nil,
		"default":       {"maxActorsPerWorker": int64(2)},
		"three-hours":   {"maxActorsPerWorker": int64(4), "maxWorkerLifetime": "3h"},
		"seconds":       {"maxActorsPerWorker": int64(4), "maxWorkerLifetime": "10800s"},
		"day":           {"maxActorsPerWorker": int64(16), "maxWorkerLifetime": "24h"},
		"hours-minutes": {"maxActorsPerWorker": int64(2), "maxWorkerLifetime": "3h30m"},
	} {
		if err := c.Create(t.Context(), profile(name, shared)); err != nil {
			t.Errorf("valid profile %s rejected: %v", name, err)
		}
	}
	for name, shared := range map[string]map[string]any{
		"short":              {"maxActorsPerWorker": int64(2), "maxWorkerLifetime": "2h"},
		"long":               {"maxActorsPerWorker": int64(2), "maxWorkerLifetime": "25h"},
		"fractional-seconds": {"maxActorsPerWorker": int64(2), "maxWorkerLifetime": "10800.5s"},
		"fractional-hours":   {"maxActorsPerWorker": int64(2), "maxWorkerLifetime": "4.5h"},
		"sub-second":         {"maxActorsPerWorker": int64(2), "maxWorkerLifetime": "3h0m0.0005s"},
		"milliseconds":       {"maxActorsPerWorker": int64(2), "maxWorkerLifetime": "10800000ms"},
		"oversized":          {"maxActorsPerWorker": int64(2), "maxWorkerLifetime": "000000000000000000000000000000003h"},
		"one-actor":          {"maxActorsPerWorker": int64(1)},
	} {
		if err := c.Create(t.Context(), profile(name, shared)); err == nil {
			t.Errorf("invalid profile %s was admitted", name)
		}
	}
}
