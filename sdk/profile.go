package workspaceprovider

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"slices"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
)

// ClassProfileHash returns the deterministic SHA-256 digest pinned into a
// concrete workspace class binding. Resolved values should include immutable
// identity and functional provider-profile or pool data, but never status or
// credentials.
func ClassProfileHash(
	spec workspacev1alpha1.ExecutionWorkspaceClassSpec,
	resolved ...any,
) (string, error) {
	canonical := *spec.DeepCopy()
	slices.Sort(canonical.RequiredFeatures)
	slices.Sort(canonical.AllowedReuseScopes)
	slices.Sort(canonical.Lifecycle.AllowedOnDetach)
	data, err := json.Marshal(struct {
		Spec     workspacev1alpha1.ExecutionWorkspaceClassSpec `json:"spec"`
		Resolved []any                                         `json:"resolved,omitempty"`
	}{Spec: canonical, Resolved: resolved})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// ParametersProfileHash freezes the API representation of provider-owned
// parameters. Callers read the object from the API server after defaulting and
// pruning. UID and generation are pinned separately in ParametersBinding; status
// and metadata do not affect this content digest. This does not change class hashes.
func ParametersProfileHash(parameters *unstructured.Unstructured) (string, error) {
	if parameters == nil || parameters.GetAPIVersion() == "" || parameters.GetKind() == "" {
		return "", fmt.Errorf("parameter profile requires API version and kind")
	}
	data, err := json.Marshal(struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Spec       any    `json:"spec,omitempty"`
	}{APIVersion: parameters.GetAPIVersion(), Kind: parameters.GetKind(), Spec: parameters.Object["spec"]})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
