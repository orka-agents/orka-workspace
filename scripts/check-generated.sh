#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
controller_gen="${1:?controller-gen path is required}"
generated_dir="$(mktemp -d)"
trap 'rm -rf "$generated_dir"' EXIT
"$controller_gen" crd:allowDangerousTypes=true,generateEmbeddedObjectMeta=true paths=./api/... "output:crd:artifacts:config=$generated_dir/crds"
"$controller_gen" object:headerFile=hack/boilerplate.go.txt paths=./api/... "output:object:dir=$generated_dir/deepcopy"
diff -ru config/crd/bases "$generated_dir/crds"
diff -u api/v1alpha1/zz_generated.deepcopy.go "$generated_dir/deepcopy/zz_generated.deepcopy.go"
