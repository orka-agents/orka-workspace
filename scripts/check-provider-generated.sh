#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../providers"
controller_gen="${1:?absolute controller-gen path is required}"
provider_name="${2:?provider name is required}"
case "$provider_name" in sandbox|substrate) ;; *) echo "unsupported provider API directory" >&2; exit 1 ;; esac
generated_dir="$(mktemp -d)"
trap 'rm -rf "$generated_dir"' EXIT
"$controller_gen" crd:allowDangerousTypes=true "paths=./$provider_name/api/..." "output:crd:artifacts:config=$generated_dir/crds"
"$controller_gen" object:headerFile=../hack/boilerplate.go.txt "paths=./$provider_name/api/..." "output:object:dir=$generated_dir/deepcopy"
diff -ru "$provider_name/config/crd" "$generated_dir/crds"
diff -u "$provider_name/api/v1alpha1/zz_generated.deepcopy.go" "$generated_dir/deepcopy/zz_generated.deepcopy.go"
