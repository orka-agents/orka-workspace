#!/usr/bin/env bash
set -Eeuo pipefail

# Adds only the upstream Sandbox backend and its standalone persistence fixture.
# The existing fake provider/core proof and shared ownership policy remain intact.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"
kindctl="${KINDCTL_BIN:-kindctl}"
command -v "${kindctl}" >/dev/null 2>&1 || {
  printf 'kindctl executable is unavailable: %s. Install kindctl on PATH or set KINDCTL_BIN.\n' "${kindctl}" >&2
  exit 1
}
cluster_tag=external-workspace
artifact_dir="${ORKA_SANDBOX_E2E_ARTIFACT_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/orka-sandbox-e2e.XXXXXX")}"
mkdir -p "${artifact_dir}"
artifact_dir="$(cd "${artifact_dir}" && pwd)"
case "${artifact_dir}" in "${repo_root}"|"${repo_root}/"*) printf 'Artifacts must be outside the source repository.\n' >&2; exit 1 ;; esac
run_id="$(date -u +%Y%m%d%H%M%S)"
printf 'Sandbox proof artifacts: %s\n' "${artifact_dir}"
k() { "${kindctl}" kubectl --tag "${cluster_tag}" "$@"; }
"${kindctl}" path --tag "${cluster_tag}" >/dev/null
k wait --for=condition=Ready nodes --all --timeout=180s
node_name="$(k get nodes -o jsonpath='{.items[0].metadata.name}')"
node_arch="$(k get nodes -o jsonpath='{.items[0].status.nodeInfo.architecture}')"
k get storageclass standard -o json >"${artifact_dir}/storage-class.json"
python3 - "${artifact_dir}/storage-class.json" <<'PY'
import json, sys
sc = json.load(open(sys.argv[1]))
assert sc.get('reclaimPolicy') == 'Delete', 'standard StorageClass must reclaim by Delete'
assert sc.get('provisioner') not in ('', None, 'kubernetes.io/no-provisioner'), 'dynamic provisioning required'
PY

# Reuse the canonical pinned upstream installer without changing Orka flags.
installer="${ORKA_AGENT_SANDBOX_INSTALLER:-${repo_root}/../orka.workspace-external-providers/hack/demos/cluster/install-agent-sandbox.sh}"
test -f "${installer}"
"${kindctl}" exec --tag "${cluster_tag}" -- env AGENTIC=0 ORKA_AGENT_SANDBOX_VERSION=v1.0.3 \
  ORKA_CONTROLLER_DEPLOYMENT=external-sandbox-no-core DEMO_NAMESPACE=external-sandbox-proof bash "${installer}"
k rollout status deployment/agent-sandbox-controller -n agent-sandbox-system --timeout=180s
k get deployment/agent-sandbox-controller -n agent-sandbox-system -o json >"${artifact_dir}/upstream-controller.json"
python3 - "${artifact_dir}/upstream-controller.json" <<'PY'
import json, sys
deployment = json.load(open(sys.argv[1]))
images = [c['image'] for c in deployment['spec']['template']['spec']['containers']]
assert images and all(':v1.0.3' in image for image in images), images
PY

snapshot="${artifact_dir}/orka-workspace"
mkdir -p "${snapshot}" "${artifact_dir}/images/provider"
rsync -a --exclude=.git --exclude=bin --exclude='*.test' "${repo_root}/" "${snapshot}/"
(cd "${snapshot}/providers" && CGO_ENABLED=0 GOOS=linux GOARCH="${node_arch}" go build -buildvcs=false -trimpath -o "${artifact_dir}/images/provider/orka-workspace-sandbox" ./sandbox/cmd/orka-workspace-sandbox)
provider_image="orka-workspace-sandbox:e2e-${run_id}"
runtime_image="orka-sandbox-persistence:e2e-${run_id}"
docker build -f "${snapshot}/hack/external-sandbox-e2e/provider.Dockerfile" -t "${provider_image}" "${artifact_dir}/images/provider"
docker build -f "${snapshot}/hack/external-sandbox-e2e/runtime.Dockerfile" -t "${runtime_image}" "${snapshot}/hack/external-sandbox-e2e"
"${kindctl}" load --tag "${cluster_tag}" "${provider_image}"
"${kindctl}" load --tag "${cluster_tag}" "${runtime_image}"
"${kindctl}" exec --tag "${cluster_tag}" -- docker exec "${node_name}" ctr --namespace=k8s.io images ls >"${artifact_dir}/node-images.txt"
runtime_digest="$(python3 - "${artifact_dir}/node-images.txt" "${runtime_image}" <<'PY'
import pathlib, sys
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    fields = line.split()
    if fields and fields[0].endswith('/' + sys.argv[2]):
        print(fields[2]); break
else:
    raise SystemExit('Imported runtime manifest digest unavailable')
PY
)"
runtime_pinned="docker.io/library/${runtime_image}@${runtime_digest}"
# CRI resolves tag@digest through the repository@digest alias. kind load imports
# the tag, so register the same manifest without depending on a remote registry.
"${kindctl}" exec --tag "${cluster_tag}" -- docker exec "${node_name}" ctr --namespace=k8s.io images tag \
  "docker.io/library/${runtime_image}" "docker.io/library/orka-sandbox-persistence@${runtime_digest}"

# Install only this provider's API/configuration. Shared CRDs and policies are
# managed by the concurrent core proof, so this script never reapplies them.
k apply --server-side -f "${snapshot}/providers/sandbox/config/crd"
k wait --for=condition=Established crd/sandboxproviderconfigs.sandbox.workspace.orka.ai crd/sandboxworkspaceprofiles.sandbox.workspace.orka.ai --timeout=120s
k apply --server-side --force-conflicts -k "${snapshot}/providers/sandbox/config"
k set image deployment/orka-workspace-sandbox -n orka-workspace-system "provider=${provider_image}"
k patch deployment/orka-workspace-sandbox -n orka-workspace-system --type=strategic -p '{"spec":{"template":{"spec":{"containers":[{"name":"provider","imagePullPolicy":"Never"}]}}}}'
k rollout status deployment/orka-workspace-sandbox -n orka-workspace-system --timeout=180s

# The standalone fixture supplies admission through its own namespace-scoped
# identity; it never changes the installed core's authority or watch namespace.
k apply -f "${snapshot}/hack/external-sandbox-e2e/fixture-rbac.yaml"

"${kindctl}" exec --tag "${cluster_tag}" -- env KINDCTL_BIN="${kindctl}" ORKA_SANDBOX_E2E_REPO_ROOT="${repo_root}" go -C "${snapshot}" run ./hack/external-sandbox-e2e/proof \
  --image "${runtime_pinned}" --run-id "${run_id}" --report "${artifact_dir}/sandbox-proof.json"
k delete -f "${snapshot}/hack/external-sandbox-e2e/fixture-rbac.yaml"
printf 'Sandbox persistence proof: %s\nCluster and proof resources remain available.\n' "${artifact_dir}/sandbox-proof.json"
