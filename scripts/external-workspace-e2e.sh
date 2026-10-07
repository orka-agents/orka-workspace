#!/usr/bin/env bash
set -Eeuo pipefail

# Uses only the repo-scoped external-workspace kindctl target. The cluster,
# resources, frozen sources, and proof results are preserved for core follow-up.
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
cd "${repo_root}"
kindctl="${KINDCTL_BIN:-kindctl}"
cluster_tag=external-workspace
mode="${1:-provider}"
artifact_dir="${ORKA_E2E_ARTIFACT_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/orka-workspace-e2e.XXXXXX")}"
mkdir -p "${artifact_dir}"
artifact_dir="$(cd "${artifact_dir}" && pwd)"
case "${artifact_dir}" in "${repo_root}"|"${repo_root}/"*)
  printf 'Proof artifacts must be outside the source repository.\n' >&2
  exit 1
esac
run_id="$(date -u +%Y%m%d%H%M%S)"
snapshot="${artifact_dir}/orka-workspace"

k() { "${kindctl}" kubectl --tag "${cluster_tag}" "$@"; }
fail() { printf '%s\n' "$*" >&2; exit 1; }

case "${mode}" in provider|prepare-runtime|core) ;; *) fail "Usage: $0 [provider|prepare-runtime|core]" ;; esac
command -v "${kindctl}" >/dev/null 2>&1 || fail "kindctl executable is unavailable: ${kindctl}. Install kindctl on PATH or set KINDCTL_BIN."
for required in go docker python3 rsync; do
  command -v "${required}" >/dev/null || fail "Required command unavailable: ${required}"
done
if ! "${kindctl}" path --tag "${cluster_tag}" >/dev/null 2>&1; then
  "${kindctl}" create --tag "${cluster_tag}"
fi
k wait --for=condition=Ready nodes --all --timeout=180s
node_name="$(k get nodes -o jsonpath='{.items[0].metadata.name}')"
node_arch="$(k get nodes -o jsonpath='{.items[0].status.nodeInfo.architecture}')"
case "${node_arch}" in amd64|arm64) ;; *) fail "Unsupported node architecture ${node_arch}" ;; esac

# Freeze one source tree before generation, compilation, or installation.
# Concurrent implementation changes remain outside this proof's build.
mkdir -p "${snapshot}"
rsync -a --exclude=.git --exclude=bin --exclude='*.test' "${repo_root}/" "${snapshot}/"
fixture="${snapshot}/hack/external-workspace-e2e"
mkdir -p "${artifact_dir}/images"
printf 'cluster tag: %s\nnode: %s\nartifacts: %s\n' "${cluster_tag}" "${node_name}" "${artifact_dir}"

if [[ "${mode}" == prepare-runtime || "${mode}" == core ]]; then
  [[ "${ORKA_CORE_BUILD_RELEASED:-}" == 1 ]] || fail "Core runtime compilation requires ORKA_CORE_BUILD_RELEASED=1 after the core build is released"
  core_source="${ORKA_CORE_SOURCE:-${repo_root}/../orka.workspace-out-of-tree}"
  [[ -f "${core_source}/cmd/orka-acp-runtime/main.go" ]] || fail "Orka ACP runtime source unavailable: ${core_source}"
  core_snapshot="${artifact_dir}/orka-core"
  mkdir -p "${core_snapshot}"
  rsync -a --exclude=.git --exclude=bin --exclude='*.test' --exclude=.tmp --exclude=node_modules "${core_source}/" "${core_snapshot}/"
  runtime_context="${artifact_dir}/images/acp-runtime"
  mkdir -p "${runtime_context}"
  (cd "${core_snapshot}" && CGO_ENABLED=0 GOOS=linux GOARCH="${node_arch}" go build -buildvcs=false -trimpath -o "${runtime_context}/orka-acp-runtime" ./cmd/orka-acp-runtime)
  (cd "${core_snapshot}" && CGO_ENABLED=0 GOOS=linux GOARCH="${node_arch}" go build -buildvcs=false -trimpath -o "${runtime_context}/orka-acp-exec-helper" ./cmd/orka-acp-exec-helper)
  (cd "${snapshot}" && CGO_ENABLED=0 GOOS=linux GOARCH="${node_arch}" go build -buildvcs=false -trimpath -o "${runtime_context}/acp-agent" ./hack/external-workspace-e2e/acp-agent)
  runtime_image="orka-external-acp-fixture:e2e-${run_id}"
  docker build -f "${fixture}/acp-runtime.Dockerfile" -t "${runtime_image}" "${runtime_context}"
  "${kindctl}" load --tag "${cluster_tag}" "${runtime_image}"
  printf '%s\n' "${runtime_image}" >"${artifact_dir}/runtime-image.txt"
  if [[ "${mode}" == prepare-runtime ]]; then
    printf 'Real harness v2 runtime fixture prepared: %s\nNo RuntimeSession or Task has been submitted.\n' "${runtime_image}"
    exit 0
  fi
  core_context="${artifact_dir}/images/core"
  mkdir -p "${core_context}"
  (cd "${core_snapshot}" && CGO_ENABLED=0 GOOS=linux GOARCH="${node_arch}" go build -buildvcs=false -trimpath -o "${core_context}/manager" ./cmd)
  core_image="orka-external-core:e2e-${run_id}"
  docker build -f "${fixture}/core.Dockerfile" -t "${core_image}" "${core_context}"
  "${kindctl}" load --tag "${cluster_tag}" "${core_image}"
fi

provider_context="${artifact_dir}/images/provider"
listener_context="${artifact_dir}/images/listener"
mkdir -p "${provider_context}" "${listener_context}"
(cd "${snapshot}/providers" && CGO_ENABLED=0 GOOS=linux GOARCH="${node_arch}" go build -buildvcs=false -trimpath -o "${provider_context}/orka-workspace-fake" ./fake/cmd/orka-workspace-fake)
(cd "${snapshot}" && CGO_ENABLED=0 GOOS=linux GOARCH="${node_arch}" go build -buildvcs=false -trimpath -o "${listener_context}/pod-runtime" ./hack/external-workspace-e2e/pod-runtime)
provider_image="orka-workspace-fake:e2e-${run_id}"
listener_image="orka-workspace-listener:e2e-${run_id}"
docker build -f "${fixture}/provider.Dockerfile" -t "${provider_image}" "${provider_context}"
docker build -f "${fixture}/pod-runtime.Dockerfile" -t "${listener_image}" "${listener_context}"
"${kindctl}" load --tag "${cluster_tag}" "${provider_image}"
"${kindctl}" load --tag "${cluster_tag}" "${listener_image}"

# Read containerd's actual imported manifest digest, not a mutable Docker tag
# or the image configuration digest. The Kubernetes request uses this fence.
"${kindctl}" exec --tag "${cluster_tag}" -- docker exec "${node_name}" ctr --namespace=k8s.io images ls >"${artifact_dir}/node-images.txt"
listener_digest="$(python3 - "${artifact_dir}/node-images.txt" "${listener_image}" <<'PY'
import pathlib, sys
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    columns = line.split()
    if columns and columns[0].endswith('/' + sys.argv[2]):
        print(columns[2])
        break
else:
    raise SystemExit('Imported listener image manifest digest unavailable')
PY
)"
[[ "${listener_digest}" == sha256:* ]] || fail "Listener manifest digest is not SHA-256"
listener_pinned="docker.io/library/${listener_image%%:*}@${listener_digest}"
"${kindctl}" exec --tag "${cluster_tag}" -- docker exec "${node_name}" ctr --namespace=k8s.io images tag --force \
  "docker.io/library/${listener_image}" "${listener_pinned}"

k apply --server-side -k "${snapshot}/config"
k wait --for=condition=Established crd --all --timeout=120s
k apply --server-side -f "${snapshot}/providers/fake/config/crd"
k wait --for=condition=Established crd --all --timeout=120s
k apply --server-side --force-conflicts -k "${snapshot}/providers/fake/config"
k set image -n orka-workspace-system deployment/orka-workspace-fake "provider=${provider_image}"
k patch deployment/orka-workspace-fake -n orka-workspace-system --type=strategic -p '{"spec":{"replicas":2,"template":{"spec":{"containers":[{"name":"provider","imagePullPolicy":"Never"}]}}}}'
k apply --server-side -f "${fixture}/rbac.yaml"
k rollout status -n orka-workspace-system deployment/orka-workspace-fake --timeout=180s
"${kindctl}" exec --tag "${cluster_tag}" -- go -C "${snapshot}" run ./hack/external-workspace-e2e/provider-proof --image "${listener_pinned}" --run-id "${run_id}" --report "${artifact_dir}/provider-proof.json"
k get pods -n orka-workspace-system -l app=orka-workspace-fake -o wide
k get leases -n orka-workspace-system orka-workspace-fake.workspace.orka.ai -o jsonpath='{.spec.holderIdentity}{"\n"}'
printf 'Proof report: %s\nCluster remains available under tag %s.\n' "${artifact_dir}/provider-proof.json" "${cluster_tag}"

if [[ "${mode}" == core ]]; then
  command -v openssl >/dev/null || fail "OpenSSL is required for local admission certificates"
  runtime_digest="$(python3 - "${artifact_dir}/node-images.txt" "${runtime_image}" <<'PY'
import pathlib, sys
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    columns = line.split()
    if columns and columns[0].endswith('/' + sys.argv[2]):
        print(columns[2])
        break
else:
    raise SystemExit('Imported runtime image manifest digest unavailable')
PY
)"
  [[ "${runtime_digest}" == sha256:* ]] || fail "Runtime image manifest digest unavailable"
  runtime_pinned="docker.io/library/${runtime_image%%:*}@${runtime_digest}"
  # Orka canonicalizes digest references by removing the tag. Import that
  # exact local containerd alias so IfNotPresent never needs a registry pull.
  "${kindctl}" exec --tag "${cluster_tag}" -- docker exec "${node_name}" ctr --namespace=k8s.io images tag --force \
    "docker.io/library/${runtime_image}" "${runtime_pinned}"
  k apply --server-side -k "${core_snapshot}/config/crd"
  k wait --for=condition=Established crd --all --timeout=120s
  secret_dir="${artifact_dir}/private"
  mkdir -m 700 "${secret_dir}"
  openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=external-workspace-core.orka-system.svc \
    -addext subjectAltName=DNS:external-workspace-core.orka-system.svc,DNS:external-workspace-core.orka-system.svc.cluster.local \
    -keyout "${secret_dir}/tls.key" -out "${secret_dir}/tls.crt" 2>"${secret_dir}/openssl.log"
  openssl rand 32 >"${secret_dir}/key"
  openssl rand -hex 32 >"${secret_dir}/token"
  chmod 600 "${secret_dir}"/*
  k create namespace orka-system --dry-run=client -o yaml | k apply -f -
  k create secret generic external-workspace-core-fixture -n orka-system \
    --from-file="${secret_dir}/key" --from-file="${secret_dir}/token" --dry-run=client -o yaml | k apply -f -
  k create secret tls external-workspace-core-tls -n orka-system \
    --key="${secret_dir}/tls.key" --cert="${secret_dir}/tls.crt" --dry-run=client -o yaml | k apply -f -
  sed 's/name: manager-role$/name: external-workspace-core/' "${core_snapshot}/config/rbac/role.yaml" >"${artifact_dir}/core-role.yaml"
  k apply -f "${artifact_dir}/core-role.yaml"
  python3 "${fixture}/core-install.py" "${core_image}" "${runtime_pinned}" "${secret_dir}/tls.crt" >"${artifact_dir}/core-install.json"
  k apply -f "${artifact_dir}/core-install.json"
  k rollout status deployment/external-workspace-core -n orka-system --timeout=180s
  "${kindctl}" exec --tag "${cluster_tag}" -- go -C "${snapshot}" run ./hack/external-workspace-e2e/core-proof \
    --run-id "${run_id}" --report "${artifact_dir}/core-proof.json"
  printf 'Core RuntimeSession proof report: %s\n' "${artifact_dir}/core-proof.json"
fi
