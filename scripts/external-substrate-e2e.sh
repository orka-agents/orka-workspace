#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"
kindctl="${KINDCTL_BIN:-kindctl}"
command -v "${kindctl}" >/dev/null 2>&1 || {
  printf 'kindctl executable is unavailable: %s. Install kindctl on PATH or set KINDCTL_BIN.\n' "${kindctl}" >&2
  exit 1
}
tag=external-substrate
upstream=https://github.com/agent-substrate/substrate.git
pin=fa6d949685a6318940a9a0195c867c864009b820
source_dir="${SUBSTRATE_SOURCE_DIR:-/tmp/orka-external-substrate-native-pin-${pin}}"

case "${1:-preflight}" in
  preflight)
    for tool in docker kind go git jq kubectl openssl python3 curl rg; do
      command -v "${tool}"
    done
    go version
    python3 hack/external-substrate-e2e/read-only-preflight.py docker info --format '{{.Architecture}} {{.OSType}} CPUs={{.NCPU}} memory={{.MemTotal}}'
    df -h /tmp "${repo_root}"
    test "$(git -C "${source_dir}" remote get-url origin)" = "${upstream}"
    test "$(git -C "${source_dir}" rev-parse HEAD)" = "${pin}"
    test -z "$(git -C "${source_dir}" status --porcelain --untracked-files=all)"
    python3 hack/external-substrate-e2e/read-only-preflight.py "${kindctl}" kubectl --tag "${tag}" --request-timeout=15s get nodes -o wide
    python3 hack/external-substrate-e2e/read-only-preflight.py "${kindctl}" kubectl --tag "${tag}" --request-timeout=15s api-resources --api-group certificates.k8s.io
    ;;
  install)
    test "$(git -C "${source_dir}" remote get-url origin)" = "${upstream}"
    test "$(git -C "${source_dir}" rev-parse HEAD)" = "${pin}"
    test -z "$(git -C "${source_dir}" status --porcelain --untracked-files=all)"
    # Only kindctl creates the cluster. The upstream cluster creator can delete
    # an existing cluster and replace its shared registry, so it is not invoked.
    if "${kindctl}" ctx --tag "${tag}" >/dev/null 2>&1; then
      test "${SUBSTRATE_REUSE_CLUSTER:-0}" = 1 || {
        printf 'Dedicated cluster already exists; inspect it, then set SUBSTRATE_REUSE_CLUSTER=1.\n' >&2
        exit 1
      }
    else
      "${kindctl}" create --tag "${tag}" --config hack/external-substrate-e2e/cluster.yaml --k8s-version v1.37.0
    fi
    "${kindctl}" exec --tag "${tag}" -- bash hack/external-substrate-e2e/install-backend.sh "${source_dir}"
    ;;
  proof)
    test "$(git -C "${source_dir}" remote get-url origin)" = "${upstream}"
    test "$(git -C "${source_dir}" rev-parse HEAD)" = "${pin}"
    test -z "$(git -C "${source_dir}" status --porcelain --untracked-files=all)"
    "${kindctl}" exec --tag "${tag}" -- bash hack/external-substrate-e2e/run-proof.sh "${source_dir}"
    ;;
  cleanup)
    test $# = 3 || { printf 'Usage: %s cleanup <failed-proof-namespace> <exact-namespace-UID>\n' "$0" >&2; exit 2; }
    "${kindctl}" exec --tag "${tag}" -- bash hack/external-substrate-e2e/run-cleanup.sh "$2" "$3"
    ;;
  *)
    printf 'Usage: %s [preflight|install|proof|cleanup <namespace> <UID>]\n' "$0" >&2
    exit 2
    ;;
esac
