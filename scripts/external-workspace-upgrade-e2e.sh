#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
cd "${repo_root}"

if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
  exec python3 hack/external-workspace-e2e/upgrade-proof/proof.py --help
fi
[[ "${ORKA_UPGRADE_PROOF_RELEASED:-}" == 1 ]] || {
  printf 'Set ORKA_UPGRADE_PROOF_RELEASED=1 after the final core image and frozen source are released.\n' >&2
  exit 1
}
exec python3 hack/external-workspace-e2e/upgrade-proof/proof.py "$@"
