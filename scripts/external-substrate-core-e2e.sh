#!/usr/bin/env bash
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
cd "${repo_root}"
kindctl="${KINDCTL_BIN:-kindctl}"
case "${1:-preflight}" in
  preflight|proof) ;;
  *) printf 'Usage: %s [preflight|proof]\n' "$0" >&2; exit 1 ;;
esac
command -v "${kindctl}" >/dev/null 2>&1 || {
  printf 'kindctl executable is unavailable: %s. Install kindctl on PATH or set KINDCTL_BIN.\n' "${kindctl}" >&2
  exit 1
}
exec "${kindctl}" exec --tag external-substrate -- bash hack/external-substrate-e2e/core/run.sh "${1:-preflight}"
