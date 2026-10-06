#!/usr/bin/env bash
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
cd "${repo_root}"
kindctl="${KINDCTL_BIN:-/Users/sozercan/projects/kindctl/bin/kindctl}"
case "${1:-preflight}" in
  preflight|proof) ;;
  *) printf 'Usage: %s [preflight|proof]\n' "$0" >&2; exit 1 ;;
esac
exec "${kindctl}" exec --tag external-substrate -- bash hack/external-substrate-e2e/core/run.sh "${1:-preflight}"
