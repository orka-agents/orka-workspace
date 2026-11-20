#!/usr/bin/env bash
# Run the kagent latency baseline on the kindctl --tag kagent-baseline cluster
# (see README.md). Usage: run.sh [N]. Writes bin/kagent-baseline-<UTC>/.
set -euo pipefail
cd "$(dirname "$0")/../.."
kindctl="${KINDCTL_BIN:-kindctl}"
export KUBECONFIG="$("${kindctl}" path --tag kagent-baseline)"
case "$(kubectl config current-context)" in
  kind-orka-workspace-*-kagent-baseline) ;;
  *) printf 'Refusing an unexpected cluster context\n' >&2; exit 1 ;;
esac
cli="${KAGENT_CLI:-bin/kagent/kagent}"
run="bin/kagent-baseline-$(date -u +%Y%m%d%H%M%S)"
mkdir -p "${run}"
port="${KAGENT_API_PORT:-18083}"
kubectl -n kagent port-forward svc/kagent-controller "${port}:8083" >"${run}/port-forward.log" 2>&1 &
forward=$!
(while kill -0 $$ 2>/dev/null; do printf '%s %s\n' "$(date -u +%H:%M:%S)" "$(cut -d' ' -f1-3 /proc/loadavg)" >>"${run}/loadavg.log"; sleep 30; done) &
loads=$!
trap 'kill "${forward}" "${loads}" 2>/dev/null || true' EXIT
for _ in $(seq 1 30); do curl -fsS -o /dev/null "http://localhost:${port}/health" 2>/dev/null && break; sleep 1; done
XDG_CONFIG_HOME="${PWD}/bin/kagent/xdg" python3 hack/kagent-baseline/measure.py "${cli}" "http://localhost:${port}" "${run}" "${1:-5}"
printf 'kagent baseline results: %s\n' "${run}"
