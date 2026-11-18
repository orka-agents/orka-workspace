#!/usr/bin/env bash
# Called only through kindctl exec --tag external-substrate from the shared repo
# (scripts/external-substrate-core-e2e.sh latency). Measures session-reused,
# DataOnly, Suspend-on-detach workspace latency against the actual Core and
# deployed provider of a passed Core proof (core/run.sh), before its cleanup.
# The measurement Job reuses the proof image and identity; it creates only
# public profiles, classes and Tasks in the run namespace, which core/cleanup.sh
# retires with the rest of the run. Only timing log lines are kept: the full
# Core, provider and worker logs never leave the cluster.
set -euo pipefail
case "$(kubectl config current-context)" in
  kind-orka-workspace-*-external-su-*) ;;
  *) printf 'Refusing foreign native Core latency cluster\n' >&2; exit 1 ;;
esac
test -n "${KUBECONFIG:-}"
namespace=external-substrate-core
client_namespace=external-substrate-e2e
provider_namespace=orka-workspace-system
fixture_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
job="$(kubectl -n "${client_namespace}" get job "${namespace}" -o json)"
jq -e 'any(.status.conditions[]?; .type == "Complete" and .status == "True")' <<<"${job}" >/dev/null || {
  printf 'Latency runs need a passed, not yet retired Core proof (core/run.sh).\n' >&2
  exit 1
}
namespace_uid="$(jq -r '.metadata.annotations["orka.workspace.e2e/namespace-uid"] // empty' <<<"${job}")"
[[ -n "${namespace_uid}" && "$(kubectl get namespace "${namespace}" -o jsonpath='{.metadata.uid}')" == "${namespace_uid}" ]] || {
  printf 'Namespace %s is not the lifetime the proof Job recorded.\n' "${namespace}" >&2
  exit 1
}
if kubectl -n "${client_namespace}" get jobs -l orka.workspace.e2e/latency=native-core -o json |
  jq -e 'any(.items[]; all(.status.conditions[]?; .status != "True" or (.type != "Complete" and .type != "Failed")))' >/dev/null; then
  printf 'Another latency Job is still running.\n' >&2
  exit 1
fi
kubectl -n "${namespace}" rollout status deployment/external-substrate-core --timeout=60s
kubectl -n "${provider_namespace}" rollout status deployment/orka-workspace-substrate --timeout=60s

run_id="$(date -u +%Y%m%d%H%M%S)"
run_dir="${PWD}/bin/${namespace}-latency-${run_id}"
mkdir -p "${run_dir}/workers"
chmod 700 "${run_dir}"
# Later runs against the same proof use fresh Session and Task names.
sequence="$(kubectl -n "${client_namespace}" get jobs -l orka.workspace.e2e/latency=native-core -o name | wc -l)"
job_name="native-core-latency-${run_id}"
since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf 'Native Core latency artifacts: %s\n' "${run_dir}"

# Worker Pods (and their supervisor output) are deleted by every suspension,
# so their timing lines are followed live. Only ACP supervisor timing lines
# are kept.
follow_workers() {
  local seen=" "
  while [[ ! -e "${run_dir}/.done" ]]; do
    for pod in $(kubectl -n "${namespace}" get pods -l substrate.workspace.orka.ai/allocation-id -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null); do
      [[ "${seen}" == *" ${pod} "* ]] && continue
      seen="${seen}${pod} "
      (kubectl -n "${namespace}" logs -f --all-containers --prefix --pod-running-timeout=5m "pod/${pod}" 2>/dev/null |
        grep --line-buffered -E 'ACP runtime session created|ACP runtime session start|runtime session timings|supervisor (boot|ready|started)' \
          >"${run_dir}/workers/${pod}.log" || true) &
    done
    sleep 1
  done
}
follow_workers &
follower=$!
trap 'touch "${run_dir}/.done"; kill "${follower}" 2>/dev/null || true; jobs -p | xargs -r kill 2>/dev/null || true' EXIT

jq --arg name "${job_name}" --arg repeats "${LATENCY_REPEATS:-3}" --arg window "${LATENCY_WINDOW:-2m}" \
  --arg scenarios "${LATENCY_SCENARIOS:-detach,warm}" --arg sequence "${sequence}" '
  {apiVersion:"batch/v1",kind:"Job",metadata:{name:$name,namespace:.metadata.namespace,labels:{"orka.workspace.e2e/latency":"native-core"}},spec:{
    backoffLimit:0,activeDeadlineSeconds:5400,template:.spec.template}} |
  .spec.template.metadata = {labels:{"orka.workspace.e2e/latency":"native-core"}} |
  .spec.template.spec.containers[0].env += [{name:"LATENCY_MEASURE",value:"1"},{name:"LATENCY_REPEATS",value:$repeats},
    {name:"LATENCY_WINDOW",value:$window},{name:"LATENCY_SCENARIOS",value:$scenarios},{name:"LATENCY_RUN_ID",value:$sequence}]' <<<"${job}" |
  kubectl create -f -
start="$(date +%s)"
status=0
while true; do
  current="$(kubectl -n "${client_namespace}" --request-timeout=15s get job "${job_name}" -o json)" || { sleep 2; continue; }
  if jq -e 'any(.status.conditions[]?; (.type == "Failed" or .type == "FailureTarget") and .status == "True")' <<<"${current}" >/dev/null; then
    status=1
    break
  fi
  if jq -e 'any(.status.conditions[]?; .type == "Complete" and .status == "True")' <<<"${current}" >/dev/null; then
    break
  fi
  (( $(date +%s) - start < 5520 )) || { status=1; break; }
  sleep 5
done
kubectl -n "${client_namespace}" logs "job/${job_name}" >"${run_dir}/latency.log" || true
grep -E '^(PASS|LATENCY_EVENT|NATIVE_CORE_LATENCY|native Core)' "${run_dir}/latency.log" | grep -v '^LATENCY_EVENT' || true
touch "${run_dir}/.done"
sleep 2
# Timing lines only; everything else stays in the cluster.
kubectl -n "${namespace}" logs deployment/external-substrate-core --since-time="${since}" |
  grep -E 'ACP workspace runtime (started|stopped)|ACP turn timings|WarmDetachWindow|warm-detach' >"${run_dir}/core-timings.log" || true
kubectl -n "${provider_namespace}" logs deployment/orka-workspace-substrate --since-time="${since}" |
  grep -E 'Substrate runtime (ready|stopped)' >"${run_dir}/provider-timings.log" || true
kubectl -n "${namespace}" get events -o json |
  jq -c '[.items[] | select(.reason | test("WarmDetach|WarmWindow|Suspend|Retention"; "i")) | {reason, message, object: .involvedObject.name, first: .firstTimestamp, last: .lastTimestamp, count}]' \
  >"${run_dir}/events.json" || true
python3 "${fixture_dir}/latency-report.py" "${run_dir}" || status=1
exit "${status}"
