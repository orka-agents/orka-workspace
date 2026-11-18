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
# The measurement client is built from this checkout, like the cleanup Job,
# so a harness change needs no new proof run. It keeps the proof identity.
arch="$(kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.architecture}')"
case "${arch}" in amd64|arm64) ;; *) printf 'Unsupported node architecture: %s\n' "${arch}" >&2; exit 1 ;; esac
mkdir -p "${run_dir}/image"
CGO_ENABLED=0 GOOS=linux GOARCH="${arch}" go -C "${fixture_dir}/.." build -p 4 -trimpath -o "${run_dir}/image/native-data-proof" ./core
docker build -q -f "${fixture_dir}/proof.Dockerfile" -t "localhost:5002/orka/native-core-proof:latency-${run_id}" "${run_dir}/image" >/dev/null
docker push -q "localhost:5002/orka/native-core-proof:latency-${run_id}" >/dev/null
client_image="$(docker inspect -f '{{index .RepoDigests 0}}' "localhost:5002/orka/native-core-proof:latency-${run_id}")"
rm -rf "${run_dir}/image"
# Later runs against the same proof use fresh Session and Task names.
sequence="$(kubectl -n "${client_namespace}" get jobs -l orka.workspace.e2e/latency=native-core -o name | wc -l)"
job_name="native-core-latency-${run_id}"
since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf 'Native Core latency artifacts: %s\n' "${run_dir}"

# Worker Pods (and their supervisor output) are deleted by every suspension,
# so they are followed live. Only the time, message and timing fields of the
# worker and supervisor boot milestones are kept, plus each Pod's own
# creation, scheduling and container start times.
worker_messages='ateworker booting|Registered worker|Reported worker sandbox runtimes|Readiness check failing|Readiness check recovered|About to run runsc create|About to run runsc start|awaiting controller credential bootstrap|Wakeup probe reached 200|credential bootstrap complete|ACP supervisor listening|ACP runtime session created|Received signal; beginning graceful shutdown|Container exited successfully'
follow_workers() {
  local seen=" " started=" "
  while [[ ! -e "${run_dir}/.done" ]]; do
    # Private, spare and shared worker Pods all belong to provider WorkerPools
    # named ate-*; shared and unclaimed spare Pods carry no allocation label.
    pods="$(kubectl -n "${namespace}" get pods -o json 2>/dev/null | jq '{items: [.items[] | select(.metadata.name | startswith("ate-"))]}' 2>/dev/null || true)"
    for pod in $(jq -r '.items[]?.metadata.name' <<<"${pods}" 2>/dev/null); do
      if [[ "${started}" != *" ${pod} "* ]] && jq -e --arg pod "${pod}" '.items[] | select(.metadata.name == $pod) | .status.containerStatuses[0].state.running.startedAt // empty' <<<"${pods}" >/dev/null 2>&1; then
        started="${started}${pod} "
        jq -c --arg pod "${pod}" '.items[] | select(.metadata.name == $pod) | {pod: .metadata.name, uid: .metadata.uid, created: .metadata.creationTimestamp,
          scheduled: ([.status.conditions[]? | select(.type == "PodScheduled") | .lastTransitionTime][0]), started: .status.containerStatuses[0].state.running.startedAt}' \
          <<<"${pods}" >>"${run_dir}/workers/pods.jsonl"
      fi
      [[ "${seen}" == *" ${pod} "* ]] && continue
      seen="${seen}${pod} "
      (kubectl -n "${namespace}" logs -f --pod-running-timeout=5m "pod/${pod}" 2>/dev/null |
        jq -R -c --unbuffered --arg pod "${pod}" --arg pattern "^(${worker_messages})$" 'fromjson? | select(type == "object" and ((.msg // "") | test($pattern))) |
          {pod: $pod, time, msg, runtimeSessionID, nativeRestore, totalMs, stagesMs, adapterMs, actorUID: .labels["ate.actor.uid"]?} | with_entries(select(.value != null))' \
        >"${run_dir}/workers/${pod}.log" || true) &
    done
    sleep 1
  done
}
follow_workers &
follower=$!
# The host's load average, which skews every phase on a shared machine.
(while [[ ! -e "${run_dir}/.done" ]]; do printf '%s %s\n' "$(date -u +%H:%M:%S)" "$(cut -d' ' -f1-3 /proc/loadavg)" >>"${run_dir}/loadavg.log"; sleep 30; done) &
trap 'touch "${run_dir}/.done"; kill "${follower}" 2>/dev/null || true; jobs -p | xargs -r kill 2>/dev/null || true' EXIT

jq --arg name "${job_name}" --arg repeats "${LATENCY_REPEATS:-3}" --arg window "${LATENCY_WINDOW:-2m}" \
  --arg scenarios "${LATENCY_SCENARIOS:-detach,warm}" --arg sequence "${sequence}" --arg image "${client_image}" '
  {apiVersion:"batch/v1",kind:"Job",metadata:{name:$name,namespace:.metadata.namespace,labels:{"orka.workspace.e2e/latency":"native-core"}},spec:{
    backoffLimit:0,activeDeadlineSeconds:5400,template:.spec.template}} |
  .spec.template.metadata = {labels:{"orka.workspace.e2e/latency":"native-core"}} |
  .spec.template.spec.containers[0].image = $image |
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
grep -E '^native Core' "${run_dir}/latency.log" || true
touch "${run_dir}/.done"
sleep 2
# Timing lines only; everything else stays in the cluster.
kubectl -n "${namespace}" logs deployment/external-substrate-core --since-time="${since}" |
  grep -E 'ACP workspace runtime (started|stopped)|ACP turn timings|WarmDetachWindow|warm-detach' >"${run_dir}/core-timings.log" || true
# Every provider replica: only the elected leader reconciles and logs.
kubectl -n "${provider_namespace}" logs -l app=orka-workspace-substrate --tail=-1 --max-log-requests=10 --since-time="${since}" |
  grep -E 'Substrate runtime (ready|stopped)|[Ss]pare|[Ss]hared (worker|pool)|quarantin' >"${run_dir}/provider-timings.log" || true
kubectl -n "${namespace}" get events -o json |
  jq -c '[.items[] | select(.reason | test("WarmDetach|WarmWindow|Suspend|Retention"; "i")) | {reason, message, object: .involvedObject.name, first: .firstTimestamp, last: .lastTimestamp, count}]' \
  >"${run_dir}/events.json" || true
python3 "${fixture_dir}/latency-report.py" "${run_dir}" || status=1
exit "${status}"
