#!/usr/bin/env bash
# Called only through kindctl exec --tag external-substrate from the shared repo.
# Retires a finished native Core proof (core/run.sh) so another run can start.
# Core and the provider keep running until they have released every Task,
# workspace, checkpoint and provider-finalized journal or catalog in the run
# namespace. Only then are the proof's own native objects, the run namespace
# and, last, the provider removed. No finalizer is removed by hand: a release
# that does not finish leaves everything in place for inspection. Once the run
# namespace is gone, a rerun resumes the remaining idempotent teardown.
set -euo pipefail
case "$(kubectl config current-context)" in
  kind-orka-workspace-*-external-su-*) ;;
  *) printf 'Refusing foreign native Core cleanup cluster\n' >&2; exit 1 ;;
esac
test -n "${KUBECONFIG:-}"
namespace=external-substrate-core
client_namespace=external-substrate-e2e
provider_namespace=orka-workspace-system
# Every finalizer the Substrate provider adds (journal protection, checkpoint
# data, reference and export) is prefixed with its controller name.
provider_finalizer=v1.substrate.workspace.orka.ai/
proof_job_json() {
  kubectl -n "${client_namespace}" get job "${namespace}" -o json --ignore-not-found
}
require_finished() {
  jq -e 'any(.status.conditions[]?; (.type == "Failed" or .type == "Complete") and .status == "True")' <<<"$1" >/dev/null || {
    printf 'The Core proof Job has not finished; refusing to retire a running proof.\n' >&2
    exit 1
  }
}
# Idempotent removal of everything outside the run namespace, the provider
# last. The credential provider's policy goes back to granting nothing, so a
# later Atespace of the same name holds no grant until its own run makes one.
teardown() {
  kubectl delete validatingwebhookconfiguration external-substrate-core --ignore-not-found
  kubectl delete namespace "${provider_namespace}" --ignore-not-found --wait=true --timeout=5m
  for name in external-substrate-core external-substrate-core-fixture external-substrate-core-proof orka-workspace-substrate; do
    kubectl delete clusterrolebinding "${name}" --ignore-not-found
    kubectl delete clusterrole "${name}" --ignore-not-found
  done
  printf 'policies: []\n' | kubectl -n ate-system create configmap k8s-credential-provider-namespace-policy --dry-run=client -o yaml \
    --from-file=namespace-policy.yaml=/dev/stdin | kubectl apply -f -
  kubectl -n ate-system rollout restart deployment/k8s-credential-provider
  kubectl -n ate-system rollout status deployment/k8s-credential-provider --timeout=180s
  kubectl -n "${client_namespace}" delete job "${namespace}" --ignore-not-found
  kubectl -n "${client_namespace}" delete job -l orka.workspace.e2e/cleanup=native-core --ignore-not-found
}

job="$(proof_job_json)"
if ! kubectl get namespace "${namespace}" >/dev/null 2>&1; then
  # A previous cleanup already proved release and deleted the run namespace.
  if [[ -n "${job}" ]]; then require_finished "${job}"; fi
  printf 'Run namespace %s is already gone; resuming the remaining teardown.\n' "${namespace}"
  teardown
  printf 'Native Core proof teardown complete.\n'
  exit 0
fi
test -n "${job}" || { printf 'The Core proof Job is missing; inspect %s by hand.\n' "${namespace}" >&2; exit 1; }
require_finished "${job}"
# Retire only the namespace lifetime that this proof Job ran against.
namespace_uid="$(jq -r '.metadata.annotations["orka.workspace.e2e/namespace-uid"] // empty' <<<"${job}")"
[[ -n "${namespace_uid}" && "$(kubectl get namespace "${namespace}" -o jsonpath='{.metadata.uid}')" == "${namespace_uid}" ]] || {
  printf 'Namespace %s is not the lifetime the proof Job recorded; inspect it by hand.\n' "${namespace}" >&2
  exit 1
}
# Core and the provider retire their own objects, so both must still serve.
kubectl -n "${namespace}" rollout status deployment/external-substrate-core --timeout=120s
kubectl -n "${provider_namespace}" rollout status deployment/orka-workspace-substrate --timeout=120s
run_id="$(date -u +%Y%m%d%H%M%S)"
run_dir="${PWD}/bin/${namespace}-cleanup-${run_id}"
mkdir -p "${run_dir}/image"
chmod 700 "${run_dir}"
printf 'Native Core proof cleanup artifacts: %s\n' "${run_dir}"

# Tasks first, so Core settles and retires their RuntimePools and workspaces,
# then whatever a failed run left behind, through the same controllers.
for resource in tasks.core.orka.ai executionworkspacecheckpoints.workspace.orka.ai executionworkspaces.workspace.orka.ai \
  runtimepools.core.orka.ai agents.core.orka.ai executionworkspaceclasses.workspace.orka.ai \
  substrateworkspaceprofiles.substrate.workspace.orka.ai; do
  kubectl -n "${namespace}" delete "${resource}" --all --wait=true --timeout=300s
done
# The provider releases its journals after verified physical cleanup and
# collects checkpoint catalogs once no owner remains. Deleting it earlier
# strands those finalizers and the namespace never finishes terminating.
start="$(date +%s)"
while true; do
  held="$(kubectl -n "${namespace}" --request-timeout=15s get configmaps,executionworkspaces.workspace.orka.ai,executionworkspacecheckpoints.workspace.orka.ai -o json |
    jq -r --arg prefix "${provider_finalizer}" '.items[] | select(any(.metadata.finalizers[]?; startswith($prefix))) | "\(.kind)/\(.metadata.name)"')"
  [[ -z "${held}" ]] && break
  if (( $(date +%s) - start >= 600 )); then
    printf 'The provider has not released:\n%s\nCore, the provider and the namespace are retained for inspection.\n' "${held}" >&2
    exit 1
  fi
  sleep 5
done
printf 'The provider released every journal and catalog in %s.\n' "${namespace}"

# The proof's own native objects: the infrastructure template and Atespace.
# The Job runs as the proof's bootstrap-owner identity and refuses to delete
# any provider-owned Actor, Data Tag or derived template that remains.
arch="$(kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.architecture}')"
case "${arch}" in amd64|arm64) ;; *) printf 'Unsupported node architecture: %s\n' "${arch}" >&2; exit 1 ;; esac
CGO_ENABLED=0 GOOS=linux GOARCH="${arch}" go -C hack/external-substrate-e2e build -p 4 -trimpath -o "${run_dir}/image/native-data-proof" ./core
docker build -f hack/external-substrate-e2e/core/proof.Dockerfile -t "localhost:5002/orka/native-core-proof:cleanup-${run_id}" "${run_dir}/image"
docker push "localhost:5002/orka/native-core-proof:cleanup-${run_id}"
cleanup_image="$(docker inspect -f '{{index .RepoDigests 0}}' "localhost:5002/orka/native-core-proof:cleanup-${run_id}")"
job_name="native-core-cleanup-${run_id}"
test "$(kubectl get namespace "${namespace}" -o jsonpath='{.metadata.uid}')" = "${namespace_uid}"
jq --arg name "${job_name}" --arg image "${cleanup_image}" '
  {apiVersion:"batch/v1",kind:"Job",metadata:{name:$name,namespace:.metadata.namespace,labels:{"orka.workspace.e2e/cleanup":"native-core"}},spec:{
    backoffLimit:0,activeDeadlineSeconds:300,template:.spec.template}} |
  .spec.template.metadata = {labels:{"orka.workspace.e2e/cleanup":"native-core"}} |
  .spec.template.spec.containers[0].image = $image |
  .spec.template.spec.containers[0].env += [{name:"CORE_PROOF_CLEANUP",value:"1"}]' <<<"${job}" |
  kubectl create -f -
start="$(date +%s)"
while true; do
  cleanup_job="$(kubectl -n "${client_namespace}" --request-timeout=15s get job "${job_name}" -o json)"
  if jq -e 'any(.status.conditions[]?; (.type == "Failed" or .type == "FailureTarget") and .status == "True")' <<<"${cleanup_job}" >/dev/null; then
    kubectl -n "${client_namespace}" logs "job/${job_name}" | tee "${run_dir}/cleanup.log"
    exit 1
  fi
  if jq -e 'any(.status.conditions[]?; .type == "Complete" and .status == "True")' <<<"${cleanup_job}" >/dev/null; then
    kubectl -n "${client_namespace}" logs "job/${job_name}" | tee "${run_dir}/cleanup.log"
    break
  fi
  (( $(date +%s) - start <= 300 )) || exit 1
  sleep 2
done
python3 - "${run_dir}/cleanup.log" "${run_dir}/report.json" <<'PY'
import json, pathlib, sys
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    if line.startswith('NATIVE_CORE_PROOF '):
        pathlib.Path(sys.argv[2]).write_text(json.dumps(json.loads(line[len('NATIVE_CORE_PROOF '):]), indent=2) + '\n')
        break
else:
    raise SystemExit('Native Core cleanup Job omitted its completion report')
PY

# Kubernetes teardown: Core's admission first, then the run namespace by exact
# UID, and the provider only after that namespace is gone. The shared CRDs and
# provider registration stay; the next run applies them again.
test "$(kubectl get namespace "${namespace}" -o jsonpath='{.metadata.uid}')" = "${namespace_uid}"
kubectl delete validatingwebhookconfiguration external-substrate-core --ignore-not-found
python3 - "${run_dir}/delete-options.json" "${namespace_uid}" <<'PY'
import json, pathlib, sys
pathlib.Path(sys.argv[1]).write_text(json.dumps({
    'apiVersion': 'v1', 'kind': 'DeleteOptions',
    'preconditions': {'uid': sys.argv[2]}, 'propagationPolicy': 'Foreground'}))
PY
kubectl delete --raw "/api/v1/namespaces/${namespace}" -f "${run_dir}/delete-options.json" >/dev/null
kubectl wait --for=delete "namespace/${namespace}" --timeout=5m
teardown
printf 'Native Core proof retired; cleanup report: %s\n' "${run_dir}/report.json"
