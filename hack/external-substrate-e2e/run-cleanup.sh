#!/usr/bin/env bash
# Called only through kindctl exec --tag external-substrate from this repo.
set -euo pipefail
namespace="$1"
expected_namespace_uid="$2"
case "$(kubectl config current-context)" in
  kind-orka-workspace-*-external-su-*) ;;
  *) printf 'Refusing foreign cleanup cluster\n' >&2; exit 1 ;;
esac
[[ "${namespace}" =~ ^external-substrate-proof-[0-9]{14}$ ]]
test "$(kubectl get namespace "${namespace}" -o jsonpath='{.metadata.uid}')" = "${expected_namespace_uid}"
kubectl -n "${namespace}" get job native-data-proof -o json |
  jq -e 'any(.status.conditions[]?; (.type == "Failed" or .type == "Complete") and .status == "True")' >/dev/null
workspace_uids="$(kubectl -n "${namespace}" get executionworkspaces -o json | jq -ce '.items | map({key:.metadata.name,value:.metadata.uid}) | from_entries')"
runtime_image="$(kubectl -n "${namespace}" get job native-data-proof -o json | jq -er '.spec.template.spec.containers[0].env[] | select(.name == "RUNTIME_IMAGE") | .value')"
run_id="$(date -u +%Y%m%d%H%M%S)"
job_name="native-data-cleanup-${run_id}"
run_dir="${PWD}/bin/${namespace}/cleanup-${run_id}"
mkdir -p "${run_dir}"
chmod 700 "${run_dir}"
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go -C hack/external-substrate-e2e build -p 4 -trimpath -o "${run_dir}/native-data-proof" ./proof
docker build -f hack/external-substrate-e2e/proof.Dockerfile -t "localhost:5002/orka/native-data-proof:cleanup-${run_id}" "${run_dir}"
docker push "localhost:5002/orka/native-data-proof:cleanup-${run_id}"
proof_image="$(docker inspect -f '{{index .RepoDigests 0}}' "localhost:5002/orka/native-data-proof:cleanup-${run_id}")"
kubectl apply -f - <<YAML
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: {name: ${job_name}}
rules:
  - apiGroups: [""]
    resources: [namespaces]
    resourceNames: [${namespace}]
    verbs: [get]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata: {name: ${job_name}}
subjects: [{kind: ServiceAccount, name: native-data-proof, namespace: ${namespace}}]
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: ${job_name}}
YAML
test "$(kubectl get namespace "${namespace}" -o jsonpath='{.metadata.uid}')" = "${expected_namespace_uid}"
kubectl -n "${namespace}" get job native-data-proof -o json |
  jq --arg name "${job_name}" --arg image "${proof_image}" --arg namespaceUID "${expected_namespace_uid}" --arg workspaces "${workspace_uids}" '
    {apiVersion:"batch/v1",kind:"Job",metadata:{name:$name,namespace:.metadata.namespace},spec:{
      backoffLimit:0,activeDeadlineSeconds:1260,template:.spec.template}} |
    .spec.template.metadata = {labels:{"orka.workspace.e2e/cleanup":"native-data"}} |
    .spec.template.spec.containers[0].image = $image |
    .spec.template.spec.containers[0].env += [
      {name:"SUBSTRATE_E2E_CLEANUP",value:"1"},
      {name:"SUBSTRATE_E2E_NAMESPACE_UID",value:$namespaceUID},
      {name:"SUBSTRATE_E2E_WORKSPACE_UIDS",value:$workspaces}]' |
  kubectl create -f -
start="$(date +%s)"
while true; do
  job="$(kubectl -n "${namespace}" --request-timeout=15s get job "${job_name}" -o json)"
  if jq -e 'any(.status.conditions[]?; (.type == "Failed" or .type == "FailureTarget") and .status == "True")' <<<"${job}" >/dev/null; then
    kubectl -n "${namespace}" logs "job/${job_name}" | tee "${run_dir}/cleanup.log"
    exit 1
  fi
  if jq -e 'any(.status.conditions[]?; .type == "Complete" and .status == "True")' <<<"${job}" >/dev/null; then
    kubectl -n "${namespace}" logs "job/${job_name}" | tee "${run_dir}/cleanup.log"
    break
  fi
  (( $(date +%s) - start <= 1260 )) || exit 1
  sleep 2
done
python3 - "${run_dir}/cleanup.log" "${run_dir}/report.json" "${expected_namespace_uid}" <<'PY'
import json, pathlib, sys
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    if line.startswith('NATIVE_CLEANUP_PROOF '):
        value = json.loads(line[len('NATIVE_CLEANUP_PROOF '):])
        if value['namespaceUID'] != sys.argv[3]:
            raise SystemExit('Cleanup report namespace lifetime changed')
        pathlib.Path(sys.argv[2]).write_text(json.dumps(value, indent=2) + '\n')
        break
else:
    raise SystemExit('Cleanup Job omitted its exact completion report')
PY
# Native absence is now proven. Kubernetes deletion uses exact UID preconditions
# and foreground collection; no provider finalizer is bypassed.
python3 - "${run_dir}/delete-options.json" "${expected_namespace_uid}" <<'PY'
import json, pathlib, sys
pathlib.Path(sys.argv[1]).write_text(json.dumps({
    'apiVersion':'v1', 'kind':'DeleteOptions',
    'preconditions':{'uid':sys.argv[2]}, 'propagationPolicy':'Foreground'}))
PY
kubectl delete --raw "/api/v1/namespaces/${namespace}" -f "${run_dir}/delete-options.json" >/dev/null
kubectl wait --for=delete "namespace/${namespace}" --timeout=2m
kubectl delete clusterrolebinding "${job_name}" "${namespace}"
kubectl delete clusterrole "${job_name}" "${namespace}"
printf 'Verified failed-run cleanup report: %s\n' "${run_dir}/report.json"
