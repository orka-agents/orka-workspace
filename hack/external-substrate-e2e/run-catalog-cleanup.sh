#!/usr/bin/env bash
# Finalize an exact collected catalog through the real public controller.
set -euo pipefail
namespace="$1"
namespace_uid="$2"
catalog_name="$3"
catalog_uid="$4"
runner_namespace="$5"
case "$(kubectl config current-context)" in
  kind-orka-workspace-*-external-su-*) ;;
  *) printf 'Refusing foreign catalog cleanup cluster\n' >&2; exit 1 ;;
esac
[[ "${namespace}" =~ ^external-substrate-proof-[0-9]{14}$ ]]
[[ "${runner_namespace}" =~ ^external-substrate-proof-[0-9]{14}$ ]]
[[ "${catalog_name}" =~ ^substrate-data-[a-f0-9]{40}$ ]]
test "$(kubectl get namespace "${namespace}" -o jsonpath='{.metadata.uid}')" = "${namespace_uid}"
test "$(kubectl -n "${namespace}" get configmap "${catalog_name}" -o jsonpath='{.metadata.uid}')" = "${catalog_uid}"
run_id="$(date -u +%Y%m%d%H%M%S)"
job_name="native-catalog-cleanup-${run_id}"
run_dir="${PWD}/bin/${namespace}/catalog-cleanup-${run_id}"
mkdir -p "${run_dir}"
chmod 700 "${run_dir}"
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go -C hack/external-substrate-e2e build -p 4 -trimpath -o "${run_dir}/native-data-proof" ./proof
docker build -f hack/external-substrate-e2e/proof.Dockerfile -t "localhost:5002/orka/native-data-proof:catalog-${run_id}" "${run_dir}"
docker push "localhost:5002/orka/native-data-proof:catalog-${run_id}"
image="$(docker inspect -f '{{index .RepoDigests 0}}' "localhost:5002/orka/native-data-proof:catalog-${run_id}")"
kubectl apply -f - <<YAML
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: {name: ${job_name}}
rules:
  - apiGroups: [""]
    resources: [namespaces]
    resourceNames: [${namespace}]
    verbs: [get]
  - apiGroups: [""]
    resources: [configmaps]
    resourceNames: [${catalog_name}]
    verbs: [get, update, delete]
  - apiGroups: [workspace.orka.ai]
    resources: [executionworkspaces]
    resourceNames: [source, imported]
    verbs: [get]
  - apiGroups: [workspace.orka.ai]
    resources: [executionworkspacecheckpoints]
    resourceNames: [independent]
    verbs: [get]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata: {name: ${job_name}}
subjects: [{kind: ServiceAccount, name: native-data-proof, namespace: ${runner_namespace}}]
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: ${job_name}}
YAML
kubectl -n "${runner_namespace}" get job native-data-proof -o json |
  jq --arg name "${job_name}" --arg image "${image}" --arg namespace "${namespace}" --arg namespaceUID "${namespace_uid}" --arg catalog "${catalog_name}" --arg catalogUID "${catalog_uid}" '
    {apiVersion:"batch/v1",kind:"Job",metadata:{name:$name,namespace:.metadata.namespace},spec:{backoffLimit:0,activeDeadlineSeconds:300,template:.spec.template}} |
    .spec.template.metadata = {labels:{"orka.workspace.e2e/cleanup":"catalog"}} |
    .spec.template.spec.containers[0].image = $image |
    .spec.template.spec.containers[0].env += [
      {name:"SUBSTRATE_E2E_CATALOG_NAMESPACE",value:$namespace},
      {name:"SUBSTRATE_E2E_CATALOG_NAME",value:$catalog},
      {name:"SUBSTRATE_E2E_CATALOG_UID",value:$catalogUID},
      {name:"SUBSTRATE_E2E_NAMESPACE_UID",value:$namespaceUID}]' |
  kubectl create -f -
kubectl -n "${runner_namespace}" wait --for=condition=complete "job/${job_name}" --timeout=5m || {
  kubectl -n "${runner_namespace}" logs "job/${job_name}" | tee "${run_dir}/catalog.log"
  exit 1
}
kubectl -n "${runner_namespace}" logs "job/${job_name}" | tee "${run_dir}/catalog.log"
kubectl wait --for=delete "namespace/${namespace}" --timeout=2m
kubectl delete clusterrolebinding "${job_name}" "${namespace}"
kubectl delete clusterrole "${job_name}" "${namespace}"
printf 'Exact catalog and namespace finalization verified: %s\n' "${run_dir}/catalog.log"
