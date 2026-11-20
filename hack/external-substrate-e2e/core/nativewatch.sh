#!/usr/bin/env bash
# Starts a read-only native watcher Job (NATIVE_WATCH=1) under the proof
# identity and streams its JSON lines to $1. Stop it by deleting the Job.
set -euo pipefail
out="${1:?output file}"
namespace=external-substrate-core
client_namespace=external-substrate-e2e
fixture_dir="$(cd "$(dirname "$0")/.." && pwd)"
job="$(kubectl -n "${client_namespace}" get job "${namespace}" -o json)"
arch="$(kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.architecture}')"
run_id="$(date -u +%Y%m%d%H%M%S)"
tmp="$(mktemp -d)"
CGO_ENABLED=0 GOOS=linux GOARCH="${arch}" go -C "${fixture_dir}" build -p 4 -trimpath -o "${tmp}/native-data-proof" ./core
docker build -q -f "${fixture_dir}/proof.Dockerfile" -t "localhost:5002/orka/native-core-proof:watch-${run_id}" "${tmp}" >/dev/null
docker push -q "localhost:5002/orka/native-core-proof:watch-${run_id}" >/dev/null
image="$(docker inspect -f '{{index .RepoDigests 0}}' "localhost:5002/orka/native-core-proof:watch-${run_id}")"
rm -rf "${tmp}"
name="native-core-watch-${run_id}"
jq --arg name "${name}" --arg image "${image}" --arg duration "${NATIVE_WATCH_DURATION:-3h}" '
  {apiVersion:"batch/v1",kind:"Job",metadata:{name:$name,namespace:.metadata.namespace,labels:{"orka.workspace.e2e/watch":"native-core"}},spec:{
    backoffLimit:0,activeDeadlineSeconds:14400,template:.spec.template}} |
  .spec.template.metadata = {labels:{"orka.workspace.e2e/watch":"native-core"}} |
  .spec.template.spec.containers[0].image = $image |
  .spec.template.spec.containers[0].env += [{name:"NATIVE_WATCH",value:"1"},{name:"NATIVE_WATCH_DURATION",value:$duration}]' <<<"${job}" | kubectl create -f - >/dev/null
kubectl -n "${client_namespace}" wait --for=condition=Ready pod -l job-name="${name}" --timeout=120s >/dev/null
echo "${name} ${image}"
nohup kubectl -n "${client_namespace}" logs -f "job/${name}" >"${out}" 2>&1 &
