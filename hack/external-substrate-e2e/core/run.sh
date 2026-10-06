#!/usr/bin/env bash
# Called only through kindctl exec --tag external-substrate from the shared repo.
set -euo pipefail
mode="$1"
case "$(kubectl config current-context)" in
  kind-orka-workspace-*-external-su-*) ;;
  *) printf 'Refusing foreign native Core proof cluster\n' >&2; exit 1 ;;
esac
test -n "${KUBECONFIG:-}"
test "$(kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.architecture}')" = arm64
node_cpu="$(kubectl get nodes -o jsonpath='{.items[0].status.allocatable.cpu}')"
node_memory="$(kubectl get nodes -o jsonpath='{.items[0].status.allocatable.memory}')"
[[ "${node_cpu}" =~ ^[0-9]+$ ]] && (( node_cpu >= 4 ))
[[ "${node_memory}" =~ ^([0-9]+)Ki$ ]] && (( BASH_REMATCH[1] >= 8 * 1024 * 1024 ))
kubectl -n ate-system get deployments ate-api-server ate-controller atenet-router -o json |
  jq -e 'all(.items[]; (.status.availableReplicas // 0) >= 1)' >/dev/null
# The isolated fixture enables direct egress explicitly. Refuse a backend still
# routing runtime traffic through its privileged egress gateway. kind proves
# policy identity and ordering; packet enforcement needs a compatible CNI.
kubectl -n ate-system get deployment ate-api-server -o json |
  jq -e '[.spec.template.spec.containers[] | select(.name == "ate-api-server") | .args[] | select(startswith("--egress-gateway-address="))] == ["--egress-gateway-address="]' >/dev/null
registry=orka-external-substrate-registry
test "$(docker inspect -f '{{index .Config.Labels "orka.workspace.e2e"}}' "${registry}")" = external-substrate
registry_ip="$(docker inspect -f '{{with index .NetworkSettings.Networks "kind"}}{{.IPAddress}}{{end}}' "${registry}")"
test -n "${registry_ip}"
printf 'Isolated pinned native installation and arm64 registry preflight passed.\n'
if [[ "${mode}" == preflight ]]; then exit 0; fi

# Compilation and installation are a separate release point from preparing this
# harness. A failed or uncertain accepted Task is retained and never replayed.
test "${ORKA_CORE_BUILD_RELEASED:-}" = 1
core_source="${ORKA_CORE_SOURCE:-${PWD}/../orka.workspace-external-providers}"
test -f "${core_source}/cmd/orka-acp-runtime/main.go"
test -f "${core_source}/cmd/main.go"
test "$(kubectl get deployments -A -l app=external-substrate-core -o json | jq '.items | length')" = 0
if kubectl -n orka-workspace-system get deployment/orka-workspace-substrate >/dev/null 2>&1; then
  printf 'Existing native provider deployment requires explicit review before another run.\n' >&2
  exit 1
fi
run_id="$(date -u +%Y%m%d%H%M%S)"
namespace="external-substrate-core-${run_id}"
run_dir="${PWD}/bin/${namespace}"
mkdir -p "${run_dir}"
chmod 700 "${run_dir}"
shared_snapshot="${run_dir}/orka-workspace"
core_snapshot="${run_dir}/orka"
mkdir -p "${shared_snapshot}" "${core_snapshot}"
rsync -a --exclude=.git --exclude=bin --exclude='*.test' "${PWD}/" "${shared_snapshot}/"
rsync -a --exclude=.git --exclude=bin --exclude='*.test' --exclude=.tmp --exclude=node_modules "${core_source}/" "${core_snapshot}/"
fixture="${shared_snapshot}/hack/external-substrate-e2e/core"
python3 - "${core_source}" "${PWD}" "${core_snapshot}" "${shared_snapshot}" "${run_dir}/source-pins.json" <<'PY'
import hashlib, json, pathlib, subprocess, sys
pins={}
for label, source, snapshot in [('core',sys.argv[1],sys.argv[3]),('shared',sys.argv[2],sys.argv[4])]:
    root=pathlib.Path(snapshot)
    files=subprocess.check_output(['rg','--files','-g','*.go','-g','go.mod','-g','go.sum'],cwd=root,text=True).splitlines()
    digest=hashlib.sha256()
    for name in sorted(files):
        digest.update(name.encode()+b'\0'+(root/name).read_bytes()+b'\0')
    pins[label]={'gitCommit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip(),
                 'goSourceSHA256':digest.hexdigest()}
pins['nativeCommit']='fa6d949685a6318940a9a0195c867c864009b820'
fixture_root=pathlib.Path(sys.argv[4])
fixture_files=[path for path in (fixture_root/'hack/external-substrate-e2e/core').iterdir() if path.is_file()]
fixture_files.append(fixture_root/'scripts/external-substrate-core-e2e.sh')
fixture_digest=hashlib.sha256()
for path in sorted(fixture_files):
    fixture_digest.update(str(path.relative_to(fixture_root)).encode()+b'\0'+path.read_bytes()+b'\0')
pins['fixtureSourceSHA256']=fixture_digest.hexdigest()
pathlib.Path(sys.argv[5]).write_text(json.dumps(pins,indent=2)+'\n')
PY
runtime_context="${run_dir}/images/runtime"
core_context="${run_dir}/images/core"
provider_context="${run_dir}/images/provider"
proof_context="${run_dir}/images/proof"
mkdir -p "${runtime_context}" "${core_context}" "${provider_context}" "${proof_context}"
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go -C "${core_snapshot}" build -p 4 -buildvcs=false -trimpath -o "${runtime_context}/orka-acp-runtime" ./cmd/orka-acp-runtime
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go -C "${core_snapshot}" build -p 4 -buildvcs=false -trimpath -o "${runtime_context}/orka-acp-exec-helper" ./cmd/orka-acp-exec-helper
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go -C "${core_snapshot}" build -p 4 -buildvcs=false -trimpath -o "${core_context}/manager" ./cmd
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go -C "${shared_snapshot}" build -p 4 -buildvcs=false -trimpath -o "${runtime_context}/acp-agent" ./hack/external-workspace-e2e/acp-agent
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go -C "${shared_snapshot}/providers" build -p 4 -buildvcs=false -trimpath -o "${provider_context}/orka-workspace-substrate" ./substrate/cmd/orka-workspace-substrate
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go -C "${shared_snapshot}/hack/external-substrate-e2e" build -p 4 -buildvcs=false -trimpath -o "${proof_context}/native-data-proof" ./core
chmod 0555 "${runtime_context}/orka-acp-runtime" "${runtime_context}/orka-acp-exec-helper" "${runtime_context}/acp-agent" "${core_context}/manager"
docker build -f "${shared_snapshot}/hack/external-workspace-e2e/acp-runtime.Dockerfile" -t "localhost:5002/orka/native-core-runtime:${run_id}" "${runtime_context}"
docker build -f "${shared_snapshot}/hack/external-workspace-e2e/core.Dockerfile" -t "localhost:5002/orka/native-core:${run_id}" "${core_context}"
docker build -f "${fixture}/provider.Dockerfile" -t "localhost:5002/orka/native-provider:${run_id}" "${provider_context}"
docker build -f "${fixture}/proof.Dockerfile" -t "localhost:5002/orka/native-core-proof:${run_id}" "${proof_context}"
for name in native-core-runtime native-core native-provider native-core-proof; do
  docker push "localhost:5002/orka/${name}:${run_id}"
done
runtime_image="$(docker inspect -f '{{index .RepoDigests 0}}' "localhost:5002/orka/native-core-runtime:${run_id}")"
core_image="$(docker inspect -f '{{index .RepoDigests 0}}' "localhost:5002/orka/native-core:${run_id}")"
provider_image="$(docker inspect -f '{{index .RepoDigests 0}}' "localhost:5002/orka/native-provider:${run_id}")"
proof_image="$(docker inspect -f '{{index .RepoDigests 0}}' "localhost:5002/orka/native-core-proof:${run_id}")"
runtime_image="${runtime_image/localhost:5002/${registry_ip}:5000}"
worker_image="${SUBSTRATE_WORKER_IMAGE:-localhost:5002/ateom-gvisor-715889664656de67e44382a8d6ab981d@sha256:6055430a2c29d7815f3b94bc0682cbb6495c5d90012b6531b86ac92bba86d372}"
python3 - "${run_dir}/images.json" "${runtime_image}" "${core_image}" "${provider_image}" "${proof_image}" "${worker_image}" <<'PY'
import json,pathlib,sys
pathlib.Path(sys.argv[1]).write_text(json.dumps(dict(zip(['runtime','core','provider','proof','worker'],sys.argv[2:])),indent=2)+'\n')
PY

kubectl apply --server-side -k "${shared_snapshot}/config"
kubectl apply --server-side -f "${shared_snapshot}/providers/substrate/config/crd"
kubectl apply --server-side -k "${core_snapshot}/config/crd"
kubectl wait --for=condition=Established crd --all --timeout=120s
mkdir -m 700 "${run_dir}/private"
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=external-substrate-core" \
  -addext "subjectAltName=DNS:external-substrate-core.${namespace}.svc,DNS:external-substrate-core.${namespace}.svc.cluster.local" \
  -keyout "${run_dir}/private/tls.key" -out "${run_dir}/private/tls.crt" 2>"${run_dir}/private/openssl.log"
openssl rand 32 >"${run_dir}/private/key"
openssl rand -hex 32 >"${run_dir}/private/token"
chmod 600 "${run_dir}/private/"*
kubectl create namespace "${namespace}"
kubectl -n "${namespace}" create secret generic external-substrate-core-fixture --from-file="${run_dir}/private/key" --from-file="${run_dir}/private/token"
kubectl -n "${namespace}" create secret tls external-substrate-core-tls --key="${run_dir}/private/tls.key" --cert="${run_dir}/private/tls.crt"
kubectl create --dry-run=client -f "${core_snapshot}/config/rbac/role.yaml" -o json >"${run_dir}/core-role.json"
python3 "${fixture}/install.py" "${namespace}" "${core_image}" "${runtime_image}" "${proof_image}" "${provider_image}" "${worker_image}" "${run_dir}/private/tls.crt" "${run_dir}/core-role.json" >"${run_dir}/install.json"
jq '{apiVersion:"v1",kind:"List",items:[.items[] | select(.kind != "Job")]}' "${run_dir}/install.json" | kubectl apply -f -
kubectl -n orka-workspace-system rollout status deployment/orka-workspace-substrate --timeout=180s
kubectl -n "${namespace}" rollout status deployment/external-substrate-core --timeout=180s
jq '.items[] | select(.kind == "Job")' "${run_dir}/install.json" | kubectl create -f -
printf 'Actual native Core Task proof namespace: %s\nArtifacts: %s\n' "${namespace}" "${run_dir}"
start="$(date +%s)"
while true; do
  job="$(kubectl -n "${namespace}" --request-timeout=15s get job native-core-proof -o json)"
  if jq -e 'any(.status.conditions[]?; (.type == "Failed" or .type == "FailureTarget") and .status == "True")' <<<"${job}" >/dev/null; then
    kubectl -n "${namespace}" logs job/native-core-proof | tee "${run_dir}/proof.log"
    printf 'Failed proof retained; never replay an uncertain accepted Task. Namespace: %s\n' "${namespace}" >&2
    exit 1
  fi
  if jq -e 'any(.status.conditions[]?; .type == "Complete" and .status == "True")' <<<"${job}" >/dev/null; then
    kubectl -n "${namespace}" logs job/native-core-proof | tee "${run_dir}/proof.log"
    break
  fi
  (( $(date +%s) - start < 900 )) || exit 1
  sleep 2
done
python3 - "${run_dir}/proof.log" "${run_dir}/report.json" <<'PY'
import json,pathlib,sys
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    if line.startswith('NATIVE_CORE_PROOF '):
        pathlib.Path(sys.argv[2]).write_text(json.dumps(json.loads(line[len('NATIVE_CORE_PROOF '):]),indent=2)+'\n')
        break
else:
    raise SystemExit('Native Core Job omitted completion evidence')
PY
printf 'Actual Core and deployed native provider Task proof report: %s\n' "${run_dir}/report.json"
