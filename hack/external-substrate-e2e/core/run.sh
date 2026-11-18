#!/usr/bin/env bash
# Called only through kindctl exec --tag external-substrate from the shared repo.
set -euo pipefail
mode="$1"
case "$(kubectl config current-context)" in
  kind-orka-workspace-*-external-su-*) ;;
  *) printf 'Refusing foreign native Core proof cluster\n' >&2; exit 1 ;;
esac
test -n "${KUBECONFIG:-}"
arch="$(kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.architecture}')"
case "${arch}" in amd64|arm64) ;; *) printf 'Unsupported node architecture: %s\n' "${arch}" >&2; exit 1 ;; esac
node_cpu="$(kubectl get nodes -o jsonpath='{.items[0].status.allocatable.cpu}')"
node_memory="$(kubectl get nodes -o jsonpath='{.items[0].status.allocatable.memory}')"
# A failed test before && does not trip set -e; reject unknown formats explicitly.
[[ "${node_cpu}" =~ ^[0-9]+$ ]] || { printf 'Unsupported allocatable CPU format: %s\n' "${node_cpu}" >&2; exit 1; }
# Four CPU is the documented minimum; a GitHub-hosted runner has exactly four.
(( node_cpu >= 4 ))
[[ "${node_memory}" =~ ^([0-9]+)Ki$ ]] || { printf 'Unsupported allocatable memory format: %s\n' "${node_memory}" >&2; exit 1; }
(( BASH_REMATCH[1] >= 8 * 1024 * 1024 ))
# Agent Substrate v1.0.0 sends all Actor egress through its gateway and routes
# ingress through atenet-router; both must be serving. No installation setting
# is patched.
# Egress credential injection needs the bundled Kubernetes Secrets credential
# provider (install-backend.sh installs the gateway with it).
if ! kubectl -n ate-system get deployments ate-api-server ate-controller atenet-router atenet-egress k8s-credential-provider -o json |
  jq -e '(.items | length) == 5 and all(.items[]; (.status.availableReplicas // 0) >= 1)' >/dev/null; then
  printf 'The native installation is not serving, or lacks the k8s.io egress credential provider; reinstall with install-backend.sh.\n' >&2
  exit 1
fi
# The installer's trace backend, which receives atenet-router's Envoy spans
# (ate-otel-config); the tracing assertion reads the router hop from it.
kubectl -n otel-system get deployments opentelemetry-collector jaeger -o json |
  jq -e '(.items | length) == 2 and all(.items[]; (.status.availableReplicas // 0) >= 1)' >/dev/null
client_namespace=external-substrate-e2e
kubectl -n "${client_namespace}" get serviceaccount native-core-proof >/dev/null
registry=orka-external-substrate-registry
test "$(docker inspect -f '{{index .Config.Labels "orka.workspace.e2e"}}' "${registry}")" = external-substrate
registry_ip="$(docker inspect -f '{{with index .NetworkSettings.Networks "kind"}}{{.IPAddress}}{{end}}' "${registry}")"
test -n "${registry_ip}"
printf 'Isolated pinned native installation and %s registry preflight passed.\n' "${arch}"
if [[ "${mode}" == preflight ]]; then exit 0; fi

# Compilation and installation are a separate release point from preparing this
# harness. A failed or uncertain accepted Task is retained and never replayed.
test "${ORKA_CORE_BUILD_RELEASED:-}" = 1
core_source="${ORKA_CORE_SOURCE:-${PWD}/../orka.workspace-out-of-tree}"
test -f "${core_source}/cmd/orka-acp-runtime/main.go"
test -f "${core_source}/cmd/main.go"
if test "$(kubectl get deployments -A -o json | jq '[.items[] | select(.metadata.name == "external-substrate-core" or .metadata.labels.app == "external-substrate-core")] | length')" != 0; then
  printf 'Existing native Core proof deployment requires explicit review before another run.\n' >&2
  exit 1
fi
if kubectl -n orka-workspace-system get deployment/orka-workspace-substrate >/dev/null 2>&1; then
  printf 'Existing native provider deployment requires explicit review before another run.\n' >&2
  exit 1
fi
run_id="$(date -u +%Y%m%d%H%M%S)"
# Core reaches routed runtimes only through atenet-router, whose ingress
# allowlist is fixed at install time (install-backend.sh). The Core manager's
# pod-certificate SPIFFE ID therefore needs a stable namespace; runs are already
# serialized by the deployment guard above.
namespace="external-substrate-core"
if kubectl get namespace "${namespace}" >/dev/null 2>&1 || kubectl -n "${client_namespace}" get job "${namespace}" >/dev/null 2>&1; then
  printf 'A retained native Core proof (%s) requires explicit review before another run.\n' "${namespace}" >&2
  exit 1
fi
run_dir="${PWD}/bin/${namespace}-${run_id}"
mkdir -p "${run_dir}"
chmod 700 "${run_dir}"
# Any failure from here on leaves redacted status/log evidence next to the
# run's artifacts. The accepted Task and its resources are retained.
diagnostics="${PWD}/hack/external-substrate-e2e/diagnostics.sh"
on_exit() {
  local code=$?
  if (( code != 0 )); then
    bash "${diagnostics}" "${run_dir}/diagnostics" "${run_dir}/private/token" || true
  fi
  exit "${code}"
}
trap on_exit EXIT
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
pins['nativeCommit']='b34fbee742906494b5eb4f1f36987176bf8770a4'
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
proxy_context="${run_dir}/images/provider-proxy"
mkdir -p "${runtime_context}" "${core_context}" "${provider_context}" "${proof_context}" "${proxy_context}"
CGO_ENABLED=0 GOOS=linux GOARCH="${arch}" go -C "${core_snapshot}" build -p 4 -buildvcs=false -trimpath -o "${runtime_context}/orka-acp-runtime" ./cmd/orka-acp-runtime
CGO_ENABLED=0 GOOS=linux GOARCH="${arch}" go -C "${core_snapshot}" build -p 4 -buildvcs=false -trimpath -o "${runtime_context}/orka-acp-exec-helper" ./cmd/orka-acp-exec-helper
CGO_ENABLED=0 GOOS=linux GOARCH="${arch}" go -C "${core_snapshot}" build -p 4 -buildvcs=false -trimpath -o "${core_context}/manager" ./cmd
CGO_ENABLED=0 GOOS=linux GOARCH="${arch}" go -C "${shared_snapshot}" build -p 4 -buildvcs=false -trimpath -o "${runtime_context}/acp-agent" ./hack/external-workspace-e2e/acp-agent
CGO_ENABLED=0 GOOS=linux GOARCH="${arch}" go -C "${shared_snapshot}/providers" build -p 4 -buildvcs=false -trimpath -o "${provider_context}/orka-workspace-substrate" ./substrate/cmd/orka-workspace-substrate
CGO_ENABLED=0 GOOS=linux GOARCH="${arch}" go -C "${shared_snapshot}/hack/external-substrate-e2e" build -p 4 -buildvcs=false -trimpath -o "${proof_context}/native-data-proof" ./core
CGO_ENABLED=0 GOOS=linux GOARCH="${arch}" go -C "${shared_snapshot}/hack/external-substrate-e2e" build -p 4 -buildvcs=false -trimpath -o "${proxy_context}/fixture-provider-proxy" ./core/providerproxy
chmod 0555 "${runtime_context}/orka-acp-runtime" "${runtime_context}/orka-acp-exec-helper" "${runtime_context}/acp-agent" "${core_context}/manager"
docker build -f "${shared_snapshot}/hack/external-workspace-e2e/acp-runtime.Dockerfile" -t "localhost:5002/orka/native-core-runtime:${run_id}" "${runtime_context}"
docker build -f "${shared_snapshot}/hack/external-workspace-e2e/core.Dockerfile" -t "localhost:5002/orka/native-core:${run_id}" "${core_context}"
docker build -f "${fixture}/provider.Dockerfile" -t "localhost:5002/orka/native-provider:${run_id}" "${provider_context}"
docker build -f "${fixture}/proof.Dockerfile" -t "localhost:5002/orka/native-core-proof:${run_id}" "${proof_context}"
docker build -f "${fixture}/provider-proxy.Dockerfile" -t "localhost:5002/orka/native-core-provider-proxy:${run_id}" "${proxy_context}"
# The OTLP collector Orka's own e2e uses, mirrored into the dedicated registry.
collector_source=otel/opentelemetry-collector:0.111.0@sha256:7020db3692a3d722fec002f39608ffe3a5d2fa4d09e14607cf33b9416f2c1df8
docker pull --platform "linux/${arch}" "${collector_source}"
docker tag "${collector_source}" "localhost:5002/orka/otel-collector:${run_id}"
for name in native-core-runtime native-core native-provider native-core-proof native-core-provider-proxy; do
  docker push "localhost:5002/orka/${name}:${run_id}"
done
# A single-platform copy of the multi-platform source has its own manifest
# digest, which only the push reports (RepoDigests keeps the source index).
collector_digest="$(docker push "localhost:5002/orka/otel-collector:${run_id}" | sed -n 's/^.*: digest: \(sha256:[0-9a-f]\{64\}\) size: .*$/\1/p')"
[[ "${collector_digest}" =~ ^sha256:[0-9a-f]{64}$ ]] || { printf 'Collector mirror push reported no digest\n' >&2; exit 1; }
runtime_image="$(docker inspect -f '{{index .RepoDigests 0}}' "localhost:5002/orka/native-core-runtime:${run_id}")"
core_image="$(docker inspect -f '{{index .RepoDigests 0}}' "localhost:5002/orka/native-core:${run_id}")"
provider_image="$(docker inspect -f '{{index .RepoDigests 0}}' "localhost:5002/orka/native-provider:${run_id}")"
proof_image="$(docker inspect -f '{{index .RepoDigests 0}}' "localhost:5002/orka/native-core-proof:${run_id}")"
proxy_image="$(docker inspect -f '{{index .RepoDigests 0}}' "localhost:5002/orka/native-core-provider-proxy:${run_id}")"
collector_image="localhost:5002/orka/otel-collector@${collector_digest}"
runtime_image="${runtime_image/localhost:5002/${registry_ip}:5000}"
# The cmd/ateworker-gvisor image built from the pinned checkout (see run-proof.sh).
# Without SUBSTRATE_WORKER_IMAGE, use the one the latest passing Data proof
# built from the pinned checkout on this installation.
worker_image="${SUBSTRATE_WORKER_IMAGE:-}"
if [[ -z "${worker_image}" ]]; then
  data_report="$(find "${PWD}/bin" -maxdepth 2 -path '*/external-substrate-proof-2*/report.json' 2>/dev/null | sort | tail -n 1)"
  [[ -n "${data_report}" ]] || { printf 'Set SUBSTRATE_WORKER_IMAGE or run the Data proof first.\n' >&2; exit 1; }
  worker_image="$(jq -r '.workerImage // empty' "${data_report}")"
  printf 'Worker image from %s\n' "${data_report}"
fi
[[ "${worker_image}" =~ ^localhost:5002/ateworker-gvisor-[0-9a-f]+@sha256:[0-9a-f]{64}$ ]] || {
  printf 'Worker image is not the pinned ateworker-gvisor digest: %s\n' "${worker_image}" >&2; exit 1; }
python3 - "${run_dir}/images.json" "${runtime_image}" "${core_image}" "${provider_image}" "${proof_image}" "${worker_image}" "${proxy_image}" "${collector_image}" <<'PY'
import json,pathlib,sys
pathlib.Path(sys.argv[1]).write_text(json.dumps(dict(zip(['runtime','core','provider','proof','worker','providerProxy','collector'],sys.argv[2:])),indent=2)+'\n')
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
# Recorded on the proof Job so core/cleanup.sh retires only this lifetime of
# the fixed namespace.
namespace_uid="$(kubectl get namespace "${namespace}" -o jsonpath='{.metadata.uid}')"
test -n "${namespace_uid}"
# ate-system's NetworkPolicy admits Substrate clients only from labeled namespaces.
kubectl label namespace "${namespace}" ate.dev/api-access=true
kubectl -n "${namespace}" create secret generic external-substrate-core-fixture --from-file="${run_dir}/private/key" --from-file="${run_dir}/private/token"
kubectl -n "${namespace}" create secret tls external-substrate-core-tls --key="${run_dir}/private/tls.key" --cert="${run_dir}/private/tls.crt"
kubectl create --dry-run=client -f "${core_snapshot}/config/rbac/role.yaml" -o json >"${run_dir}/core-role.json"
python3 "${fixture}/install.py" "${namespace}" "${core_image}" "${runtime_image}" "${proof_image}" "${provider_image}" "${worker_image}" "${run_dir}/private/tls.crt" "${run_dir}/core-role.json" "${proxy_image}" "${collector_image}" >"${run_dir}/install.json"
# Grant this run's Atespace (the profile templateRef namespace, which is the
# run namespace) only Core's labeled provider Secrets in its own runtime
# namespace, then restart the credential provider, which reads its policy only
# at startup. This dedicated installation serves no other Atespace.
printf 'policies:\n- atespace: %s\n  allowedNamespaces: [%s]\n  secretSelector:\n    matchLabels:\n      orka.ai/runtime-pool-provider-credential: "true"\n' "${namespace}" "${namespace}" >"${run_dir}/namespace-policy.yaml"
kubectl -n ate-system create configmap k8s-credential-provider-namespace-policy --dry-run=client -o yaml \
  --from-file=namespace-policy.yaml="${run_dir}/namespace-policy.yaml" | kubectl apply -f -
kubectl -n ate-system rollout restart deployment/k8s-credential-provider
kubectl -n ate-system rollout status deployment/k8s-credential-provider --timeout=180s
jq '{apiVersion:"v1",kind:"List",items:[.items[] | select(.kind != "Job")]}' "${run_dir}/install.json" | kubectl apply -f -
kubectl -n orka-workspace-system rollout status deployment/orka-workspace-substrate --timeout=180s
kubectl -n "${namespace}" rollout status deployment/fixture-provider-proxy --timeout=180s
kubectl -n "${namespace}" rollout status deployment/orka-otel-collector --timeout=180s
kubectl -n "${namespace}" rollout status deployment/external-substrate-core --timeout=180s
printf 'Actual native Core Task proof namespace: %s\nArtifacts: %s\n' "${namespace}" "${run_dir}"
# run_job creates one rendered proof Job, waits for it and extracts its report.
# Each Job records the namespace lifetime it ran against for core/cleanup.sh.
run_job() {
  local name="$1" log="$2" report="$3" deadline="$4" start job
  jq --arg name "${name}" --arg uid "${namespace_uid}" '.items[] | select(.kind == "Job" and .metadata.name == $name) |
    .metadata.annotations["orka.workspace.e2e/namespace-uid"] = $uid' "${run_dir}/install.json" | kubectl create -f -
  start="$(date +%s)"
  while true; do
    job="$(kubectl -n "${client_namespace}" --request-timeout=15s get job "${name}" -o json)"
    if jq -e 'any(.status.conditions[]?; (.type == "Failed" or .type == "FailureTarget") and .status == "True")' <<<"${job}" >/dev/null; then
      kubectl -n "${client_namespace}" logs "job/${name}" | tee "${log}"
      printf 'Failed proof retained; never replay an uncertain accepted Task. Namespace: %s\n' "${namespace}" >&2
      exit 1
    fi
    if jq -e 'any(.status.conditions[]?; .type == "Complete" and .status == "True")' <<<"${job}" >/dev/null; then
      kubectl -n "${client_namespace}" logs "job/${name}" | tee "${log}"
      break
    fi
    (( $(date +%s) - start < deadline )) || { printf 'Native Core proof Job %s timed out; resources retained.\n' "${name}" >&2; exit 1; }
    sleep 2
  done
  python3 - "${log}" "${report}" <<'PY'
import json,pathlib,sys
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    if line.startswith('NATIVE_CORE_PROOF '):
        pathlib.Path(sys.argv[2]).write_text(json.dumps(json.loads(line[len('NATIVE_CORE_PROOF '):]),indent=2)+'\n')
        break
else:
    raise SystemExit('Native Core Job omitted completion evidence')
PY
}

run_job "${namespace}" "${run_dir}/proof.log" "${run_dir}/report.json" 1200
# The Session (a suspendable native runtime, across a worker eviction) runs on
# the same Core with telemetry: Core fits supervisor telemetry into the native
# 32-entry environment limit and the proof requires TelemetryReady=True.
run_job "${namespace}-session" "${run_dir}/session-proof.log" "${run_dir}/session-report.json" 1800
# The single Task and at least one Session turn must each have one trace
# joining Core's ACP prompt spans with the supervisor's GenAI spans; the batch
# exporters flush within seconds, so retry briefly. atenet-router's Envoy span
# sits between Core's routed request and the supervisor; the router reports it
# to the installation's own Jaeger (otel-system, installed by ate-setup), so
# each candidate trace is read from there, read-only, into private/: the proof
# keeps only span names and IDs from it (traces.json).
router_traces="${run_dir}/private/router-traces"
mkdir -p "${router_traces}"
tracing_failed=0
for attempt in $(seq 1 12); do
  # Raw spans stay under private/, which evidence uploads exclude: a failed
  # privacy check below must not publish what it rejected.
  kubectl -n "${namespace}" logs deployment/orka-otel-collector --tail=-1 >"${run_dir}/private/collector.log"
  for trace_id in $(python3 -I "${fixture}/traces.py" trace-ids "${run_dir}/private/collector.log" "${run_dir}/report.json" "${run_dir}/session-report.json"); do
    kubectl --request-timeout=15s get --raw "/api/v1/namespaces/otel-system/services/jaeger:16686/proxy/api/traces/${trace_id}" \
      >"${router_traces}/${trace_id}.json" 2>/dev/null || rm -f "${router_traces}/${trace_id}.json"
  done
  if python3 -I "${fixture}/traces.py" check "${run_dir}/private/collector.log" "${run_dir}/report.json" "${run_dir}/session-report.json" \
    "${router_traces}" "${run_dir}/private/token" "${run_dir}/traces.json"; then
    break
  fi
  if (( attempt == 12 )); then
    tracing_failed=1
    printf 'Tracing assertion FAILED. See %s\n' "${run_dir}/traces.json" >&2
    break
  fi
  sleep 5
done

python3 - "${run_dir}/report.json" "${run_dir}/traces.json" "${run_dir}/session-report.json" <<'PY'
import json,pathlib,sys
report=json.loads(pathlib.Path(sys.argv[1]).read_text())
report['tracing']=json.loads(pathlib.Path(sys.argv[2]).read_text())
report['session']=json.loads(pathlib.Path(sys.argv[3]).read_text())
pathlib.Path(sys.argv[1]).write_text(json.dumps(report,indent=2)+'\n')
PY
if (( tracing_failed )); then
  printf 'Core proof failed: Core and supervisor GenAI spans were not joined in one trace for the Task and a Session turn (report: %s)\n' "${run_dir}/report.json" >&2
  exit 1
fi
printf 'Actual Core and deployed native provider Task proof report: %s\n' "${run_dir}/report.json"
