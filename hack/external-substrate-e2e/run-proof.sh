#!/usr/bin/env bash
# Called only by kindctl exec --tag external-substrate from the shared repo.
set -euo pipefail
source_dir="$1"
repo_root="$PWD"
test -n "${KUBECONFIG:-}"
context="$(kubectl config current-context)"
case "${context}" in
  kind-orka-workspace-*-external-su-*) ;;
  *) printf 'Refusing unexpected scoped cluster: %s\n' "${context}" >&2; exit 1 ;;
esac
run_id="$(date -u +%Y%m%d%H%M%S)"
namespace="external-substrate-proof-${run_id}"
run_dir="${repo_root}/bin/${namespace}"
mkdir -p "${run_dir}"
chmod 700 "${run_dir}"
registry=orka-external-substrate-registry
test "$(docker inspect -f '{{index .Config.Labels "orka.workspace.e2e"}}' "${registry}")" = external-substrate
registry_ip="$(docker inspect -f '{{with index .NetworkSettings.Networks "kind"}}{{.IPAddress}}{{end}}' "${registry}")"
test -n "${registry_ip}"

# The upstream DNS controller configures GKE kube-dns automatically. kind uses
# CoreDNS; its documented integration requires this isolated-cluster stub rule.
bash hack/external-substrate-e2e/configure-kind-dns.sh

# The pinned backend supports direct egress as an installation setting. Only
# the isolated native API deployment is changed, with exact UID/version tests.
patch="$(kubectl -n ate-system get deployment ate-api-server -o json | jq -ce '
  [.spec.template.spec.containers | to_entries[] | select(.value.name == "ate-api-server")] as $containers |
  if ($containers | length) != 1 then error("expected one native API container") else
    $containers[0] as $container |
    [$container.value.args | to_entries[] | select(.value | startswith("--egress-gateway-address="))] as $args |
    if ($args | length) != 1 then error("expected one native gateway argument") else
      [{op:"test",path:"/metadata/uid",value:.metadata.uid},
       {op:"test",path:"/metadata/resourceVersion",value:.metadata.resourceVersion},
       {op:"replace",path:("/spec/template/spec/containers/"+($container.key|tostring)+"/args/"+($args[0].key|tostring)),value:"--egress-gateway-address="}]
    end
  end')"
kubectl -n ate-system patch deployment ate-api-server --type=json -p "${patch}" >/dev/null
kubectl -n ate-system rollout status deployment/ate-api-server --timeout=5m

CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go -C hack/external-substrate-e2e build -p 4 -trimpath -o "${run_dir}/native-data-runtime" ./runtime
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go -C hack/external-substrate-e2e build -p 4 -trimpath -o "${run_dir}/native-data-proof" ./proof
docker build -f hack/external-substrate-e2e/runtime.Dockerfile -t "localhost:5002/orka/native-data-runtime:${run_id}" "${run_dir}"
docker build -f hack/external-substrate-e2e/proof.Dockerfile -t "localhost:5002/orka/native-data-proof:${run_id}" "${run_dir}"
docker push "localhost:5002/orka/native-data-runtime:${run_id}"
docker push "localhost:5002/orka/native-data-proof:${run_id}"
runtime_image="$(docker inspect -f '{{index .RepoDigests 0}}' "localhost:5002/orka/native-data-runtime:${run_id}")"
proof_image="$(docker inspect -f '{{index .RepoDigests 0}}' "localhost:5002/orka/native-data-proof:${run_id}")"
runtime_image="${runtime_image/localhost:5002/${registry_ip}:5000}"
worker_image="$(cd "${source_dir}" && KO_DOCKER_REPO=localhost:5002 bash hack/run-tool.sh ko build --platform=linux/arm64 ./cmd/ateom-gvisor)"
test -z "$(git -C "${source_dir}" status --porcelain --untracked-files=all)"

kubectl apply --server-side -f config/crd/bases
kubectl apply --server-side -f providers/substrate/config/crd
kubectl apply -f providers/substrate/config/registration.yaml
kubectl create namespace "${namespace}"
kubectl -n "${namespace}" apply -f - <<YAML
apiVersion: ate.dev/v1alpha1
kind: WorkerPool
metadata:
  name: native-data
  labels: {orka.workspace.e2e/pool: ${namespace}}
spec:
  replicas: 1
  workerImage: ${worker_image}
  template:
    resources:
      requests: {cpu: 250m, memory: 512Mi}
      limits: {cpu: "2", memory: 2Gi}
---
apiVersion: v1
kind: ServiceAccount
metadata: {name: native-data-proof}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: native-data-proof}
rules:
  - apiGroups: [workspace.orka.ai]
    resources: [executionworkspaces, executionworkspaces/status, executionworkspacecheckpoints, executionworkspacecheckpoints/status]
    verbs: [get, list, create, update, patch, delete]
  - apiGroups: [substrate.workspace.orka.ai]
    resources: [substrateworkspaceprofiles]
    verbs: [get, list, create]
  - apiGroups: [ate.dev]
    resources: [workerpools]
    verbs: [get, list, create, delete]
  - apiGroups: [""]
    resources: [configmaps]
    verbs: [get, list, create, update, delete]
  - apiGroups: [""]
    resources: [pods]
    verbs: [get, list, delete]
  - apiGroups: [networking.k8s.io]
    resources: [networkpolicies]
    verbs: [get, list, create, delete]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: native-data-proof}
subjects: [{kind: ServiceAccount, name: native-data-proof}]
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: native-data-proof}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: {name: ${namespace}}
rules:
  - apiGroups: [workspace.orka.ai]
    resources: [executionworkspaceproviders]
    resourceNames: [substrate]
    verbs: [get]
  - apiGroups: [ate.dev]
    resources: [workerpools]
    verbs: [get, list]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata: {name: ${namespace}}
subjects: [{kind: ServiceAccount, name: native-data-proof, namespace: ${namespace}}]
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: ${namespace}}
---
apiVersion: batch/v1
kind: Job
metadata: {name: native-data-proof}
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 1260
  template:
    metadata: {labels: {orka.workspace.e2e/proof: native-data}}
    spec:
      restartPolicy: Never
      serviceAccountName: native-data-proof
      securityContext: {runAsUser: 65532, runAsGroup: 65532, fsGroup: 65532}
      containers:
        - name: proof
          image: ${proof_image}
          env:
            - name: POD_NAMESPACE
              valueFrom: {fieldRef: {fieldPath: metadata.namespace}}
            - {name: RUNTIME_IMAGE, value: '${runtime_image}'}
            - {name: PROOF_IMAGE, value: '${proof_image}'}
            - {name: WORKER_IMAGE, value: '${worker_image}'}
          volumeMounts:
            - {name: client, mountPath: /native/client, readOnly: true}
            - {name: server, mountPath: /native/server, readOnly: true}
      volumes:
        - name: client
          projected:
            sources:
              - podCertificate:
                  signerName: podidentity.podcert.ate.dev/identity
                  keyType: ECDSAP256
                  credentialBundlePath: credential-bundle.pem
        - name: server
          projected:
            sources:
              - clusterTrustBundle:
                  signerName: servicedns.podcert.ate.dev/identity
                  labelSelector: {matchLabels: {podcert.ate.dev/canarying: live}}
                  path: trust-bundle.pem
YAML

printf '%s\n' "${namespace}" >"${repo_root}/bin/external-substrate-proof-namespace"
printf 'Native Data proof namespace: %s\n' "${namespace}"
start="$(date +%s)"
while true; do
  job="$(kubectl -n "${namespace}" --request-timeout=15s get job native-data-proof -o json)"
  if jq -e 'any(.status.conditions[]?; (.type == "Failed" or .type == "FailureTarget") and .status == "True")' <<<"${job}" >/dev/null; then
    kubectl -n "${namespace}" logs job/native-data-proof >"${run_dir}/proof.log" 2>&1 || true
    cat "${run_dir}/proof.log"
    printf 'Native Data proof failed. Resources are retained in %s.\n' "${namespace}" >&2
    exit 1
  fi
  if jq -e 'any(.status.conditions[]?; .type == "Complete" and .status == "True")' <<<"${job}" >/dev/null; then
    kubectl -n "${namespace}" logs job/native-data-proof | tee "${run_dir}/proof.log"
    python3 - "${run_dir}/proof.log" "${run_dir}/report.json" <<'PY'
import json, pathlib, sys
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    if line.startswith('NATIVE_DATA_PROOF '):
        value = json.loads(line[len('NATIVE_DATA_PROOF '):])
        pathlib.Path(sys.argv[2]).write_text(json.dumps(value, indent=2) + '\n')
        break
else:
    raise SystemExit('Job omitted native Data proof report')
PY
    printf 'Verified report: %s\n' "${run_dir}/report.json"
    break
  fi
  if (( $(date +%s) - start > 1260 )); then
    printf 'Native Data proof timed out; resources retained in %s.\n' "${namespace}" >&2
    exit 1
  fi
  sleep 2
done
