#!/usr/bin/env bash
# Called only by kindctl exec --tag external-substrate from the shared repo.
# Installs the pinned, unmodified Agent Substrate v1.0.0 checkout with its
# official ate-setup installer onto this dedicated cluster. A v0.1.0 install
# cannot be upgraded in place: use a fresh dedicated cluster.
set -euo pipefail
source_dir="$1"
test -n "${KUBECONFIG:-}"
context="$(kubectl config current-context)"
case "${context}" in
  kind-orka-workspace-*-external-su-*) ;;
  *) printf 'Refusing unexpected scoped cluster context: %s\n' "${context}" >&2; exit 1 ;;
esac
source "$(dirname "$0")/../../providers/substrate/nativepb/upstream.env"
test "$(git -C "${source_dir}" rev-parse HEAD)" = "${SUBSTRATE_UPSTREAM_COMMIT}"

# A registry dedicated to this harness. The shared kind-registry (:5001) that
# the upstream kind profile defaults to is never used or reconfigured.
registry=orka-external-substrate-registry
registry_port=5002
if docker inspect "${registry}" >/dev/null 2>&1; then
  test "$(docker inspect -f '{{index .Config.Labels "orka.workspace.e2e"}}' "${registry}")" = external-substrate
  test "$(docker inspect -f '{{(index (index .HostConfig.PortBindings "5000/tcp") 0).HostPort}}' "${registry}")" = "${registry_port}"
else
  docker run -d --restart=unless-stopped --label orka.workspace.e2e=external-substrate \
    -p "127.0.0.1:${registry_port}:5000" --name "${registry}" registry:2 >/dev/null
fi
if [[ -z "$(docker inspect -f '{{with index .NetworkSettings.Networks "kind"}}{{.IPAddress}}{{end}}' "${registry}")" ]]; then
  docker network connect kind "${registry}"
fi

nodes="$(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')"
while IFS= read -r node; do
  test -n "${node}"
  case "${node}" in
    "${context#kind-}"-*) ;;
    *) printf 'Refusing foreign node: %s\n' "${node}" >&2; exit 1 ;;
  esac
  # Upstream hack/create-kind-cluster.sh: proxy ARP/NDP for gVisor sandboxes.
  docker exec "${node}" sysctl -w net.ipv4.conf.all.proxy_arp=1
  docker exec "${node}" sysctl -e -w net.ipv6.conf.all.proxy_ndp=1
  docker exec "${node}" sh -ec 'mkdir -p "$1"; printf "%s\n" "$2" > "$1/hosts.toml"' _ \
    "/etc/containerd/certs.d/localhost:${registry_port}" \
    "[host.\"http://${registry}:5000\"]"
done <<<"${nodes}"

# Test principals only. The proof Jobs run in this fixed client namespace so
# their pod-certificate SPIFFE IDs are stable across runs. They are bootstrap
# owners of this disposable installation and router clients. The deployed
# provider is a router client and receives only Atespace editor and global
# viewer access policies from the proof (see providers/substrate/README.md).
client_namespace=external-substrate-e2e
proof_id="spiffe://cluster.local/ns/${client_namespace}/sa/native-data-proof"
core_proof_id="spiffe://cluster.local/ns/${client_namespace}/sa/native-core-proof"
provider_id="spiffe://cluster.local/ns/orka-workspace-system/sa/orka-workspace-substrate"
# The Orka core manager of the Core Task proof seeds credentials and probes
# routed runtimes through atenet-router (fixed namespace, see core/run.sh).
core_manager_id="spiffe://cluster.local/ns/external-substrate-core/sa/orka-controller-manager"
ingress_ids="${proof_id},${core_proof_id},${provider_id},${core_manager_id}"
if [[ -n "${SUBSTRATE_E2E_EXTRA_INGRESS_SPIFFE_IDS:-}" ]]; then
  ingress_ids="${ingress_ids},${SUBSTRATE_E2E_EXTRA_INGRESS_SPIFFE_IDS}"
fi

cd "${source_dir}"
# This is the official pinned installer, with no source or manifest edits.
ATE_INSTALL_KIND=true KIND_CLUSTER_NAME="${context#kind-}" KUBECTL_CONTEXT="${context}" \
  KO_DOCKER_REPO="localhost:${registry_port}" NO_DEV_ENV=1 \
  go run ./cmd/ate-setup deploy ate-system \
    --rollout-timeout=5m \
    --sandbox-classes=gvisor \
    --credential-provider='{"enabled":false}' \
    --ateapi-authz-bootstrap-owners="${proof_id},${core_proof_id}" \
    --ingress-allowed-spiffe-ids="${ingress_ids}"
test -z "$(git status --porcelain --untracked-files=all)"
cd - >/dev/null
kubectl create namespace "${client_namespace}" --dry-run=client -o yaml | kubectl apply -f -
# ate-system's NetworkPolicy admits ate-api clients only from labeled namespaces.
kubectl label namespace "${client_namespace}" ate.dev/api-access=true --overwrite
for account in native-data-proof native-core-proof; do
  kubectl -n "${client_namespace}" create serviceaccount "${account}" --dry-run=client -o yaml | kubectl apply -f -
done
