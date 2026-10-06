#!/usr/bin/env bash
# Called only by kindctl exec --tag external-substrate from the shared repo.
set -euo pipefail
source_dir="$1"
test -n "${KUBECONFIG:-}"
context="$(kubectl config current-context)"
case "${context}" in
  kind-orka-workspace-*-external-su-*) ;;
  *) printf 'Refusing unexpected scoped cluster context: %s\n' "${context}" >&2; exit 1 ;;
esac

registry=orka-external-substrate-registry
registry_port=5002
if docker inspect "${registry}" >/dev/null 2>&1; then
  test "$(docker inspect -f '{{index .Config.Labels "orka.workspace.e2e"}}' "${registry}")" = external-substrate
  test "$(docker inspect -f '{{(index (index .HostConfig.PortBindings "5000/tcp") 0).HostPort}}' "${registry}")" = "${registry_port}"
else
  docker run -d --restart=unless-stopped --label orka.workspace.e2e=external-substrate \
    -p "127.0.0.1:${registry_port}:5000" --name "${registry}" registry:2 >/dev/null
fi
if ! docker inspect -f '{{with index .NetworkSettings.Networks "kind"}}{{.IPAddress}}{{end}}' "${registry}" | rg -q .; then
  docker network connect kind "${registry}"
fi

nodes="$(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')"
while IFS= read -r node; do
  test -n "${node}"
  case "${node}" in
    "${context#kind-}"-*) ;;
    *) printf 'Refusing foreign node: %s\n' "${node}" >&2; exit 1 ;;
  esac
  docker exec "${node}" sysctl -w net.ipv4.conf.all.proxy_arp=1
  docker exec "${node}" sh -ec 'mkdir -p "$1"; printf "%s\n" "$2" > "$1/hosts.toml"' _ \
    "/etc/containerd/certs.d/localhost:${registry_port}" \
    "[host.\"http://${registry}:5000\"]"
done <<<"${nodes}"

cd "${source_dir}"
export KIND_CLUSTER_NAME="${context#kind-}"
export KUBECTL_CONTEXT="${context}"
export KO_DOCKER_REPO="localhost:${registry_port}"
# This is the official pinned installer, with no source or manifest edits.
bash hack/install-ate-kind.sh --deploy-ate-system --rollout-timeout=5m
test -z "$(git status --porcelain --untracked-files=all)"
cd - >/dev/null
bash hack/external-substrate-e2e/configure-kind-dns.sh
