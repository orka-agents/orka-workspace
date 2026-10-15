#!/usr/bin/env bash
# fiberd provider spike driver. Builds Linux binaries locally, builds images
# and runs a dedicated kind cluster on the remote Docker host reached as
# `ssh ${SPIKE_HOST}`. Nothing is loaded into a local Docker daemon.
#
#   hack/fiberd-spike/spike.sh build    cross-compile and sync build contexts
#   hack/fiberd-spike/spike.sh image    build fiberd:kind and the home image remotely
#   hack/fiberd-spike/spike.sh up       create the kind cluster and fiberd's issuer
#   hack/fiberd-spike/spike.sh home     (re)deploy the Orka gVisor home
#   hack/fiberd-spike/spike.sh stage1 [check flags...]
#   hack/fiberd-spike/spike.sh stage2   provider conformance on the real home
#   hack/fiberd-spike/spike.sh stage3   actual Core Task through the fiberd provider
#   hack/fiberd-spike/spike.sh substrate-up  Substrate cluster via Orka's E2E setup
#   hack/fiberd-spike/spike.sh substrate     actual Core Task through the Substrate provider
#   hack/fiberd-spike/spike.sh measure <fiberd|substrate> <runs> <burst>
#   hack/fiberd-spike/spike.sh footprint <fiberd|substrate>  one Task's instance memory
#   hack/fiberd-spike/spike.sh memory   per-grant cgroup memory in the home
#   hack/fiberd-spike/spike.sh baseline plain runc Pod supervisor start
#   hack/fiberd-spike/spike.sh k <kubectl args...>
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
host="${SPIKE_HOST:-vm}"
remote="${SPIKE_REMOTE_DIR:-spike/fiberd-orka}"
cluster="${SPIKE_CLUSTER:-fiberd-spike}"
core_source="${ORKA_CORE_SOURCE:-${repo_root}/../orka.fiberd-spike}"
fiberd_source="${FIBERD_SOURCE:-${repo_root}/../fiberd.orka-spike}"
arch="${SPIKE_ARCH:-amd64}"
substrate_cluster="${SUBSTRATE_CLUSTER:-substrate-compare}"
substrate_remote="${SUBSTRATE_REMOTE_DIR:-spike/substrate-compare}"
# Orka's test/workspace-provider-e2e branch: scripts/workspace-substrate-cluster.sh.
orka_e2e_source="${ORKA_E2E_SOURCE:-${repo_root}/../orka.workspace-provider-e2e}"
bin="${repo_root}/bin/fiberd-spike"

on_host() { ssh "${host}" "export PATH=/home/linuxbrew/.linuxbrew/bin:\$PATH KUBECONFIG=\$HOME/.kube/kind/${cluster}.kubeconfig; cd \$HOME/${remote} && $*"; }
k() { on_host kubectl "$(printf '%q ' "$@")"; }
# The Substrate cluster's kubeconfig is the one Orka's setup script wrote.
on_substrate() { ssh "${host}" "export PATH=/home/linuxbrew/.linuxbrew/bin:\$PATH KUBECONFIG=\$HOME/${substrate_remote}/orka/bin/substrate-e2e-${substrate_cluster}/kubeconfig; cd \$HOME/${remote} && $*"; }
on_cluster() { if [ "$1" = substrate ]; then shift; on_substrate "$@"; else shift; on_host "$@"; fi; }

build() {
  local out="${bin}/bin"
  mkdir -p "${out}"
  export CGO_ENABLED=0 GOOS=linux GOARCH="${arch}"
  go -C "${core_source}" build -trimpath -buildvcs=false -o "${out}/" ./cmd/orka-acp-runtime ./cmd/orka-acp-exec-helper
  go -C "${repo_root}" build -trimpath -buildvcs=false -o "${out}/acp-agent" ./hack/external-workspace-e2e/acp-agent
  go -C "${repo_root}/providers" build -trimpath -buildvcs=false -o "${out}/" ./fiberd/cmd/orka-fiber-shim ./fiberd/cmd/orka-fiber-egress ./fiberd/cmd/orka-workspace-fiberd ./substrate/cmd/orka-workspace-substrate
  go -C "${script_dir}" build -trimpath -buildvcs=false -o "${out}/fiberd-spike-check" ./check
  for ctx in runtime home provider substrate-provider; do
    mkdir -p "${bin}/${ctx}/bin"
    cp "${script_dir}/${ctx}/"* "${bin}/${ctx}/"
  done
  cp "${out}/orka-acp-runtime" "${out}/orka-acp-exec-helper" "${out}/acp-agent" "${bin}/runtime/bin/"
  cp "${out}/orka-fiber-shim" "${out}/orka-fiber-egress" "${out}/fiberd-spike-check" "${bin}/home/bin/"
  cp "${out}/orka-workspace-fiberd" "${bin}/provider/bin/"
  cp "${out}/orka-workspace-substrate" "${bin}/substrate-provider/bin/"
  # Stage 3: Core manager, its CRDs and role, the provider bundle and the proof.
  [ -d "${core_source}/internal/uiembed/dist" ] || { mkdir -p "${core_source}/internal/uiembed/dist" && echo '<!doctype html><html><body>stub</body></html>' >"${core_source}/internal/uiembed/dist/index.html"; }
  mkdir -p "${bin}/core"
  go -C "${core_source}" build -trimpath -buildvcs=false -o "${bin}/core/manager" ./cmd
  cp "${script_dir}/core-proof/core.Dockerfile" "${bin}/core/Dockerfile"
  go -C "${script_dir}" build -trimpath -buildvcs=false -o "${out}/fiberd-core-proof" ./core-proof
  ssh "${host}" "mkdir -p \$HOME/${remote}"
  for ctx in runtime home provider substrate-provider core; do rsync -a --delete "${bin}/${ctx}/" "${host}:${remote}/${ctx}/"; done
  rsync -a --delete "${repo_root}/config/" "${host}:${remote}/ws-config/"
  rsync -a --delete "${repo_root}/providers/fiberd/config/" "${host}:${remote}/fiberd-config/"
  rsync -a --delete "${repo_root}/providers/substrate/config/" "${host}:${remote}/substrate-config/"
  ssh "${host}" "mkdir -p \$HOME/${remote}/core-config"
  rsync -a --delete "${core_source}/config/crd/" "${host}:${remote}/core-config/crd/"
  rsync -a "${core_source}/config/rbac/role.yaml" "${host}:${remote}/core-config/role.yaml"
  rsync -a "${script_dir}/core-install.py" "${script_dir}/core.sh" "${script_dir}/stage3.sh" "${script_dir}/substrate.sh" \
    "${script_dir}/measure.sh" "${out}/fiberd-core-proof" "${host}:${remote}/"
  rsync -a --delete "${script_dir}/manifests/" "${host}:${remote}/manifests/"
  rsync -a --delete "${script_dir}/measure/" "${host}:${remote}/measure/"
  rsync -a --delete --exclude .git --exclude bin "${fiberd_source}/" "${host}:${remote}/fiberd/"
}

image() {
  on_host "docker build -q -t fiberd:kind -f fiberd/docker/kubernetes/Dockerfile fiberd"
  on_host "docker build -q -t orka-acp-runtime:spike runtime && docker build -q -t orka-fiber-home:spike home && docker build -q -t orka-workspace-fiberd:spike provider && docker build -q -t orka-workspace-substrate:spike substrate-provider && docker build -q -t orka-core:spike core"
  # The served supervisor image is pinned by the runtime image's own ID: the
  # home rootfs is built from exactly that image, and nothing pulls it.
  on_host "echo orka-acp-runtime@\$(docker image inspect -f '{{.Id}}' orka-acp-runtime:spike) > supervisor-image"
}

up() {
  if ! on_host "kind get clusters" | grep -qx "${cluster}"; then
    on_host "mkdir -p \$HOME/.kube/kind && kind create cluster --name ${cluster} --image kindest/node:v1.37.0 --kubeconfig \$KUBECONFIG --wait 120s"
  fi
  on_host "kind load docker-image --name ${cluster} fiberd:kind orka-fiber-home:spike orka-workspace-fiberd:spike"
  local m=fiberd/examples/kubernetes/kind/manifests
  on_host "kubectl apply -f $m/00-namespaces.yaml -f $m/10-crd.yaml && kubectl wait --for=condition=Established crd/capacitygrants.fiberd.io --timeout=60s && kubectl apply -f $m/20-issuer.yaml -f $m/30-grant-rbac.yaml && kubectl -n fiberd-system rollout status deploy/grant-issuer --timeout=180s"
}

home() {
  on_host "kind load docker-image --name ${cluster} orka-fiber-home:spike"
  on_host "kubectl -n tenant-a delete capacitygrant orka-home --ignore-not-found --wait=true && kubectl -n tenant-a delete pod orka-home-grant --ignore-not-found --wait=true"
  on_host "kubectl apply -f manifests/home.yaml"
  on_host "for i in \$(seq 60); do kubectl -n tenant-a get pod orka-home-grant >/dev/null 2>&1 && break; sleep 1; done; kubectl -n tenant-a wait --for=condition=Ready pod/orka-home-grant --timeout=300s"
}

stage1() {
  local ip client=spike-check
  ip="$(on_host "kubectl -n tenant-a get pod orka-home-grant -o jsonpath='{.status.podIP}'")"
  on_host "kubectl -n tenant-a get pod ${client} >/dev/null 2>&1 || { kubectl -n tenant-a run ${client} --image=orka-fiber-home:spike --image-pull-policy=Never --restart=Never --command -- sleep infinity && kubectl -n tenant-a wait --for=condition=Ready pod/${client} --timeout=120s; }"
  on_host "kubectl -n fiberd-system get secret grant-issuer-key -o jsonpath='{.data.key\\.json}' | base64 -d | kubectl -n tenant-a exec -i ${client} -- sh -c 'cat >/tmp/issuer-key.json'"
  on_host "kubectl -n tenant-a exec ${client} -- fiberd-spike-check -target ${ip}:8484 -node-id orka-home-grant -issuer-key /tmp/issuer-key.json -issuer http://grant-issuer.fiberd-system.svc:8080 $(printf '%q ' "$@")"
}

issuer_secret() {
  on_host "kubectl -n fiberd-system get secret grant-issuer-key -o jsonpath='{.data.key\\.json}' | base64 -d > issuer-key.json && kubectl -n $1 create secret generic fiberd-issuer-key --from-file=key.json=issuer-key.json --dry-run=client -o yaml | kubectl apply -f - >/dev/null && rm -f issuer-key.json"
}

stage2() {
  issuer_secret tenant-a
  on_host "kubectl -n tenant-a delete job fiberd-conformance --ignore-not-found --wait=true >/dev/null && sed \"s#SUPERVISOR_IMAGE#\$(cat supervisor-image)#\" manifests/conformance.yaml | kubectl apply -f - >/dev/null"
  on_host "rc=0; kubectl -n tenant-a wait --for=condition=Complete job/fiberd-conformance --timeout=600s || rc=\$?; kubectl -n tenant-a logs job/fiberd-conformance --tail=40; exit \$rc"
}

# Orka's workspace provider E2E setup: the official Substrate pin, its
# ate-system, a gVisor WorkerPool, the orka-acp-infra ActorTemplate and Actor
# DNS, on a dedicated cluster. It reuses the shared kind-registry.
substrate_up() {
  local dir="${substrate_remote}/orka"
  ssh "${host}" "mkdir -p \$HOME/${dir}/scripts/lib \$HOME/${dir}/hack/agent-substrate"
  rsync -a "${orka_e2e_source}/scripts/workspace-substrate-cluster.sh" "${host}:${dir}/scripts/"
  rsync -a "${orka_e2e_source}/scripts/lib/substrate-upstream.sh" "${host}:${dir}/scripts/lib/"
  rsync -a "${orka_e2e_source}/hack/agent-substrate/upstream.env" "${host}:${dir}/hack/agent-substrate/"
  ssh "${host}" "cd \$HOME/${dir} && PATH=/home/linuxbrew/.linuxbrew/bin:\$PATH KIND_CLUSTER=${substrate_cluster} bash scripts/workspace-substrate-cluster.sh"
}

case "${1:-}" in
  build) build ;;
  image) image ;;
  up) up ;;
  home) home ;;
  stage1) shift; stage1 "$@" ;;
  stage2) stage2 ;;
  stage3) on_host "bash stage3.sh ${cluster}" ;;
  substrate-up) substrate_up ;;
  substrate) on_substrate "bash substrate.sh ${substrate_cluster}" ;;
  measure) on_cluster "$2" "bash measure.sh $2 ${3:-3} ${4:-4}" ;;
  footprint)
    node="${cluster}-control-plane"; [ "$2" = substrate ] && node="${substrate_cluster}-control-plane"
    on_cluster "$2" "bash measure/footprint.sh $2 ${node}" ;;
  memory) on_host "kubectl -n tenant-a exec -i orka-home-grant -- sh < measure/memory.sh" ;;
  baseline) on_host "bash measure/pod-baseline.sh" ;;
  k) shift; k "$@" ;;
  sh) shift; on_host "$*" ;;
  *) sed -n '2,20p' "$0" >&2; exit 2 ;;
esac
