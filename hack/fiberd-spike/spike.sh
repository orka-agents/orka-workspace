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
fiberd_source="${FIBERD_SOURCE:-${repo_root}/../fiberd}"
arch="${SPIKE_ARCH:-amd64}"
bin="${repo_root}/bin/fiberd-spike"

on_host() { ssh "${host}" "export PATH=/home/linuxbrew/.linuxbrew/bin:\$PATH KUBECONFIG=\$HOME/.kube/kind/${cluster}.kubeconfig; cd \$HOME/${remote} && $*"; }
k() { on_host kubectl "$(printf '%q ' "$@")"; }

build() {
  mkdir -p "${bin}/home"
  local out="${bin}/home/bin"
  mkdir -p "${out}"
  export CGO_ENABLED=0 GOOS=linux GOARCH="${arch}"
  go -C "${core_source}" build -trimpath -buildvcs=false -o "${out}/" ./cmd/orka-acp-runtime ./cmd/orka-acp-exec-helper
  go -C "${repo_root}" build -trimpath -buildvcs=false -o "${out}/acp-agent" ./hack/external-workspace-e2e/acp-agent
  go -C "${repo_root}/providers" build -trimpath -buildvcs=false -o "${out}/" ./fiberd/cmd/orka-fiber-shim ./fiberd/cmd/orka-fiber-egress
  go -C "${script_dir}" build -trimpath -buildvcs=false -o "${out}/fiberd-spike-check" ./check
  cp "${script_dir}/home/Dockerfile" "${script_dir}/home/fiberd-k8s-wrapper.sh" "${bin}/home/"
  ssh "${host}" "mkdir -p \$HOME/${remote}"
  rsync -a --delete "${bin}/home/" "${host}:${remote}/home/"
  rsync -a --delete "${script_dir}/manifests/" "${host}:${remote}/manifests/"
  rsync -a --delete "${script_dir}/measure/" "${host}:${remote}/measure/"
  rsync -a --delete --exclude .git --exclude bin "${fiberd_source}/" "${host}:${remote}/fiberd/"
}

image() {
  on_host "docker build -q -t fiberd:kind -f fiberd/docker/kubernetes/Dockerfile fiberd"
  on_host "docker build -q -t orka-fiber-home:spike home"
}

up() {
  if ! on_host "kind get clusters" | grep -qx "${cluster}"; then
    on_host "mkdir -p \$HOME/.kube/kind && kind create cluster --name ${cluster} --image kindest/node:v1.37.0 --kubeconfig \$KUBECONFIG --wait 120s"
  fi
  on_host "kind load docker-image --name ${cluster} fiberd:kind orka-fiber-home:spike"
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

case "${1:-}" in
  build) build ;;
  image) image ;;
  up) up ;;
  home) home ;;
  stage1) shift; stage1 "$@" ;;
  memory) on_host "kubectl -n tenant-a exec -i orka-home-grant -- sh < measure/memory.sh" ;;
  baseline) on_host "bash measure/pod-baseline.sh" ;;
  k) shift; k "$@" ;;
  sh) shift; on_host "$*" ;;
  *) sed -n '2,14p' "$0" >&2; exit 2 ;;
esac
