#!/usr/bin/env bash
set -euo pipefail
context="$(kubectl config current-context)"
case "${context}" in
  kind-orka-workspace-*-external-su-*) ;;
  *) printf 'Refusing foreign DNS cluster: %s\n' "${context}" >&2; exit 1 ;;
esac
dns_ip="$(kubectl -n ate-system get service dns -o jsonpath='{.spec.clusterIP}')"
kubectl -n kube-system get configmap coredns -o json |
  python3 hack/external-substrate-e2e/configure-kind-dns.py "${dns_ip}" |
  kubectl -n kube-system patch configmap coredns --type=json --patch-file=/dev/stdin
corefile_hash="$(kubectl -n kube-system get configmap coredns -o jsonpath='{.data.Corefile}' | shasum -a 256 | cut -d ' ' -f 1)"
# Wait for both replicas to run the same mounted Corefile before any boot.
# Updating this isolated installation setting is fenced by UID/version.
patch="$(kubectl -n kube-system get deployment coredns -o json | jq -ce --arg hash "${corefile_hash}" '
  (.spec.template.metadata.annotations // {}) as $annotations |
  [{op:"test",path:"/metadata/uid",value:.metadata.uid},
   {op:"test",path:"/metadata/resourceVersion",value:.metadata.resourceVersion},
   {op:"add",path:"/spec/template/metadata/annotations",value:($annotations + {"substrate.workspace.e2e/dns-config":$hash})}]')"
kubectl -n kube-system patch deployment coredns --type=json -p "${patch}"
kubectl -n kube-system rollout status deployment/coredns --timeout=2m
