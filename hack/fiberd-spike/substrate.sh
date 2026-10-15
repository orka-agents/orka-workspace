#!/usr/bin/env bash
# Install the Substrate provider and the fixture Core on the cluster that
# Orka's scripts/workspace-substrate-cluster.sh prepared, then run one Task.
# The supervisor is the runtime image the fiberd home's rootfs is built from,
# served to Substrate workers from the shared kind-registry. Run by spike.sh.
set -euo pipefail
cluster="$1"
run_id="$(date -u +%Y%m%d%H%M%S)"
out="results/substrate-${run_id}"
mkdir -p "${out}/private" && chmod 700 "${out}/private"
registry_ip="$(docker inspect -f '{{with index .NetworkSettings.Networks "kind"}}{{.IPAddress}}{{end}}' kind-registry)"
docker tag orka-acp-runtime:spike localhost:5001/orka-acp-runtime:spike
digest="$(docker push localhost:5001/orka-acp-runtime:spike | awk '/digest:/ {print $3}')"
runtime="${registry_ip}:5000/orka-acp-runtime@${digest}"
kind load docker-image --name "${cluster}" orka-core:spike orka-workspace-substrate:spike >/dev/null

kubectl apply --server-side -k ws-config >/dev/null
kubectl apply --server-side -f substrate-config/crd >/dev/null
kubectl apply --server-side -k core-config/crd >/dev/null
kubectl wait --for=condition=Established crd --all --timeout=120s >/dev/null

# As Orka's workspace provider E2E deploys it: a native Pod certificate,
# the native server trust bundle and direct Actor egress.
kubectl kustomize substrate-config | sed -e "s#orka-workspace-substrate:dev#orka-workspace-substrate:spike#" \
  | kubectl apply --server-side --force-conflicts -f - >/dev/null
kubectl -n orka-workspace-system patch deployment orka-workspace-substrate --type=json -p '[
  {"op": "add", "path": "/spec/template/spec/containers/0/imagePullPolicy", "value": "Never"},
  {"op": "add", "path": "/spec/template/spec/securityContext/runAsUser", "value": 65532},
  {"op": "add", "path": "/spec/template/spec/securityContext/runAsGroup", "value": 65532},
  {"op": "add", "path": "/spec/template/spec/securityContext/fsGroup", "value": 65532},
  {"op": "replace", "path": "/spec/template/spec/containers/0/args", "value": [
    "--leader-elect=true", "--leader-election-namespace=orka-workspace-system",
    "--native-api-endpoint=api.ate-system.svc:443", "--native-ca-file=/native/server/trust-bundle.pem",
    "--native-cert-file=/native/client/credential-bundle.pem", "--native-key-file=/native/client/credential-bundle.pem",
    "--native-direct-egress=true", "--actor-dns-suffix=actors.resources.substrate.ate.dev"]},
  {"op": "replace", "path": "/spec/template/spec/containers/0/volumeMounts", "value": [
    {"name": "native-client", "mountPath": "/native/client", "readOnly": true},
    {"name": "native-server", "mountPath": "/native/server", "readOnly": true}]},
  {"op": "replace", "path": "/spec/template/spec/volumes", "value": [
    {"name": "native-client", "projected": {"sources": [{"podCertificate": {"signerName": "podidentity.podcert.ate.dev/identity",
      "keyType": "ECDSAP256", "credentialBundlePath": "credential-bundle.pem"}}]}},
    {"name": "native-server", "projected": {"sources": [{"clusterTrustBundle": {"signerName": "servicedns.podcert.ate.dev/identity",
      "path": "trust-bundle.pem", "labelSelector": {"matchLabels": {"podcert.ate.dev/canarying": "live"}}}}]}}]}
]' >/dev/null
kubectl -n orka-workspace-system rollout status deployment/orka-workspace-substrate --timeout=180s

bash core.sh substrate "${runtime}" "${out}"
echo "supervisor image: ${runtime}"
./fiberd-core-proof --provider substrate --run-id "${run_id}" --report "${out}/core-proof.json"
cat "${out}/core-proof.json"
