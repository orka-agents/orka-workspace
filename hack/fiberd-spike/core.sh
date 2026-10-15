#!/usr/bin/env bash
# Install the fixture Core for one provider on the current KUBECONFIG's
# cluster: core.sh <fiberd|substrate> <runtime image> <output dir>.
# Run by stage3.sh and substrate.sh on the remote host.
set -euo pipefail
provider="$1" runtime="$2" out="$3"
mkdir -p "${out}/private" && chmod 700 "${out}/private"
kubectl create namespace orka-system --dry-run=client -o yaml | kubectl apply -f - >/dev/null
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=external-workspace-core.orka-system.svc \
  -addext subjectAltName=DNS:external-workspace-core.orka-system.svc,DNS:external-workspace-core.orka-system.svc.cluster.local \
  -keyout "${out}/private/tls.key" -out "${out}/private/tls.crt" 2>/dev/null
openssl rand 32 >"${out}/private/key"
openssl rand -hex 32 >"${out}/private/token"
kubectl -n orka-system create secret generic external-workspace-core-fixture --from-file="${out}/private/key" \
  --from-file="${out}/private/token" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n orka-system create secret tls external-workspace-core-tls --key="${out}/private/tls.key" \
  --cert="${out}/private/tls.crt" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
sed 's/name: manager-role$/name: external-workspace-core/' core-config/role.yaml | kubectl apply -f - >/dev/null
python3 core-install.py "${provider}" orka-core:spike "${runtime}" "${out}/private/tls.crt" >"${out}/core-install.json"
kubectl apply -f "${out}/core-install.json" >/dev/null
kubectl -n orka-system rollout restart deployment/external-workspace-core >/dev/null
kubectl -n orka-system rollout status deployment/external-workspace-core --timeout=180s
