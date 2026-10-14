#!/usr/bin/env bash
# Stage 3 on the remote host: install shared CRDs, the fiberd provider and
# Core with shared native workers, then run an actual Task. Run by spike.sh.
set -euo pipefail
cluster="$1"
run_id="$(date -u +%Y%m%d%H%M%S)"
out="results/stage3-${run_id}"
mkdir -p "${out}/private" && chmod 700 "${out}/private"
supervisor="docker.io/library/$(cat supervisor-image)"
kind load docker-image --name "${cluster}" orka-core:spike orka-workspace-fiberd:spike orka-fiber-home:spike >/dev/null

kubectl apply --server-side -k ws-config >/dev/null
kubectl apply --server-side -f fiberd-config/crd >/dev/null
kubectl apply --server-side -k core-config/crd >/dev/null
kubectl wait --for=condition=Established crd --all --timeout=120s >/dev/null

kubectl create namespace orka-workspace-system --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n fiberd-system get secret grant-issuer-key -o jsonpath='{.data.key\.json}' | base64 -d >"${out}/private/issuer-key.json"
kubectl -n orka-workspace-system create secret generic fiberd-issuer-key --from-file=key.json="${out}/private/issuer-key.json" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl kustomize fiberd-config | sed -e "s#orka-workspace-fiberd:dev#orka-workspace-fiberd:spike#" -e "s#SUPERVISOR_IMAGE#${supervisor}#" \
  | kubectl apply --server-side --force-conflicts -f - >/dev/null
kubectl -n orka-workspace-system patch deployment orka-workspace-fiberd --type=strategic \
  -p '{"spec":{"template":{"spec":{"containers":[{"name":"provider","imagePullPolicy":"Never"}]}}}}' >/dev/null
kubectl -n orka-workspace-system rollout restart deployment/orka-workspace-fiberd >/dev/null
kubectl -n orka-workspace-system rollout status deployment/orka-workspace-fiberd --timeout=180s

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
python3 core-install.py orka-core:spike "${supervisor}" "${out}/private/tls.crt" >"${out}/core-install.json"
kubectl apply -f "${out}/core-install.json" >/dev/null
kubectl -n orka-system rollout restart deployment/external-workspace-core >/dev/null
kubectl -n orka-system rollout status deployment/external-workspace-core --timeout=180s
echo "supervisor image: ${supervisor}"
./fiberd-core-proof --run-id "${run_id}" --report "${out}/core-proof.json"
cat "${out}/core-proof.json"
