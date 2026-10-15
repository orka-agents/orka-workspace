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

bash core.sh fiberd "${supervisor}" "${out}"
echo "supervisor image: ${supervisor}"
./fiberd-core-proof --provider fiberd --run-id "${run_id}" --report "${out}/core-proof.json"
cat "${out}/core-proof.json"
