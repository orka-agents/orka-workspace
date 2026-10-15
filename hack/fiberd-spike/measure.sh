#!/usr/bin/env bash
# Time actual Core Tasks through an installed provider:
#   measure.sh <fiberd|substrate> <sequential runs> <burst size>
# Core restarts first, so the first sequential run is cold. The burst
# creates that many Tasks at once. Run by spike.sh after stage3/substrate.
set -euo pipefail
provider="$1" runs="$2" burst="$3"
batch="$(date -u +%Y%m%d%H%M%S)"
out="results/measure-${provider}-${batch}"
mkdir -p "${out}"
kubectl -n orka-system rollout restart deployment/external-workspace-core >/dev/null
kubectl -n orka-system rollout status deployment/external-workspace-core --timeout=180s >/dev/null
for i in $(seq 1 "${runs}"); do
  ./fiberd-core-proof --provider "${provider}" --run-id "${batch}-s${i}" --report "${out}/seq-${i}.json" >"${out}/seq-${i}.log" 2>&1 ||
    { echo "sequential run ${i} failed"; tail -3 "${out}/seq-${i}.log"; }
done
pids=()
for i in $(seq 1 "${burst}"); do
  ./fiberd-core-proof --provider "${provider}" --run-id "${batch}-b${i}" --report "${out}/burst-${i}.json" >"${out}/burst-${i}.log" 2>&1 &
  pids+=("$!")
done
failed=0
for pid in "${pids[@]}"; do wait "${pid}" || failed=$((failed + 1)); done
[ "${failed}" = 0 ] || echo "${failed} burst runs failed"
# Delete this batch's objects while Core still holds their records; Core's
# fixture store is an emptyDir, so Tasks left across a restart never finalize.
kind=fiberdpoolparameters; [ "${provider}" = substrate ] && kind=substrateworkspaceprofiles
names="$(kubectl -n orka-system get tasks -o name | grep -F "core-${batch}-" | sed 's#.*/##' || true)"
for name in ${names}; do
  kubectl -n orka-system delete task "${name}" --wait=true --timeout=120s >/dev/null || echo "task ${name} did not finalize"
  kubectl -n orka-system delete agent,executionworkspaceclass,"${kind}" "${name}" --ignore-not-found --wait=true --timeout=60s >/dev/null || true
done
for f in "${out}"/seq-*.json "${out}"/burst-*.json; do
  [ -f "$f" ] && jq -c --arg run "$(basename "$f" .json)" '{run: $run} + .timingsMS' "$f"
done | tee "${out}/timings.jsonl"
