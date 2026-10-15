#!/usr/bin/env bash
# footprint.sh <fiberd|substrate> <kind node>: run one actual Core Task and
# sample, from the kind node's cgroups, the memory of every cgroup it added:
# fiberd's new grant inside the home, or Substrate's private worker Pod.
# Reports each one's peak and what it still holds after retirement.
set -euo pipefail
provider="$1" node="$2"
run_id="$(date -u +%Y%m%d%H%M%S)"
out="results/footprint-${provider}-${run_id}"
mkdir -p "${out}"
pods=/sys/fs/cgroup/kubelet.slice/kubelet-kubepods.slice
if [ "${provider}" = fiberd ]; then
  uid="$(kubectl -n tenant-a get pod orka-home-grant -o jsonpath='{.metadata.uid}' | tr - _)"
  glob="${pods}/*/*pod${uid}.slice/*/grants/*/"
else
  glob="${pods}/*/*pod*.slice/"
fi
docker exec "${node}" sh -c "rm -f /tmp/footprint-stop; ls -d ${glob} > /tmp/footprint-before 2>/dev/null || true"
docker exec "${node}" sh -c "while [ ! -f /tmp/footprint-stop ]; do for d in ${glob}; do
    [ -f \$d/memory.current ] && ! grep -qxF \$d /tmp/footprint-before &&
      echo \$d \$(cat \$d/memory.current) \$(awk '/^anon /{print \$2}' \$d/memory.stat)
  done; sleep 0.25; done" >"${out}/samples" 2>/dev/null &
sampler=$!
./fiberd-core-proof --provider "${provider}" --run-id "fp-${run_id}" --report "${out}/core-proof.json" >"${out}/proof.log" 2>&1 || echo "proof failed"
sleep 2
docker exec "${node}" touch /tmp/footprint-stop
wait "${sampler}" || true
retained="$(docker exec -i "${node}" sh -c "for d in \$(awk '{print \$1}' /dev/stdin | sort -u); do [ -f \$d/memory.current ] && echo \$d \$(cat \$d/memory.current); done" <"${out}/samples" || true)"
awk -v retained="${retained}" '
  BEGIN { n = split(retained, r, "\n"); for (i = 1; i <= n; i++) { split(r[i], f, " "); if (f[1] != "") held[f[1]] = f[2] } }
  { if ($2 > peak[$1]) peak[$1] = $2; if ($3 > anon[$1]) anon[$1] = $3 }
  END { for (d in peak) printf "{\"cgroup\":\"%s\",\"peakMiB\":%.1f,\"peakAnonMiB\":%.1f,\"retainedMiB\":%.1f}\n", d, peak[d]/1048576, anon[d]/1048576, held[d]/1048576 }
' "${out}/samples" | tee "${out}/footprint.jsonl"
jq -c '.timingsMS' "${out}/core-proof.json" 2>/dev/null || true
