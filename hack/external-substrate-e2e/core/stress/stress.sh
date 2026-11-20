#!/usr/bin/env bash
# Concurrent start/suspend/resume/warm-attach stress for the Substrate provider (live validation only).
# Usage: stress.sh <out-dir> <first-round> <last-round> <gap-seconds...>
# Sessions: 4 shared + 4 private, half with a 15s and half with a 60s class warm window, so rounds
# overlap cold starts, window-expiry suspends, resumes from Tag and warm attaches. After every turn the
# driver writes a per-session, per-round marker into the still-warm sandbox and, on the next turn,
# verifies every earlier marker of that session (and that no other session's marker is present).
set -u
out=$1; first=$2; rounds=$3; shift 3; gaps=("$@")
NS=external-substrate-core
N=${KIND_NODE:-orka-workspace-66e25a-external-su-3034-control-plane}
mkdir -p "$out"; log="$out/stress.log"; exec > >(tee -a "$log") 2>&1
sessions=(sh15a:st-sh-15 sh15b:st-sh-15 sh60a:st-sh-60 sh60b:st-sh-60 pr15a:st-pr-15 pr15b:st-pr-15 pr60a:st-pr-60 pr60b:st-pr-60)
ts() { date -u +%H:%M:%S.%N | cut -c1-12; }
mktask() { # name session class create
  cat <<YAML | kubectl -n $NS create -f - -o name >/dev/null
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata: {name: $1}
spec:
  type: agent
  agentRef: {name: native-core}
  prompt: "Run the deterministic external native workspace fixture (stress turn)."
  timeout: 10m
  sessionRef: {name: $2, create: $4, append: true}
  execution: {workspace: {classRef: {name: $3}, reusePolicy: session, onDetach: Suspend}}
  agentRuntime: {maxTurns: 1, allowedTools: [Glob, Grep, Read], allowBash: false}
YAML
}
sx() { docker exec "$N" sandbox-exec.sh "$@"; }
turn() { # session class round
  local s=$1 c=$2 r=$3 t="st-$1-r$3" create=false; [ "$r" = 1 ] && create=true
  local t0; t0=$(ts); mktask "$t" "stress-$s" "$c" "$create"
  local phase=""; for i in $(seq 1 400); do phase=$(kubectl -n $NS get task "$t" -o jsonpath='{.status.phase}' 2>/dev/null); case "$phase" in Succeeded|Failed|Cancelled) break;; esac; sleep 1.5; done
  local ws uid seq wpod; ws=$(kubectl -n $NS get task "$t" -o jsonpath='{.status.executionWorkspace.workspaceRef.name}')
  read -r uid seq wpod pname <<<"$(kubectl -n $NS get executionworkspace "$ws" -o jsonpath='{.status.allocation.startup.process.uid} {.status.allocation.sequence} {.status.allocation.startup.process.worker.name} {.status.allocation.startup.process.name}')"
  local verify="skip" write="skip"
  if [ "$phase" = Succeeded ] && [ -n "$uid" ]; then
    # verify all earlier markers of this session, and that no foreign marker exists
    verify=$(sx "$uid" "cd /durable/orka-workspace 2>/dev/null || exit 0; bad=0; ok=0; for f in marker-*.bin; do [ -e \"\$f\" ] || continue; case \"\$f\" in marker-$s-r*) ;; *) echo FOREIGN:\$f; bad=1; continue;; esac; want=\$(cat \"\$f.sha\" 2>/dev/null); got=\$(sha256sum \"\$f\" | cut -d' ' -f1); [ \"\$want\" = \"\$got\" ] && ok=\$((ok+1)) || { echo MISMATCH:\$f; bad=1; }; done; echo ok=\$ok bad=\$bad" | tail -1)
    write=$(sx "$uid" "cd /durable/orka-workspace && head -c 2097152 /dev/urandom > .tmp-$s-r$r && sha256sum .tmp-$s-r$r | cut -d' ' -f1 > marker-$s-r$r.bin.sha && mv .tmp-$s-r$r marker-$s-r$r.bin && echo wrote" | tail -1)
  fi
  echo "TURN $(ts) start=$t0 task=$t phase=$phase ws=$ws seq=$seq actor=$pname uid=$uid workerPod=$wpod verify=[$verify] write=[$write]"
}
for r in $(seq "$first" "$rounds"); do
  echo "ROUND $r start $(ts) load=$(cut -d' ' -f1 /proc/loadavg)"
  pids=(); for x in "${sessions[@]}"; do turn "${x%%:*}" "${x#*:}" "$r" & pids+=($!); done; wait "${pids[@]}"
  echo "ROUND $r end $(ts)"
  g=${gaps[$((r-first))]:-0}; [ "$r" -lt "$rounds" ] && { echo "GAP ${g}s"; sleep "$g"; }
done
echo DONE
