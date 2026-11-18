#!/usr/bin/env bash
# Called only by kindctl exec --tag external-substrate from the shared repo,
# directly or from a failing proof. Collects redacted failure evidence:
# status of the Orka/workspace objects (never their specs), deployment logs and
# events. It never reads Pod specs or Secrets and never writes under private/.
set -uo pipefail
out="${1:-${PWD}/bin/diagnostics-$(date -u +%Y%m%d%H%M%S)}"
redact_file="${2:-}"
# Bounded so a stalled API server cannot hold a caller's EXIT handler: every
# API request times out after 15 seconds, and the whole collection after
# DIAGNOSTICS_DEADLINE_SECONDS (default 300). Evidence is written file by file,
# so a deadline keeps what was collected and returns control to the caller.
if [[ -z "${ORKA_DIAGNOSTICS_BOUNDED:-}" ]] && command -v timeout >/dev/null 2>&1; then
  ORKA_DIAGNOSTICS_BOUNDED=1 timeout --kill-after=10s "${DIAGNOSTICS_DEADLINE_SECONDS:-300}" bash "${BASH_SOURCE[0]}" "${out}" "${redact_file}"
  code=$?
  if (( code == 124 || code == 137 )); then
    printf 'Diagnostics deadline reached; partial evidence kept in %s\n' "${out}" >&2
  fi
  exit "${code}"
fi
kubectl() { command kubectl --request-timeout=15s "$@"; }
case "$(kubectl config current-context)" in
  kind-orka-workspace-*-external-su-*) ;;
  *) printf 'Refusing unexpected scoped cluster context\n' >&2; exit 1 ;;
esac
case "${out}" in */private|*/private/*) printf 'Refusing to write diagnostics under private/\n' >&2; exit 1 ;; esac
mkdir -p "${out}"
chmod 700 "${out}"

# redact replaces bearer values, JWTs, PEM blocks and, when given, the exact
# fixture token file's value before anything is written.
redact() {
  python3 -I -c '
import pathlib, re, sys
text = sys.stdin.read()
if len(sys.argv) > 1 and sys.argv[1]:
    try:
        secret = pathlib.Path(sys.argv[1]).read_text().strip()
    except OSError:
        secret = ""
    if secret:
        text = text.replace(secret, "[REDACTED]")
text = re.sub(r"-----BEGIN [A-Z ]+-----.*?-----END [A-Z ]+-----", "[REDACTED PEM]", text, flags=re.S)
text = re.sub(r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}", "[REDACTED JWT]", text)
text = re.sub(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]{8,}", r"\1[REDACTED]", text)
text = re.sub(r"(?i)((?:token|secret|password|api[_-]?key)\"?\s*[:=]\s*\"?)[A-Za-z0-9._~+/=-]{16,}", r"\1[REDACTED]", text)
sys.stdout.write(text)
' "${redact_file}"
}

status_only() {
  local file="$1"; shift
  kubectl "$@" -o json 2>&1 | jq '[.items[]? | {kind, metadata: {namespace: .metadata.namespace, name: .metadata.name, uid: .metadata.uid, generation: .metadata.generation, deletionTimestamp: .metadata.deletionTimestamp, evictionRecord: .metadata.annotations["orka.ai/external-runtime-eviction"]}, status}]' 2>&1 | redact >"${out}/${file}"
}

namespaces="$(kubectl get namespaces -o json | jq -r '.items[].metadata.name | select(test("^external-substrate-(core|proof-[0-9]+)$"))')"
status_only cluster-classes.json get executionworkspaceproviders.workspace.orka.ai
for ns in ${namespaces}; do
  status_only "${ns}-tasks.json" -n "${ns}" get tasks.core.orka.ai
  status_only "${ns}-runtimepools.json" -n "${ns}" get runtimepools.core.orka.ai
  status_only "${ns}-executionworkspaces.json" -n "${ns}" get executionworkspaces.workspace.orka.ai
  status_only "${ns}-executionworkspaceclasses.json" -n "${ns}" get executionworkspaceclasses.workspace.orka.ai
  status_only "${ns}-checkpoints.json" -n "${ns}" get executionworkspacecheckpoints.workspace.orka.ai
  status_only "${ns}-workerpools.json" -n "${ns}" get workerpools.ate.dev
  kubectl -n "${ns}" get pods -o wide 2>&1 | redact >"${out}/${ns}-pods.txt"
  kubectl -n "${ns}" get events --sort-by=.lastTimestamp 2>&1 | redact >"${out}/${ns}-events.txt"
done
status_only proof-jobs.json -n external-substrate-e2e get jobs
kubectl -n external-substrate-e2e get events --sort-by=.lastTimestamp 2>&1 | redact >"${out}/external-substrate-e2e-events.txt"

log() {
  local ns="$1" deployment="$2"
  kubectl -n "${ns}" get deployment "${deployment}" >/dev/null 2>&1 || return 0
  kubectl -n "${ns}" logs "deployment/${deployment}" --all-containers --prefix --tail=3000 2>&1 | redact >"${out}/logs-${ns}-${deployment}.txt"
}
for deployment in ate-api-server ate-controller atenet-router atenet-egress k8s-credential-provider; do
  log ate-system "${deployment}"
done
log orka-workspace-system orka-workspace-substrate
log external-substrate-core external-substrate-core
log external-substrate-core fixture-provider-proxy
for job in $(kubectl -n external-substrate-e2e get jobs -o name 2>/dev/null); do
  kubectl -n external-substrate-e2e logs "${job}" --tail=3000 2>&1 | redact >"${out}/logs-external-substrate-e2e-${job#job.batch/}.txt"
done
printf 'Redacted diagnostics: %s\n' "${out}"
