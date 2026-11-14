#!/usr/bin/env python3
"""Assert one trace joins Core and supervisor GenAI spans for a proof prompt.

Usage:
  traces.py trace-ids <collector-debug-log> <report.json> <session-report.json>
  traces.py check <collector-debug-log> <report.json> <session-report.json> <router-traces-dir> <token-file> <out.json>

Parses the OpenTelemetry collector debug exporter (verbosity: detailed). For
each proof Task it looks for the supervisor's acp.supervisor.prompt span
carrying orka.task.uid and checks, in the same trace: Core's acp.prompt; a
Core substrate.router PUT child of it (the routed ACP prompt request); that
acp.supervisor.prompt is the child of this routed request, either directly or
through the atenet-router Envoy ingress span that is the request's child;
invoke_agent codex under acp.supervisor.prompt; and chat <model> under
invoke_agent. atenet-router reports its Envoy spans to the installation's own
trace backend, not to the proof's collector, so trace-ids lists the candidate
traces and run.sh fetches them (Jaeger JSON) into router-traces-dir; only
atenet-router-envoy spans are read from there. The single-Task phase's Task
and at least one Session turn must each have a complete trace. Neither the
collector log nor the router traces may contain prompt content (the fixture
markers) or the provider token. Writes only span names, IDs and service names.
"""
import json
import pathlib
import re
import sys

SUPERVISOR = "orka-acp-supervisor"
MODEL = "gpt-5.4"
# The debug exporter's first line of each batch carries the log timestamp and
# level ("2026-01-02T03:04:05.678Z<TAB>info<TAB>ResourceSpans #0").
PREFIX = re.compile(r"^\d{4}-\d\d-\d\dT\S+\s+(?:info|debug|warn|error)\s+")


def parse(text):
    spans, service, current = [], "", None
    for line in text.splitlines():
        line = line.strip()
        # Debug exporter lines may carry a log prefix; keep the payload.
        line = PREFIX.sub("", line)
        if line.startswith("ResourceSpans #"):
            service, current = "", None
            continue
        if line.startswith("Span #"):
            current = {"service": service, "attrs": {}}
            spans.append(current)
            continue
        if line.startswith(("ScopeSpans #", "Metric #", "ResourceMetrics #")):
            current = None
            continue
        match = re.match(r"^->\s*([^:]+):\s*\w+\((.*)\)$", line)
        if match:
            key, value = match.group(1).strip(), match.group(2)
            if current is None and key == "service.name":
                service = value
            elif current is not None:
                current["attrs"][key] = value
            continue
        if current is None:
            continue
        key, sep, value = line.partition(":")
        if not sep:
            continue
        key, value = key.strip(), value.strip()
        if key == "Trace ID":
            current["trace"] = value
        elif key == "Parent ID":
            current["parent"] = value
        elif key == "ID":
            current["id"] = value
        elif key == "Name":
            current["name"] = value
    return [span for span in spans if span.get("trace") and span.get("id") and span.get("name")]


ROUTER_ENVOY = "atenet-router-envoy"


def router_spans(directory):
    """atenet-router Envoy spans from the Jaeger trace JSON files in directory."""
    spans = []
    for path in sorted(pathlib.Path(directory).glob("*.json")):
        for trace in json.loads(path.read_text()).get("data") or []:
            processes = trace.get("processes") or {}
            for span in trace.get("spans") or []:
                service = (processes.get(span.get("processID")) or {}).get("serviceName", "")
                if service != ROUTER_ENVOY:
                    continue
                parents = [r.get("spanID") for r in span.get("references") or [] if r.get("refType") == "CHILD_OF"]
                spans.append({"service": service, "name": span.get("operationName", ""), "trace": span.get("traceID", ""),
                              "id": span.get("spanID", ""), "parent": parents[0] if parents else "", "attrs": {}})
    return spans


def prompt_spans(spans, task_uid):
    return [s for s in spans if s["name"] == "acp.supervisor.prompt" and s["service"] == SUPERVISOR and s["attrs"].get("orka.task.uid") == task_uid]


def chain(spans, routed, task_uid):
    result = {"taskUID": task_uid, "complete": False}
    prompts = prompt_spans(spans, task_uid)
    if not prompts:
        result["missing"] = ["acp.supervisor.prompt"]
        return result
    best = None
    for prompt in prompts:
        trace = [s for s in spans if s["trace"] == prompt["trace"]]
        core = [s for s in trace if s["service"] not in (SUPERVISOR, ROUTER_ENVOY)]
        prompt_ids = {s["id"] for s in core if s["name"] == "acp.prompt"}
        requests = {s["id"] for s in core if s["name"] == "substrate.router PUT" and s.get("parent") in prompt_ids}
        ingress = {s["id"] for s in routed if s["trace"] == prompt["trace"] and s["name"] == "ingress" and s.get("parent") in requests}
        via = "direct" if prompt.get("parent") in requests else ROUTER_ENVOY + " ingress" if prompt.get("parent") in ingress else ""
        invoke = [s for s in trace if s["name"] == "invoke_agent codex" and s["service"] == SUPERVISOR and s.get("parent") == prompt["id"]]
        chat = [s for s in trace if s["name"] == "chat " + MODEL and s["service"] == SUPERVISOR and any(s.get("parent") == i["id"] for i in invoke)]
        found = {
            "acp.prompt": bool(prompt_ids),
            "substrate.router PUT under acp.prompt": bool(requests),
            "acp.supervisor.prompt under the routed request": bool(via),
            "invoke_agent codex": bool(invoke),
            "chat " + MODEL: bool(chat),
        }
        candidate = {"taskUID": task_uid, "traceID": prompt["trace"], "complete": all(found.values()), "via": via,
                     "missing": [name for name, ok in found.items() if not ok],
                     "spans": sorted({(s["service"], s["name"]) for s in trace + [r for r in routed if r["trace"] == prompt["trace"]]})}
        if best is None or len(candidate["missing"]) < len(best["missing"]):
            best = candidate
    best["spans"] = [{"service": service, "name": name} for service, name in best["spans"]]
    return best


def supervisor_metrics(text):
    """Names of metrics the supervisor exported: evidence its OTLP export
    reaches the collector through the egress gateway."""
    names, service, in_metrics = set(), "", False
    for line in text.splitlines():
        line = PREFIX.sub("", line.strip())
        if line.startswith("ResourceMetrics #"):
            service, in_metrics = "", True
        elif line.startswith("ResourceSpans #"):
            in_metrics = False
        match = re.match(r"^->\s*service\.name:\s*\w+\((.*)\)$", line)
        if match and in_metrics:
            service = match.group(1)
        if in_metrics and service == SUPERVISOR and line.startswith("-> Name:"):
            names.add(line.split(":", 1)[1].strip())
    return sorted(names)


def task_uids(report_path, session_path):
    report = json.loads(pathlib.Path(report_path).read_text())
    session = json.loads(pathlib.Path(session_path).read_text())
    # Turn 1's worker is evicted while its prompt is held; turn 2 cold-resumes.
    return report["taskUID"], {name: session[name]["taskUID"] for name in ("turn1", "turn2")}


def main():
    mode, args = sys.argv[1], sys.argv[2:]
    if mode == "trace-ids":
        log_path, report_path, session_path = args
        spans = parse(pathlib.Path(log_path).read_text(errors="replace"))
        task, turns = task_uids(report_path, session_path)
        ids = {s["trace"] for uid in [task, *turns.values()] for s in prompt_spans(spans, uid)}
        for trace_id in sorted(ids):
            if re.fullmatch(r"[0-9a-f]{32}", trace_id):
                print(trace_id)
        return
    if mode != "check":
        sys.exit("unknown mode %r" % mode)
    log_path, report_path, session_path, router_dir, token_path, out_path = args
    text = pathlib.Path(log_path).read_text(errors="replace")
    router_text = "".join(path.read_text(errors="replace") for path in sorted(pathlib.Path(router_dir).glob("*.json")))
    token = pathlib.Path(token_path).read_text().strip()
    if token and (token in text or token in router_text):
        sys.exit("telemetry received the provider token")
    if "orka-fixture:" in text or "orka-fixture:" in router_text:
        sys.exit("telemetry received prompt content")
    spans = parse(text)
    routed = router_spans(router_dir)
    task_uid, turn_uids = task_uids(report_path, session_path)
    task = chain(spans, routed, task_uid)
    turns = {name: chain(spans, routed, uid) for name, uid in turn_uids.items()}
    result = {"spansParsed": len(spans), "routerSpansRead": len(routed), "services": sorted({s["service"] for s in spans + routed}),
              "task": task, "session": turns,
              "supervisorMetrics": supervisor_metrics(text), "contentCaptured": False, "providerTokenInTelemetry": False}
    pathlib.Path(out_path).write_text(json.dumps(result, indent=2) + "\n")
    if not task["complete"]:
        sys.exit("the single Task produced no trace joining Core and supervisor GenAI spans: %s" % json.dumps(task))
    if not any(turn["complete"] for turn in turns.values()):
        sys.exit("no Session turn produced a trace joining Core and supervisor GenAI spans: %s" % json.dumps(turns))


if __name__ == "__main__":
    main()
