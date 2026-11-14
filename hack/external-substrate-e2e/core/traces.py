#!/usr/bin/env python3
"""Assert one trace joins Core and supervisor GenAI spans for a proof prompt.

Usage: traces.py <collector-debug-log> <report.json> <session-report.json> <token-file> <out.json>

Parses the OpenTelemetry collector debug exporter (verbosity: detailed). For
each proof Task it looks for the supervisor's acp.supervisor.prompt span
carrying orka.task.uid and checks, in the same trace, Core's acp.prompt and
substrate.router POST spans, acp.supervisor.prompt parented by a Core span,
invoke_agent codex under it and chat <model> under invoke_agent. The
single-Task phase's Task and at least one Session turn (both run with
telemetry on suspendable and non-suspendable native runtimes) must each have
a complete trace. The log must contain neither prompt content (the fixture
markers) nor the provider token. Writes only span names, IDs and service
names.
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


def chain(spans, task_uid):
    result = {"taskUID": task_uid, "complete": False}
    prompts = [s for s in spans if s["name"] == "acp.supervisor.prompt" and s["service"] == SUPERVISOR and s["attrs"].get("orka.task.uid") == task_uid]
    if not prompts:
        result["missing"] = ["acp.supervisor.prompt"]
        return result
    best = None
    for prompt in prompts:
        trace = [s for s in spans if s["trace"] == prompt["trace"]]
        core = [s for s in trace if s["service"] != SUPERVISOR]
        core_ids = {s["id"] for s in core}
        invoke = [s for s in trace if s["name"] == "invoke_agent codex" and s["service"] == SUPERVISOR and s.get("parent") == prompt["id"]]
        chat = [s for s in trace if s["name"] == "chat " + MODEL and s["service"] == SUPERVISOR and any(s.get("parent") == i["id"] for i in invoke)]
        found = {
            "acp.prompt": any(s["name"] == "acp.prompt" for s in core),
            "substrate.router POST": any(s["name"] == "substrate.router POST" for s in core),
            "acp.supervisor.prompt under a Core span": prompt.get("parent") in core_ids,
            "invoke_agent codex": bool(invoke),
            "chat " + MODEL: bool(chat),
        }
        candidate = {"taskUID": task_uid, "traceID": prompt["trace"], "complete": all(found.values()),
                     "missing": [name for name, ok in found.items() if not ok],
                     "spans": sorted({(s["service"], s["name"]) for s in trace})}
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


def main():
    log_path, report_path, session_path, token_path, out_path = sys.argv[1:]
    text = pathlib.Path(log_path).read_text(errors="replace")
    token = pathlib.Path(token_path).read_text().strip()
    if token and token in text:
        sys.exit("collector received the provider token")
    if "orka-fixture:" in text:
        sys.exit("collector received prompt content")
    report = json.loads(pathlib.Path(report_path).read_text())
    session = json.loads(pathlib.Path(session_path).read_text())
    spans = parse(text)
    task = chain(spans, report["taskUID"])
    # Turn 1's worker is evicted while its prompt is held; turn 2 cold-resumes.
    turns = {name: chain(spans, session[name]["taskUID"]) for name in ("turn1", "turn2")}
    result = {"spansParsed": len(spans), "services": sorted({s["service"] for s in spans}), "task": task, "session": turns,
              "supervisorMetrics": supervisor_metrics(text), "contentCaptured": False, "providerTokenInTelemetry": False}
    pathlib.Path(out_path).write_text(json.dumps(result, indent=2) + "\n")
    if not task["complete"]:
        sys.exit("the single Task produced no trace joining Core and supervisor GenAI spans: %s" % json.dumps(task))
    if not any(turn["complete"] for turn in turns.values()):
        sys.exit("no Session turn produced a trace joining Core and supervisor GenAI spans: %s" % json.dumps(turns))


if __name__ == "__main__":
    main()
