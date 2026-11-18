#!/usr/bin/env python3
"""Join a core/latency.sh run into report.json and summary.md.

Inputs in the run directory: latency.log (measurement Job), core-timings.log,
provider-timings.log and workers/*.log (timing lines only). Every turn is
matched to Core's "ACP turn timings" by Task name, to Core's "ACP workspace
runtime started" by RuntimePool and workload sequence, to the provider's
"Substrate runtime ready" by workspace and sequence, and to the supervisor's
"ACP runtime session created" by RuntimeSession ID. Every suspension is
matched to the "stopped" lines of its sequence.
"""
import json
import pathlib
import re
import statistics
import sys

run = pathlib.Path(sys.argv[1])


def fields(line):
    """Return the structured fields of a zap (JSON or console) or slog line."""
    line = re.sub(r'^\[[^\]]*\]\s*', '', line.strip())
    if line.startswith('{'):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            pass
    brace = line.find('{')
    if brace >= 0:
        try:
            value = json.loads(line[brace:])
            value.setdefault('msg', line[:brace].split('\t')[-2] if '\t' in line[:brace] else line[:brace].strip())
            return value
        except json.JSONDecodeError:
            pass
    value = {}
    for key, quoted, bare in re.findall(r'([A-Za-z_][\w.]*)=(?:"((?:[^"\\]|\\.)*)"|(\S*))', line):
        value[key] = quoted if quoted or bare == '' else bare
    return value


def number(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


def lines(path):
    if not path.exists():
        return []
    return [fields(line) for line in path.read_text().splitlines() if line.strip()]


def message(entry):
    return entry.get('msg') or entry.get('message') or ''


core = lines(run / 'core-timings.log')
provider = lines(run / 'provider-timings.log')
supervisor = [entry for path in sorted((run / 'workers').glob('*.log')) for entry in lines(path)]
events, final = [], None
for line in (run / 'latency.log').read_text().splitlines():
    if line.startswith('LATENCY_EVENT '):
        events.append(json.loads(line[len('LATENCY_EVENT '):]))
    elif line.startswith('NATIVE_CORE_LATENCY '):
        final = json.loads(line[len('NATIVE_CORE_LATENCY '):])


def ms_fields(entry, prefix):
    return {prefix + key: number(value) for key, value in entry.items() if key.endswith('Ms') and number(value) is not None}


def find(entries, text, **match):
    found = []
    for entry in entries:
        if text not in message(entry):
            continue
        if all(str(entry.get(key)) == str(value) for key, value in match.items()):
            found.append(entry)
    return found


def turn_sample(event):
    task, after = event['task'], event['workspaceAfter']
    sample = {'clientAcceptedMs': task['acceptedMs'], 'clientDoneMs': task['doneMs'], 'clientSettledMs': task.get('settledMs')}
    timings = [entry for entry in find(core, 'ACP turn timings', task=task['name']) if str(entry.get('promptAccepted')).lower() == 'true']
    if timings:
        sample.update(ms_fields(timings[-1], 'turn.'))
    pool, sequence = task['identity']['runtimePoolName'], after['sequence']
    before = event.get('workspaceBefore') or {}
    started_new = before.get('sequence') != sequence or event['turn'] == 't1'
    evidence = {'sequence': sequence, 'newSequence': started_new}
    if started_new:
        started = find(core, 'ACP workspace runtime started', runtimePool=pool, sequence=sequence)
        if started:
            sample.update(ms_fields(started[-1], 'coreStart.'))
            evidence['corePath'] = started[-1].get('path')
        ready = find(provider, 'Substrate runtime ready', workspaceName=task['workspace'], sequence=sequence)
        if ready:
            sample.update(ms_fields(ready[-1], 'providerReady.'))
            evidence['providerPath'] = ready[-1].get('path')
    session = task['identity']['runtimeSessionUID']
    created = [entry for entry in supervisor if 'ACP runtime session created' in message(entry) and session and session in str(entry.get('runtimeSessionID', ''))]
    if created:
        sample.update(ms_fields(created[-1], 'supervisor.'))
    return {key: value for key, value in sample.items() if value is not None}, evidence


def stop_sample(event, sequence):
    snapshot = event['snapshot']
    sample = {}
    pool = (snapshot.get('runtimePool') or {})
    stopped = [entry for entry in find(core, 'ACP workspace runtime stopped', sequence=sequence) if entry.get('action') == 'suspend']
    stopped = [entry for entry in stopped if not pool or entry.get('runtimePool', '').startswith('acp-ws')]
    workspace_stopped = find(provider, 'Substrate runtime stopped', workspaceName=event['workspace'], sequence=sequence)
    pools = {entry.get('runtimePool') for entry in workspace_stopped if entry.get('runtimePool')}
    stopped = [entry for entry in stopped if not pools or entry.get('runtimePool') in pools]
    if stopped:
        sample.update(ms_fields(stopped[-1], 'coreStop.'))
    if workspace_stopped:
        sample.update(ms_fields(workspace_stopped[-1], 'providerStop.'))
    for key in ('observedAfterSettleMs', 'observedAfterDeadlineMs'):
        if key in event:
            sample['client.' + key] = event[key]
    return sample


groups = {'cold (turn 1)': [], 'resume from Tag, window unset (turn 2)': [], 'warm-window follow-up (turn 2)': [],
          'resume from Tag after window expiry (turn 3)': []}
stops = {'suspend on detach, window unset': [], 'suspend at window expiry': []}
evidence = []
last_sequence = {}
for event in events:
    if event['event'] == 'turn':
        sample, info = turn_sample(event)
        key = {'t1': 'cold (turn 1)', 't2': 'resume from Tag, window unset (turn 2)' if event['scenario'] == 'detach' else 'warm-window follow-up (turn 2)',
               't3': 'resume from Tag after window expiry (turn 3)'}[event['turn']]
        groups[key].append(sample)
        evidence.append({'scenario': event['scenario'], 'rep': event['rep'], 'turn': event['turn'], 'task': event['task']['name'], **info,
                         'runtimeInstanceID': event['task']['identity']['runtimeInstanceID'], 'actorUID': event['workspaceAfter'].get('actorUID')})
        last_sequence[event['task']['workspace']] = event['workspaceAfter']['sequence']
    elif event['event'] == 'suspended':
        sequence = last_sequence.get(event['workspace'])
        key = 'suspend at window expiry' if event['scenario'] == 'warm' else 'suspend on detach, window unset'
        if event.get('after') in ('t1', 't2', 'window'):
            stops[key].append(stop_sample(event, sequence))
    elif event['event'] in ('warmAttach', 'windowHeld'):
        evidence.append(event)


def summarize(samples):
    keys = sorted({key for sample in samples for key in sample})
    summary = {}
    for key in keys:
        values = [sample[key] for sample in samples if isinstance(sample.get(key), (int, float))]
        if values:
            summary[key] = {'n': len(values), 'median': statistics.median(values), 'min': min(values), 'max': max(values), 'values': values}
    return summary


report = {'turns': {name: summarize(samples) for name, samples in groups.items()},
          'stops': {name: summarize(samples) for name, samples in stops.items()},
          'evidence': evidence, 'final': final}
(run / 'report.json').write_text(json.dumps(report, indent=2) + '\n')


def cell(summary, key):
    value = summary.get(key)
    if not value:
        return '-'
    if value['n'] == 1:
        return '%d' % value['median']
    return '%d (%d–%d, n=%d)' % (value['median'], value['min'], value['max'], value['n'])


out = ['# Substrate workspace latency (ms, median (min–max, n))', '']
for title, table in (('Turns', report['turns']), ('Stops', report['stops'])):
    names = list(table)
    keys = sorted({key for name in names for key in table[name]})
    out.append('## ' + title)
    out.append('')
    out.append('| phase | ' + ' | '.join(names) + ' |')
    out.append('| --- |' + ' --- |' * len(names))
    for key in keys:
        out.append('| %s | %s |' % (key, ' | '.join(cell(table[name], key) for name in names)))
    out.append('')
(run / 'summary.md').write_text('\n'.join(out) + '\n')
print('\n'.join(out))
if final is None:
    sys.exit('measurement Job omitted its final report')
