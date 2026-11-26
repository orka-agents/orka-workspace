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
pods = {entry['pod']: entry for entry in lines(run / 'workers' / 'pods.jsonl') if entry.get('pod')}


def when(text):
    """Parse an RFC3339 time with up to nanoseconds and any offset into epoch seconds."""
    from datetime import datetime
    if not text:
        return None
    match = re.match(r'^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)$', text)
    if not match:
        return None
    whole, fraction, zone = match.groups()
    value = datetime.fromisoformat(whole + ('+00:00' if zone == 'Z' else zone)).timestamp()
    return value + (float('0.' + fraction) if fraction else 0.0)


# Worker and supervisor boot milestones, in order; each phase is named by its end.
WORKER_STEPS = [('workerBooting', 'ateworker booting'), ('workerRegistered', 'Registered worker'),
                ('sandboxRuntimesReported', 'Reported worker sandbox runtimes'), ('readinessRecovered', 'Readiness check recovered'),
                ('runscCreate', 'About to run runsc create'), ('awaitingBootstrap', 'awaiting controller credential bootstrap'),
                ('bootstrapComplete', 'credential bootstrap complete'), ('supervisorListening', 'ACP supervisor listening')]


def worker_timeline(pod_name, created_at):
    """Phases of one worker Pod's boot, plus their offsets from Task creation."""
    entries = [entry for entry in supervisor if entry.get('pod') == pod_name]
    first = {}
    for entry in entries:
        for key, text in WORKER_STEPS:
            if message(entry) == text and key not in first:
                first[key] = when(entry.get('time'))
    meta = pods.get(pod_name, {})
    marks = [('podCreated', when(meta.get('created'))), ('containerStarted', when(meta.get('started')))] + [(key, first.get(key)) for key, _ in WORKER_STEPS]
    sample, previous = {}, None
    for key, at in marks:
        if at is None:
            continue
        if created_at is not None:
            sample['worker.sinceTaskCreated.' + key + 'Ms'] = int((at - created_at) * 1000)
        if previous is not None:
            # Named by the milestone the step ends at.
            sample['worker.step.%sMs' % key] = int((at - previous[1]) * 1000)
        previous = (key, at)
    return sample
events, final = [], None
for line in (run / 'latency.log').read_text().splitlines():
    if line.startswith('LATENCY_EVENT '):
        events.append(json.loads(line[len('LATENCY_EVENT '):]))
    elif line.startswith('NATIVE_CORE_LATENCY '):
        final = json.loads(line[len('NATIVE_CORE_LATENCY '):])


def ms_fields(entry, prefix):
    values = {}
    for key, value in entry.items():
        if isinstance(value, dict) and key.endswith('Ms'):
            values.update({prefix + key[:-2] + '.' + inner + 'Ms': number(item) for inner, item in value.items() if number(item) is not None})
        elif key.endswith('Ms') and number(value) is not None:
            values[prefix + key] = number(value)
    return values


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
    # Client-observed first sighting of the pre-dispatch execution states.
    for state in ('Queued', 'Reserved', 'SessionStarting', 'Submitting'):
        sample['clientState.%sMs' % state] = task['statesFirstSeenMs'].get('state:' + state)
    timings = [entry for entry in find(core, 'ACP turn timings', task=task['name']) if str(entry.get('promptAccepted')).lower() == 'true']
    if timings:
        sample.update(ms_fields(timings[-1], 'turn.'))
    pool, sequence = task['identity']['runtimePoolName'], after['sequence']
    before = event.get('workspaceBefore') or {}
    started_new = before.get('sequence') != sequence or event['turn'] == 't1'
    evidence = {'sequence': sequence, 'newSequence': started_new}
    if started_new:
        admission = find(core, 'ACP workspace runtime admission timings', runtimePool=pool, sequence=sequence)
        if admission:
            initial = [a for a in admission if str(a.get('readmission')).lower() != 'true']
            readmit = [a for a in admission if str(a.get('readmission')).lower() == 'true']
            if initial:
                sample.update(ms_fields(initial[0], 'admit.'))
            if readmit:
                sample.update(ms_fields(readmit[-1], 'readmit.'))
        started = find(core, 'ACP workspace runtime started', runtimePool=pool, sequence=sequence)
        if started:
            sample.update(ms_fields(started[-1], 'coreStart.'))
            evidence['corePath'] = started[-1].get('path')
        ready = find(provider, 'Substrate runtime ready', workspaceName=task['workspace'], sequence=sequence)
        if ready:
            sample.update(ms_fields(ready[-1], 'providerReady.'))
            evidence['providerPath'] = ready[-1].get('path')
    # The supervisor names each RuntimeSession generation
    # runtime-<RuntimeSession UID>-g<generation>; a Session keeps its UID
    # across turns. Without a recorded generation, the turn's own time window
    # selects it.
    identity = task['identity']
    created = [entry for entry in supervisor if message(entry) == 'ACP runtime session created' and
               identity['runtimeSessionUID'] and identity['runtimeSessionUID'] in str(entry.get('runtimeSessionID', ''))]
    if identity.get('runtimeSessionGeneration'):
        created = [entry for entry in created if str(entry.get('runtimeSessionID', '')).endswith('-g' + identity['runtimeSessionGeneration'])]
    else:
        begin, end = when(task.get('createdAt')), when(task.get('createdAt'))
        if begin is not None:
            end = begin + task['doneMs'] / 1000.0
            created = [entry for entry in created if (when(entry.get('time')) or 0) >= begin and (when(entry.get('time')) or 0) <= end]
    if created:
        sample.update(ms_fields(created[-1], 'supervisor.'))
        created_at = when(task.get('createdAt'))
        at = when(created[-1].get('time'))
        if created_at is not None and at is not None:
            sample['supervisor.sessionCreatedSinceTaskCreatedMs'] = int((at - created_at) * 1000)
    if started_new and after.get('workerPod'):
        sample.update(worker_timeline(after['workerPod'].split('/', 1)[-1], when(task.get('createdAt'))))
    return {key: value for key, value in sample.items() if value is not None}, evidence


def stop_sample(event, sequence, pool_name):
    sample = {}
    workspace_stopped = find(provider, 'Substrate runtime stopped', workspaceName=event['workspace'], sequence=sequence)
    pools = {entry.get('runtimePool') for entry in workspace_stopped if entry.get('runtimePool')}
    if pool_name:
        pools.add(pool_name)
    if pools:
        stopped = [entry for entry in find(core, 'ACP workspace runtime stopped', sequence=sequence)
                   if entry.get('action') == 'suspend' and entry.get('runtimePool') in pools]
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
last_pool = {}
for event in events:
    if event['event'] == 'turn':
        sample, info = turn_sample(event)
        key = {'t1': 'cold (turn 1)', 't2': 'resume from Tag, window unset (turn 2)' if event['scenario'] == 'detach' else 'warm-window follow-up (turn 2)',
               't3': 'resume from Tag after window expiry (turn 3)'}[event['turn']]
        groups[key].append(sample)
        evidence.append({'scenario': event['scenario'], 'rep': event['rep'], 'turn': event['turn'], 'task': event['task']['name'], **info,
                         'runtimeInstanceID': event['task']['identity']['runtimeInstanceID'], 'actorUID': event['workspaceAfter'].get('actorUID')})
        last_sequence[event['task']['workspace']] = event['workspaceAfter']['sequence']
        last_pool[event['task']['workspace']] = event['task']['identity'].get('runtimePoolName', '')
    elif event['event'] == 'suspended':
        sequence = last_sequence.get(event['workspace'])
        pool_name = last_pool.get(event['workspace'], '')
        key = 'suspend at window expiry' if event['scenario'] == 'warm' else 'suspend on detach, window unset'
        if event.get('after') in ('t1', 't2', 'window'):
            stops[key].append(stop_sample(event, sequence, pool_name))
    elif event['event'] in ('warmAttach', 'windowHeld', 'violation'):
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
checks = [event for event in evidence if event.get('event') == 'warmAttach']
if checks:
    out.append('## Warm attach checks')
    out.append('')
    for event in checks:
        failed = [name for name, ok in event['checks'].items() if not ok]
        out.append('- repetition %s: %d/%d passed%s' % (event['rep'], len(event['checks']) - len(failed), len(event['checks']),
                                                    (' (failed: %s)' % ', '.join(failed)) if failed else ''))
    out.append('')
held = [event for event in evidence if event.get('event') == 'windowHeld']
for event in held:
    replicas = sorted({sample['pool'].get('desiredReplicas') for sample in event['samples']})
    out.append('- window %s: %d samples, pool desiredReplicas %s, Actor and worker present in all: %s' % (
        event['rep'], len(event['samples']), replicas, all(s['actorExists'] and s['workerPodExists'] for s in event['samples'])))
(run / 'summary.md').write_text('\n'.join(out) + '\n')
print('\n'.join(out))
if final is None:
    sys.exit('measurement Job omitted its final report')
