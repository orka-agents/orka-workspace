#!/usr/bin/env python3
"""Time kagent turns on the kindctl --tag kagent-baseline cluster.

Paths, each N times (default 5):
  cold      Session create plus its first turn.
  suspended a follow-up after the Session's Actor is SUSPENDED.
  immediate a follow-up sent as soon as the previous turn returned; whether
            kagent's quiescence suspend was superseded is recorded.

"Created" is the client clock just before the CLI call (Session create for a
cold turn, invoke otherwise). "Prompt accepted" is the mock model's
server-side receipt of the turn's unique marker; "done" is when the blocking
invoke returned the completed task. Actor states come from the atewatch Pod.
Usage: measure.py <kagent CLI> <api-url> <run dir> [N]
"""
import json
import os
import re
import statistics
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

cli, api, run = sys.argv[1], sys.argv[2], sys.argv[3]
repeats = int(sys.argv[4]) if len(sys.argv) > 4 else 5
os.makedirs(run, exist_ok=True)


def now():
    return time.time()


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).isoformat()


def parse(text):
    m = re.match(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?', text)
    base = datetime.strptime(m.group(1), '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc).timestamp()
    return base + (float('0.' + m.group(2)) if m.group(2) else 0.0)


def kagent(*args):
    out = subprocess.run([cli, '--api-url', api, '--gateway-url', api, '-o', 'json', *args],
                         capture_output=True, text=True, timeout=600)
    if out.returncode != 0:
        raise SystemExit('kagent %s failed: %s' % (args[:3], out.stderr.strip()[-500:]))
    return json.loads(out.stdout)


def logs(namespace, target, since):
    out = subprocess.run(['kubectl', '-n', namespace, 'logs', target, '--since-time=' + since],
                         capture_output=True, text=True, timeout=120)
    rows = []
    for line in out.stdout.splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return rows


def actor_states(session, since):
    name = 'session-' + session
    return [(parse(r['time']), r['state']) for r in logs('ate-system', 'atewatch', since)
            if r.get('msg') == 'actor' and r.get('actor', '').endswith('/' + name)]


def mock_receipt(mark, since):
    for r in logs('kagent', 'deploy/mock-llm', since):
        if r.get('msg') == 'mock request received' and r.get('marker') == mark:
            return parse(r['time'])
    return None


def wait_state(session, state, after, timeout=180):
    start = now()
    since = iso(after - 5)
    while now() - start < timeout:
        for at, seen in actor_states(session, since):
            if seen == state and at >= after:
                return at
        time.sleep(0.5)
    raise SystemExit('Actor of session %s did not reach %s' % (session, state))


def turn(session, path, rep):
    mark = 'LATMARK-%s-%d-%s' % (path, rep, uuid.uuid4().hex[:8])
    created = now()
    result = kagent('agent', 'invoke', '--session', session, '-t', mark + ' Reply with the fixed fixture text.')
    done = now()
    state = result.get('task', {}).get('status', {}).get('state')
    if state != 'TASK_STATE_COMPLETED':
        raise SystemExit('turn %s ended %s' % (mark, state))
    return {'path': path, 'rep': rep, 'session': session, 'marker': mark, 'created': created, 'done': done,
            'taskCreatedAt': result['task'].get('metadata', {}).get('kagent.dev/a2a/task-created-at')}


samples = []
for rep in range(1, repeats + 1):
    created = now()
    session = kagent('agent', 'session', 'create', '--agent', 'lat-baseline')['session']['id']
    sessioned = now()
    cold = turn(session, 'cold', rep)
    cold['created'], cold['sessionCreatedMs'] = created, int((sessioned - created) * 1000)
    samples.append(cold)
    suspended_at = wait_state(session, 'ACTOR_STATE_SUSPENDED', cold['done'])
    cold['suspendedAfterDoneMs'] = int((suspended_at - cold['done']) * 1000)
    time.sleep(1)
    follow = turn(session, 'suspended', rep)
    samples.append(follow)
    immediate = turn(session, 'immediate', rep)
    samples.append(immediate)
    wait_state(session, 'ACTOR_STATE_SUSPENDED', immediate['done'])
    print('rep %d done' % rep, flush=True)

since = iso(min(s['created'] for s in samples) - 30)
for s in samples:
    received = mock_receipt(s['marker'], since)
    s['acceptedMs'] = int((received - s['created']) * 1000) if received else None
    s['doneMs'] = int((s['done'] - s['created']) * 1000)
    states = actor_states(s['session'], since)
    # Actor transitions between this turn's creation and its prompt receipt.
    window = [(at, st) for at, st in states if s['created'] - 0.05 <= at <= (received or s['done'])]
    s['transitions'] = [{'state': st, 'ms': int((at - s['created']) * 1000)} for at, st in window]
    for at, st in window:
        if st == 'ACTOR_STATE_RESUMING' and 'resumingMs' not in s:
            s['resumingMs'] = int((at - s['created']) * 1000)
        if st == 'ACTOR_STATE_RUNNING' and 'runningMs' not in s:
            s['runningMs'] = int((at - s['created']) * 1000)
    if s['path'] == 'immediate':
        before = [st for at, st in states if at <= s['created']]
        s['actorStateAtSend'] = before[-1] if before else None
        # Superseded: the Actor never suspended between the previous turn and this prompt.
        s['suspendSuperseded'] = not any(st in ('ACTOR_STATE_SUSPENDING', 'ACTOR_STATE_SUSPENDED') for _, st in window) and \
            s['actorStateAtSend'] == 'ACTOR_STATE_RUNNING'
with open(os.path.join(run, 'samples.json'), 'w') as f:
    json.dump(samples, f, indent=2)


def cell(values):
    values = [v for v in values if isinstance(v, (int, float))]
    if not values:
        return '-'
    return '%d (%d–%d, n=%d)' % (statistics.median(values), min(values), max(values), len(values))


lines = ['| path | prompt accepted | done | actor RESUMING | actor RUNNING |', '| --- | --- | --- | --- | --- |']
for path in ('cold', 'suspended', 'immediate'):
    rows = [s for s in samples if s['path'] == path]
    lines.append('| %s | %s | %s | %s | %s |' % (path, cell([s['acceptedMs'] for s in rows]), cell([s['doneMs'] for s in rows]),
                                                cell([s.get('resumingMs') for s in rows]), cell([s.get('runningMs') for s in rows])))
imm = [s for s in samples if s['path'] == 'immediate']
lines.append('')
lines.append('- immediate follow-ups that superseded the suspend: %d/%d' % (sum(1 for s in imm if s.get('suspendSuperseded')), len(imm)))
lines.append('- cold Session create: %s ms; suspended after cold turn returned: %s ms' % (
    cell([s.get('sessionCreatedMs') for s in samples if s['path'] == 'cold']), cell([s.get('suspendedAfterDoneMs') for s in samples if s['path'] == 'cold'])))
with open(os.path.join(run, 'summary.md'), 'w') as f:
    f.write('\n'.join(lines) + '\n')
print('\n'.join(lines))
