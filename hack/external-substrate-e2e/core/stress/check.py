#!/usr/bin/env python3
"""Checks a stress.sh run against the native watch log: identity, pool, duplicates, data."""
import json, re, sys, collections
stress_log, watch_log = sys.argv[1], sys.argv[2]
turns = []
for l in open(stress_log):
    if not l.startswith('TURN '):
        continue
    kv = dict(re.findall(r'(\w+)=([^ \[]+|\[[^\]]*\])', l))
    kv['time'] = l.split()[1]
    kv['session'] = kv['task'].split('-')[1]
    kv['round'] = int(kv['task'].split('-r')[-1])
    turns.append(kv)
watch = []
for l in open(watch_log):
    if l.startswith('NATIVE_WATCH '):
        watch.append(json.loads(l.split(' ', 1)[1]))
problems = []
# 1. every Task succeeded
for t in turns:
    if t['phase'] != 'Succeeded':
        problems.append(f"task {t['task']} phase {t['phase']}")
# 2. data: each verify sees every earlier successfully written marker, none foreign or mismatched
written = collections.defaultdict(int)
for t in sorted(turns, key=lambda t: (t['session'], t['round'])):
    v = t['verify']
    m = re.search(r'ok=(\d+) bad=(\d+)', v)
    if 'FOREIGN' in v or 'MISMATCH' in v or (m and m.group(2) != '0'):
        problems.append(f"data {t['task']}: {v}")
    if m and int(m.group(1)) != written[t['session']]:
        problems.append(f"data {t['task']}: {m.group(1)} markers verified, {written[t['session']]} written before")
    if not m:
        problems.append(f"data {t['task']}: verify not run ({v})")
    if 'wrote' in t['write']:
        written[t['session']] += 1
# 3. identity and pool: the native Actor named by the workspace has that UID and worker Pod
actors = collections.defaultdict(list)
for w in watch:
    if w['kind'] == 'actor':
        actors[w['key']].append(w)
name_session = {t['actor']: t['session'] for t in turns}
for t in turns:
    hist = [w for w in actors.get(t['actor'], []) if not w.get('gone') and w['time'][11:23] <= t['time']]
    running = [w for w in hist if w.get('state') == 'ACTOR_STATE_RUNNING']
    if not running:
        problems.append(f"identity {t['task']}: no RUNNING native record for {t['actor']}")
        continue
    last = running[-1]
    if last['uid'] != t['uid'] or last.get('workerPod') != t['workerPod']:
        problems.append(f"identity {t['task']}: native uid/pod {last['uid']}/{last.get('workerPod')} != workspace {t['uid']}/{t['workerPod']}")
    shared = t['session'].startswith('sh')
    pod = last.get('workerPod', '')
    if shared != pod.startswith('ate-shared-'):
        problems.append(f"pool {t['task']}: shared={shared} but worker {pod}")
    if not shared and not pod.startswith(t['actor'] + '-workers'):
        problems.append(f"pool {t['task']}: private Actor {t['actor']} on {pod}")
# every Actor ever seen running on a shared pool belongs to a shared session, and vice versa
for name, hist in actors.items():
    sess = name_session.get(name)
    for w in hist:
        pod = w.get('workerPod') or ''
        if w.get('state') == 'ACTOR_STATE_RUNNING' and sess and sess.startswith('sh') != pod.startswith('ate-shared-'):
            problems.append(f"pool {name} ({sess}) RUNNING on {pod} at {w['time']}")
# 4. duplicates: at most one live (RESUMING/RUNNING/SUSPENDING) Actor per session at any time
live = {}
dup = 0
for w in sorted((w for w in watch if w['kind'] == 'actor'), key=lambda w: w['time']):
    sess = name_session.get(w['key'])
    if not sess:
        continue
    if not w.get('gone') and w.get('state') in ('ACTOR_STATE_RESUMING', 'ACTOR_STATE_RUNNING', 'ACTOR_STATE_SUSPENDING'):
        live[w['key']] = sess
    else:
        live.pop(w['key'], None)
    per = collections.Counter(live.values())
    if per[sess] > 1:
        dup += 1
        problems.append(f"duplicate live Actors for {sess} at {w['time']}: {[k for k, v in live.items() if v == sess]}")
# per-session Actor UIDs per sequence are unique
seen = {}
for t in turns:
    key = (t['session'], t['seq'])
    if key in seen and seen[key] != t['uid']:
        problems.append(f"stale UID: {t['session']} sequence {t['seq']} seen with {seen[key]} and {t['uid']}")
    seen[key] = t['uid']
kinds = collections.Counter()
prev = {}
for t in sorted(turns, key=lambda t: (t['session'], t['round'])):
    p = prev.get(t['session'])
    kinds['cold' if p is None else ('warm' if p['seq'] == t['seq'] else 'resume')] += 1
    prev[t['session']] = t
print(json.dumps({'turns': len(turns), 'turnKinds': kinds, 'actorsSeen': len(actors), 'markersWritten': sum(written.values()),
                  'duplicates': dup, 'problems': problems}, indent=1))
