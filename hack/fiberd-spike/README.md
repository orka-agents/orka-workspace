# fiberd provider spike

This spike asks whether an Orka ACP workspace can run as a
[fiberd](https://github.com/helayoty/fiberd) fiber, and at what cost. It is a
throwaway feasibility probe, not a supported provider. It pins fiberd
`cddb385` (2026-10-14; first run `b3b5b47`) with the local `-gvisor-caps` patch,
and Orka `feat/workspace-out-of-tree` `d5d887e` plus the one-flag Core patch on
`spike/fiberd-provider`.

## Layout

| Path | Purpose |
| --- | --- |
| `providers/fiberd/cmd/orka-fiber-shim` | gVisor template: claims the fiber identity, writes the supervisor identity projection, bridges the endpoint, egress and Core URLs, then starts the supervisor |
| `providers/fiberd/cmd/orka-fiber-egress` | Home-side HTTP CONNECT proxy on each grant's run directory, with a host:port allowlist |
| `providers/fiberd/payload` | The Clone payload: identity, command, arguments and literal environment |
| `providers/fiberd` | Provider forked from the fake provider's reconcile flow, with fibers instead of Pods |
| `hack/fiberd-spike/check` | Stage 1 harness: clones supervisors and reads their sealed bootstrap challenges |
| `hack/fiberd-spike/core-proof`, `stage3.sh`, `core.sh`, `core-install.py` | Stage 3: actual, timed Core Task through the fiberd or Substrate provider |
| `hack/fiberd-spike/substrate.sh` | Substrate provider and fixture Core on the comparison cluster |
| `hack/fiberd-spike/measure.sh`, `measure/footprint.sh` | Timed Task batches, and one instance's memory, for either provider |
| `hack/fiberd-spike/spike.sh` | Driver: builds locally, runs images and a kind cluster on a remote Docker host |

## Run

```sh
hack/fiberd-spike/spike.sh build && hack/fiberd-spike/spike.sh image
hack/fiberd-spike/spike.sh up && hack/fiberd-spike/spike.sh home
hack/fiberd-spike/spike.sh stage1 -mode shared -workspaces 4 -probe 2
hack/fiberd-spike/spike.sh stage2
hack/fiberd-spike/spike.sh stage3
# Substrate comparison; ORKA_E2E_SOURCE is an Orka test/workspace-provider-e2e checkout.
hack/fiberd-spike/spike.sh substrate-up && hack/fiberd-spike/spike.sh substrate
hack/fiberd-spike/spike.sh measure fiberd 4 4 && hack/fiberd-spike/spike.sh measure substrate 4 4
hack/fiberd-spike/spike.sh footprint fiberd && hack/fiberd-spike/spike.sh footprint substrate
```

`SPIKE_HOST` (default `vm`) names the Docker host; nothing is built or loaded
into a local daemon. Substrate runs on its own cluster (`substrate-compare`),
created by Orka's `scripts/workspace-substrate-cluster.sh`.

## Results (2026-10-14, kind v1.37.0 on one 16-vCPU KVM VM, runsc release-20260817.0)

### Stage 1: supervisor as a gVisor fiber

The unmodified Orka supervisor runs in restored gVisor fibers behind
`orka-fiber-shim`. In every run each fiber served its own sealed bootstrap
challenge, bound to the identity in its Clone payload, and no two challenges
matched. Release left the endpoint refusing connections.

| Mode (first clone of a grant excluded) | Clone | Clone to supervisor health |
| --- | ---: | ---: |
| One grant shared by workspaces, on-demand restore | 90–120 ms | 135–165 ms |
| Shared grant, warm pool of 4, settled | 8–15 ms | 45–65 ms |
| Shared grant, 7 concurrent clones | 175–205 ms | ~330 ms p50 |
| One grant per workspace (warms its template) | 580–625 ms | ~660 ms |
| Plain runc Pod, same supervisor, image cached | — | 0.90–0.98 s |

An idle warm template costs ~22 MiB per grant, a pooled sandbox ~24 MiB and a
running idle supervisor fiber ~34 MiB.

Isolation: fibers of one grant share `/host`, the grant's run directory. Each
fiber reached the others' supervisor endpoints and wrote into the shared
directory. Workspaces therefore need one grant each, which forfeits the warm
pool. Direct egress and DNS fail (`--network=none`); a CONNECT proxy on a
host socket in the run directory works with an allowlist.

### Stage 2: provider

`providers/fiberd` gives each instance its own grant (one fiber, pressure
watermarks at 100 so a supervisor is never parked to disk) and names the
session after the instance, so a lost Clone response is recovered by ATTACH.
Stop releases the fiber and requires Watch to report it gone. A per-instance
Service maps Core's bootstrap port 80 to the fiber's relay port. Requests must
name the pinned image the home's rootfs was built from, use literal
environment, and add only capabilities the home's sandboxes hold. Lifecycle
and replacement conformance pass in memory and on the real home (0.7 s and
1.4 s).

### Stage 3: actual Core Task

Core admitted the Task and verified the fiber's native evidence. It completed
sealed credential bootstrap and authenticated Serving, executed the
deterministic ACP prompt and persisted the result. Retirement released the
fiber and removed its Service; no attachment Secrets remained. Three runs:

| | Cold Core | Warm Core (2 runs) |
| --- | ---: | ---: |
| Task to startup evidence | 15.7 s | 5.5 s |
| Task to Serving | 17.7 s | 7.5 s |
| Task to Succeeded | 24.1 s | 13.9 s |

The clone is ~0.6 s of the time to startup evidence; Core's pipeline before
the workload is published dominates.

Changes this needed beyond the provider:

- Core: `--external-native-shared-workers` (Orka `spike/fiberd-provider`). A
  native worker Pod must otherwise carry one instance's identity labels. With
  the flag, Core's controller and provider-proxy ingress admits every fiber on
  the home; per-instance authority rests on sealed credentials alone.
- fiberd: `-gvisor-caps` (fiberd `spike/orka-capabilities`, local only). The
  supervisor needs CHOWN, KILL, SETGID and SETUID inside the sandbox to run the
  agent under its own uid; fiberd's sandbox spec granted none.
- Shim: the supervisor's Core clients ignore proxy variables, so each admitted
  URL host resolves to a loopback forwarder through the home proxy; the session
  base directory defaults to the sandbox tmpfs because the rootfs is read-only.

### Rerun on fiberd `cddb385` (2026-10-15)

fiberd now hands each gVisor fiber its Clone session in a tenth identity-record
field, and moved the Go client to `sdk/go/client`. The shim reads the new
record and refuses a payload whose identity is not the session fiberd attests.
The capability patch rebased unchanged. Stages 1 to 3 pass again with unique,
identity-bound challenges, the same isolation findings and clean retirement.

The VM was shared with other sessions' clusters (load average 35 to 50), so
absolute latency rose. The plain runc Pod control rose by the same factor, to
1.8 to 2.3 s, so the slowdown is the host, not fiberd:

| Mode, 2026-10-15 (first clone of a grant excluded) | Clone | Clone to supervisor health |
| --- | ---: | ---: |
| Shared grant, on-demand restore | 124–156 ms | 176–222 ms |
| Shared grant, warm pool of 4, settled | 27–105 ms | 210–322 ms |
| Shared grant, 8 concurrent clones | 582–1352 ms | 1.29–1.59 s |
| One grant per workspace | 720–1182 ms | 0.77–1.29 s |
| Plain runc Pod, image cached | — | 1.8–2.3 s |

Stage 2 lifecycle and replacement conformance take 1.5 s and 2.7 s.

### Comparison with Substrate (2026-10-15)

Both providers ran the same deterministic Core Task on the same VM, with the
same Core build and the same supervisor image digest. fiberd alone needs the
shared-worker flag. Substrate is the official v0.1.0 pin (`fa6d949`), set up
by Orka's workspace provider E2E script on its own single-node kind cluster.
Its provider is this branch's, deployed with a native Pod certificate and
direct Actor egress as that E2E deploys it. Three interleaved batch pairs ran
at load average 13 to 40. Each batch restarted Core, then ran 4 Tasks in
sequence and 4 at once.

| Sequential Tasks, median (range), n=12 each | fiberd | Substrate |
| --- | ---: | ---: |
| Task to startup evidence | 6.2 s (4.0–9.2) | 8.0 s (6.9–28.0) |
| Startup evidence to Serving | 2.0 s (2.0–4.2) | 0.2 s (0.1–0.8) |
| Serving to Succeeded (the prompt) | 6.4 s (4.3–11.4) | 4.5 s (3.8–7.0) |
| Task to Succeeded | 14.6 s (12.4–22.1) | 12.6 s (11.0–32.8) |
| Succeeded to instance released | 0.4 s (0.0–2.6) | 9.0 s (7.1–13.3) |
| Task to instance released | 15.1 s (12.9–22.1) | 22.1 s (19.7–41.8) |

Four concurrent Tasks (n=12 each) took 17.7 s (11.2–36.2) for fiberd and
14.8 s (12.3–34.6) for Substrate to Succeeded.

- fiberd provisions about 2 s sooner, and releases about 9 s sooner, because
  Release is one call while Substrate deletes an Actor, a private WorkerPool
  and its Pod.
- Substrate serves about 2 s sooner after startup evidence and runs the prompt
  about 2 s faster. fiberd's supervisor traffic crosses more hops: Core reaches
  it through the per-instance Service, the home relay and the shim's bridge;
  it reaches Core through a loopback forwarder and the home's CONNECT proxy.
  The constant 2.0 s before Serving was not isolated further.
- The fiberd provider reconciles one workspace at a time and Clone blocks, so
  concurrent starts queue (up to 17 s to startup evidence).

Memory comes from the kind node's cgroups during one Task, twice each:

| | fiberd | Substrate |
| --- | ---: | ---: |
| One instance at peak | 63–67 MiB: its grant's template and fiber | 42–43 MiB: its private worker Pod, sandbox included |
| Held after retirement | 22 MiB, until the grant lease lapses (24 h) | 0 |
| Fixed | ~30 MiB: home baseline and issuer | ~0.9 GiB of `ate-system` on one node (half is `atelet`, page cache included), plus two idle operator workers |

A fiber is restored from the template's fully read image, so it is larger
than Substrate's cold-booted sandbox. After about 30 Tasks the home held 28
leftover templates, 633 MiB. fiberd's smaller fixed footprint is its only
memory advantage, and it erodes at about 40 workspaces a day per home.

The fixture Core keeps its store on an emptyDir. Tasks left across a Core
restart never finalize and are retried every second (13 on the fiberd
cluster, 9 on Substrate's); `measure.sh` now deletes its own Tasks first.
kind's CNI enforces neither provider's network policy.

### Gaps the spike leaves open

- Each retired instance leaves its grant's warm template (~22 MiB) until the
  grant lease lapses (24 h here). A caller cannot remove a grant, and a running
  supervisor ends when its lease lapses; nothing renews it.
- The provider holds the home's issuer key and calls the plaintext API.
- `Running` reads Watch's initial snapshot within a fixed window; Watch has no
  batch boundary or tombstone, and Release is best effort upstream.
- Core's NetworkPolicy envelope is not translated; egress is the home proxy's
  coarse allowlist (`*.svc`). kind's CNI does not prove packet enforcement.
- No suspension, persistent volume, or checkpoint import.
- The provider reconciles one workspace at a time, so concurrent starts queue.
- The supervisor image is pinned by its local image ID, not a registry digest.
