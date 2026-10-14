# fiberd provider spike

This spike asks whether an Orka ACP workspace can run as a
[fiberd](https://github.com/helayoty/fiberd) fiber, and at what cost. It is a
throwaway feasibility probe, not a supported provider. It pins fiberd
`b3b5b47` (2026-10-13) and Orka `feat/workspace-out-of-tree` `d5d887e`, plus the
one-flag Core patch on `spike/fiberd-provider`.

## Layout

| Path | Purpose |
| --- | --- |
| `providers/fiberd/cmd/orka-fiber-shim` | gVisor template: claims the fiber identity, writes the supervisor identity projection, bridges the endpoint, egress and Core URLs, then starts the supervisor |
| `providers/fiberd/cmd/orka-fiber-egress` | Home-side HTTP CONNECT proxy on each grant's run directory, with a host:port allowlist |
| `providers/fiberd/payload` | The Clone payload: identity, command, arguments and literal environment |
| `providers/fiberd` | Provider forked from the fake provider's reconcile flow, with fibers instead of Pods |
| `hack/fiberd-spike/check` | Stage 1 harness: clones supervisors and reads their sealed bootstrap challenges |
| `hack/fiberd-spike/core-proof`, `stage3.sh`, `core-install.py` | Stage 3: actual Core Task through the provider |
| `hack/fiberd-spike/spike.sh` | Driver: builds locally, runs images and a kind cluster on a remote Docker host |

## Run

```sh
hack/fiberd-spike/spike.sh build && hack/fiberd-spike/spike.sh image
hack/fiberd-spike/spike.sh up && hack/fiberd-spike/spike.sh home
hack/fiberd-spike/spike.sh stage1 -mode shared -workspaces 4 -probe 2
hack/fiberd-spike/spike.sh stage2
hack/fiberd-spike/spike.sh stage3
```

`SPIKE_HOST` (default `vm`) names the Docker host; nothing is built or loaded
into a local daemon.

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
- The supervisor image is pinned by its local image ID, not a registry digest.
