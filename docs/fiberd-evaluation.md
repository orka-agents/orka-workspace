# fiberd provider evaluation

This evaluation asks whether an Orka ACP workspace should run as a
[fiberd](https://github.com/helayoty/fiberd) fiber. A spike provider ran actual
Core Tasks and was measured against the Substrate provider. It covers
2026-10-13 to 2026-10-15, fiberd `b3b5b47` and then `cddb385`, and Orka
`feat/workspace-out-of-tree` `d5d887e` with one added Core flag. The harness
and its run steps are in [hack/fiberd-spike](../hack/fiberd-spike/README.md).

## Recommendation

Do not build a supported fiberd provider now. A fiberd provider works end to
end, but it gives Orka nothing the Substrate provider lacks that is worth its
costs:

- A single Task performs about the same: 13.0 s against 13.8 s to Succeeded.
  fiberd starts the instance about 1 s sooner and releases it about 5 s sooner.
- Under concurrency the spike provider is slower, because it reconciles one
  workspace at a time.
- One running instance uses about 50% more memory.
- Its fastest mode, a shared warm pool, is unsafe because fibers of one grant
  reach each other.
- It needs a Core flag that weakens per-instance network binding, a fiberd
  patch, and fiberd changes Orka cannot make.
- Orka requires a fresh, fenced process with fresh bootstrap material for
  every runtime execution. That rule removes most of what fiberd's snapshot
  reuse offers.

Keep the provider code off `main`. Revisit when fiberd ships the changes under
[What would change the recommendation](#what-would-change-the-recommendation).

## What the spike built

- `providers/fiberd`: a provider forked from the fake provider's reconcile
  flow. Each workspace instance gets its own fiberd grant holding one fiber.
  A per-instance Service maps Core's bootstrap port 80 to the fiber's relay
  port.
- `orka-fiber-shim`: the gVisor template. It claims the fiber identity and
  writes the supervisor identity projection. It then bridges the endpoint,
  egress and Core URLs, and starts the unmodified Orka supervisor.
- `orka-fiber-egress`: an HTTP CONNECT proxy on the fiberd home, with a
  host:port allowlist. Fibers run with `--network=none`.
- A harness: Stage 1 clones supervisors directly, Stage 2 runs the shared
  conformance suite, and Stage 3 runs actual Core Tasks with timing and
  memory sampling for fiberd or Substrate.

The Core Task path also needed three changes outside the provider:

| Change | Why |
| --- | --- |
| Core `--external-native-shared-workers` (Orka `spike/fiberd-provider`) | Core binds ingress to one instance's worker Pod labels. Every fiber shares the home Pod, so the flag drops that binding and per-instance authority rests on sealed credentials alone. |
| fiberd `-gvisor-caps` (local patch, unpublished) | The supervisor needs `CHOWN`, `KILL`, `SETGID` and `SETUID` in its sandbox to run the agent under its own uid. fiberd granted none. |
| Shim URL forwarders and session directory | The supervisor's Core clients ignore proxy variables, so each admitted URL host resolves to a loopback forwarder through the home proxy. The read-only rootfs needs a tmpfs session directory. |

## Fit with Orka's design

What fits:

- Core's native-process startup evidence (process identity, worker Pod and
  challenge digest) already exists for Substrate. The provider mapped onto it
  with no SDK change and passes the shared conformance suite.
- The Clone payload carries only public identity, the bootstrap nonce and
  literal environment. Credentials arrive through Core's sealed bootstrap, as
  [ADR 0002](adr/0002-startup-and-retirement.md) requires.
- fiberd's fence (grant, epoch, sequence) and named-session attach let the
  provider tell a retry from a replacement. One grant per instance means a new
  workload sequence never shares a run directory.
- A dead fiber is never restarted, which matches `restartPolicy: Never`.
- gVisor with a root supervisor is the model Core already assumes for native
  runtimes.

What does not fit:

1. **Fresh process, not snapshot reuse.**
   [ADR 0005](adr/0005-persisted-workload-sequences.md) says a runtime process
   and its bootstrap material do not survive a cold resume, and full-memory
   restoration stays closed. The template therefore snapshots only the shim,
   and the supervisor starts fresh after each restore. That forfeits most of
   fiberd's prewarming. fiberd's memory-preserving park conflicts with the
   same rule.
2. **Proven termination.** ADR 0002 treats a missing response or stale
   observation as unknown. fiberd's Release is best effort. Watch has no
   tombstone and fiberd has no per-fiber status call, so the provider infers
   absence from a complete status batch.
3. **One worker identity per instance.** Core binds evidence and ingress
   policy to one instance's worker Pod. fiberd's worker is a shared home Pod,
   hence the Core flag. Core's per-runtime NetworkPolicy cannot be translated
   per fiber.
4. **Truthful disposition.** After retirement, the grant and its warm template
   remain until the lease lapses. There is no durable data, no data-only
   suspension and no checkpoint export.
5. **Trust.** The provider holds the home's grant issuer key, so it can mint a
   grant for any tenant on that home. It also calls an unauthenticated
   plaintext API. Substrate's provider uses a rotating Pod certificate over
   TLS. fiberd's production-readiness document still lists unauthenticated
   control calls and cleanup and revocation gaps.
6. **Leases.** A grant has a fixed lease and its supervisor ends when it
   lapses. Orka governs session lifetime itself, and the caller cannot renew a
   grant.
7. **Unmodified upstream.** Orka runs Sandbox and Substrate at unmodified
   upstream pins. fiberd needs the capability patch.
8. **One image per home.** A home's rootfs is built from one supervisor
   image. Orka selects runtime images per agent or class.

Items 2, 3 and 5 are closable upstream. Item 1 is a choice in Orka's own
design, not something a provider can fix.

## Isolation and security findings

- Fibers of one grant share `/host`, the grant's run directory. Each fiber
  reached the other fibers' supervisor endpoints and wrote into the shared
  directory. Workspaces therefore need one grant each, which forfeits the warm
  pool.
- Each fiber served its own sealed bootstrap challenge, bound to the identity
  in its Clone payload, and no two challenges matched. After Release, the
  endpoint refused connections.
- Direct egress and DNS fail inside a fiber. A CONNECT proxy on a socket in the
  run directory works with an allowlist. The allowlist is coarse (`*.svc`).
- The grant directory is writable by every fiber of the grant. Any root
  process on the home that touches a path there must not follow symlinks. The
  spike's egress proxy did so, and review caught it.
- The provider admits only what a sandbox holds:
  - uid 0, and no `runAsNonRoot`
  - the home's exact capability set
  - literal environment
  - no volumes
  - the image the home's rootfs was built from

  Anything else is rejected before a Clone.

## Benchmarks

### Environment and method

- Kubernetes v1.37.0 in kind, on one 16-vCPU KVM VM; runsc
  `release-20260817.0`.
- The 2026-10-15 runs shared the VM with unrelated clusters at load average 13
  to 50. A plain runc Pod start served as a control (see below). Read the
  absolute numbers with that in mind; fiberd and Substrate were interleaved so
  they saw similar load.
- The Core Task is a deterministic ACP prompt with no model calls. It runs
  through real class, provenance and authority admission, sealed bootstrap and
  authenticated Serving, and its result persists.
- Each comparison batch restarted Core, then ran 4 Tasks in sequence and 4 at
  once. Three interleaved batch pairs gave 12 sequential and 12 concurrent
  Tasks per provider.
- The proof host reads Task, workspace and RuntimePool status every 100 ms.
  The first comparison also read fiberd's bootstrap challenge directly,
  inside that loop. The VM has no route to Pod IPs, so each read timed out
  after 2 s. That delayed every fiberd observation after startup evidence. It
  produced a constant 2.0 s before Serving and inflated the prompt phase. The
  numbers below come from a rerun with that read disabled, at load average 38
  to 58.
- Both providers used the same Core build and the same supervisor image
  digest. Substrate was the official v0.1.0 pin `fa6d949`, set up by Orka's
  `scripts/workspace-substrate-cluster.sh` on its own single-node cluster. Its
  provider came from this branch and was deployed as Orka's provider E2E deploys
  it.

### Stage 1: supervisor as a gVisor fiber

The first clone of a grant warms its template and is excluded from the shared
modes.

| Mode | Clone (2026-10-14) | Clone to health (2026-10-14) | Clone (2026-10-15) | Clone to health (2026-10-15) |
| --- | ---: | ---: | ---: | ---: |
| Shared grant, on-demand restore | 90–120 ms | 135–165 ms | 124–156 ms | 176–222 ms |
| Shared grant, warm pool of 4, settled | 8–15 ms | 45–65 ms | 27–105 ms | 210–322 ms |
| Shared grant, 8 concurrent clones | 175–205 ms | ~330 ms p50 | 582–1352 ms | 1.29–1.59 s |
| One grant per workspace | 580–625 ms | ~660 ms | 720–1182 ms | 0.77–1.29 s |
| Plain runc Pod, image cached | — | 0.90–0.98 s | — | 1.8–2.3 s |

Both fiberd and the plain Pod control slowed on 2026-10-15. The Pod slowed
about 2x, roughly as much as fiberd's on-demand and per-workspace modes. The
pooled and concurrent modes slowed 4–5x and were not rechecked on a quiet host.

Measured with the home's cgroups on a quiet VM:

| | Memory |
| --- | ---: |
| Warm template per grant | ~22 MiB |
| Pooled sandbox | ~24 MiB |
| Running idle supervisor fiber | ~34 MiB |

### Stage 2: provider conformance on the real home

| fiberd | Lifecycle | Replacement |
| --- | ---: | ---: |
| `b3b5b47` | 0.7 s | 1.4 s |
| `cddb385` | 1.5–1.7 s | 2.5–2.7 s |

The `cddb385` runs were under load.

### Stage 3: actual Core Task through fiberd, 2026-10-14

| | Cold Core | Warm Core (2 runs) |
| --- | ---: | ---: |
| Task to startup evidence | 15.7 s | 5.5 s |
| Task to Serving | 17.7 s | 7.5 s |
| Task to Succeeded | 24.1 s | 13.9 s |

The clone was about 0.6 s of the time to startup evidence. Core's pipeline
before it publishes the workload dominates. These runs included the blocking
challenge read described above, so Serving and Succeeded may each be up to 2 s
late; startup evidence is unaffected.

### fiberd and Substrate on the same Core Task, 2026-10-15

Sequential Tasks, median (range); n=12 for fiberd, 11 for Substrate plus one
failure:

| Phase | fiberd | Substrate |
| --- | ---: | ---: |
| Task to startup evidence | 7.1 s (4.8–9.3) | 8.3 s (6.7–23.7) |
| Startup evidence to Serving | 1.3 s (0.5–3.7) | 0.3 s (0.1–0.6) |
| Serving to Succeeded (the prompt) | 4.7 s (4.0–6.9) | 4.8 s (4.3–6.3) |
| Task to Succeeded | 13.0 s (10.2–17.9) | 13.8 s (11.5–30.4) |
| Succeeded to instance released | 5.4 s (3.6–6.0) | 10.1 s (9.3–13.1) |
| Task to instance released | 18.2 s (16.0–21.6) | 23.7 s (21.3–40.1) |

Four Tasks at once, median (range), n=12 for each provider:

| Phase | fiberd | Substrate |
| --- | ---: | ---: |
| Task to startup evidence | 8.6 s (5.6–24.4) | 12.9 s (5.7–29.8) |
| Task to Serving | 18.8 s (7.1–25.5) | 15.8 s (6.4–34.8) |
| Task to Succeeded | 26.2 s (11.5–31.2) | 23.4 s (12.5–45.6) |
| Task to instance released | 32.1 s (25.1–36.6) | 34.6 s (23.9–58.1) |

- For a single Task, the two providers finish within a second of each other,
  and the prompt runs at the same speed. fiberd's extra hops do not slow the
  prompt measurably:
  - Core to supervisor: the per-instance Service, the home relay, then the
    shim's bridge.
  - Supervisor to Core: a loopback forwarder, then the home's CONNECT proxy.
- fiberd provisions about 1 s sooner but reaches Serving about 1 s later after
  startup evidence.
- fiberd releases about 5 s sooner, because Release is one call while
  Substrate deletes an Actor, a private WorkerPool and its Pod.
- Under concurrency fiberd reaches Serving and Succeeded about 3 s later. Its
  provider reconciles one workspace at a time and Clone blocks, so starts
  queue. That is a provider limit, not a fiberd one.
- One Substrate Task timed out before runtime admission. Substrate refused its
  boot ("no free workers available"), and this provider build does not retry a
  refused boot; #5 adds that. No fiberd Task failed.

The first comparison, with the blocking challenge read, showed:
- Substrate about 2 s faster to Succeeded, with a 2 s faster prompt.
- fiberd releasing 0.4 s after Succeeded.

All three were artifacts of that read.

### Memory

Sampled from the kind node's cgroups during one Task, twice per provider:

| | fiberd | Substrate |
| --- | ---: | ---: |
| One instance at peak | 63–67 MiB (anon 42–44): its grant's template and fiber | 42–43 MiB (anon 29): its private worker Pod, sandbox included |
| Held after retirement | 22 MiB, until the grant lease lapses (24 h) | 0 |
| Fixed | ~30 MiB: home baseline 22 MiB, issuer 9 MiB | ~0.9 GiB of `ate-system` on one node, plus two operator workers |

- A fiber is restored from the template's fully read image, so it is larger
  than Substrate's cold-booted sandbox.
- After about 30 Tasks, the home held 28 leftover templates totalling 633 MiB.
- Substrate's fixed cost breaks down as `atelet` 468 MiB (page cache
  included), object store 108 MiB, Postgres 99 MiB, router 63 MiB, two API
  servers 84 MiB, plus controller, egress and DNS.
- fiberd's smaller fixed footprint is its only memory advantage. It erodes at
  about 40 workspaces a day per home.

### Caveats

- The VM was shared and the runs were on a single node. Load was higher during
  the corrected rerun than the first comparison, so the two sets of absolute
  numbers are not comparable. Ranges are wide.
- The fixture Core keeps its store on an emptyDir. Tasks left across a Core
  restart never finalize, and Core retries them every second: 13 on the fiberd
  cluster and 9 on Substrate's. The measurement script now deletes its own
  Tasks first.
- kind's CNI enforces neither provider's network policy.
- The Substrate provider predates the parity fixes merged on 2026-10-15
  (#5). Those mainly change failure handling.

## fiberd's advantages, and why they do not land in Orka today

| Advantage | Status for Orka |
| --- | --- |
| Pooled clone in 8–15 ms, supervisor healthy in 45–65 ms | Needs a shared grant, which is unsafe until fiberd isolates fibers within a grant |
| Provisioning about 1 s and release about 5 s sooner than Substrate | Measured; Task to Succeeded is about the same, since about 6 s of Core's own pipeline dominates |
| Small fixed footprint and simple operation | Real, but offset by 22 MiB per finished workspace for a day |
| No Pod, IP or scheduling per workspace | Not measured; might help high churn at scale |
| Memory-preserving park and resume | Conflicts with Orka's fresh-process rule; a parked supervisor's checkpoint contains its credentials |
| PSI-driven parking for overcommit | Disabled (watermarks at 100) for the same credential reason |
| Restore of prewarmed runtimes | The test supervisor is a small Go binary. Agent processes start per session after credentials arrive, so the template cannot prewarm them. |

## Code review findings

Automated review on the PR raised 19 comments covering 17 issues. All were
assessed against the code. Unit tests cover the lifecycle and validation fixes,
and the shim and harness fixes were verified on the live cluster.

| Finding | Resolution |
| --- | --- |
| The API client stayed bound to the home Pod IP read at startup | Redial when the home Pod's IP changes. The recorded home UID still fences existing instances. |
| A Clone response lost before retirement left a live, unrecorded fiber | Retirement attaches through the named session, records the fiber and releases it. Test added. |
| Observation did not revalidate the bootstrap Service | Each observation rechecks the Service UID, deletion state, selector, protocol and ports, and withdraws evidence on mismatch. Test added. |
| `runAsNonRoot` was admitted while the supervisor runs as uid 0 | Effective pod and container settings are resolved; non-root requirements are rejected. |
| Any emptyDir volume was admitted though fibers have no mounts | Every volume, mount and device is rejected. Core already strips scratch emptyDirs from native templates. |
| `Running` treated an unobserved grant as stopped | Parent-context errors are now returned, and an incomplete batch is an error. Reporting a grant missing from a complete batch as unknown was declined: fiberd reports every admitted grant in each batch, and an unknown result would block retirement whenever a Clone never landed. |
| A wildcard supervisor listener could collide with a URL forwarder on the same port | The supervisor listens only on the bridge's loopback address. Verified live. |
| Capability drops were accepted but never applied | The effective set is computed; a drop the sandbox cannot honor is rejected. Core's drop-ALL-then-add shape is still admitted. |
| A Clone in flight when retirement began could start a fiber after retirement saw none | The Clone deadline is journaled before each Clone, and fiberd enforces it. Retirement waits until the deadline plus grace before trusting absence. Test added; it fails with the fence disabled. |
| Ensure could create the bootstrap Service after retirement had found none, leaving it after the workspace was deleted | When Ensure's journal update conflicts after it creates the Service, Ensure deletes that Service by UID. Test added; it fails without the fix. |
| A container working directory was admitted but never applied | Any working directory other than `/` is rejected. Core sets none. |
| `HTTP_PROXY` pointed plain HTTP clients at a CONNECT-only proxy | Only `HTTPS_PROXY` is set. |
| The timed proof's blocking challenge read biased fiberd's timings | The read is opt-in and off for timed runs. The comparison was rerun, which reversed two earlier conclusions. |
| A host-load sentence overstated the Stage 1 control | Qualified by mode. |
| The root egress proxy chmodded its socket in the fiber-writable grant directory, so a swapped-in symlink could make a home file world-writable | The socket's mode is set at bind through the umask, and no path in a grant directory is chmodded. Test added; verified live. |
| Stage 1 passed when a released supervisor still served | A still-healthy endpoint now counts as a failure. |
| Stage 2 exited 0 when its conformance Job failed | The wait status propagates after the logs print. |

## fiberd changes since the first pin

`cddb385` (merged 2026-10-14) mostly adds a Python SDK and reorganizes the
tree. Two changes broke the spike and were fixed:

- The Go client moved from `pkg/consumer` to `sdk/go/client`.
- Each gVisor fiber's identity record gained a tenth field, the Clone session.

The shim now refuses a payload whose identity is not the session fiberd
attests. The capability patch rebased cleanly. No change addressed the
blockers below.

## Open gaps

- Retired instances leave warm templates until their lease lapses. The caller
  can neither remove nor renew a grant.
- The provider holds the home's issuer key and calls the plaintext API.
- Termination is inferred from Watch batches: there is no tombstone or
  per-fiber status call, and Release is best effort.
- Core's NetworkPolicy envelope is not translated, and egress uses a coarse
  allowlist.
- There is no suspension, persistent volume or checkpoint import.
- Reconciles are serial, so concurrent starts queue.
- Admitted CPU and memory are not mapped to grant limits.
- The supervisor image is pinned by its local image ID.

## What would change the recommendation

These fiberd changes would be needed:

- Isolation between fibers of one grant, which would make shared warm pools
  safe.
- Caller-side grant removal and lease renewal.
- Authenticated control calls.
- Sandbox capabilities upstream.
- A per-fiber status query with an explicit end state.

They would also need a workload where start latency or density dominates, such
as many short Tasks, or many mostly idle interactive sessions. Even then, Core's
pipeline before publishing the workload is the larger share of Task latency,
and every provider benefits from speeding it up.

## Agent Sandbox

Agent Sandbox was not measured. It starts one Pod per workspace, and Orka's
provider pins its warm pool to zero replicas, so each workspace is a cold Pod
start. fiberd starts no Pod per workspace: every workspace is a gVisor sandbox
inside one shared home Pod. Its execution model is closest to Substrate's.
Expected Sandbox results:

- Provisioning within about a second of fiberd, since a plain Pod start was
  0.90–0.98 s against fiberd's 0.6 s per-workspace clone.
- Possibly about 1 s sooner from startup evidence to Serving, since no bridges
  are involved. The prompt itself ran at the same speed on fiberd and Substrate.

The comparison is not like-for-like unless the Pod runs under a gVisor
RuntimeClass.

## Where the work lives

| Branch | Content |
| --- | --- |
| orka-workspace `fiberd` | The spike provider, harness and this evaluation |
| Orka `spike/fiberd-provider` | `--external-native-shared-workers`, based on `feat/workspace-out-of-tree` |
| fiberd `spike/orka-capabilities` | `-gvisor-caps`, rebased on `cddb385`; local only, not proposed upstream |
