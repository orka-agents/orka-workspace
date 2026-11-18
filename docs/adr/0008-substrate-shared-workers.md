# ADR 0008: Share Substrate workers among one namespace's Actors, with interim termination evidence

Date: 2026-11-18

Status: Accepted as an interim decision. For `SubstrateWorkspaceProfile`s that set
`spec.placement.sharedWorkers` only, it supersedes ADR 0006's single-Actor worker
sizing and its "proves termination by exact worker Pod UID absence" rule, and it
replaces ADR 0007's spare private workers. Every other profile keeps ADRs 0006 and
0007 unchanged. Orka core records the same decision as Orka ADR 0056.

## Context

ADR 0006 runs every Actor on its own private, single-replica gVisor WorkerPool and
proves that an Actor stopped by deleting that worker Pod and observing its exact UID
absent. ADR 0007 moved worker birth off the start path with pre-created spares, but
each spare is still one Pod per Actor, and each resume still waits for a worker that
has never hosted anyone.

Agent Substrate v1.0.0 (`b34fbee742906494b5eb4f1f36987176bf8770a4`) is built for
shared workers:

- Each Actor runs in its own `runsc` sandbox, cgroup leaf and network namespace on a
  worker. A `VOLUMES` checkpoint runs only `runsc fscheckpoint` over durable
  directories and writes no memory image (`cmd/ateworker-gvisor/main.go`, the
  `SNAPSHOT_FIDELITY_VOLUMES` case). Checkpoint first deactivates the Actor's tunnel
  and fails if it cannot (`Checkpoint`, `s.tunnel.Deactivate`).
- Teardown after a checkpoint (`terminateWorkload`: deactivate tunnel, stop and
  `runsc delete` containers, unhost the network) is best effort. `checkpointIsolated`
  calls `_ = terminate()` (`cmd/ateworker-gvisor/ckptcgroup.go`), and the control
  plane still marks the Actor `SUSPENDED` and releases its assignment. `SUSPENDED`
  with no assignment is bookkeeping, not proof that the sandbox exited. A leaked
  sandbox has no tunnel, but its memory lives until the worker Pod dies. Deleting a
  `SUSPENDED` Actor names no worker and tears nothing down
  (`terminateWorkloadOnWorker`, `cmd/atelet/main.go`).
- A worker process restart is strong evidence: the worker owns every sandbox, a new
  process registers a higher epoch, and the control plane crashes and releases the
  earlier Actors.
- Co-hosted Actors share one root worker process with `SYS_ADMIN`, `NET_ADMIN` and
  `DAC_OVERRIDE`, the node's `/var/lib/ate` hostPath, and every hosted Actor's egress
  mTLS key in worker memory. Upstream's threat model says "workers are not a boundary
  between actors" (`docs/security/threat-model.md`). Placement checks no Atespace or
  tenant: any template that names a pool's labels can land on it.
- Workers report `--max-actors` (default 1000) as their Actor count, which no
  WorkerPool field sets. CPU and memory capacity are the Pod limits less 64Mi, and
  gVisor books no per-Actor overhead.

There is no upstream per-Actor sandbox-exit attestation. Without one, a shared worker
cannot give today's proof that one Actor's sandbox is gone while its neighbours keep
running. The project owner approved the weaker, interim evidence below. It is accepted
only because sharing is opt-in per profile, confined to one namespace, bounded in time,
and paired with revocation of everything a leaked sandbox could use.

## Decision

### Opt-in

- **A profile field, so opting in needs a new profile.** `SubstrateWorkspaceProfile.spec.placement.sharedWorkers` is:
  - `maxActorsPerWorker`: required, from 2 to 16;
  - `maxWorkerLifetime`: optional, default 24h, at least 1h and at most 24h.
- **Hashed and immutable.** The spec is immutable and hashed by `sdk.ParametersProfileHash`. Sharing changes the security model, so it belongs in the frozen spec, never in a mutable annotation.
- **Today's behavior when absent.** An absent field keeps ADRs 0006 and 0007 exactly: same private pool, same proof, same API calls. Existing profile hashes do not change.
- **Validation.** The field requires `suspend.mode: DataOnly`, checked by CEL and by `Validate`.

### What may share

Every Actor starts private unless all of these hold when its allocation is prepared:

- its profile opts in;
- it is data-only (`VOLUMES`);
- its request has no `restoreFrom`, so explicit checkpoint restore and recovery stay private;
- its workspace is not operator-tainted (`acp.workspace.orka.ai/operator-tainted`), quarantined, or `Failed`;
- its egress injects the provider-proxy credential at the gateway (`EgressCredentialInjection` required and used). The long-lived provider token then never enters the sandbox, so a leaked sandbox holds no credential that stays valid.

Anything else takes the private single-Actor path of ADR 0006.

### Scope

- **One pool, one scope.** A shared pool serves exactly one scope: workspace namespace, Atespace, profile UID and hash, Actor size `(C, M)`, operator WorkerPool UID, and private-shape digest. It is never shared across namespaces or profiles.
- **Sizing.** A pool is one single-replica gVisor WorkerPool sized `cpu=N·C, memory=N·M+64Mi` for `N = maxActorsPerWorker`. Its worker must report capacity exactly `(N·C, N·M)`.
- **Labels.** Pool labels are the operator pool's labels plus `substrate.workspace.orka.ai/shared-pool: <random>`. The random value is journaled. The derived template selects the operator selector plus that value. A shared pool is never an operator-pool candidate, and the label is reserved like the birth labels.

### Ledger

- **Where it lives.** The pool's `substrate.workspace.orka.ai/shared-ledger` annotation (`substrate.workspace.shared-ledger.v1`) records:
  - the scope;
  - the pool's birth time;
  - its admission state: `Admitting`, `Draining`, `Retiring`, or `Quarantined`;
  - the first fenced worker (Worker UID, Pod UID and registered epoch; a different worker or epoch is doubt);
  - one entry per hosted allocation: the journal marker `<namespace>/<name>/<uid>/<sequence>`, the Actor reference, and the Actor UID once it is known.
- **Writes.** Every write is an Update preconditioned on the read resourceVersion.
- **Claiming a slot.** An allocation journals its selection and `claimIssued` before it adds its own entry. It binds only after observing that entry. A pool it creates is created with its entry already in the ledger. Selection or claim doubt before the template exists falls back to another pool or a new pool.
- **The Actor UID comes before Resume.** The entry receives the Actor UID before Resume, so an assignment can never precede its ledger entry.

### Startup

These replace "ACTIVE and empty" for shared pools. Before Resume the allocation requires all of these:

- its pool is bound, `Admitting` or `Draining`, and inside its lifetime;
- the pool's only worker is `ACTIVE`, gVisor, and sized exactly `(N·C, N·M)`;
- the worker has `registered_epoch > 0` and `observed_epoch == registered_epoch`;
- the worker is the ledger's fenced worker;
- fewer than `N` Actors are assigned;
- every assignment is a ledger entry with a matching Actor UID. Assignments are listed first, then the ledger is re-read.

Ready observation keeps the same ledger check.

- **A foreign Actor fails closed.** Any assigned Actor that is not in the ledger fails Observe. The leader quarantines the pool: it stops admitting and retires through the strong path.

### Interim termination evidence

The normal retirement path on a shared worker takes these steps:

1. **Revoke egress first.** Before `SuspendActor` is issued, the Actor's `default` EgressPolicy is replaced by deny-all and read back. Its credential-injection effects go with it. A Stop issues the same synchronous `VOLUMES` suspend, purely as termination evidence, and keeps no Tag.
2. **The Actor is suspended.** The Actor is `SUSPENDED`, under its recorded UID, with no worker assignment, at a version above the one read just before suspend. It holds a new, completed, Actor-owned `VOLUMES` snapshot whose digest matches the one recorded.
3. **The fenced worker no longer hosts it.** The worker still has the fenced Worker UID, Pod UID, namespace, pool and Pod, `registered_epoch` equals the fence's, and `observed_epoch == registered_epoch`. `ListWorkerActorAssignments` no longer lists the Actor and lists nothing outside the ledger. The worker is re-read with its version unchanged. A version change alone is retried, at most five times.
4. **Egress is still deny-all.** The deny-all policy reads back unchanged.
5. **Delete, release, report.** The proof is journaled. The Actor is deleted with its UID precondition, and its ledger entry is released. Only then is `Stopped` reported, with the Tag as retained data for a suspension.

- **Credentials Core owns.** Orka ADR 0056 records how Core makes these unusable, and where the provider's gate above makes Core's guarantee hold:
  - the attachment bearer only authenticates requests into the runtime, and the router no longer routes this Actor to the leaked sandbox;
  - the pool's controller bearer and capability secret are accepted only while Core's exact active instance is set, which Core clears when it observes `Stopped`, and they are rotated before any successor boots;
  - the provider-proxy token never entered the sandbox.

### Strong fallback and strong retirement

The strong path closes the ledger (`Retiring`, or `Quarantined` for a foreign Actor) and deletes the WorkerPool with UID precondition and foreground propagation. Each hosted allocation then proves termination exactly as ADR 0006 does: the exact fenced worker Pod UID is absent and the pool is gone. Deleting the worker Pod is how co-hosted Actors are suspended. Orka core observes the terminating worker and drains each one for data-only suspension inside its eviction window (Orka ADR 0046).

These take the strong path:

- **Doubt during the interim proof:** the assignment is still present, the worker's identity or epoch changes, the worker is unavailable, a foreign assignment appears, or any upstream error occurs.
- **The Actor or worker is already lost:** the Actor crashed or is in any state other than the expected `SUSPENDED`, the worker Pod is terminating or gone, or the pool is deleting, closed for retirement, or no longer holds the entry.
- **The workspace is suspect:** it is operator-tainted, quarantined, or `Failed`, or the instance recorded a failure.

Retiring one Actor never touches its neighbours except through this explicit strong path. An Actor that never booted holds no sandbox and only releases its slot.

### Maximum worker lifetime

- **Admission closes first.** A pool stops admitting new Actors (`Draining`) at `maxWorkerLifetime − min(1h, maxWorkerLifetime/2)` from its birth.
- **The pool is retired at the lifetime.** At `maxWorkerLifetime` it is retired through the strong path, even if Actors remain, so any leaked sandbox's memory is bounded by the worker's lifetime.
- **Other reasons to close admission early.** It also closes when the operator pool's image or shape changes, the profile disappears, or a hosted workspace becomes tainted, quarantined or `Failed`.

### Warm capacity

- **No private spares.** For opted-in profiles, free slots in admitting shared pools are the warm capacity, and ADR 0007 private spares are not created.
- **What the annotation counts.** The `substrate.workspace.orka.ai/spare-workers` annotation counts free admitting slots to keep. The leader creates empty shared pools to reach it.
- **Collection.** The leader deletes an empty pool once it has been empty for 5 minutes and is not needed for warm slots, or once its admission has closed. Deletion first proves that no Actor is assigned, and carries UID and resourceVersion preconditions.
- **Orphans.** A ledger entry whose exact journal record no longer holds it, and whose Actor is not assigned, is removed. A missing journal is not proof.

### Recovery

The journal and the ledger are both authoritative and fenced against each other:

- **The journal.** It records the pool, its random label, its scope, `createIssued`, `claimIssued`, egress revocation, the suspend fence, the interim proof, and any strong reason, and each is saved before its effect.
- **The ledger.** It records which journals hold slots.
- **Restart.** A restarted provider resumes each record from its last saved step.

### Timing

The ready and stopped timing logs report `workerSource: shared`. The stopped log adds `terminationProof: interim` or `strong`. `workerTeardownMs` measures from teardown start to the journaled proof.

## Consequences

- **Faster starts, fewer Pods.** Up to 16 Actors of one namespace and profile share one warm worker Pod, and a resume onto a pool with a free slot skips worker birth.
- **Weaker evidence.** This is weaker than ADRs 0006 and 0031. A per-Actor stop is proven by control-plane bookkeeping plus revocation, not by the sandbox's exit.
- **Residual risk: a leaked sandbox.** If the best-effort teardown failed, that sandbox's memory may persist on the node until the worker is recycled, at most `maxWorkerLifetime`. That memory includes the instance's already-revoked credentials and workspace data. It has no tunnel and deny-all egress.
- **Residual risk: worker compromise.** A compromised worker exposes every co-hosted Actor of the same namespace and profile, including their egress mTLS keys and any leaked memory.
- **Lower capacity than reported.** A leaked sandbox can also consume capacity the worker reports as free until it is recycled.
- **Strong retirements disturb neighbours.** A strong retirement suspends a pool's neighbours through Core's eviction path. A neighbour that cannot drain in time fails without replay.
- **Exit criterion.** Replace the interim evidence with an upstream per-Actor sandbox-exit attestation as soon as Agent Substrate provides one. Until then, this decision must not be widened: no cross-namespace pools, no non-`VOLUMES` Actors, no profile without the field, no lifetime over 24h.
- **Live validation is still needed.** It must show label propagation, the capacity arithmetic for `N` Actors, `observed_epoch` behaviour, egress revocation on a running Actor, and the eviction path on a strongly retired shared worker, on a live Agent Substrate cluster.
