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
  tenant: it matches only the Worker's labels, which are copied from the WorkerPool's
  metadata labels (`cmd/ateapi/internal/scheduling/scheduling.go`,
  `cmd/atecontroller/internal/workersync/syncer.go`). Any template whose selector those
  labels satisfy, including an empty one, can land on it.
- A worker Pod has a hardcoded 3600 s termination grace
  (`cmd/atecontroller/internal/controllers/workerpool_apply.go`), of which the worker
  spends at most 30 minutes draining its Actors (`cmd/ateworker-gvisor/main.go`,
  `workloadGracePeriod`).
- The egress gateway caches each Actor's policy for `--egress-policy-cache-ttl`,
  default 10 s (`cmd/atenet/internal/router/cmd.go`, `egress/policycache.go`). Cache
  expiry governs only new requests. It does not end streams already admitted.
- The worker's DNS relay caches the policy for 5 s, and keeps a stale policy when a
  refresh fails (`internal/atunnel/egresspolicy.go`).
- A checkpoint first calls `Tunnel.Deactivate` (`internal/atunnel/tunnel.go`) and fails
  if it cannot. That call stops ingress and TCP egress and drains their active streams.
  It does not stop the DNS relay, which ends only in the later, best-effort teardown
  (`unhostActor`, then `SandboxSession.Close`).
- `MarkWorkerUnavailable` changes a Worker's state without changing its identity or
  epochs (`cmd/ateapi/internal/controlapi/worker.go`).
- Workers report `--max-actors` (default 1000) as their Actor count, which no
  WorkerPool field sets. CPU and memory capacity are the Pod limits less 64Mi, and
  gVisor books no per-Actor overhead.

There is no upstream per-Actor sandbox-exit attestation. Without one, a shared worker
cannot give today's proof that one Actor's sandbox is gone while its neighbours keep
running. Labels and a ledger cannot stop another tenant's template from reaching a
worker either. The project owner approved the weaker, interim evidence below. It is
accepted only because sharing is opt-in per profile, allowed only on an Agent Substrate
install dedicated to this Orka and continuously verified there, bounded in time, and
paired with revocation of everything a leaked sandbox could use.

## Decision

### Opt-in

- **A profile field, so opting in needs a new profile.** `SubstrateWorkspaceProfile.spec.placement.sharedWorkers` is:
  - `maxActorsPerWorker`: required, from 2 to 16;
  - `maxWorkerLifetime`: optional, default 24h, at least 3h and at most 24h, written in whole hours, minutes or seconds (no fractions or sub-second units, at most 32 characters). The API server enforces this with a cheap pattern rule.
- **Hashed and immutable.** The spec is immutable and hashed by `sdk.ParametersProfileHash`. Sharing changes the security model, so it belongs in the frozen spec, never in a mutable annotation.
- **Today's behavior when absent.** An absent field keeps ADRs 0006 and 0007 on the allocation path: same private pool, same proof, same API calls. Existing profile hashes do not change. The leader sweep adds one Kubernetes list of shared pools per pass, so it can enforce lifetimes left by deleted profiles. It makes no native call while no profile opts in and no shared pool exists.
- **Validation.** The field requires `suspend.mode: DataOnly`, checked by CEL and by `Validate`.

### Tenancy: a dedicated install only

- **The operator's assertion.** Sharing is allowed only when the provider runs with `--substrate-dedicated-install` (default false). The flag asserts that no tenant other than this Orka creates Actors or ActorTemplates on the Agent Substrate install.
- **Without the flag.** An allocation whose profile opts in is refused with an error that reaches the workspace's `Provisioned` condition. The leader logs the refusal for each such profile, and strongly retires any shared pool that exists.
- **Verified even with the flag.** The provider holds global viewer and lists every ActorTemplate. A template is foreign if its worker selector is empty or is satisfied by a shared pool's labels, and either:
  - it is outside the pool's Atespace; or
  - it is inside that Atespace but is not a derived template this provider recorded (in a journal, an archived record, or a checkpoint catalog), with the recorded UID once known.

  Any foreign template fails closed: the pool is quarantined and strongly retired, and the admission is refused.
  - The provider keeps the recorded templates in memory. A journal save adds the templates it records, and each leader sweep replaces the set from the journals, archived records and checkpoint catalogs.
  - A check whose templates are all in that set makes no ConfigMap call. When a template that reaches the pool is missing, the check rereads the records once, with two Lists and no per-catalog Gets, before it treats the template as foreign. A stale set can therefore only cause a reread, never a template counted as recorded that is not.

  This check runs:
  - before a slot is claimed or a pool is created;
  - immediately before each Resume;
  - on every leader sweep.
- **Placed Actors.** The ledger's assignment check (below) also catches any Actor that lands on a shared worker without a ledger entry.
- **Provider-owned labels only.** A shared pool carries only `substrate.workspace.orka.ai/shared-pool: <random>`, a value that is journaled and chosen per pool. It carries none of the operator pool's labels, which a foreign template might select. The derived template selects exactly that label.
  - Upstream reads WorkerPool metadata labels only to copy them onto Workers for scheduling.
  - Image, sandbox class, pinned SandboxConfig, Pod template and scheduling all come from the copied spec.
  - The operator pool's labels are kept in the journal as resume and checkpoint provenance.
- **Residual risk.** A foreign template or Actor created between two checks can reach a shared worker until the next check finds it. Detection then strongly retires the pool. A foreign Actor that lands is also caught by the ledger's assignment check. Neither check prevents the race; they bound it.

### What may share

Every Actor starts private unless all of these hold:

- its profile opts in, and the install is asserted dedicated;
- it is data-only (`VOLUMES`);
- its request has no `restoreFrom`, so explicit checkpoint restore and recovery stay private;
- its workspace is not operator-tainted (`acp.workspace.orka.ai/operator-tainted`), quarantined, or `Failed`;
- its egress injects the provider-proxy credential at the gateway (`EgressCredentialInjection` required and used). The long-lived provider token then never enters the sandbox.

The workspace check is repeated:

- when the slot is claimed. A workspace that became suspect before its template exists moves to the private path.
- immediately before Resume. Then the template already names the pool, so a suspect workspace or a suspect neighbour fails closed.

Anything else takes the private single-Actor path of ADR 0006.

### Scope

- **One pool, one scope.** A shared pool serves exactly one scope: workspace namespace, Atespace, profile UID and hash, Actor size `(C, M)`, operator WorkerPool UID, and private-shape digest. This is the provider's own bookkeeping. Isolation from other tenants rests on the dedicated install and its verification above, not on labels.
- **Sizing.** A pool is one single-replica gVisor WorkerPool sized `cpu=N·C, memory=N·M+64Mi` for `N = maxActorsPerWorker`. Its worker must report capacity exactly `(N·C, N·M)`. A shared pool is never an operator-pool candidate, and its label is reserved like the birth labels.

### Ledger

- **Where it lives.** The pool's `substrate.workspace.orka.ai/shared-ledger` annotation (`substrate.workspace.shared-ledger.v1`) records:
  - the scope;
  - the pool's birth time;
  - its admission state: `Admitting`, `Draining`, `Retiring`, or `Quarantined`;
  - the first fenced worker (Worker UID, Pod UID and registered epoch; a different worker or epoch is doubt);
  - one entry per hosted allocation: the journal marker `<namespace>/<name>/<uid>/<sequence>`, the Actor reference, and the Actor UID once it is known.
- **Writes.** Every write is an Update preconditioned on the read resourceVersion.
- **Claiming a slot.** An allocation journals its selection and `claimIssued` before it adds its own entry. It binds only after observing that entry. A pool it creates is created with its entry already in the ledger. Selection or claim doubt before the template exists falls back to another pool or a new pool.
- **Parallel allocations.** Workspaces are reconciled in parallel, so the provider serializes, per scope and within its own process, the choice between joining a pool and creating one; a burst of cold starts then fills a pool before it creates another. A claim that loses a ledger write race re-reads the pool and repeats every admission check, at most five times in one pass, before it falls back. Neither changes what a claim requires: the ledger write's resourceVersion precondition is still what keeps a pool from being overfilled or claimed after it stops admitting.
- **The Actor UID comes before Resume.** The entry receives the Actor UID before Resume, so an assignment can never precede its ledger entry.

### Startup

These replace "ACTIVE and empty" for shared pools. Before Resume the allocation requires all of these:

- the install is asserted dedicated, and no foreign template can reach the pool;
- its pool is bound, `Admitting` or `Draining`, before its strong retirement time, and hosts no suspect workspace;
- its own workspace is not suspect;
- the pool's only worker is `ACTIVE`, gVisor, and sized exactly `(N·C, N·M)`;
- the worker has `registered_epoch > 0` and `observed_epoch == registered_epoch`, and is the ledger's fenced worker;
- fewer than `N` Actors are assigned;
- every assignment is a ledger entry with a matching Actor UID. Assignments are listed first, then the ledger is re-read, and an unmatched assignment counts only if a second listing still reports it.

Ready observation keeps the same ledger check.

- **A foreign Actor fails closed.** Any assigned Actor that is not in the ledger fails Observe. The leader quarantines the pool: it stops admitting and retires through the strong path.

### Interim termination evidence

The normal retirement path on a shared worker takes these steps:

1. **Revoke egress, and let it settle.** The Actor's `default` EgressPolicy is replaced by deny-all and read back. Its credential-injection effects go with it.
   - `SuspendActor` is issued only after deny-all has been stored for longer than the larger of the gateway's policy cache and the DNS relay's 5 s cache, plus a 5 s margin: 15 s with the default gateway TTL.
   - The gateway's TTL is the provider flag `--substrate-egress-policy-cache-ttl` (default 10 s, the upstream default). Operators of a sharing install must set it to the gateway's own `--egress-policy-cache-ttl`.
   - The provider cannot verify that value. Reading the gateway's Deployment would need access to Substrate's system namespace, outside its least-privilege RBAC. Upstream exposes no enforcement acknowledgement either.
   - The checkpoint itself then deactivates the tunnel synchronously and drains its streams.
   - A Stop issues the same synchronous `VOLUMES` suspend, purely as termination evidence, and keeps no Tag.
2. **The Actor is suspended.** The Actor is `SUSPENDED`, under its recorded UID, with no worker assignment, at a version above the one read just before suspend. It holds a new, completed, Actor-owned `VOLUMES` snapshot whose digest matches the one recorded.
3. **The fenced worker no longer hosts it.** The checks below hold on two reads of the worker, and the version is unchanged between them; a version change alone is retried, at most five times.
   - The worker is `ACTIVE` and has the fenced Worker UID, Pod UID, namespace, pool and Pod.
   - `registered_epoch` equals the fence's, and `observed_epoch == registered_epoch`.
   - `ListWorkerActorAssignments`, taken between the two reads, no longer lists the Actor and lists nothing outside the ledger.
4. **Egress is still deny-all.** The deny-all policy reads back unchanged.
5. **Delete, release, report.** The proof is journaled. The Actor is deleted with its UID precondition, and its ledger entry is released. Only then is `Stopped` reported, with the Tag as retained data for a suspension.

- **Credentials Core owns.** Orka ADR 0056 records how Core makes these unusable, and where the provider's gates make Core's guarantee hold:
  - the attachment bearer only authenticates requests into the runtime, and the router no longer routes this Actor to the leaked sandbox;
  - the pool's controller bearer and capability secret are accepted only while Core's exact active instance is set. Core clears it asynchronously, on its first RuntimePool reconcile after it observes `Stopped`, which the workspace watch triggers and a 10 s requeue bounds while the controller runs. Core rotates both before any successor boots. During that interval the leaked sandbox has no tunnel and deny-all egress, so it cannot present them;
  - the provider-proxy token never entered the sandbox.

### Strong fallback and strong retirement

- **What it does.** The strong path first deletes the WorkerPool, with UID precondition and foreground propagation, using Kubernetes alone. It then records the ledger state (`Retiring`, or `Quarantined` for a foreign Actor or template) as best effort.
- **The proof.** Each hosted allocation then proves termination exactly as ADR 0006 does: the exact fenced worker Pod UID is absent and the pool is gone.
- **Neighbours.** Deleting the worker Pod is how co-hosted Actors are suspended. Orka core observes the terminating worker and drains each one for data-only suspension inside its eviction window (Orka ADR 0046).

These take the strong path:

- **Doubt during the interim proof:**
  - the assignment is still present;
  - the worker's identity, epoch or `ACTIVE` state changes;
  - a foreign assignment or template appears;
  - any upstream error occurs.
- **Uncertainty in the suspend path, never replayed.** Once the suspension intent is persisted, every error on a shared worker selects the strong path, apart from a journal write conflict, which is retried. That includes:
  - the Actor cannot be read, or is not `RUNNING` when its suspension begins or before the suspend is issued;
  - egress revocation fails, or the shared worker cannot be verified;
  - the template cannot be verified;
  - a suspend was issued, possibly without taking effect, and the Actor is still `RUNNING` or in any state other than `SUSPENDING` or `SUSPENDED`;
  - the snapshot evidence is invalid;
  - any Tag-capture step fails;
  - the evidence-only suspend issued by a Stop is rejected or times out, or any later step of that Stop's interim evidence fails, including a journal write (a write conflict is retried). The strong reason is persisted, and the pool's retirement started, before that pass returns.

  Retirement of the pool starts at once, and the data capture still proceeds when it can. A suspend whose outcome is unresolved is never reissued against a `RUNNING` Actor.
  - **Continuing a stalled suspension.** A SuspendActor call can fail after the Actor was marked `SUSPENDING`, for example on a transient database timeout in `FinalizeSuspended`. The Actor then stays `SUSPENDING`, and the strong path's pool deletion would crash it and reclaim its node state (`workflow_worker_delete.go`), losing the data. The provider therefore re-issues the same synchronous SuspendActor for a shared Actor seen `SUSPENDING`, at most 10 times, at least 10 s apart, recording each attempt in the journal before it is sent.
  - This continues the same upstream suspension and is not a replay. Agent Substrate's workflow is re-entrant (`workflow_suspend.go`): it skips `MarkSuspending` for a `SUSPENDING` Actor, so no new snapshot is minted. It sends the persisted snapshot to atelet, which answers a checkpoint it already committed without taking another. `FinalizeSuspended` then records it. The Actor lease serializes the workflow, so a continuation sent while the original call is still running is refused with `Aborted` and has no effect.
  - The continuation must finish before the worker Pod is gone. A pool deleted for the strong path keeps its Pod for its termination grace, which leaves room for it, but a continuation that loses that race leaves the instance lost and keeps its last verified checkpoint. A private worker, which faces no such deletion, keeps observing without resubmitting.
  - Once the worker Pod is proven absent, a capture that still fails one of the provider's own evidence checks (not a retryable native or Kubernetes error) is final. That instance is reported lost and retires through Stop, keeping its last verified checkpoint; no Tag is invented.
  - A Stop whose shared worker, ledger or workspace cannot be verified before its proof also takes the strong path.
- **A persisted strong marker always finishes.** The marker is saved before the pool is deleted. Every later retirement pass drives the pool's deletion and the absence proof whatever the checkpoint does. The bounds loop, reading Kubernetes alone, also deletes a pool when:
  - a hosted entry's journal holds the marker or a recorded instance failure, or cannot be decoded (its markers are then unreadable, which is doubt, and also blocks admission);
  - a booted entry (one with an Actor UID, or an assigned Actor) whose exact journal does not hold that slot under the same Actor name and UID. Such an entry would hide whose markers apply, so it is doubt.
    - Doubt holds only if a pool read taken after the journal still shows the entry. A release removes the entry before its journal records `Released`, so this never mistakes a concurrent release for doubt.
    - Every retirement it triggers is preconditioned on that pool snapshot's resourceVersion.
  - a hosted workspace is operator-tainted, quarantined or `Failed`;
  - its ledger was saved as `Retiring` or `Quarantined`;
  - it carries the retirement-intent annotation `substrate.workspace.orka.ai/shared-retire`.

  A crash between saving any of these intents and deleting the pool therefore cannot leave the pool running, even when the native sweep runs out of time or the native API is down. Every strong trigger (the marker, a failure, a pool's retirement) is persisted, and the pool's deletion started, under a fresh context bounded to 30 s and detached from the request's own deadline, so a native call that outlived the request cannot prevent it.
- **The Actor or worker is already lost:** the Actor crashed, the worker Pod is terminating or gone, or the pool is deleting, closed for retirement, or no longer holds the entry.
- **A suspect workspace:**
  - a workspace on the pool becomes operator-tainted, quarantined, or `Failed`, or its instance records a failure in its journal, even before the workspace reports `Failed`. The leader then strongly retires the whole pool, not just closes its admission, and admission never places an Actor on it.
  - an unbooted claim on such a pool fails closed.

Retiring one Actor never touches its neighbours except through this explicit strong path. An Actor that never booted holds no sandbox and only releases its slot.

### Maximum worker lifetime: a hard bound

`maxWorkerLifetime` bounds when the worker Pod, and any sandbox memory it holds, is gone, not only when deletion begins. From the pool's birth:

- **Strong retirement starts** at `maxWorkerLifetime − 3600 s − 5 min`. That leaves the worker Pod's whole termination grace, which contains the worker's 30-minute drain and Orka core's 30-minute eviction window, plus a margin.
- **Admission closes** one hour before that, which is why the minimum lifetime is 3h.
- **Escalation.** At `maxWorkerLifetime − 5 min`, a worker Pod that still exists is re-deleted with a one-second grace. By then every neighbour was suspended or failed without replay under Core's existing eviction semantics. One second rather than zero, because a zero grace removes the Pod object without the kubelet confirming its containers stopped, which would forge the absence proof.
- **Its own loop, Kubernetes alone.** Lifetime enforcement and escalation run in their own leader loop every 10 s. That loop makes no native call, is registered even when the provider's native configuration is missing or invalid (including invalid egress credentials), or native control cannot be reached (the provider then advertises nothing), and is never queued behind the native sweep, so an unavailable control plane or slow native discovery cannot postpone it. The native sweep itself has a one-minute aggregate deadline per pass.
- **Independent provenance.** The pool's mutable annotations alone never set the bound. The effective deadline is the earliest of:
  - the ledger's birth plus lifetime;
  - the creation-time `substrate.workspace.orka.ai/shared-deadline` annotation, which no ledger update touches;
  - the pool's server-set `metadata.creationTimestamp` plus the shortest lifetime frozen in any journal or archived record that recorded the pool's UID;
  - that creation time plus the 24h maximum, an absolute ceiling.

  If the journal inventory cannot be listed, provenance is unavailable. That is doubt, and every pool is then bounded by its creation time plus the 3h minimum, never by its annotations.
- **Doubt.** Unreadable or inconsistent lifetime evidence is doubt, and the pool is then strongly retired at once. That covers a corrupt ledger; a missing, malformed or mismatched deadline; or a ledger scope that disagrees with a journal's frozen scope. Its escalation is bounded by the shortest of these bounds, so a rewrite of both annotations can never lengthen it.
- **Discovery.** It does not depend on the removable label.
  - The bounds loop lists every WorkerPool and recognizes shared pools by provider-written evidence only: the label, the ledger or deadline annotation, or a provider journal or archived record that recorded the pool's UID.
  - A name alone is never evidence, so an operator pool named `ate-shared-...` is never touched.
  - A pool with shared evidence but without its intact provider-owned label is doubt and is strongly retired.
- **Recorded markers.** An unreleased strong marker or failure in any journal that recorded a pool's UID retires the pool, whatever the mutable entry list says.
- **Hard retirement.** It is never blocked by a ledger write.
  - Every strong retirement issues the UID-preconditioned Delete first, on its own fresh bounded context. A deletionTimestamp already stops admission and startup.
  - The retirement state and reason are written afterwards, best effort, with a separate 5 s timeout. Neither the result nor the duration of that write can affect the Delete.
  - **A failed Delete keeps the decision.** If the Delete itself fails, the provider still writes the retirement-intent annotation and the retirement state together, in one Update on its own short context, and returns the error. Admission and startup refuse a pool that carries the annotation, and every bounds pass retries its Delete. A decision to retire therefore survives a failed Delete and a restart, and no claim lands on the pool in between. The write is bound to the decided pool's UID. It reads the pool, writes nothing if the live object has another UID, and updates under the read's resourceVersion, rereading on a conflict. Nothing is written when the Delete failed its UID precondition. A stale decision therefore never marks a replacement pool under the same name. The write needs only the `get` and `update` verbs the provider already holds on WorkerPools, not `patch`.
  - A bounds pass works in phases:
    1. It decides every action, re-reading the clock per pool.
    2. It issues every due escalation. Pools escalate concurrently, at most 16 at a time, and each Pod list and Pod delete runs on its own 10 s context. A pool takes at most two such calls, so even when every call stalls to its timeout, one pass reaches 240 due pools within the 5-minute margin.
    3. It issues the pool Deletes concurrently, at most 8 at a time, so one slow DELETE never holds up the others. A Delete that fails records the retirement intent described above.
    4. It runs the bookkeeping writes within a 30 s total budget.

    Escalation also runs in its own 10 s loop that never waits on a pool Delete, so a slow pass cannot push a worker Pod's escalation past its deadline.
  - No durable write gates a mandatory deletion. A strong marker or instance failure that cannot be saved still has its pool's deletion attempted, and the persistence error is returned. A deleting pool or an absent worker Pod is itself strong evidence on every later pass, so the interim path is never taken afterwards.
  - Escalation does not wait for that deletion to have started.
  - Every variable-length ledger field is size-bounded, including the fenced worker's identity.
  - The ledger is encoded without HTML escaping, and an accepted ledger must leave explicit headroom both as stored and as re-encoded.
  - Only the non-strong `Draining` transition keeps a resourceVersion-conflict recheck.
- **Other reasons to close admission early.** Admission also closes when the operator pool's image or shape changes, the profile disappears, or the template inventory cannot be verified.
- **Limits.** The bound assumes a responsive kubelet. The Pod's absence on a partitioned node cannot be proven, and its instances then never report termination.

### Warm capacity

- **No private spares.** For opted-in profiles, free slots in admitting shared pools are the warm capacity, and ADR 0007 private spares are not created.
- **What the annotation counts.** The `substrate.workspace.orka.ai/spare-workers` annotation counts free admitting slots to keep. The leader creates empty shared pools to reach it. An emptied pool that the count still needs keeps admitting until its admission closes, so a resume or cold start of the profile normally lands on a running worker. Without the annotation an emptied pool is kept only for the 5-minute idle period below.
- **Inventory first.** Every pool creation, by an allocation or for warm capacity, requires a successful, complete template inventory showing no foreign template that could reach it. A failed inventory creates nothing.
- **Collection.** The leader deletes an empty pool once it has been empty for 5 minutes and is not needed for warm slots, or once its admission has closed. Deletion first proves that no Actor is assigned, and carries UID and resourceVersion preconditions.
- **Orphans.** A ledger entry whose exact journal record no longer holds it, and whose Actor is not assigned, is removed. A missing journal is not proof.

### Recovery

The journal and the ledger are both authoritative and fenced against each other:

- **The journal.** It records each of these, and each is saved before its effect:
  - the pool, its random label, its scope and the operator labels;
  - `createIssued` and `claimIssued`;
  - egress revocation and when it was stored;
  - the suspend fence and the interim proof;
  - any strong reason.
- **The ledger.** It records which journals hold slots.
- **Restart.** A restarted provider resumes each record from its last saved step.

### Timing

The ready and stopped timing logs report `workerSource: shared`. The stopped log adds `terminationProof: interim` or `strong`. `workerTeardownMs` measures from teardown start to the journaled proof.

## Consequences

- **Faster starts, fewer Pods.** Up to 16 Actors of one namespace and profile share one warm worker Pod, and a resume onto a pool with a free slot skips worker birth.
- **Weaker evidence.** This is weaker than ADRs 0006 and 0031. A per-Actor stop is proven by control-plane bookkeeping plus revocation, not by the sandbox's exit.
- **Residual risk: a leaked sandbox.** If the best-effort teardown failed, that sandbox's memory may persist on the node until the worker Pod is gone.
  - The `maxWorkerLifetime` bound holds only while the provider's bounds loop can reach the Kubernetes API to delete the pool and re-delete its Pod, and while the kubelet honours that deletion.
  - A provider or API server outage that spans the retirement deadline extends the leak until both recover. The bounds loop then retires the pool on its next pass, because the deadline is read from Kubernetes state, not from memory.
  - That memory includes workspace data and the instance's credentials. Core makes those credentials unusable asynchronously, as described above.
  - The sandbox has no tunnel and deny-all egress.
- **Residual risk: stale egress and DNS.**
  - A gateway TTL set higher than the provider's declared value makes the settle window too short.
  - Streams admitted before revocation continue until the checkpoint deactivates the tunnel.
  - The DNS relay is not stopped by that deactivation. It answers by its last fetched policy until teardown. After the settle window that policy is deny-all, unless its refresh failed, in which case it keeps the stale allowlist.
  - For a leaked sandbox whose teardown failed, that DNS relay session may persist until the worker Pod is gone.
- **Residual risk: foreign tenants.** Isolation from other tenants depends on the operator's dedicated-install assertion. Continuous template and assignment checks detect a violation only after it happens, and then strongly retire the pool.
- **Residual risk: worker compromise.** A compromised worker exposes every co-hosted Actor of the same namespace and profile, including their egress mTLS keys and any leaked memory.
- **Lower capacity than reported.** A leaked sandbox can also consume capacity the worker reports as free until it is recycled.
- **Strong retirements disturb neighbours.** A strong retirement suspends a pool's neighbours through Core's eviction path. A neighbour that cannot drain in time fails without replay.
- **Exit criterion.** Replace the interim evidence with an upstream per-Actor sandbox-exit attestation, and the template check with upstream tenant-scoped placement, as soon as Agent Substrate provides them. Until then, this decision must not be widened:
  - no install that is not asserted dedicated;
  - no cross-namespace pools;
  - no non-`VOLUMES` Actors;
  - no profile without the field;
  - no lifetime over 24h.
- **Live validation is still needed.** On a live Agent Substrate cluster it must show:
  - label propagation;
  - the capacity arithmetic for `N` Actors;
  - `observed_epoch` behaviour;
  - egress revocation and its settle window on a running Actor;
  - the eviction path and one-second escalation on a strongly retired shared worker;
  - a suspension continued after a `FinalizeSuspended` failure, finishing before the deleted pool's worker Pod is gone.
