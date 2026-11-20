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
  - Each save's entry is stamped. A reread never replaces an entry saved after the reread began, so a reread that listed a journal before its template's UID was written cannot restore the unknown UID over the known one.
  - A check whose templates are all in that set makes no ConfigMap call. When a template that reaches the pool is missing, or its recorded UID is still unknown, the check rereads the records once, with two Lists and no per-catalog Gets, before it treats the template as foreign. A stale set can therefore only cause a reread, never a template counted as recorded that is not. A recorded name whose UID is still unknown matches a template only on that authoritative reread, never from the in-memory set.
  - The set is bounded. Only shared-relevant templates enter it, from a save or a reread: those of records with a shared placement, and checkpoint-catalog templates in an Atespace that has one. A private template never reaches a shared pool. A reread still decides its own check against the whole inventory. If what it would cache exceeds 4096 entries, it caches nothing and clears the set, so later checks miss and reread rather than hit. A save that would exceed the cap drops the set whole. A sweep that finds no shared profile or pool clears it. Every entry that is then missing is reread before it counts, so pruning costs reads, never a wrong match.

  - **Classification order.** A check classifies what it can before any fallible read. In the sweep, a reaching template outside a pool's Atespace quarantines that pool right after the native template List, before the provider's records are read; only same-Atespace templates wait for that read. A reaching template outside the pool's Atespace is foreign whatever the record says, and its pool's strong retirement starts at once. Only a same-Atespace template the in-process record does not know by its exact UID waits on the reread of the provider's records.
  - **When the native ActorTemplate inventory cannot be listed.** Admission refuses at once, and the sweep closes the pools it maintains. The outage is tracked by its own instance of the same outage rule, which only a complete, successful inventory refreshes.
    - Once the outage has lasted past 60 s, every shared pool is strongly retired. This happens only if a cheap native worker read still succeeds, run on its own 10 s context so the sweep's deadline cannot postpone it, meaning the control plane can still place a foreign Actor.
    - When the control plane looks down from the provider, retiring would strand every shared Actor's data, which cannot be suspended without it. So retirement is deferred, but only for short outages: once the total outage reaches 5 minutes (`sharedControlPlaneDownGrace`), every shared pool is strongly retired regardless, even though neighbours may then fail without replay. A provider-side transport failure does not prove the control plane is down for everyone. During a provider-only network partition, other clients may still place foreign Actors, so that exposure is bounded by 5 minutes. After that, isolation wins over data.
    - **Degraded start.** When native control cannot be set up at startup (an invalid configuration, or a dial that fails), no native sweep runs, so the Kubernetes-only bounds loop applies this rule itself. Admission is already closed, because no lifecycle controller runs. The loop stamps each shared pool's outage start durably, keeping an earlier stamp, and strongly retires the pool at 60 s. The dial never connects, so such a failure is always this provider's own local misconfiguration (unreadable or invalid TLS material, token or endpoint). It is treated like a credential rejection, never like a control plane that is down.
    - The outage age is durable. When the sweep first sees the inventory fail, it stamps each shared pool with `substrate.workspace.orka.ai/native-outage-since`, and clears the stamp on the next successful inventory. The age runs from the earlier of that stamp and this process's first failure, so a provider that restarts in a loop never resets the clock. Writing the stamp is best effort; if every write fails, the age is this process's own.
    - Only a transport or availability failure of that worker read counts as the control plane being down: gRPC `Unavailable`, `DeadlineExceeded` or `Canceled`, a context timeout, or a refused or reset connection.
    - A rejection of the provider's own credentials (`Unauthenticated` or `PermissionDenied`, for example an expired token or a revoked viewer permission) does not, and gets no deferral. Other clients may still place Actors, so the pools are retired at 60 s.
    - **That trade-off favours isolation.** Suspending the neighbours also needs those rejected credentials, so they may fail without replay and keep only their last checkpoint. The same holds for the sweep's per-pool isolation checks (see "Isolation checks have their own budgets" below). A credential rejection lasting past 60 s is an operator misconfiguration. The provider makes it visible: it logs an error, and on every heartbeat it records a `NativeCredentialsRejected` Warning Event on the ExecutionWorkspaceProvider, until a native read succeeds again.
    - The bounds loop never lists templates, so the sweep enforces this.
  - **When the inventory cannot be read.** Without the provider's recorded-template inventory, a reaching template outside the pool's Atespace is still foreign, and the pool is quarantined at once. A reaching template inside the Atespace that the in-process record knows by its exact UID stays fine. Any other reaching template is unclassifiable. Admission then refuses the pool at once, the sweep closes it (`Draining`), and the pool is strongly retired once the inventory has stayed unavailable for 60 s. This uses the same outage rule and bound as the journal inventory, but tracks its own reads, because the bounds loop's labelled journal List succeeds independently and would otherwise clear an outage that never ends.

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
  - the evidence-only suspend issued by a Stop is rejected or times out, or any later step of that Stop's interim evidence fails, including a journal write (a write conflict is retried). The strong reason is persisted, and the pool's retirement started, before that pass returns;
  - a Stop finds its Actor `SUSPENDING` after its evidence-only suspend was issued, for example after a crash between that RPC and its result. A synchronous suspend that succeeded returns `SUSPENDED`, so this one is unresolved, and a failed upstream workflow can leave it `SUSPENDING` for good. A Stop needs no data, so it neither waits for nor continues that suspension; it takes the strong path at once.

  Retirement of the pool starts at once, and the data capture still proceeds when it can. A suspend whose outcome is unresolved is never reissued against a `RUNNING` Actor.
  - **Continuing a stalled suspension.** A SuspendActor call can fail after the Actor was marked `SUSPENDING`, for example on a transient database timeout in `FinalizeSuspended`. The Actor then stays `SUSPENDING`, and the strong path's pool deletion would crash it and reclaim its node state (`workflow_worker_delete.go`), losing the data. The provider therefore re-issues the same synchronous SuspendActor for a shared Actor seen `SUSPENDING`, at most 10 times, at least 10 s apart, recording each attempt in the journal before it is sent. Only the data-keeping Suspend continues. If all 10 continuations end without a result (for example a crash each time), the suspension is doubt and takes the strong path instead of waiting for the lifetime bound.
  - This continues the same upstream suspension and is not a replay. Agent Substrate's workflow is re-entrant (`workflow_suspend.go`): it skips `MarkSuspending` for a `SUSPENDING` Actor, so no new snapshot is minted. It sends the persisted snapshot to atelet, which answers a checkpoint it already committed without taking another. `FinalizeSuspended` then records it. The Actor lease serializes the workflow, so a continuation sent while the original call is still running is refused with `Aborted` and has no effect.
  - A failing retirement never stops a data-keeping suspend or its capture, before or after the suspend is issued. If doubt before the suspend starts the strong path and the worker Pod's own delete succeeds while the pool's keeps failing, the suspend is still fenced, issued and captured, and the retirement error is returned with it. Only an evidence-only Stop, which keeps no data, returns at once.
  - A failing pool delete never stops it. The worker Pod is deleted first under its bounded grace, so the continuation and the Tag capture proceed in the same pass while the Pod terminates. The pool delete's error is recorded as retirement intent and retried on the next pass.
  - The continuation must finish before the worker Pod is gone. A pool deleted for the strong path keeps its Pod for its termination grace, which leaves room for it, but a continuation that loses that race leaves the instance lost and keeps its last verified checkpoint. A private worker, which faces no such deletion, keeps observing without resubmitting.
  - Once the worker Pod is proven absent, a capture that still fails one of the provider's own evidence checks (not a retryable native, Kubernetes API, transport or context error, such as a refused or reset connection, a TLS or network timeout, or an unexpected EOF) is final. That instance is reported lost and retires through Stop, keeping its last verified checkpoint; no Tag is invented.
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
  - **The admission window** is therefore `maxWorkerLifetime − 3600 s grace − 5 min margin − 1 h lead`, that is `maxWorkerLifetime − 2 h 5 min` after the pool's birth: 55 minutes at the 3h minimum, and 21 h 55 min at the 24h default.
  - The short window at the minimum is intended. The one-hour lead gives an Actor admitted just before admission closes at least an hour of work before strong retirement starts evicting it, and the Pod's whole 3600 s grace must fit before the bound. At 3h, a pool therefore admits for under an hour, and resumes after that land on a new pool. Profiles with frequent resumes should use a longer lifetime (6h gives 3 h 55 min).
  - A shorter or lifetime-scaled lead would trade that guaranteed hour for a longer window. It is not configurable today; a profile field bounded, for example, to 15 min – 1 h is a possible extension.
- **The worker Pod's first delete carries the bound.** Every retirement (lifetime, strong, outage, doubt and quarantine) first deletes the pool's worker Pods directly, each with a UID precondition, with a grace of `clamp(maxWorkerLifetime − now − 5 min, 1 s, 3600 s)`. Only then is the pool deleted.
  - A retirement at its scheduled time therefore still gives the full 3600 s, as does any earlier strong, doubt or quarantine retirement.
  - The bounds pass uses its provenance-checked bound for that grace. Other paths use the pool's annotations' bound under the 24h ceiling; that a writer could lengthen it is covered by the threat model below.
  - Live validation showed why this is needed. A kubelet that has started a 3600 s stop does not shorten it when a later delete lowers the Pod's grace: the API object drops to 1 s, but the container stop already in progress keeps its original timeout. The worker then exited only at its own 30-minute drain limit.
  - The API server never lengthens an in-progress grace, so the Deployment's later cascade cannot undo the cap.
  - Worker drain and Core's eviction both start when the delete lands, and both run inside that grace. When the grace is shorter than a neighbour needs to checkpoint (only for a retirement issued late, after a provider or API outage), that neighbour fails without replay and keeps its last checkpoint. This is the accepted lifetime residue.
  - Every cleanup step has its own bound. The direct Pod cleanup and the pool Delete each get their own 30 s cleanup budget, so a slow Pod cleanup never leaves the Delete an expired context. Each Pod List, pool read and Pod delete within the cleanup also has its own 10 s bound, and each ledger or intent write has its own 5 s bound.
  - **The grace cap is best effort when Pod operations themselves fail.** If the direct Pod cleanup fails, for example because the Pod List keeps failing, the UID-preconditioned pool Delete is still issued, and the Pod error is returned with it. Isolation wins over the cap. The Deployment's cascade may then give the worker Pod its default 3600 s grace, while the escalation loop keeps re-deleting it every 10 s and later passes retry the capped delete.
  - The ReplicaSet may create a replacement worker Pod between the direct Pod delete and the pool's cascade. That Pod gets the full 3600 s from the cascade, but it hosts no Actor: startup refuses any worker that is not the pool's fenced one.
- **Escalation.** At `maxWorkerLifetime − 5 min`, a worker Pod that still exists is re-deleted with a one-second grace. That re-delete is best effort: it ends the bound only for a Pod whose stop has not already begun with a longer grace, which the capped first delete prevents for the Pods this provider deletes. Escalation never waits for provenance discovery. Each pass first escalates every pool already due by the latest bound any provenance could give it: its annotations' bound under the 24h ceiling, which journal or profile provenance can only bring earlier. It then reads provenance (one journal List, one pool List, one cluster-wide profile List and, for the bounds pass, one workspace List per namespace, concurrently) under a 60 s overall deadline, and escalates what that made due. The pool List has its own 60 s deadline, never the provenance reads' remainder, so a journal read that uses up the budget still leaves the pools listed. The unavailable-provenance bound and the 60 s outage retirement then apply to them on time. The outage state is the journal reads' own record, evaluated before any early return. If the pool List itself fails, nothing can be retired by name that pass: the loop retries within its 10 s period, and a failed journal read keeps admission closed meanwhile. The unavailable-provenance bound of creation plus 3h is never used for the first step, because it would escalate a healthy longer-lived pool before its retirement and eviction. By then every neighbour was suspended or failed without replay under Core's existing eviction semantics. One second rather than zero, because a zero grace removes the Pod object without the kubelet confirming its containers stopped, which would forge the absence proof. Only the provider's own deletes are kept on the kubelet path this way; see the accepted restriction under Limits.
  - **Tied to the pool's UID.** Each Pod it deletes belongs to the retired pool's UID, never only to its name, so a pool replaced under the same name never loses its Pods. Upstream worker Pods are owned through a ReplicaSet and Deployment, so a Pod's own owner reference does not name the pool.
  - The Pods are listed by the pool-name label, and the pool is then read again. The pass's pool List saw the retired UID before the Pod List, and the read sees it after. A UID is never reused, so the name held that pool throughout, and every listed Pod is that pool's or older.
  - If the pool is gone or replaced by then, only Pods whose UID the pool's ledger or a journal fenced on that pool are deleted.
  - Each delete is preconditioned on the listed Pod's UID. The strong absence proof already deletes only journal-fenced Pods, by name, with their UID checked and as the precondition.
- **Its own loop, Kubernetes alone.** Lifetime enforcement and escalation run in their own leader loop every 10 s. That loop makes no native call, is registered even when the provider's native configuration is missing or invalid (including invalid egress credentials), or native control cannot be reached (the provider then advertises nothing), and is never queued behind the native sweep, so an unavailable control plane or slow native discovery cannot postpone it. The native sweep itself has a one-minute aggregate deadline per pass.
- **Independent provenance.** The pool's mutable annotations alone never set the bound. The effective deadline is the earliest of:
  - the ledger's birth plus lifetime;
  - the creation-time `substrate.workspace.orka.ai/shared-deadline` annotation, which no ledger update touches;
  - the pool's server-set `metadata.creationTimestamp` plus the shortest lifetime frozen in any journal or archived record that recorded the pool's UID;
  - for a pool no journal recorded, such as an unused warm pool: that creation time plus the `maxWorkerLifetime` of the profile its ledger scope names. Profiles are immutable and hashed, so the profile in the scope's namespace with the scope's UID applies only if its hash still equals the scope's. A profile that is gone, mismatched, unreadable or no longer opted in gives the 3h minimum instead. Both loops read profiles with one List per namespace;
    - the ledger scope counts here only if the pool's name commits to it. A new pool is named `ate-shared-<commitment>-<nonce>`, where the commitment is 16 hex characters of a domain-separated SHA-256 of the canonical scope, including the profile UID, profile hash and lifetime, and the nonce is 16 random hex characters. The name is immutable and set at creation;
    - a scope the name does not commit to is doubt: the pool is strongly retired at once, and admission refuses it. A ledger rewritten to name a different valid 24h profile is caught this way;
    - a pool whose name follows no commitment (one created before the scheme) commits to nothing and gets no profile bound. Without journals, it is bounded by its creation time plus the 3h minimum;
  - that creation time plus the 24h maximum, an absolute ceiling.

  If the journal inventory cannot be listed, provenance is unavailable. Admission closes at once. Inside the 60 s outage window below, retirement and escalation use the annotations' bound under the 24h ceiling, so one transient failed List never retires or escalates a healthy pool. Once the outage outlasts that bound, every pool is bounded by its creation time plus the 3h minimum, never by its annotations, and every shared pool is strongly retired.
- **Unavailable provenance closes admission at once, and retires only past a bound.**
  - A new claim, a new pool, and every Resume onto a shared pool need a successful journal inventory read within the last 30 s. The bounds loop reads it every 10 s, and admission reads it itself when that read is older or has failed since.
  - A failed read closes admission at once.
  - The leader tracks the failure in process, from its first failed read. If reads are still failing 60 s later, every shared pool is strongly retired as doubt. Any successful read resets that tracking.
  - A newly elected leader starts with no successful read, so it admits nothing until its own read succeeds. Its 60 s bound starts from its own first failure.
  - Each read's result is stamped with the time the read started. A success clears a failure only if it started after the latest failure started, so a slow success that finishes after a newer failure never reopens admission. A failure always counts. Admission's own read admits only if, once it finishes, health is fresh: a read that started before a newer failure, or took longer than the 30 s window, refuses the claim or Resume.
  - **The trade-off.** Retiring every shared pool on a single failed List would turn one transient API error into an eviction of every shared Actor on the install. So a short outage only stops new Actors from joining a pool whose annotations cannot be cross-checked. During that window, a strong marker hidden by a tampered ledger entry, or annotations rewritten past the pool's frozen lifetime, are not acted on for up to 60 s plus one pass, but no Actor can join such a pool. An outage that outlasts the bound is treated as doubt about every pool: the 3h creation-time ceiling then applies and every shared pool is strongly retired.
- **Doubt.** Unreadable or inconsistent lifetime evidence is doubt, and the pool is then strongly retired at once. That covers a corrupt ledger; a missing, malformed or mismatched deadline; or a ledger scope that disagrees with a journal's frozen scope. Its escalation is bounded by the shortest of these bounds, so a rewrite of both annotations can never lengthen it.
- **Discovery.** It does not depend on the removable label.
  - The bounds loop lists every WorkerPool and recognizes shared pools by provider-written evidence only: the label, the ledger or deadline annotation, or a provider journal or archived record that recorded the pool's UID.
  - A name alone is never evidence, so an operator pool named `ate-shared-...` is never touched.
  - A pool with shared evidence but without its intact provider-owned label is doubt and is strongly retired.
- **Recorded markers.** An unreleased strong marker or failure in any journal that recorded a pool's UID retires the pool, whatever the mutable entry list says.
  - So does a tainted, quarantined or `Failed` workspace of such an unreleased journal, under the journal's exact workspace UID. Removing an entry from the ledger therefore cannot hide a suspect workspace.
  - The bounds loop reads those workspaces with one ExecutionWorkspace List per namespace. A namespace whose List fails is reported, and its pools are still checked entry by entry.
- **Hard retirement.** It is never blocked by a ledger write.
  - Every strong retirement issues the UID-preconditioned Delete first, on its own fresh bounded context. A deletionTimestamp already stops admission and startup.
  - The retirement state and reason are written afterwards, best effort, with a separate 5 s timeout. Neither the result nor the duration of that write can affect the Delete.
  - **A failed Delete keeps the decision.** If the Delete itself fails, the provider still writes the retirement-intent annotation and the retirement state together, in one Update on its own short context, and returns the error. Admission and startup refuse a pool that carries the annotation, and every bounds pass retries its Delete. A decision to retire therefore survives a failed Delete and a restart, and no claim lands on the pool in between. The write is bound to the decided pool's UID. It reads the pool, writes nothing if the live object has another UID, and updates under the read's resourceVersion, rereading on a conflict. Nothing is written when the Delete failed its UID precondition. A stale decision therefore never marks a replacement pool under the same name. The write needs only the `get` and `update` verbs the provider already holds on WorkerPools, not `patch`.
  - A bounds pass works in phases:
    1. It decides every action, re-reading the clock per pool.
    2. It issues every due escalation. Pools escalate concurrently, at most 16 at a time, and each Pod list, pool read and Pod delete runs on its own 10 s context. A pool takes at most three such calls, so even when every call stalls to its timeout, one pass reaches 160 due pools within the 5-minute margin.
    3. It issues the pool Deletes concurrently, at most 8 at a time, so one slow DELETE never holds up the others. A Delete that fails records the retirement intent described above.
    4. It runs the bookkeeping writes within a 30 s total budget.

    Escalation also runs in its own 10 s loop that never waits on a pool Delete, so a slow pass cannot push a worker Pod's escalation past its deadline.
  - No durable write gates a mandatory deletion. A strong marker or instance failure that cannot be saved still has its pool's deletion attempted, and the persistence error is returned. A deleting pool or an absent worker Pod is itself strong evidence on every later pass, so the interim path is never taken afterwards.
  - Escalation does not wait for that deletion to have started.
  - Every variable-length ledger field is size-bounded, including the fenced worker's identity.
  - The ledger is encoded without HTML escaping, and an accepted ledger must leave explicit headroom both as stored and as re-encoded.
  - Only the non-strong `Draining` transition keeps a resourceVersion-conflict recheck.
- **Only shared records are inventoried.** A journal or archived record is labelled `substrate.workspace.orka.ai/shared: "true"` in the same write that first records a shared placement. Archives copy the label, and the provider never removes it. Once a process has confirmed that every shared record carries the label, the bounds, escalation and admission inventory lists only labelled records. A private-only install then lists and decodes none.
  - Shared records written before the label existed are labelled by a separate leader-only backfill loop, never by discovery or enforcement. It runs every 30 s until done, each pass within its own 60 s bound, labelling concurrently (at most 8 writes at a time), each write preconditioned on the listed resourceVersion.
  - Until a full scan finds no unlabelled shared record left, discovery keeps scanning in full and counts unlabelled shared records as shared, so nothing waits on a relabel. A newly elected leader is a new process, so it confirms once again.
  - An operator who strips the label hides that record's provenance. Operators are trusted, so this is defence in depth only. A pool with shared evidence (its label, ledger or deadline annotation) is still found by the pool List.
- **Reads per pass.** The bounds pass and the native sweep share one snapshot per pass: the labeled journal and archived-record inventory from that pass's one List, and each namespace's ExecutionWorkspaces from one List. Per-pool checks read from it, never with a Get per entry. A lookup the snapshot cannot answer falls back to a live Get, so the snapshot never turns a present object into an absent one. When a namespace's workspace List, or the pass's journal List, fails, that failure is cached for the rest of the pass. No pool re-requests it, and every pool that needed it is unverified at once, so its outage bound keeps running. The snapshot is taken before the pools are listed, so an entry it shows as not held (for example by an allocation that saved its Actor UID or moved to a new sequence in between) is confirmed with a live read of that one journal before the pool is retired or the entry collected. Only such a mismatch costs a live read. Every read on the enforcement paths has its own bound: the escalation step's pool List 30 s, discovery 60 s, the bounds pass's hosted-journal and workspace checks one shared 60 s deadline, and the native sweep its one-minute deadline. A pool past its strong retirement time is retired before any hosted read. The sweep lists each worker's native assignments once, shared by the foreign-assignment and ledger checks. It re-reads a pool only when that snapshot shows an assignment its ledger lacks. Admission and a single allocation's retirement always read live.
- **Shared safety runs without the profiles.** If the sweep cannot list profiles, it still runs every shared-pool safety check: foreign templates, hosted suspicion, workers and assignments, and the outage trackers and their retirements. Only work that needs profile shapes is skipped: closing on a changed scope, collection, ledger writes and warm creation. Shared admission is closed while the List keeps failing.
  - A profile List outage alone never retires a pool, even past 60 s. Every isolation check still runs on each sweep, and the only check that needs profiles, whether the pool's scope is still current, protects admission, not isolation. Each claim and Resume also verifies its own profile binding and refuses a scope that no longer matches.
- **Outage stamps never delay a safety action.** Durable outage stamps and their removals are written in the background as each pool is decided, concurrently (at most 8 at a time), each pool read fresh and UID-checked. The pass's end waits for those still in flight only within a 30 s budget of its own. A stalled stamp write can delay durability, never a quarantine or a retirement. Meanwhile the in-process age applies.
- **Isolation checks have their own budgets, and an incomplete check is an outage.**
  - Each isolation check runs on a fresh bounded context of its own, never on a deadline another step already spent: the native ActorTemplate List and the provider's records (30 s each), and, per pool, the hosted-workspace check, the worker and assignment checks, and the ledger check (15 s each). Each budget is capped by the sweep's configured deadline, and each still ends at once when the loop's own context is cancelled (shutdown or lost leadership), even after the sweep's deadline has expired.
  - Every pool's known strong triggers, which need no read (its persisted state, its lifetime, the pass's template inventory and the outage bounds), are evaluated for all pools first. The retirements they decide are then issued concurrently, at most 8 at a time, quarantines first, each on its own short bound, so many due retirements never hold up a required quarantine.
  - A confirmed decision is never dropped by a later fallible read. A foreign assignment confirmed on two listings retires the pool at once from the identity already in hand, its Delete preconditioned on that UID, which already guards against a replaced pool. When the pool cannot be re-read before its worker Pods are deleted, only the Pods fenced on that pool are deleted, and the pool's Delete still proceeds. A read before a decision is confirmed that fails counts toward the pool's unverified bound instead.
  - Shutdown or lost leadership stops every pass promptly. Each per-item loop, and each hand-off waiting for a free worker, checks the loop's own cancellation between items, so no new retirement, check or write starts afterwards. Mandatory deletes already in flight still finish within their own short bound. The pools' fallible checks then run concurrently, at most 8 pools at a time. Stalled reads on some pools therefore never postpone a quarantine the inventory already proves. They do not add up across pools either: a pass takes about the per-pool check budget times the number of pools divided by 8, rounded up. Each namespace's workspaces and workers are still listed once per pass.
  - Non-isolation work (profile discovery, closing, collection, ledger writes and warm creation) has its own budget, started only after every pool's isolation checks, so a long isolation scan never leaves it an expired context. A slow profile lookup therefore never fails a safety check.
  - A pool whose isolation checks do not all complete, whether a check fails or times out, is **unverified**. This is tracked per pool, by pool UID, and durably, with a `substrate.workspace.orka.ai/unverified-since` stamp. Admission refuses an unverified pool at once.
  - It is strongly retired once unverified for 60 s. Only a native check's transport or availability failure, while the control plane looks down from the provider, takes the 5-minute control-plane-down grace instead. A credential rejection never does.
  - **The bounds loop enforces the durable stamps too.** A long sweep scan visits each pool once, so the Kubernetes-only bounds loop also retires, every 10 s, any pool whose durable `unverified-since` stamp is past its bound, whatever the sweep's progress or a restart. The sweep writes the stamp and its classification, `unverified-grace`, in the same Update: `none` takes the 60 s bound, and `control-plane-down` takes the control-plane-down grace because native looked down at that failure. A stamp whose classification is missing keeps the grace until the sweep classifies it, so the bounds loop never retires early. The loop also retires a pool whose `native-outage-since` stamp is past the control-plane-down grace, the longest bound that stamp can carry; the sweep applies the precise 60 s.
  - Outage stamps are written at once, in the background, as each pool is decided: concurrently, at most 8 at a time, each on its own short bound and retried on a write conflict. They never wait for the scan to end, so a restart mid-scan keeps their age, and they never delay a safety action. Each write carries the time of the decision it represents and is applied through a fresh read and a resourceVersion-preconditioned Update. It is applied only if that stamp's recorded decision is not newer, so a late set can never revive a stamp a newer decision cleared, and a late clear can never remove a newer outage. A writer whose pass a later pass of the same loop has superseded stops before writing. Each decision is recorded whenever the pool's snapshot does not yet reflect it, or this process's own last decision for that stamp was the opposite one. A success records its clear when the snapshot shows the stamp or this process last decided a set, and a new failure records itself when this process last decided a clear. A stamp write still in flight from an earlier failure is then outranked, even if it lands after the success.
  - A pool hosting booted Actors whose worker cannot be listed cannot be checked, so it is unverified, never "nothing to check". A pool whose claims are still booting, or an empty warm pool, stays neutral.
  - A pool's record is cleared only when all of that pool's own checks complete. Another pool's success, or a pool with no worker to read, never clears it. Every outage tracker is cleared only by a success that covers what it guards: the journal inventory by a complete shared-journal List, the recorded-template inventory by a complete read of journals, archives and catalogs, and the native template inventory by a complete ActorTemplate List.
  - The sweep runs its own copy of the bounds pass last. The dedicated bounds loop enforces lifetime bounds on its own period, so stalled pool deletes never delay the sweep's isolation checks.
- **Shared isolation work runs before spare maintenance.** Each sweep runs the shared-pool safety work (foreign templates, hosted suspicion, outage retirement) first, on its own deadline, and only then private spare replenishment, which is latency work, on a separate one. A stalled spare lookup can never starve the shared checks.
- **Known strong triggers come first in the sweep.** In each pool's maintenance, a strong trigger already known, or learnable without the read that failed, acts before any fallible read that could return early. The pass's foreign-template inventory is checked before any hosted read. A failed hosted-workspace or journal check is held back, so the worker-replaced, worker-restarted and foreign-assignment checks still run. Only the non-strong maintenance (closing, collection and ledger writes) is skipped for that pool. Strong retirement uses its own detached context, so it also works for pools reached after an earlier pool used up the sweep deadline.
- **Other reasons to close admission early.** Admission also closes when the operator pool's image or shape changes, the profile disappears, or the template inventory cannot be verified.
- **Limits.** The bound assumes a responsive kubelet. The Pod's absence on a partitioned node cannot be proven, and its instances then never report termination.
  - **Accepted residual risk: a WorkerPool List that stalls on every pass.** Each pass bounds that List (30 s for the escalation step, 60 s for discovery), but if it never completes, no pool is examined, and neither retirement nor escalation happens. There is no Pod-only fallback, because it would not be safe:
    - a Pod's deadline lives on its WorkerPool (the ledger, the deadline annotation and the profile its scope names), and the Pod's own creation time is later than its pool's;
    - a replacement worker Pod has no fenced identity;
    - the guard that keeps escalation off a replacement pool's Pods needs the pool read.

    An API server that cannot serve that List also stops Kubernetes' own controllers and the kubelet-confirmed Pod deletion the bound relies on. The bound resumes on the first pass whose List completes, because every deadline is read from Kubernetes state.
  - **Kubelet and API behaviour this relies on.**
    - Guaranteed by the API: a delete never lengthens a grace already in progress, and a Pod deleted with a non-zero grace is removed only after the kubelet confirms its containers stopped.
    - Not guaranteed, and not relied on: the kubelet shortening a stop already under way when a later delete lowers the grace. The live result above showed it does not.
    - **What the bound guarantees, precisely.** `maxWorkerLifetime` is enforced only through graceful deletion that the kubelet honours, with a capped grace. A kubelet restart during a stop restarts it with the grace the Pod object then carries: the capped value, or one second once escalation has lowered it, never the original 3600 s, because the API server never lengthens it. A restart can therefore extend termination by up to one capped grace past the escalation time. A node or kubelet failure is outside the bound. This is the accepted limit of the claimed hard bound. The provider has no enforceable mechanism beyond it within its RBAC: it has no node access, an eviction carries a grace too, and a zero-grace force delete would only remove the object without stopping the containers.
  - **Accepted operational restriction: Pod absence is kubelet-confirmed only for the provider's own deletes.** The provider deletes worker Pods only with a non-zero grace, which stays on the kubelet path. Two other paths remove the Pod object without kubelet confirmation:
    - PodGC deletes Pods bound to a Node object that no longer exists, for example when a partitioned node's Node object is deleted while its worker keeps running;
    - anyone may force-delete the Pod.

    Exact Pod UID absence then does not prove the hosted sandboxes stopped. This is the same limit as Orka's existing private-path proof (ADRs 0031 and 0049), which also rests on exact worker Pod UID absence. Operators must not force-delete Substrate worker Pods, and must not delete the Node object of a partitioned node without first fencing that node.
  - The provider does not detect these cases. Doing so would need each fence's node and an observed graceful deletion persisted before absence, plus Node read access, which the provider does not hold.

### Warm capacity

- **No private spares.** For opted-in profiles, free slots in admitting shared pools are the warm capacity, and ADR 0007 private spares are not created.
- **What the annotation counts.** The `substrate.workspace.orka.ai/spare-workers` annotation counts free admitting slots to keep. The leader creates empty shared pools to reach it.
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
- **Threat model: writers in the provider's namespaces are operator-equivalent.** A principal with WorkerPool or ConfigMap write access in the provider's namespaces can already disrupt or delete workers, so it is treated as operator-equivalent. Making the lifetime bound tamper-resistant against such a writer is defence in depth. It relies only on immutable, server-set data (the pool's name, UID and creationTimestamp) and the provider's own journals, never on mutable annotations alone. A rewrite of an annotation is therefore not by itself a new hole.
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
