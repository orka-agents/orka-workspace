# ADR 0007: Claim pre-created spare private workers

Date: 2026-11-18

Status: Accepted. Extends ADR 0006 for the native Substrate provider. ADR 0008 replaces spares for profiles that set `spec.placement.sharedWorkers`.

## Context

Every Substrate allocation, and every cold resume from a Tag, creates a new
private single-replica WorkerPool from the profile's operator pool. The Actor
cannot Resume until that worker Pod is scheduled, its image is pulled, and the
worker registers `ACTIVE` and empty, so worker birth is on the critical path of
every start. Other runtimes avoid this by resuming onto already running
workers. ADR 0006's isolation model must stay as it is. One Actor runs per
private worker, sized so only it fits. The worker is proven empty before
Resume. The worker Pod is deleted, and its absence proven, before the Actor is
deleted.

## Decision

**Opt-in.** The mutable `SubstrateWorkspaceProfile` annotation
`substrate.workspace.orka.ai/spare-workers` requests `N` spares (`0` to `8`).
`sdk.ParametersProfileHash` hashes only apiVersion, kind and spec, and the spec
is immutable, so an annotation can change without invalidating admitted
workspaces. Absent, `0`, or invalid (logged) keeps the existing behavior with
no additional API calls on the allocation path.

**Spares.** A spare is built by the same function as a per-instance pool, from
the profile's operator pool and the infrastructure template's Actor size,
which is what every allocation computes because core sends native requests
without container resources. A spare differs from the pool an allocation
would create only in its birth label values. Agent Substrate copies WorkerPool
labels onto Workers, so an unclaimed spare's WorkerPool carries only a
spare-profile label and its unique birth labels. No selector matches them, so
no Actor can be placed on it. A leader-only replenisher keeps `N` unclaimed
spares per profile and deletes stale ones. A spare is stale when its profile is
gone, the count was lowered, the operator pool UID changed, or the
specification drifted. Deletion proves no Actor is assigned and carries UID
and resourceVersion preconditions.

**Claim.** Before creating its pool, an allocation selects a spare whose
specification is byte-identical except for birth labels and whose only worker
is `ACTIVE`, registered, gVisor, exactly sized, empty, and unassigned. The
journal records the selection and `claimIssued`. One Update, preconditioned on
the read resourceVersion, rewrites the spare's metadata into exactly the
private-pool metadata the allocation would have created: labels, anchor
ownership, and a claim marker naming the journal UID and sequence. Two claims,
or a claim and a delete, can never both apply. The pool UID is saved only after
the record's own marker is observed. Any conflict or doubt falls back to the
fresh pool before any template or Actor exists. The derived template selects
the spare's birth labels, which were on its worker Pod from birth. The claim
never touches the Pod, and Startup waits until the Worker reflects the claimed
WorkerPool labels. All pre-Resume checks are unchanged, and so are retirement
and teardown. A claimed spare never returns to the spare set.

**Recovery.** The journal is authoritative. An issued claim binds only through
the record's own marker on the pool. If retirement finds no such marker, it
releases the claim in the journal. A claim that lands after its allocation fell
back or released it is an orphan. The replenisher deletes a claimed pool only
when the exact journal's record for that sequence does not hold it and no
Actor is assigned. A missing or replaced journal is not proof, so that pool
waits for its anchor ownerReference.

## Consequences

Each spare is one idle worker Pod with the Actor's requests plus the gVisor
reservation, held per profile until claimed. A claimed spare's worker has
already run, empty, since its birth. Startup still proves its exact Pod,
process epoch, capacity and empty assignment list immediately before Resume.
Workspaces resumed after an operator image roll claim only spares built from
the current operator pool. Proving the start-time benefit and the
WorkerPool-to-Worker label propagation needs a live Agent Substrate cluster.
