# ADR 0006: Adopt the Agent Substrate v1.0.0 contract

Date: 2026-11-06

Status: Accepted. Extends ADRs 0002, 0004 and 0005 for the native Substrate provider.

## Context

The native provider was pinned to Agent Substrate v0.1.0. Agent Substrate v1.0.0
(`b34fbee742906494b5eb4f1f36987176bf8770a4`, `api/ateapi/v1/ateapi.proto`,
package `substrate.ateapi.v1`) changed every boundary the provider relied on:

- Direct Actor egress is gone. All Actor TCP egress traverses the egress gateway
  under a per-Actor `EgressPolicy` of host names and ports; everything else,
  including DNS for unlisted names, is denied. A Kubernetes `NetworkPolicy`
  admitted by Orka can no longer describe or confine Actor traffic.
- Actor DNS names are gone. Clients reach an Actor through `atenet-router` over
  mTLS from an allowlisted SPIFFE ID, selecting it with
  `ate-target-actor: <atespace>/<actor>`.
- Snapshots carry a fidelity (`VOLUMES`, `ROOTFS`, `MEMORY`); templates default
  to `MEMORY` and to golden snapshots, which seed fresh Actors. Resume has no
  boot flag. Actors no longer report a running template UID, and Tags no longer
  report a source Actor UID.
- Workers report `--max-actors` (default 1000) as their Actor count. Authorization
  is always enforced; `DrainWorker` needs global ownership.
- A v0.1.0 installation cannot be upgraded in place.

## Decision

**Shared API.** `RuntimeWorkload` gains `egress`, a `RuntimeEgressPolicy` of
rules `{protocol, host, ports}`. Protocols are `HTTP` (cleartext), `HTTPS`
(gateway-intercepted) and `TLSPassthrough`. A host is a lowercase FQDN with at
least two labels, or such a name with `*` as its entire leftmost label; bare
`*`, IP addresses, ports and URLs are invalid. Ports are explicit (1 to 16, no
all-ports form). A host and port may be granted by only one rule, at most 256
rules are allowed, and an empty list denies all egress. `egress` and
`networkPolicy` are mutually exclusive. Core must include every origin the
runtime itself needs, such as its in-cluster provider-authentication proxy.

`NativeProcessEvidence` gains `routerTarget`. When set it must equal
`<namespace>/<name>`, the startup endpoint must be the router's HTTPS URL, and
the admitted bootstrap port must be the process's default port 80 rather than
the endpoint's port. Evidence without `routerTarget` keeps the exact
endpoint-port rule, so a routed shape cannot be mistaken for a direct one.

**Provider.** The provider:

- rejects requests that carry `networkPolicy` or lack `egress`, compiles the
  allowlist into the Actor's `default` EgressPolicy before every Resume with
  metadata preconditions, re-verifies it on every Ready observation, and installs
  deny-all and refuses to boot when compilation fails. Templates project the
  `egress-mitm.ate.dev` and `system-roots.ate.dev` trust bundles and set common
  CA environment variables;
- compiles templates with `preferredFidelity: VOLUMES`, golden snapshots
  `DISABLED`, required Actor limits and every other server-defaulted field set
  explicitly, so immutable template digests survive server defaulting. A fresh
  Actor must hold no snapshot (cold boot); a restored Actor must hold exactly
  the Tag's completed `VOLUMES` snapshot under the checkpoint's template. Every
  captured snapshot and Tag must be `VOLUMES`, `COMPLETED`, from the expected
  owner and template UID. Suspension uses `DURABILITY_MODE_SYNCHRONOUS`, and the
  Actor UID, version and snapshot are fenced before and after `CreateTag`;
- replaces the `capacity.actors == 1` check: each private `ate.dev/v1` gVisor
  WorkerPool is sized at `cpu=C, memory=M+64Mi` for Actor limits `(C, M)`, and the
  worker must report capacity exactly `(C, M)`, the gVisor class, a registered
  epoch that the Actor runs under, and assignments for no other Actor;
- reads the bootstrap challenge through the router with its own pod-certificate
  identity, and reports the router endpoint with `routerTarget`;
- uses least privilege (Atespace `editor`, global `viewer`), never calls
  `DrainWorker`, and proves termination by exact worker Pod UID absence after
  verifying assignment exclusivity. Deletes carry `DeleteOptions{uid}`;
- advertises backend API `ateapi.v1.0.0` and writes journal
  `substrate.workspace.journal.v2` and catalogs `substrate.workspace.checkpoint.v2`.

**Legacy state.** v0.1.0 journals (`substrate.workspace.journal.v1`) and
catalogs or export indexes (`substrate.workspace.checkpoint.v1`) are never
adopted, migrated or cleaned up. Every operation touching them fails closed and
workspaces report `Provisioned=Unknown`, reason `LegacySubstrateV0State`.
Operators retire all v0.1.0 workspaces and checkpoints with the previous
provider release, reinstall Agent Substrate v1.0.0, then upgrade the provider
and Orka core together.

## Consequences

- Orka core must admit `RuntimeWorkload.Egress` for this provider instead of a
  NetworkPolicy, consume `routerTarget`, and reach the runtime through
  `atenet-router` with an allowlisted identity.
- The baseline shared CRDs are unchanged; `workload` and `allocation` are the
  Phase 2 additions that ADR 0005 already excludes from the baseline digests.
- Packet-level egress enforcement is the installed egress gateway's. The
  provider proves only that the admitted allowlist is applied before execution
  and unchanged while Ready.
- A compatibility row naming Agent Substrate v1.0.0 requires the installed proofs
  to pass against v1.0.0 with a core that implements the consequences above.
