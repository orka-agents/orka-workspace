# Implementation status

Source plan: `orka-workspace-implementation-plan.md`, revised 2026-10-02 against Orka `1cd16c88b43410e0ea46463f8a39b473d8e5250e` and fiberd `6e58535`. The plan takes precedence over the older repository-family proposal in [issue #1](https://github.com/orka-agents/orka-workspace/issues/1).

The [Core implementation at `2599695ae`](https://github.com/orka-agents/orka/commit/2599695ae06f5213d0411721347865888a0eff97)
pins the shared module at [production revision `63f33e3`](https://github.com/orka-agents/orka-workspace/commit/63f33e3dd3e4ce30d1066a99f0ea8bd184bd44a1).
Both are pushed on `feat/workspace-external-providers`. #572 is included and does
not block this implementation.

| Stage | Delivered or remaining |
| --- | --- |
| Phase 0 | ADRs 0001 through 0004 and a source-backed provider writer inventory |
| Phase 1 shared repository | Two modules, preserved APIs/CRDs/helpers, workload lifecycle contract, ConfigMap-backed fake implementation, executable conformance suite, admission policy and RBAC templates, build/test/generation/dependency checks |
| Phase 1 Orka consumer | Separate task branch replaces the old shared packages and sources generated workspace CRDs from the shared module; resolver regression checks use unchanged baseline hashes |
| Phase 2 | #572 is implemented and pushed. Shared workload sequences, generic class/API resolution, exact startup verification, ordered retirement, and upgrade gate are implemented. The two-replica ownership proof and actual core Task/RuntimeSession execution and retirement proof passed. |
| Phase 3 | Separate Sandbox controller and parameter schemas implemented. Lifecycle, suspension, storage-retention, identity, recovery, and race tests pass. Installed unmodified v1.0.3 passed all six filesystem persistence and deletion checks. |
| Phase 4 | Separate native ACP controller and independent checkpoint export/import implemented; lifecycle/suspension conformance and native recovery tests pass. Installed unchanged native v0.1.0 passed real Data capture, independent export, cold import after source deletion, and final native artifact collection. An actual Core Task through the deployed native provider passed authenticated Serving, RuntimeSession execution, persisted result, and exact compute/credential retirement. Pooled MCP Tools remain an explicit in-tree boundary. Full-memory restoration remains closed. |
| Phase 5 | Still gated. Upstream HEAD remains `6e58535`, verified 2026-10-05. Its [production-readiness document](https://github.com/helayoty/fiberd/blob/6e58535675df5eb0b55ab12f813892f0577c6bbf/docs/production-readiness.md) still lists unauthenticated control calls, unsafe cleanup failure handling, and persisted revocation/deletion gaps; selected-image launch and credential-bearing pressure-parking controls are also required. |
| Phase 6 | In-tree ACP Sandbox, native Substrate, and fake implementations, parameter APIs, schemas, and flags removed. Native MCP Tools remain behind `--substrate-mcp-tools-enabled`. Installation, compatibility, drain, upgrade, and uninstall procedures published. Stock Core passes fresh execution and rejects retained legacy resources before and after schema pruning without adopting or mutating them. |

## Decisions resolved during implementation

- Start in one repository with shared and providers Go modules.
- Give each provider its own config and profile kinds. Provider schemas stay with its deployment; the shared bundle does not acquire provider-specific schema. Retire the ACP union kinds after migration.
- Keep the controller-created RuntimePool API in `v1alpha1`. Enforce an upgrade preflight or workspace-dispatch startup gate that names every old-shaped pool and in-tree-bound workspace. After pruning, a pool missing the new workload is finalize-only. Retained workspaces retire under the old owner or retain that owner during coexistence; external controllers must not adopt them.
- A `Draining` provider accepts continuation only through an existing workspace for the exact session, slot, and provider binding. Cold resume qualifies. A newly planned session UID does not. `Disabled` blocks continuation and permits cleanup.

## Scope of verification

The fake has a Pod mode for real materialization and a separate simulation mode for lifecycle conformance. `make check` builds and vets both modules, runs race tests and conformance, validates CRD pruning/defaulting and CEL rules, checks generated files, and checks module boundaries.

The standalone live proof uses two provider replicas sharing one leader Lease and distinct core/untrusted ServiceAccounts. It passed 16 checks covering ownership policy denials, attachment acknowledgement before Pod creation, exact Pod UID/revision startup, stale identity, and authorized retirement. The listener was independently observed Running and Ready with zero restarts.

The real-core proof ran an actual Task through the separate fake provider and
Orka supervisor, using a deterministic ACP agent. It verified sealed credential
bootstrap, authenticated Serving admission, the current controller epoch, exact runtime
instance and supervisor boot, RuntimeSession and prompt identities, persisted
result, exact Pod retirement, and zero remaining attachment Secrets. No model
provider credentials were required. The default kind CNI does not prove
packet-level NetworkPolicy enforcement.

The installed Sandbox proof passed six checks with a non-root process and dynamic
Delete-reclaim storage: marker persistence, exact suspension and Pod absence,
fresh Pod UID with a rotated public nonce, unchanged PVC/PV identities, and
independently verified Pod/PVC/PV deletion. It uses standalone fixture admission,
so it does not prove real-core credential bootstrap or RuntimeSession behavior.

The installed native Substrate proof captured a random durable marker, terminated
the exact source Actor and worker Pod, exported an independent checkpoint, and
deleted the source public workspace. A fresh Actor and worker read that marker
without initializing it. The worker UID and public challenge rotated. Deleting
the public checkpoint preserved the inheriting workspace data; deleting its last
owner collected all native Tags and runtime templates. Templates remained
Data/Data/ColdBoot. This proof invokes the public reconciler with fixture core
admission and a deterministic native runtime. It does not claim authenticated ACP
or RuntimeSession execution. Exact preboot worker selection and policy ordering
were verified; packet-level enforcement remains outside the default kind CNI.

The additional native Core proof uses deployed Core and provider controllers and
an actual Task created through fail-closed class-use, provenance, and authority
admission. It passed authenticated Serving on the native endpoint's port 80,
matched the Actor UID/version and worker Pod to the RuntimeSession instance and
supervisor boot, and persisted the prompt result. Controller retirement removed
the exact Actor, private WorkerPool, worker Pod, Core RuntimePool, and all
attachment and pool credential Secrets. The report is retained at
`bin/external-substrate-core-20261006022705/report.json`. The independent client
observed the one-time public challenge listener already closed; actual Core
sealed bootstrap and authenticated Serving prove that startup protocol.

Two earlier native proof requests remain archived without replay. The original
Core request's unsupported storage quotas remained immutable and created no native
compute; its never-bootstrapped Core pool finalized normally. A later fixture
capacity rejection also cancelled and retired normally before a fresh Task used
the corrected worker capacity. These failures did not alter admitted requests or
require finalizer removal or manual native cleanup.

The retained-resource proof persists an old-shaped RuntimePool and legacy
workspaces in Ready, Suspended, Failed, and Deleted states through the API server.
Stock Core names all five blockers and exits before dispatch. It also rejects the
pool after schema pruning removes its old backend fields. Exact UIDs, spec,
status, and reserved metadata remain unchanged, and no runtime credentials or
compute are created. After guarded deletion of those synthetic objects, the same
Core image starts successfully. This proves the scoped workspace migration gate,
not general release or database upgrades.
