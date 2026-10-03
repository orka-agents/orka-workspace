# Implementation status

Source plan: `orka-workspace-implementation-plan.md`, revised 2026-10-02 against Orka `1cd16c88b43410e0ea46463f8a39b473d8e5250e` and fiberd `6e58535`. The plan takes precedence over the older repository-family proposal in [issue #1](https://github.com/orka-agents/orka-workspace/issues/1).

| Stage | Delivered or remaining |
| --- | --- |
| Phase 0 | ADRs 0001 through 0004 and a source-backed provider writer inventory |
| Phase 1 shared repository | Two modules, preserved APIs/CRDs/helpers, workload lifecycle contract, ConfigMap-backed fake implementation, executable conformance suite, admission policy and RBAC templates, build/test/generation/dependency checks |
| Phase 1 Orka consumer | Separate task branch replaces the old shared packages and sources generated workspace CRDs from the shared module; resolver regression checks use unchanged baseline hashes |
| Phase 2 | Blocked on Orka #572. Persist immutable workload and startup evidence; open selectors; add provider-owned parameter kinds; enforce upgrade/drain gates; integrate generic startup and ordered retirement; prove a RuntimeSession through a separate Deployment |
| Phase 3 | Extract agent-sandbox after Phase 2; preserve PVC cold resume and existing allocation ownership |
| Phase 4 | Extract Substrate after Phase 2; include inventory-discovered native writers and preserve data-only checkpoints and full-memory restrictions |
| Phase 5 | Wait for Phases 2 through 4 and fiberd's upstream control authentication, persisted epochs/revocation, deletion, selected-image launch, and pressure-parking controls |
| Phase 6 | Remove old provider code and flags only after extraction and retained-resource migration tests; finish installation and uninstall documentation |

## Decisions resolved during implementation

- Start in one repository with shared and providers Go modules.
- Give each provider its own config and profile kinds. Provider schemas stay with its deployment; the shared bundle does not acquire provider-specific schema. Retire the ACP union kinds after migration.
- Keep the controller-created RuntimePool API in `v1alpha1`. Enforce an upgrade preflight or workspace-dispatch startup gate that names every old-shaped pool and in-tree-bound suspended workspace. After pruning, a pool missing the new workload is finalize-only. Retained workspaces retire under the old owner or retain that owner during coexistence; external controllers must not adopt them.
- A `Draining` provider accepts continuation only through an existing workspace for the exact session, slot, and provider binding. Cold resume qualifies. A newly planned session UID does not. `Disabled` blocks continuation and permits cleanup.

## Scope of verification

The fake is a persistent simulation. Its ConfigMaps represent backend effects and survive construction of a new driver. Conformance checks identity, request immutability, recovery, stop/deletion ordering, and terminal tombstones. CEL tests evaluate the bundle with Kubernetes' admission compiler and simulated ServiceAccount authorization.

These checks do not establish runtime admission, actual compute termination, two-replica leader election in a cluster, or Sandbox/Substrate resume behavior. Those belong to the later proof stages. The preserved shared schema does not yet carry the SDK workload or startup evidence.
