# ADR 0004: Version each agreement separately

Date: 2026-10-02

Status: Accepted. There is no released external Orka/provider combination yet.

## Decision

Use exact contract discriminators rather than interpreting a Go module version as protocol compatibility. An unknown discriminator fails closed with the expected and observed versions in a bounded error message. Providers advertise only capabilities they exercise in their release CI. Orka retains the observable verification described in [ADR 0002](0002-startup-and-retirement.md).

| Versioned agreement | Identifier or rule | Compatibility owner |
| --- | --- | --- |
| Shared Go SDK | Shared module semantic version; initial prerelease `v0.1.0-alpha.1` | Shared repository maintainers |
| Shared Kubernetes schema | `workspace.orka.ai/v1alpha1`, with the five baseline CRDs preserved byte for byte in Phase 1 | Platform shared-bundle installation |
| Provider lifecycle/workload contract | Exact `orka.workspace.lifecycle.v1` | Shared SDK and provider; Phase 1 Go contract, persisted handoff pending |
| Existing workspace control advertisement | Exact `workspace.orka.ai/v1` in preserved API contract fields | Existing core and adapters |
| Workspace-agent command/file protocol | Exact `workspace.orka.ai/v1` | `sdk/workspaceagent` client and agent |
| Runtime supervisor protocol | Exact `orka.harness.v2` | Orka and supervisor; not implemented by a generic workspace connection |
| ACP runtime profile | Exact `acp.v1` at the reviewed baseline, plus pinned adapter and runtime profile digests | Orka and selected runtime |
| Provider-native APIs | Provider-specific pinned native schema/API revision | Each provider's release and conformance evidence |
| Operation journals | Fake `fake.workspace.journal.v2`; each production provider owns its exact discriminator and migration | Owning provider |
| Data layout/checkpoint format | Provider-specific digest, source provenance, content scope, and restore compatibility | Owning provider; no cross-provider portability promise |

The preserved API and workspace-agent happen to use the same `workspace.orka.ai/v1` string. This historical overlap does not make ACP and workspace-agent interchangeable. The lifecycle contract has its own discriminator, following the exact-match pattern used by Orka's versioned protocols.

## Supported combination

There is one development combination. It is deliberately narrower than the eventual release table.

| Shared source | CRDs | Provider | Verified scope | Orka/runtime integration |
| --- | --- | --- | --- | --- |
| `v0.1.0-alpha.1`, Phase 1 | Byte-identical to Orka `1cd16c88b43410e0ea46463f8a39b473d8e5250e` | Fake from the same checkout | Shared API/helpers, lifecycle conformance, and simulated provider behavior; no real compute backend | No supported external RuntimePool/RuntimeSession combination yet; secure startup and cluster proof remain Phase 2 |

The Orka commit is the schema and behavior reference, not a claim that its unchanged controller can use an external provider. Do not add a released compatibility row until the exact shared tag, provider image digest, Kubernetes admission behavior, Orka version, runtime image/protocol, and native backend version have passed the required tests. An SDK test or advertised feature alone cannot establish that row.

## Upgrade, drain, and rollback

Provider schema and journal upgrades need explicit readers, migration steps, and rollback limits. The baseline Substrate lifecycle journal uses `orka.substrate-runtime.v1`; moving that implementation must preserve its decoder, identity rules, recovery markers, and retained catalog references until a tested migration replaces them. Do not reuse its discriminator for a different record format.

In Phase 1, source moves preserve class hash results, API versions, UID/generation inputs, raw parameter specs, and generated schemas. A preferred API-version change can alter hashes even when the apparent configuration is unchanged. Freeze regression fixtures before the corresponding type moves.

The core selector and RuntimePool workload changes stay in `v1alpha1`. An enforced pre-upgrade gate must reject cutover while any old-shaped RuntimePool or any in-tree-bound suspended workspace exists. Such allocations must retire under their existing deletion/retention policy, or coexist with their original provider owner until retirement. Documentation alone does not satisfy this gate. A legacy pool whose provider-specific fields have been pruned and which lacks a valid workload request is finalize-only; it must never trigger new materialization. Coexistence keeps the original owner responsible for retained data and does not authorize external adoption. Do not rewrite an old-shaped binding into a new owner.

Draining is provider state. It rejects a new workspace identity and permits continuation, including cold resume, only through an existing workspace with the exact session UID, workspace slot, and provider binding. A matching session UID alone is insufficient. Disabled rejects continuation and cold resume while preserving cleanup. Cleanup includes suspension authorized by Core for the exact existing instance: the provider verifies retained data before terminating that instance, without starting a new workload sequence or boot. New public checkpoint exports remain blocked. Class readiness must not replace these provider-state rules. Phase 2 must enforce the choice consistently and cover new-identity, continuation, wrong-slot, wrong-provider, cold-resume, and Disabled cases.

Use new registrations and classes for new external allocations. Existing allocations, journals, native templates, PVCs, and checkpoint references stay with their original in-tree owner until retired. Renaming a controller or copying labels does not transfer ownership. Provider uninstall waits for every live allocation and retained artifact obligation; shared CRDs remain installed. Fiberd is unsupported until Phases 2 through 4 and its control-authentication, epoch persistence, cleanup, image-launching, and pressure-parking prerequisites are complete.
