# Orka workspace

Shared Kubernetes workspace API, Go SDK, lifecycle conformance tests, and independently built provider controllers.

This repository implements the shared foundation of [orka-workspace#1](https://github.com/orka-agents/orka-workspace/issues/1). The five `workspace.orka.ai/v1alpha1` CRDs are byte-identical to Orka commit `1cd16c88b43410e0ea46463f8a39b473d8e5250e`. The first provider is a persistent fake for lifecycle and controller tests. It does not run an ACP runtime.

## Build and check

Use Go 1.27 and Python 3, matching the extraction baseline.

```sh
make check
```

This builds, vets, and race-tests both Go modules, regenerates the shared API into a temporary directory, compares the generated CRDs and deepcopy code, and checks dependency boundaries. The shared module must not import Orka or provider implementations. Provider implementations must not import Orka or another provider.

| Directory | Module and purpose |
| --- | --- |
| `api/v1alpha1` | Stored workspace types, with the original groups, kinds, scope, and schemas |
| `sdk` | Conditions, class hashes, identity, connection helpers, and the allocation lifecycle contract |
| `sdk/workspaceagent` | Existing command/file protocol and client |
| `conformance` | Required allocation, recovery, fencing, stop, and deletion behavior |
| `config` | Shared CRDs, admission policies, and RBAC templates |
| `providers/fake` | Separate module and binary, provider parameter CRDs, ConfigMap journal, and deployment example |
| `docs/adr` | Ownership, startup/retirement, configuration/trust, and compatibility decisions |

The `sdk` import retains the Go package name `workspaceprovider` to keep consumer changes mechanical. `providers/go.mod` uses a local replacement for development; it is not part of the shared module's dependency graph.

## Provider contract

Implement `sdk.Lifecycle`: `EnsureAllocation`, `Observe`, `StopInstance`, and `DeleteAllocation`. Persist immutable intent before backend effects. Recover the same instance after a lost response. Stop and delete require the full allocation, instance, and request-revision fence. Keep the deletion tombstone until the owning workspace is finalized.

A workload request binds the workspace and provider UIDs, image digest, command, resources, and optional pinned parameter revision. A runtime request also freezes the supervisor template, pool/class bindings, and required protocol. Startup evidence binds the reported endpoint to the exact allocation and request. Consumers must retain their available read-only materialization checks, one-time bootstrap exchange, and authenticated runtime fence probe.

Call `conformance.Check` with a fresh admitted fixture and a factory that reconnects to the same durable store. Passing this suite proves the exercised lifecycle behavior. Optional capabilities need their own tests before advertisement.

## Integration status

The shared schemas remain unchanged during extraction. The SDK's new workload and startup-evidence types are not yet fields on `ExecutionWorkspace`. A standalone provider cannot yet admit an Orka `RuntimeSession` through this boundary.

The next stage depends on [Orka #572](https://github.com/orka-agents/orka/issues/572). It adds the persisted handoff and generic RuntimePool integration before extracting Sandbox and Substrate. Fiberd remains gated on those providers and its upstream authentication, revocation, cleanup, image-launching, and pressure-parking requirements.

The accepted migration choices are separate provider-owned config/profile kinds, in-place `v1alpha1` pool changes with an enforced upgrade gate, and continuation while a provider is `Draining` only for an existing workspace bound to the same session, slot, and provider. Cold resume counts as continuation. `Disabled` permits cleanup only. See the [configuration and trust ADR](docs/adr/0003-configuration-and-trust.md) and [compatibility ADR](docs/adr/0004-compatibility.md).

See [CONTRIBUTING.md](CONTRIBUTING.md) for checks and [NOTICE](NOTICE) for extraction attribution.
