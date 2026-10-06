# Orka workspace

Shared Kubernetes workspace API, Go SDK, lifecycle conformance tests, and independently built provider controllers.

The shared `workspace.orka.ai/v1alpha1` API persists numbered, immutable workload requests and exact allocation evidence. Separate fake, Agent Sandbox, and Substrate controllers own compute and recovery journals. Orka owns authorization, credentials, authenticated runtime admission, and drain settlement.

## Build and check

Use Go 1.27 and Python 3, matching the extraction baseline.

```sh
make check
```

This builds, vets, and race-tests both Go modules, regenerates the shared API into a temporary directory, compares the generated CRDs and deepcopy code, and checks dependency boundaries. The shared module must not import Orka or provider implementations. Provider implementations must not import Orka or another provider.

| Directory | Module and purpose |
| --- | --- |
| `api/v1alpha1` | Workspace types, immutable workloads, startup evidence, and checkpoint references |
| `sdk` | Conditions, class hashes, identity, connection helpers, and the allocation lifecycle contract |
| `sdk/workspaceagent` | Existing command/file protocol and client |
| `conformance` | Required allocation, recovery, fencing, stop, and deletion behavior |
| `config` | Shared CRDs, admission policies, and RBAC templates |
| `providers/fake` | Pod materialization, persistent journal, separate binary and deployment |
| `providers/sandbox` | Agent Sandbox allocation, exact Pod/PVC evidence, data-only suspension |
| `providers/substrate` | Native ACP actor lifecycle, sealed bootstrap evidence, data-only capture |
| `docs/adr` | Ownership, startup/retirement, configuration/trust, and compatibility decisions |

The `sdk` import retains the Go package name `workspaceprovider` to keep consumer changes mechanical. `providers/go.mod` uses a local replacement for development; it is not part of the shared module's dependency graph.

## Provider contract

Implement `sdk.Lifecycle`: `EnsureAllocation`, `Observe`, `StopInstance`, and `DeleteAllocation`. Persist immutable intent before backend effects. Recover the same instance after a lost response. Stop and delete require the full allocation, instance, and request-revision fence. Keep the deletion tombstone until the owning workspace is finalized.

A workload request binds the workspace and provider UIDs, image digest, command, resources, and optional pinned parameter revision. A runtime request also freezes the supervisor template, pool/class bindings, and required protocol. Startup evidence binds the reported endpoint to the exact allocation and request. Consumers must retain their available read-only materialization checks, one-time bootstrap exchange, and authenticated runtime fence probe.

Call `conformance.Check` with a fresh admitted fixture and a factory that reconnects to the same durable store. Passing this suite proves the exercised lifecycle behavior. Optional capabilities need their own tests before advertisement.

## Integration status

Orka #572 is implemented on the integration branch. The generic RuntimePool path consumes provider evidence, independently verifies accessible Pods and storage, binds private credentials to the exact instance, and admits only an authenticated supervisor fence. See [installation and retirement](docs/external-providers.md) and [implementation status](docs/implementation-status.md) for validation results and remaining migration work.

Fiberd remains gated on upstream authentication, persisted revocation, explicit deletion, selected-image launching, and pressure-parking controls.

The accepted migration choices are separate provider-owned config/profile kinds, in-place `v1alpha1` pool changes with an enforced upgrade gate, and continuation while a provider is `Draining` only for an existing workspace bound to the same session, slot, and provider. Cold resume counts as continuation. `Disabled` permits cleanup only. See the [configuration and trust ADR](docs/adr/0003-configuration-and-trust.md) and [compatibility ADR](docs/adr/0004-compatibility.md).

See [CONTRIBUTING.md](CONTRIBUTING.md) for checks and [NOTICE](NOTICE) for extraction attribution.
