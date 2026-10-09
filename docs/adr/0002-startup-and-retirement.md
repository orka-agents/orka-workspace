# ADR 0002: Separate allocation, runtime admission, and retirement

Date: 2026-10-02

Status: Accepted contract design. Phase 1 defines Go request and evidence types. Persisting the handoff on shared objects and adapting Orka are Phase 2 work.

## Decision

Use four required operations: ensure allocation, observe allocation, stop the exact instance, and delete allocation. Observation is read-only. Repeating ensure with the same operation identity must recover the original result, including after a lost response. A timeout means unknown outcome. Stop must confirm termination; delete must account for retained data as well as removed resources. Optional command/file, attachment, capacity, checkpoint, and service behavior stays in separate interfaces.

The existing generic workspace API has no immutable workload request or startup-evidence fields. Preserve its generated schema in Phase 1. SDK types are not yet a cross-process handoff. Phase 2 adds the persisted representation together with core admission checks, ownership policy, migration, and integration tests.

## Workload request and evidence

Orka constructs one immutable request for an admitted workspace. It binds the exact workspace UID, RuntimePool UID and revision, class/provider revisions and profile digest, operation identity, and request revision. It contains the admitted image or artifact digest, command and arguments, allowed resources, storage layout, isolation and network requirements, public bootstrap material, required connection protocol, and required provider capabilities. Credential bytes and bootstrap signing authority never enter the request or provider journal.

Phase 1's `WorkloadRequest.Runtime` carries the frozen `PodTemplateSpec`, RuntimePool and class bindings, protocol and required features. Parameter references also require an immutable UID/generation/hash binding. Validation rejects mutable environment sources, credential Secret mounts/projections, and automatic ServiceAccount token mounting in the supervisor template. Backend credential references remain in provider-owned configuration. The request/evidence structs are usable by lifecycle implementations now; the corresponding shared-object fields and Orka startup integration remain Phase 2 work.

The provider returns enough evidence to distinguish a retry from a replacement. Every observation binds to the original operation, request revision, workload digest, workspace UID, pool UID, allocation identity, and instance identity. An endpoint alone proves none of these.

| Evidence | Provider responsibility | Core acceptance |
| --- | --- | --- |
| Exact allocation and instance identifiers | Persist before publishing; never reuse a retired instance identity | Match workspace/pool/request binding; reject stale or foreign results |
| Observed request revision and definition digest | Report what was materialized | Exact match to admitted immutable request |
| Endpoint or connection reference and protocol | Identify the startup listener for that instance; no embedded credentials | Protocol match and direct authenticated probe; no acceptance from URL equality alone |
| Kubernetes workload reference when available | Namespace, name, immutable Pod UID, expected revision | Read the Pod through core's reader and retain UID binding |
| Materialized workload description and storage references | Describe created image, command, resources, isolation, volume identities and provenance | Independently compare observable Pod/PVC fields to admitted request; reject unexpected mutations |
| Native process/boot identity and public startup challenge when applicable | Bind challenge to exact new process and journal it | Verify native identity evidence and complete process-bound credential exchange |
| Termination and disposition | Report exact stopped instance, operation/revision and each data category | Check current request and policy; unknown or stale evidence never releases a finalizer |

Provider-native object validation moves to the owning provider. Core retains provider-neutral checks it can observe. For Kubernetes workloads, core still reads the exact Pod, compares the admitted workload to realized fields and UID, and verifies durable volume identity where accessible. It must not silently replace these checks with a `ready: true` claim. Provider-private evidence stays out of Task status, events, logs, and metric labels.

## Startup sequence

1. Core resolves the class and provider, checks the class `use` authorization, freezes configuration digests and immutable bindings, and writes workspace admission. A replaced parameter, class, provider, or pool does not inherit the original UID's authority.
2. The provider acknowledges workspace admission and attachment enforcement without waiting for a runtime. Orka may now establish RuntimePool demand. This gate is separate from startup readiness.
3. Core writes the immutable workload request. The provider persists intent in its ConfigMap journal, allocates or recovers the same allocation, and reports the exact created instance and observed request. Provider readiness never waits for Orka to admit that runtime.
4. Core validates evidence and performs available read-only materialization checks. The baseline Sandbox path verifies claim-to-Sandbox-to-Pod ownership, claim UID labels, template revision, secure Pod defaults, annotations, realized PodSpec, volume claims, and durable PVC lineage. Phase 3 retains native-chain checks in the Sandbox provider while preserving the generic Pod/PVC checks in core.
5. Before delivering credentials, core durably binds its private auth Secret UID to the exact workload UID. A different instance cannot consume the same credential generation. Recycle a rejected disposable instance; preserve the only retained data copy when a cold-resumed instance fails attestation.
6. Core probes and seeds the one-time bootstrap listener. Lost replies trigger observation of the same instance. The baseline listener's 404 means the listener may already have handed over to the authenticated supervisor; it does not prove successful startup. A conflicting seed keeps admission closed.
7. Core performs the exact authenticated runtime fence probe. It checks `orka.harness.v2`, ACP profile, adapter digests, provider/model configuration, cancel/tools/drain support, strict workspace governance, profile digest and schema, RuntimePool UID/generation, controller epoch, and the instance identity derived from workload UID plus supervisor boot ID. It also checks credential generation and usable session capacity. Only then does core publish `RuntimePool.status.activeInstance` and open prompt admission.

These steps preserve [the baseline startup path](https://github.com/orka-agents/orka/blob/1cd16c88b43410e0ea46463f8a39b473d8e5250e/internal/controller/runtime_pool_workspace_backend.go#L633-L737), [materialization checks](https://github.com/orka-agents/orka/blob/1cd16c88b43410e0ea46463f8a39b473d8e5250e/internal/controller/runtime_pool_workspace_backend.go#L1447), [Secret/workload binding](https://github.com/orka-agents/orka/blob/1cd16c88b43410e0ea46463f8a39b473d8e5250e/internal/controller/runtime_pool_controller.go#L2074), and [authenticated fence validation](https://github.com/orka-agents/orka/blob/1cd16c88b43410e0ea46463f8a39b473d8e5250e/internal/controller/runtime_pool_controller.go#L3175).

`workspace.orka.ai/v1` workspace-agent commands/files and `orka.harness.v2` supervisor traffic are different protocols. Neither an `Exec` implementation nor a workspace-agent connection Secret proves ACP runtime acceptance.

## Retirement sequence

1. Core closes prompt admission, revokes attachment authority, drains responsive runtimes, and requests retirement. In the preserved workspace schema the terminal intent is `desiredState: Deleted`; "retire" is the operation, not a new enum value. Core keeps the RuntimePool and workspace ownership records.
2. The provider persists retirement intent, stops the exact instance, and observes termination. Unreachable controller, missing response, and stale observation remain unknown. They do not permit a replacement or release of capacity that might still execute.
3. The provider deletes or retains backend resources according to the frozen deletion policy. It reports compute, access credentials, ephemeral secrets, workspace data, persistent volumes, checkpoints, and provider resources separately. A data checkpoint must be verified before the sole running copy is destroyed. Core separately removes its credentials and settles Task/RuntimeSession outcomes.
4. Core's RuntimePool finalizer waits for current, identity-bound termination and data disposition, then completes core-owned cleanup and releases. Core workspace finalization follows, requiring current-generation `Deleted` state and the shared disposition validator. A provider cannot remove either core finalizer.
5. The provider removes completed operation journals last, after terminal disposition is durably available and accepted. Retained artifacts keep their own ownership/catalog records until their retention obligations end. Automatic garbage collection must not erase the sole recovery record while cleanup is uncertain.

This intentionally changes [baseline `finalizeRuntimePool`](https://github.com/orka-agents/orka/blob/1cd16c88b43410e0ea46463f8a39b473d8e5250e/internal/controller/runtime_pool_controller.go#L3488), which deletes provider children itself, while the ACP adapter waits for pool disappearance. Copying that cycle into separate controllers would deadlock cleanup. [Core workspace finalization](https://github.com/orka-agents/orka/blob/1cd16c88b43410e0ea46463f8a39b473d8e5250e/internal/controller/execution_workspace_controller.go#L670) remains the final policy check.

## Required proof before external RuntimePool support

Phase 2 tests replacement, replay, lost create/seed/stop responses, stale revisions, exact-instance stop, and retained-data attestation failure. A provider that never responds must leave both pool and workspace finalizers intact with a useful cleanup-blocked condition. Provider outage closes admission without claiming termination. The cluster proof must exercise a RuntimeSession through the separately deployed fake provider, unauthorized-ServiceAccount rejection, and two replicas sharing the correct leader-election lock.

The Phase 1 fake and Go conformance suite can prove lifecycle semantics. They do not prove Orka's secure startup, Kubernetes admission, backend fencing, or that a real provider supports data recovery.
