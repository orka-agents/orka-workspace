# Provider extraction inventory

Baseline: Orka [`1cd16c88b43410e0ea46463f8a39b473d8e5250e`](https://github.com/orka-agents/orka/tree/1cd16c88b43410e0ea46463f8a39b473d8e5250e), inspected 2026-10-02. Paths below are relative to that repository. Target paths are planned unless the row explicitly says Phase 1. No existing production allocation changes owner in Phase 1.

The plan's seed count is 21 non-test Go files that reference the closed workspace-provider or ACP-backend constants. It is not 21 literal `switch` statements. The count includes type definitions, validation, configuration, and projection paths. A native-writer inventory must also include files with no enum reference, especially checkpoint, template, journal, and MCP Tool cleanup.

Reproduce the seed set with:

```sh
rg -l 'WorkspaceProvider(AgentSandbox|Substrate)|RuntimeProviderBackend(AgentSandbox|Substrate)' \
  --glob '*.go' --glob '!**/*_test.go' --glob '!**/zz_generated*' | sort
```

In the tables, "core" means the future provider-neutral Orka path. A split keeps authorization and user-facing status in Orka while moving the native write into the provider module. Dependencies name the relevant coupling, not every standard-library import.

## Enum-reference inventory

| Current file | Current responsibility or native effect | Target | Dependencies and persisted state | Phase |
| --- | --- | --- | --- | --- |
| `api/v1alpha1/execution_types.go` | Defines closed provider selector and legacy workspace binding/status shapes | Core API, opened selector after #572 | Core stored bindings and hashes; CRD enums | 2a |
| `api/acp.workspace/v1alpha1/runtimeproviderconfig_types.go` | Closed Sandbox/Substrate config union | Separate provider configuration/profile kinds | Stored `RuntimeProviderConfig`/profile union and CEL | 2a, 3, 4 |
| `cmd/main.go` | Registers native controllers, creates native executors, provider flags | Core loses native registration; separate provider binaries | Controller-runtime, agent-sandbox and Substrate configuration; installation flags | 2, 3, 4, 6 |
| `internal/controller/agent_execution_plan.go` | Selects backend templates, validates execution configuration | Class-only core resolution after #572 | Task template/provider/pool/boot/snapshot legacy fields | 2a |
| `internal/controller/execution_workspace_config.go` | Provider-specific environment/config request translation | Split provider configuration from core workload request | Core API, sandbox/Substrate config; worker configuration | 2a, 3, 4 |
| `internal/controller/execution_workspace_class_controller.go` | Resolves typed parameters and hashes provider-specific profile content | Core generic REST-mapper resolution; provider validates profile | Core/shared/fake/ACP API kinds; class status and immutable profile hash | 1 hash fixtures, 2a |
| `internal/controller/acp_runtime_profile.go` | Freezes backend selection into runtime profile | Core generic workload/profile binding | Runtime profile digest, provider configuration, core API | 2a |
| `internal/controller/acp_workspace_class.go` | Requires in-tree controller name, backend-specific profile/template resolution | Core contract/capability/authorization resolution; native validation moves | ACP config/profile and native template identities; class bindings | 2a, 3, 4 |
| `internal/controller/acp_workspace_provider_adapter.go` | Advertises in-tree provider, gates by backend flags, pins parameters | Separate provider advertisement | Shared provider status, ACP parameter UID annotation/status pin | 2, 3, 4 |
| `internal/controller/acp_workspace_binding.go` | Constructs RuntimePool backend sub-specs and validates frozen binding | Core generic immutable workload request | Class, provider, template, runtime profile identities; RuntimePool spec | 2a, 2b |
| `internal/controller/acp_workspace_lifecycle.go` | Creates workspace/pool links, attachment and retention intent | Core, with provider-specific configuration removed | Workspace/Task annotations, attachment Secrets, RuntimePool links | 2a, 2b, 2c |
| `internal/controller/acp_workspace_adapter.go` | Writes provider-observed workspace status; requests pool deletion, waits for absence | Generic provider observation consumer in core; lifecycle observations move to provider | Workspace status/disposition, runtime pool link, durable-data annotations | 2b, 2c |
| `internal/controller/acp_task_queue.go` | Determines provider-backed demand/admission | Core generic class/workload demand | Task and RuntimePool readiness/bindings | 2a |
| `internal/controller/runtime_pool_controller.go` | Owns runtime acceptance and core resources; branches native creation and deletion | Split: core keeps authenticated probe, credentials, admission, RuntimePool status; providers own native children | Deployments/Pods/Services/NetworkPolicies/Secrets, bootstrap UID binding, ActiveInstance | 2b, 2c, 3, 4 |
| `internal/controller/runtime_pool_workspace_backend.go` | Creates/deletes SandboxTemplate and SandboxClaim, attests materialization, seeds credentials | `providers/sandbox`; generic read-only attestation and seeding stay in core | agent-sandbox API/controllers, Kubernetes; Sandbox/PVC identity, pool annotations | 3, after 2b |
| `internal/controller/runtime_pool_workspace_suspend.go` | Provider-specific cold suspend/resume, volume checks, cleanup | `providers/sandbox` and `providers/substrate`; generic retirement in core | Sandbox claims/PVCs, Substrate actor state; lineage and retention annotations | 3, 4 |
| `internal/controller/runtime_pool_substrate_backend.go` | Native actor/template operations, legacy injectable backend, policies and cleanup | `providers/substrate`, with core acceptance extracted | `internal/workspace`, harness v2, core API, Kubernetes; actor/template/worker identity and old annotation records | 4 |
| `internal/controller/task_controller.go` | Legacy workspace validation, executor dispatch, cleanup/status projection | Core class-based orchestration; remove legacy selector path with #572 | Core Task/session status, old workspace executor and cleanup records | 2a, 6 |
| `internal/controller/tool_controller.go` | Direct `SubstrateExecutor.Claim`, readiness, actor release/deletion and lease handling for MCP Tools | Native lifecycle in `providers/substrate`; Tool intent/outcome projection remains core | Core Tool/SubstrateActorPool, executor, actor leases, finalizers and actor status | 4 |
| `internal/controller/substrate_mcp_identity.go` | Migrates provider-qualified MCP actor identity and cleanup protection | Core migration only while legacy Tool resources remain; native cleanup in provider | Tool actor binding/finalizer; legacy actor namespace/template identity | 4, 6 |
| `internal/workspace/statusrules/status.go` | Validates/sanitizes legacy core workspace status and enum values | Core projection/legacy migration code, retired with its callers | Direct core `api/v1alpha1` import; Task workspace status | 2a, 6 |

`api/v1alpha1/runtime_pool_types.go`, `task_types.go`, and `tool_types.go` also store these types without referencing both enum constants. The RuntimePool provider selector and `substrate`/`agentSandbox` union, generated CRDs, cross-field CEL, admission manifests, installation examples, and hash inputs must be audited with the seed set. Replacing a Go string type alone does not open the extension boundary.

## Native writers outside the enum set

| Current file | Native write or supporting ownership | Target | Dependencies and persisted state | Phase |
| --- | --- | --- | --- | --- |
| `internal/controller/runtime_pool_substrate_native.go` | Create/resume actor, materialize immutable template revision, bootstrap native runtime | `providers/substrate`, retaining core credential authority | Native Control protobuf API, harness v2, core RuntimePool; lifecycle ConfigMap and template history | 4 |
| `internal/controller/substrate_native_state.go` | Create/update CAS journal, drain native worker, delete exact worker Pod | `providers/substrate` | Native protobufs and Kubernetes client; `orka.substrate-runtime.v1`, UID worker fence, recovery markers, 256 KiB bound | 4 |
| `internal/controller/substrate_native_checkpoint.go` | Suspend actor, create/delete Tag, delete actor, prove worker absence, delete journals/templates/policies | `providers/substrate` | Native Control API; checkpoint intent, immutable source/tag identities, termination and data disposition | 4 |
| `internal/controller/substrate_checkpoint_controller.go` | Export public checkpoint, acquire/release artifact owner, collect orphan catalog/template records | `providers/substrate` | Shared checkpoint API, core RuntimePool, native catalog; checkpoint status and `orka.ai/substrate-checkpoint-reference` finalizer | 4 |
| `internal/controller/substrate_checkpoint_catalog.go` | Write artifact ConfigMaps and ownership sets; delete unreferenced native Tags and records | `providers/substrate` | Native Control API, shared checkpoints, core pool identity; artifact digest/provenance/catalog owners | 4 |
| `internal/controller/substrate_checkpoint_restore.go` | Import verified checkpoint into pool journal and acquire retained ownership | `providers/substrate` | Native template/data-layout validation; catalog and lifecycle journal reference | 4 |
| `internal/controller/substrate_template_store.go` | Write template binding/history ConfigMaps; create/delete native ActorTemplates | `providers/substrate` | Native protobuf Control API, Kubernetes; immutable native template revisions and owner bindings | 4 |
| `internal/controller/substrate_template_gc.go` | Delete unused native template revisions and update history | `providers/substrate` | Native Control API, lifecycle/catalog references; template-history ConfigMap | 4 |
| `internal/controller/substrate_native_actor_control.go` | Adapt Create/Resume/Settle to native template identities | `providers/substrate` | `internal/workspace` actor control and native template store; actor/template identity | 4 |
| `internal/controller/substrate_actor_pool_controller.go` | Converge/prune native actors, including teardown, respecting active leases | `providers/substrate`; legacy core kind migration remains explicit | Core `SubstrateActorPool`, actor pool executor; deterministic actor prefix, finalizer, counts/status | 4 |
| `internal/controller/substrate_actor_pool_leases.go` | Construct/validate actor ownership Leases and remove stale ownership records | Split core Tool authority from provider capacity reservation | Kubernetes Lease, Task/Tool UID checks; deterministic actor lease names and holders | 4 |
| `internal/controller/substrate_native_retirement_cleanup.go` | Verify native retirement and record Task cleanup outcome | Split provider termination evidence from core Task cleanup projection | Core Task/RuntimePool and lifecycle journal; failed-retirement markers | 2c, 4 |
| `internal/controller/substrate_native_settlement.go` | Wait for exact Task/session settlement before physical deletion | Core settlement signal consumed by provider | Core Task/RuntimeSession state and accepted ActiveInstance; no independent native ownership | 2c, 4 |
| `internal/controller/substrate_sealed_bootstrap.go` | Read native identity; send encrypted credentials to a process-bound listener | Core credential exchange with provider-reported evidence | Harness v2 sealed bootstrap, native identity through control adapter; public challenge in journal | 2b, 4 |
| `internal/controller/substrate_native_template.go` | Render native templates and calculate immutable revision digests | `providers/substrate` | Kubernetes container shapes, native protobufs; template digest | 4 |
| `internal/controller/substrate_template_validation.go` | Read/validate provider-native template routes before allocation | `providers/substrate` | Native client and Substrate config; no direct persisted state | 4 |
| `internal/controller/substrate_recovery_discovery.go` | Discover retained native journals/MCP recovery ownership | Provider diagnostic helper; core migration caller until retirement | Kubernetes ConfigMap/Lease reads; existing recovery records | 4, 6 |
| `internal/controller/agent_sandbox_config.go`, `substrate_config.go` | Parse native flags/env, construct client config | Corresponding provider binaries | Native URLs, trust/credential references and timeouts; no new shared settings | 3, 4, 6 |
| `internal/controller/task_workspace_credentials.go`, `harness_v1_workspace.go` | Legacy workspace handoff credentials and executor caller | Core while legacy harness exists; coordinate #568 removal | Core Tasks/Secrets, old executor; credential handoff state | Coordinate #568, no Phase 1 dependency |
| `internal/controller/fake_workspace_provider_controller.go` | Simulate provider advertisement, counts, attachment and disposition | `providers/fake` | Shared API/helpers and Kubernetes client; no native compute | 1 adaptation |

Read-only helper rows stay in the inventory because moving their native writers without them would leave provider dependencies in Orka. Native cleanup and garbage collection have the same owner as creation. No separate core garbage collector may delete a provider's live template, actor, Tag, or journal.

## `internal/workspace` disposition

This directory is not a permanently in-tree package. It mixes a legacy facade with reusable protocol code and native implementations. Moving it unchanged would import Orka internals and preserve the wrong lifecycle contract.

| File | Dependency and current effect | Decision and target | Persisted state / phase |
| --- | --- | --- | --- |
| `types.go` | Standard-library-only `WorkspaceExecutor` combines Claim/WaitReady/Exec/files/Release/Delete/Describe; includes native SandboxName, Boot, snapshot URI and placement fields | Keep legacy facade only while callers remain. Replace lifecycle with `sdk.Lifecycle`; extract optional data operations only for actual users, not an alias of this entire interface | No persistence itself; 2 through 4 and #568 cleanup |
| `errors.go` | Standard-library error kinds, retryability and context conversion | Reuse useful error semantics in native adapters; shared lifecycle has its own explicit identity/conflict errors | No persistence; 3/4 |
| `helpers.go` | Standard-library timeouts, keys, path cleanup, digests and output limits | Per-helper disposition below; no blanket package move | No persistence; 3/4 |
| `agent_sandbox.go` | `sigs.k8s.io/agent-sandbox` API and Go client; claim/reattach, commands/files, release/delete | `providers/sandbox`, preserving exact identity, timeout, and error behavior | Native claim/Sandbox/files; local handle/reuse cache is not a durable journal; 3 |
| `substrate.go` | Core package's `internal/workspace/daemonprotocol`; creates/resumes/suspends/deletes actors, command/file operations and handoff identity | `providers/substrate`, with lifecycle and optional data-plane responsibilities separated | Native actor state; in-memory handoff cache; 4 |
| `substrate_actor_control.go` | Substrate client; ensure/converge/prune actors for pools | `providers/substrate` capacity implementation | Deterministic actor identities, native actor set; 4 |
| `substrate_runtime_actor_control.go` | Substrate lifecycle facade, snapshot/template/worker/credential fence types | `providers/substrate`; retain native evidence validation, adapt results to shared lifecycle | Native actor/template/data snapshot identities; 4 |
| `substrate_client.go` | Direct `internal/substratepb`, gRPC and TLS; create/resume/suspend/delete, pagination, session identity | `providers/substrate`; native protobuf package must move or be supplied by the pinned upstream API, never imported from Orka internals | Native Control API, token file references; 4 |
| `substrate_resources.go` | Direct `internal/substratepb`; native template/tag client | `providers/substrate` | Read-only native object listing; 4 |
| `substrate_diagnostics.go` | Direct `internal/substratepb`; backend diagnostic checks | `providers/substrate` diagnostic command/helpers | No mutation or shared status ownership; 4 |
| `substrate_bootstrap.go` | Direct `internal/harness/v2`; native handoff credential seed/recovery and process-lifetime checks | Split generic public evidence into shared types, keep private credential authority/protocol implementation in core; provider performs native identity lookup | Public challenge/lifetime proof and credential channel; 2b/4 |
| `daemonprotocol/protocol.go` | Aliases `pkg/workspaceagent` DTOs | Repoint consumers to `sdk/workspaceagent`; retire redundant aliases when callers migrate | Exact workspace-agent wire format; 1 package move, 4 caller migration |
| `daemonprotocol/client.go` | HTTP workspace-agent requests with Substrate actor route and identity headers | Native route/auth assembly in `providers/substrate`; use shared workspace-agent client where compatible | No lifecycle persistence; 4 |
| `statusrules/status.go` | Direct core `api/v1alpha1`; legacy Task status validation/projection | Core projection code until legacy status disappears. It must not enter the shared module | Legacy core workspace status; 2a/6 |

Per helper in `helpers.go`:

| Helper | Disposition |
| --- | --- |
| `contextWithTimeout`, `sleepContext` | Reuse privately inside adapters that need cancellable polling; do not add a public utility package |
| `workspaceKey`, `reuseIndexKey` | Legacy local-cache keys; retire with caches rather than turn names into shared identity fences |
| `copyStringMap` | Use standard-library map cloning at actual call sites |
| `cleanArtifactPath` | Keep with optional command/file adapter; validate against that backend's filesystem rules |
| `digest` | Reuse existing shared hashing or a local checksum where semantics match; do not change profile/request hashing inputs |
| `truncateBytes` | Keep with command-output handling and its existing truncation behavior |
| `releaseMessage` | Keep local to the legacy facade until its removal |

The extraction must distinguish native process bootstrap from ACP authority. Importing `internal/harness/v2` into a provider would violate the new module boundary; copying private signing/credential logic into the provider would violate ownership. Define public evidence at the boundary, preserve Orka's verification, and keep native protobuf/client dependencies in the providers module.

## Handoff gates and remaining decisions

Phase 1 extracts the shared API, provider helpers, and workspace-agent protocol and adapts the fake provider against a new lifecycle contract. The existing `pkg/workspaceprovider.Driver` has no production implementer and its conformance runner has no production caller. It is not evidence that Sandbox or Substrate already use this boundary.

Before Phase 2, #572 must retire Task's legacy provider/template/pool/boot/snapshot selectors and default-provider flag. Coordinate #568's harness v1 retirement without blocking Phase 1. Freeze all in-tree parameter-kind hash outputs before their type moves. Use separate config/profile kinds per provider and retain `v1alpha1` with an enforced pre-upgrade gate. Any old-shaped pool or in-tree-bound suspended workspace blocks external cutover and must retire under its existing policy or coexist with the old provider until retirement. A pruned legacy pool without a workload request is finalize-only and must never materialize. Draining permits continuation, including cold resume, only for the exact existing session, slot, and provider workspace binding; Disabled blocks continuation but preserves cleanup.

The MCP Tool path is in extraction scope. Core retains Tool authorization, MCP configuration and outcomes. Provider actor creation, readiness, stop/delete, native leases, and retained cleanup move to the Substrate provider. A legacy in-tree cleanup reader can remain until its exact old resources retire; it is not a permanent exception permitting new native writes.

Phase 2 must prove a separately deployed fake admits a RuntimeSession without provider-specific core branches or manifests. Phase 3 then preserves Sandbox PVC cold resume. Phase 4 preserves native Substrate data-only capture, uncertain-operation recovery, and the closed full-memory gate. Fiberd follows only after those phases and its named upstream blockers. None of those production milestones is satisfied by this inventory or by Phase 1 unit tests.
