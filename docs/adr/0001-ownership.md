# ADR 0001: Give each shared field one writer

Date: 2026-10-02

Status: Accepted for the shared contract. Existing Orka controllers require the migration corrections listed below.

## Decision

Preserve `workspace.orka.ai/v1alpha1`, all five kinds, their names, scope, and storage version. An operator owns configuration. Orka owns admission, workload intent, attachments, and user-facing outcomes. The selected provider owns observed allocation state and physical cleanup. Kubernetes supplies object identity and server metadata.

The tables cover every declared spec and status field. A listed object includes all its nested fields unless a more specific row splits ownership. Condition ownership includes `status`, `reason`, `message`, `observedGeneration`, and `lastTransitionTime`, keyed by condition type. Writers merge only their own entries and use resource-version conflict detection; they never replace another controller's conditions.

For every kind, the API server owns `apiVersion`/`kind` validation and `metadata.uid`, `resourceVersion`, `generation`, timestamps, and managed-field bookkeeping. The creator chooses `name` and `namespace` where applicable. An operator may manage unreserved descriptive labels and annotations. Reserved metadata and owner references are assigned below. Unlisted controller conditions or reserved metadata must be assigned an owner before use; they are not an extension that any writer can claim.

In the extracted bundle, core owns `workspace.orka.ai/*` labels and annotations. Core establishes an immutable DNS-compatible `workspace.orka.ai/controller-name` routing label; provider-owned observations and cleanup finalizers use that label value plus `/` as their prefix. These prefix rules apply to provider-bound kinds in every table. They do not transfer legacy ACP annotations or Substrate finalizers, which remain with the original Orka owner and policy during migration.

## ExecutionWorkspaceProvider

Cluster-scoped. The operator registers one adapter installation.

| Field or entry | Sole writer | Rule |
| --- | --- | --- |
| `spec.controllerName` | Operator | Immutable controller identity |
| `spec.parametersRef.{group,kind,name}` | Operator | Immutable, resolves to a cluster-scoped kind |
| `spec.requiredContracts` | Operator | Immutable exact contract identifiers |
| `spec.lifecycleState` | Operator | Mutable Active, Draining, or Disabled intent |
| `spec.usagePolicy.allowedNamespaceSelector` | Operator | Mutable namespace policy |
| `status.observedGeneration` | Selected provider | Generation observed by the adapter, not core |
| `status.adapter.{version,digest}` | Selected provider | Adapter build identity |
| `status.backend.{version,apiVersions}` | Selected provider | Non-secret native compatibility metadata |
| `status.supportedContracts`, `status.supportedFeatures` | Selected provider | Claims checked by provider CI and core compatibility checks |
| `status.lastHeartbeat` | Selected provider | Observation time |
| `status.pinnedParametersUID` | Selected provider | Pin first accepted parameter object; same-name recreation cannot reset it |
| `status.conditions[Ready]` | Orka core | Overall usability |
| `status.conditions[Compatible]` | Orka core | Required versus advertised contract comparison |
| `status.conditions[HeartbeatFresh]` | Orka core | Freshness assessment |
| `metadata.annotations[acp.workspace.orka.ai/provider-config-uid]` | In-tree ACP provider adapter | Legacy pin, migrated to `status.pinnedParametersUID`; no new external use |
| `metadata.finalizers[workspace.orka.ai/provider-protection]` | Orka core | Remains until dependent resources permit provider deletion |
| Controller owner references and controller routing labels | None | No parent controller or routing label is required; `spec.controllerName` is authoritative |

## ExecutionWorkspaceClass

Namespaced. A user selects a class; Orka checks the `use` virtual verb before admission.

| Field or entry | Sole writer | Rule |
| --- | --- | --- |
| `spec.providerRef.name` | Operator | Immutable direct provisioning choice |
| `spec.parametersRef.{group,kind,name}` | Operator | Immutable namespaced profile reference |
| `spec.poolRef.name` | Operator | Immutable pool alternative to direct provisioning |
| `spec.mode` | Operator | Immutable Interactive or Service mode |
| `spec.requiredFeatures` | Operator | Immutable requirements |
| `spec.allowedReuseScopes` | Operator | Immutable reuse policy |
| `spec.lifecycle.defaultOnDetach`, `allowedOnDetach`, `detachTimeout`, `idleTimeout`, `maxLifetime` | Operator | Immutable lifecycle policy |
| `spec.lifecycle.deletionPolicy.{providerResources,persistentVolumes,checkpoints}` | Operator | Immutable deletion requirements |
| `status.observedGeneration`, `status.providerRef.name`, `status.profileHash` | Orka core | Resolved class identity and functional parameter digest |
| `status.conditions[Ready]` | Orka core | Resolution and authorization prerequisites |
| `metadata.labels[workspace.orka.ai/legacy-generated]` | Orka migration controller | Marks generated legacy configuration only |
| `metadata.finalizers[workspace.orka.ai/class-protection]` | Orka core | Protects immutable bindings referenced by workspaces |
| Controller annotations and owner references | None | No provider may rewrite a class |

## ExecutionWorkspacePool

Namespaced shared capacity. This is distinct from Orka's core `RuntimePool`.

| Field or entry | Sole writer | Rule |
| --- | --- | --- |
| `spec.providerRef.name` | Operator | Immutable provider registration |
| `spec.parametersRef.{group,kind,name}` | Operator | Immutable namespaced pool configuration |
| `spec.capacity.{minReady,maxSize}` | Operator | Mutable capacity request |
| `status.observedGeneration` | Selected provider | Observed capacity request |
| `status.available`, `allocated`, `suspended`, `total` | Selected provider | Disjoint capacity counts; total is their sum |
| `status.conditions[Ready]` | Selected provider | Capacity reconciled |
| `status.conditions[Admitted]` | Selected provider | Provider reports spare capacity; does not authorize a Task |
| Controller routing label, if materialized | Orka core | Resolves the immutable provider binding |
| Controller annotations, finalizers, and owner references | None in the preserved schema contract | A future provider cleanup finalizer needs an explicit owner before installation |

Core validates pool references and serializes workspace reservations with its own Lease. That Lease does not make core a second writer of pool counts or `Admitted`.

## ExecutionWorkspace

Namespaced and controller-created. The immutable provider binding selects one owner for its lifetime, including during deletion or provider outage.

| Field or entry | Sole writer | Rule |
| --- | --- | --- |
| `spec.mode` | Orka core | Immutable |
| `spec.classBinding.{name,uid,generation,profileHash}` | Orka core | Immutable class revision |
| `spec.providerBinding.{name,uid,generation,profileHash}` | Orka core | Immutable provider revision |
| `spec.coreAdmission.classBinding`, `providerBinding`, `poolBinding`, `admittedGeneration` | Orka core | Privileged admission record; exact UID and generation checks |
| `spec.sessionRef.{name,uid}`, `spec.slot` | Orka core | Immutable continuity identity |
| `spec.lifecycle`, including every deletion-policy field | Orka core | Immutable resolved class policy |
| `spec.desiredState` | Orka core | Ready, Suspended, Deleted, or Quarantined intent |
| `spec.attachmentEpoch` | Orka core | Monotonic attachment counter |
| `spec.attachment.taskRef`, `epoch`, `tokenSHA256`, `tokenSecretRef`, `expiresAt` | Orka core | Exclusive attachment intent; token bytes stay in a Secret |
| `spec.service.ports[].{name,port,protocol}` | Orka core | Requested service endpoints |
| `status.observedGeneration`, `status.state`, `status.externalID` | Selected provider | Exact observed request and allocation, sanitized |
| `status.attachedEpoch` | Selected provider | Acknowledges enforcement, not merely receipt |
| `status.providerBinding.{contractVersion,adapterVersion,adapterDigest,backendAPIVersion}` | Selected provider | Observed implementation binding |
| `status.connectionSecretRef.name` | Selected provider | Provider connection Secret in the workspace namespace |
| `status.endpoints[].{name,url,protocol}` | Selected provider | Sanitized requested service endpoints |
| `status.disposition.{compute,accessCredentials,ephemeralSecrets,workspaceData,persistentVolumes,checkpoints,providerResources}` | Selected provider | Physical cleanup and data result; core independently verifies its own credentials before finalizing |
| `status.conditions[Admitted]` | Orka core | Core authorization and binding checks |
| `status.conditions[Quarantined]` | Orka core | Core decision to prohibit reuse |
| `status.conditions[Provisioned]` | Selected provider | Workspace allocation lifecycle milestone |
| `status.conditions[DataPlaneReady]` | Selected provider | Readiness for the advertised data protocol, never runtime prompt admission |
| `status.conditions[Attached]` | Selected provider | Attachment enforced or revoked without depending on a live runtime |
| `status.conditions[Finalized]` | Selected provider | Provider cleanup complete; does not remove core's finalizer |
| `metadata.ownerReferences` | Orka core | Exact Task, Session, or Tool owner |
| `metadata.labels[workspace.orka.ai/controller-name]` | Orka core | Immutable provider routing after admission; a label is not authorization |
| `metadata.labels[workspace.orka.ai/quarantined]` | Orka core | Prevents reuse |
| `metadata.labels[workspace.orka.ai/legacy-generated]` | Orka migration controller | Legacy materialization marker |
| `metadata.annotations[acp.workspace.orka.ai/*]` | In-tree Orka controllers | Existing materialization, retention, revocation, and pool-link records; no external adapter acquires these |
| `metadata.finalizers[workspace.orka.ai/finalizer]` | Orka core | Removed only after current-generation terminal status and policy-compliant disposition |

The ACP annotation family includes `runtime-pool`, `detach-action`, `last-settled-task-uid`, `provider-config-uid`, `backend`, `durable-workspace`, `substrate-suspend-mode`, `resumed-lineage`, `runtime-namespace`, `durable-data-absent`, `durable-session-committed`, `revocation-started-at`, `resume-requested-at`, `last-detached-at`, `max-suspended`, and `legacy-retention-deadline`. In-tree core and its in-process adapter currently share the same trust identity. Phase 2 replaces provider observations in these annotations with the explicitly provider-owned handoff in [ADR 0002](0002-startup-and-retirement.md). It must not grant an external adapter blanket write access to the ACP family.

## ExecutionWorkspaceCheckpoint

Namespaced. This exports an already verified data checkpoint; it does not imply process-memory recovery or cross-provider portability.

| Field or entry | Sole writer | Rule |
| --- | --- | --- |
| `spec.workspaceRef.{name,uid}` | Requesting operator | Immutable exact source workspace |
| `spec.recoverLastCheckpoint` | Requesting operator | Immutable explicit recovery consent |
| `status.phase`, `status.digest`, `status.createdAt` | Source workspace's selected provider | Verified data artifact result; no native URI in public status |
| `status.classBinding.{name,uid,generation,profileHash}` | Source workspace's selected provider | Exact captured source class |
| `status.conditions[Ready]` | Source workspace's selected provider | Export result |
| `metadata.finalizers[orka.ai/substrate-checkpoint-reference]` | In-tree Substrate checkpoint controller | Protects existing catalog reference; old owner retains it until retired |
| Future provider checkpoint cleanup finalizer | Source workspace's selected provider | Must be assigned a provider-specific key and preserve other finalizers |
| `metadata.labels[workspace.orka.ai/provider-name]`, `metadata.labels[workspace.orka.ai/controller-name]` | Orka core | Resolve the source workspace and establish immutable routing before provider status; absent at baseline |
| Other controller annotations and owner references | None at baseline | No provider may self-assign source ownership |

## Existing conflicts and enforcement scope

The baseline [ADR 0021](https://github.com/orka-agents/orka/blob/1cd16c88b43410e0ea46463f8a39b473d8e5250e/docs/adr/0021-execution-workspace-domain-and-ownership.md) supplies the ownership model. The live [admission policy](https://github.com/orka-agents/orka/blob/1cd16c88b43410e0ea46463f8a39b473d8e5250e/config/policy/workspace_core_admission_policy.yaml) only protects advancement of core admission, attachment intent, and two hardcoded ACP routing markers plus ACP annotations. Its `admit` authorizer check is reusable; it is not complete field ownership enforcement.

Two baseline overlaps need correction when those implementations move. The fake provider writes and clears Provider `Compatible`, which core also writes. The ACP adapter writes Workspace `Quarantined`, which core also owns. Follow the sole writers above. A provider can report `state=Quarantined` and failed provider conditions; core projects the quarantine decision.

The Phase 1 bundle enforces this partition for all five kinds without changing CRD bytes, using `admit`, `configure`, and registration-scoped `provider-status`. Checkpoint writes require core to establish its provider-registration routing label. Wiring that routing, the workload handoff, and all production status paths into Orka remains a Phase 2 gate. `RuntimePool.status`, its accepted `ActiveInstance`, Task/Tool outcomes, and repository publication remain exclusively in Orka.
