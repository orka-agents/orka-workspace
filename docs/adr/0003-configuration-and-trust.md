# ADR 0003: Use typed parameters, RBAC approval, and bounded journals

Date: 2026-10-02

Status: Accepted. Production integration and API-server enforcement proof remain Phase 2 validation gates.

## Configuration and module boundary

Start with one repository and two Go modules. The shared module contains `api`, `sdk`, and `conformance`. The `providers` module contains independently built provider binaries under `providers/<name>`. Orka may depend on the shared module; it must never depend on the providers module or import a provider's native SDK through the shared module. Further repository or module splits need a concrete dependency or release reason.

Keep provider-owned typed parameters resolved through `TypedObjectReference` and the REST mapper. A namespaced class or pool resolves parameters in its own namespace. A cluster-scoped provider may reference only cluster-scoped kinds. The nonempty group requirement means an ordinary ConfigMap is not a substitute for these configuration references.

Resolve and validate configuration before allocation, bind it to the selected provider, and freeze its identity and functional digest. Preserve the baseline class hash input, including parameter API version, UID, generation, and raw spec. A same-name object recreated with a different UID must fail closed. An adapter must not reinterpret an already allocated request using newly changed parameters.

Use a separate typed configuration kind and profile kind for each provider. This decision is authorized. Keep the existing `acp.workspace.orka.ai` config/profile union installed read-only while legacy allocations retire; new allocations use the selected provider's kinds. A generic unchecked configuration envelope is rejected. No new core workspace kind is needed.

The platform installation owns the shared CRDs and admission bundle. Provider releases own their configuration CRDs, deployment, ServiceAccount, native permissions, and controller image. Provider uninstall must not delete the shared CRDs.

## Trust and authorization

Installed providers are trusted infrastructure. The operator approves a provider by granting its ServiceAccount a virtual verb through RBAC. A controller-name predicate only filters events; it does not authenticate the controller. Provider readiness and a CI conformance result are claims, not proof that a runtime has been admitted.

Keep the existing `admit` virtual verb for Orka's core-owned admission and attachment writes. Add `provider-status` for provider observations, checked on the cluster-scoped `executionworkspaceproviders` resource with its exact registration name. Operators receive `configure` on the relevant shared configuration kinds. This third verb is needed because ordinary metadata patch permission would otherwise also permit provider lifecycle or pool capacity edits. Do not grant a provider Orka's `admit` authority. The provider also needs ordinary Kubernetes `get/list/watch` and the explicitly allowed patch/update permissions; the virtual verb is an additional admission gate.

The provider policy must compare both old and new ownership selectors. Otherwise a caller could change the label to an identity it controls, patch foreign status, then restore the label. Core establishes routing from an immutable provider binding. Providers cannot change core spec, owner references, core conditions, or core finalizers. Core cannot bypass provider-owned field checks merely by holding `admit`.

The CEL mechanism follows the existing policy's authorizer call:

```cel
authorizer.group('workspace.orka.ai')
  .resource('executionworkspaces')
  .namespace(request.namespace)
  .name(object.metadata.name)
  .check('admit').allowed()
```

The [shipped policy](../../config/policy/ownership.yaml) uses this exact provider check, where `providerName` comes from the Provider object's name, a Workspace's `spec.providerBinding.name`, a Pool's `spec.providerRef.name`, or a Checkpoint's core-established `workspace.orka.ai/provider-name` label:

```cel
authorizer.group('workspace.orka.ai').resource('executionworkspaceproviders')
  .namespace('').name(variables.providerName).check('provider-status').allowed()
```

Both old and new provider names must match. The policy protects every shared kind and partitions conditions according to [ADR 0001](0001-ownership.md). Checkpoint status remains blocked until core resolves the exact source workspace and pins its provider registration label. A provider cannot self-assign that label. Routing labels cannot change once established. The [RBAC templates](../../config/rbac/README.md) grant distinct core, operator and provider authority.

Generalize marker protection beyond the two hardcoded ACP controller labels. The new shared policy reserves `workspace.orka.ai/*` for core and derives each provider's annotation and cleanup-finalizer prefix from its immutable routing label. It contains no production provider names. Existing ACP annotations and Substrate checkpoint finalizers remain with legacy controllers under Orka's baseline policy until the transition; the new bundle is not a replacement policy for those unadapted controllers.

Provider deployments have separate ServiceAccounts, immutable image digests, and leader-election locks. Replicas of one installation share one lock; different installations use different locks. Provider leadership is separate from Orka's runtime-control epoch. Losing leadership cannot cancel a backend request already in flight.

Kubernetes administrators and provider infrastructure operators can alter the environment hosting a workload. The contract does not protect against a malicious infrastructure owner or an administrator who changes RBAC/admission. It does preserve the distinction between hosting the workload and holding Orka's private bootstrap signing authority or repository publication credentials.

## Operation journals

Use provider-owned ConfigMaps for non-secret operation intent and progress. Use Secrets for credentials. Do not add a journal CRD or database.

The baseline [Substrate journal](https://github.com/orka-agents/orka/blob/1cd16c88b43410e0ea46463f8a39b473d8e5250e/internal/controller/substrate_native_state.go#L210) bounds serialized state to 256 KiB and uses ConfigMap `resourceVersion` on update. Adopt that ceiling for an individual serialized operation record, with a smaller provider limit permitted. Reject oversized records before a backend side effect; do not silently truncate identity, provenance, or cleanup obligations.

Every record includes an exact schema discriminator, owner workspace UID, provider UID, immutable request/revision, stable operation identity, exact allocation/instance identity when known, current operation phase, and terminal disposition or recovery obligation. Backend-native identifiers and artifact references may remain in these private records. Credential values may not.

1. Read current journal state through an uncached client before deciding on an external action.
2. Persist intent before calling the backend. Use Kubernetes create for the first record and resource-version compare-and-swap for changes. On conflict, reread and reassess instead of replaying a stale action.
3. After a lost reply or controller restart, recover the same operation through observation. Do not interpret a missing response as a failed create. Do not choose a new allocation until duplicate execution has been ruled out.
4. Preserve a completed deletion tombstone while the owning workspace remains. It prevents a late ensure retry from recreating the allocation. Delete the journal only after confirmed backend cleanup and durable terminal disposition accepted by core.
5. Keep retained artifact/catalog records until all owners release them. Never delete the sole recovery record through an owner reference while a backend operation remains uncertain.

Provider-specific ConfigMap finalizers protect current and historical journals
before backend effects, including during foreground and namespace deletion.
The provider releases that protection only after verified terminal cleanup and
Core finalization or confirmed owner absence; malformed or nonterminal orphan
records require recovery.
Legacy live journals can acquire protection before effects. An unprotected
journal already being deleted cannot safely acquire a new finalizer.

ConfigMap CAS serializes journal updates. It is not a backend fence or proof that a native mutation executes exactly once. At the baseline, native Substrate mutation APIs do not accept the caller's UID/version preconditions. That implementation relies on exclusive infrastructure ownership, immutable identity checks, non-reused native names, durable uncertain-operation recovery, and fresh runtime acceptance. It must continue to say so. Conformance must not report stronger backend guarantees than tested.

## Acceptance gates

Phase 1 requires executable CEL ownership rules and focused negative tests using an identity without the virtual verb. Such tests are distinct from a running Kubernetes API server enforcing them. Phase 2's cluster proof must show that an unauthorized ServiceAccount cannot change provider status, swap routing, forge core admission, alter attachment intent, remove core finalizers, or overwrite core conditions. An authorized provider must be able to update only its assigned fields, and a different provider identity must be rejected. The fake provider must retain a tombstone after deletion and reject stale instance operations.

The admission rules do not implement the still-pending RuntimePool API migration or config-kind split. Those changes and #572's class-only execution migration remain prerequisites for production external providers. Provider Draining permits continuation and cold resume only for an existing workspace with the exact session, slot, and provider binding. Disabled blocks continuation but leaves cleanup available.
