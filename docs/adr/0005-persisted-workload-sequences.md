# ADR 0005: Fence each runtime execution and authorize retirement

Date: 2026-10-02

Status: Accepted. Extends ADRs 0001 and 0002 for the persisted lifecycle handoff.

## Decision

An ExecutionWorkspace survives a cold resume. Its runtime process and bootstrap material do not. Core therefore writes `spec.workload` as an immutable request within a numbered sequence. The first sequence is one. A replacement advances by exactly one and pins the previous allocation ID, instance ID, and request revision. Kubernetes validation rejects changes within a sequence, sequence skips, and removal of an established workload.

Core and provider both validate replacements. Core checks the published observation; the provider checks its private durable journal. The predecessor must be exactly identified and stopped, with its startup evidence withdrawn. A deleted allocation is terminal. Repeating a retired request returns its tombstone and never restarts it.

Cold resume additionally pins `retainedData`: an opaque workspace-owned lineage ID, its source instance, and a digest of the provider's verified lineage record. Stopped state alone cannot authorize resume. The provider must revalidate the journal and retained backend resources before booting. Changing the RuntimePool does not transfer or discard that lineage. Providers never adopt resources from the retired in-tree controller.

## Retirement ordering

`desiredState` expresses lifecycle intent. It does not authorize immediately killing a runtime that may still be executing a prompt. Core first closes admission, performs authenticated drain, and settles Task and RuntimeSession obligations. It then writes `spec.retirement`, binding the current sequence and exact instance to Stop, Suspend, or Delete.

Provider controllers observe and publish allocation identity while waiting for that authorization. They call physical lifecycle operations only after `ValidateWorkloadRetirement` accepts it. Delete authorization permits the required preceding Stop. Suspend authorization does not permit data deletion. An established retirement authorization can escalate to Delete; it cannot change instance or disappear until the next workload sequence.

Each provider records a protected journal-required marker before native effects. Missing journals after that marker block both new allocation and cleanup. Journal absence is never substituted for termination evidence, including when the creation response was lost before status publication. Journals and old sequence tombstones survive until the workspace's cleanup obligations are accepted.

Provider lifecycle disposition covers provider-owned compute, data, and credentials. A provider that never held core credentials reports those categories as NotApplicable. Core independently verifies revocation and deletion of its own attachment and runtime credentials before releasing its finalizers. Lifecycle conformance checks provider disposition; it does not certify core credential cleanup.

## Stored fields and compatibility

| Field | Writer | Meaning |
| --- | --- | --- |
| `ExecutionWorkspace.spec.workload` | Core | Immutable public workload for one sequence |
| `ExecutionWorkspace.spec.retirement` | Core | Exact-instance authorization after drain and settlement |
| `ExecutionWorkspace.status.allocation` | Selected provider | Sequence, identity, startup evidence, verified retained lineage, disposition |
| `ExecutionWorkspaceProvider.spec.serviceAccountRef` | Operator | Immutable adapter principal checked for registration-scoped `provider-status` authority |

Startup evidence can report exact Pod, PVC, and PV identities. Core keeps independent Kubernetes reads, materialization checks, private credential binding, one-time bootstrap exchange, and the authenticated runtime fence probe. Provider readiness never waits on runtime admission. Attachment acknowledgement never waits on a running runtime.

The shared schemas remain `workspace.orka.ai/v1alpha1`. These additions preserve every baseline field, validation, and class-profile hash input. Tests verify legacy schemas after removing only the listed additions, preserve the old deepcopy behavior, compile the generated schemas with Kubernetes validation, and prove that API pruning preserves the workload digest. Embedded Pod metadata must be generated explicitly, or pruning would erase labels and namespace from the frozen request.

The new fake journal is `fake.workspace.journal.v2`. Its v1 simulation journals are not silently reinterpreted. No production external-provider compatibility is claimed until Orka startup, retirement, admission policy, and cluster integration pass together.
