# Agent Sandbox provider

`orka-workspace-sandbox` implements the shared workspace lifecycle against the
unmodified `sigs.k8s.io/agent-sandbox` v1.0.3 APIs. Run its core and extensions
controllers separately. This deployment does not install them.

Build from the repository root:

```sh
docker build -f providers/sandbox/Dockerfile -t orka-workspace-sandbox:dev .
kubectl apply -k providers/sandbox/config
```

Install the shared workspace CRDs and admission policies first. Pin the provider
image to a digest for deployment. The registration names the installed ServiceAccount
and pins the first valid `SandboxProviderConfig` UID in protected provider status.
Replacing that config requires a new provider registration. The deployment uses
leader election and an uncached Kubernetes client for journal and identity reads.

The provider accepts core-admitted public runtime templates with one supervisor
container and `restartPolicy: Never`. Private bootstrap credentials, runtime
authentication, exact runtime probes, session policy, and drain settlement remain
with Orka. Attachment acknowledgement precedes runtime startup. Infrastructure
readiness reports the exact Pod and its bootstrap endpoint without waiting for
runtime admission. Stop, suspension, and deletion require core's matching
sequence and instance retirement authorization.

Core owns and installs the admitted NetworkPolicies before publishing a workload.
Sandbox keeps upstream network-policy management `Unmanaged` and preserves the
admitted runtime namespace and labels. When a workload supplies `NetworkPolicy`,
the provider verifies the combined rules of every selecting policy before native
allocation or resume and before each startup-ready observation. It requires the
admitted isolation directions, rejects permissions outside the admitted envelope,
and repeats verification against the realized Pod's upstream labels. Missing,
terminating, or unreadable policies fail closed. The provider needs read-only
NetworkPolicy list permission; it does not create or delete Core's policies.
Host-networked runtimes with admitted policies are unsupported. The cluster's
network plugin must enforce Kubernetes NetworkPolicies.

The provider advertises ACP runtime allocation and data-only suspension. Every
allocation gets an isolated zero-replica warm pool, template, and claim. It does
not advertise pooled capacity, exec, files, independent checkpoints, TLS, or
memory restore. `SandboxWorkspaceProfile` enables the durable workspace layout
at `/durable/orka-workspace`; core must include that mount and environment in
the admitted template. A suspend-capable class must use session reuse and bounded
retention. PVC storage must be dynamically provisioned with Delete reclaim policy.

A workspace-owned ConfigMap stores bounded intent and exact native UIDs. A
protected workspace annotation requires that journal before native effects.
The runtime namespace has a separate ownership anchor, so runtime Pods may live
outside the workspace namespace without illegal cross-namespace owner references.
Neither missing journals nor copied labels authorize adopting an existing in-tree
allocation. Previous sequence tombstones remain until workspace finalization.

Suspension records the exact claim, Sandbox, PVC, and PV, waits for the current
upstream `Suspended` condition and exact Pod absence, then publishes retained
lineage. Resume requires that lineage and updates the template and the same
Sandbox blueprint while no Pod exists. The replacement Pod must have a new UID.
Cleanup observes native deletion and the backing PV's deletion before completion;
the native ownership anchor is removed last. Core independently revokes the
credentials it owns.

Run provider tests with `cd providers && go test ./sandbox/...`. The suite runs
shared lifecycle and suspension conformance against simulated native object
materialization, plus identity, mutation, journal-loss, and lost-response tests.

The [installed-backend proof](../../hack/external-sandbox-e2e/README.md) passed
against unmodified upstream v1.0.3 on Kubernetes v1.37.0. A non-root process
wrote a random marker to dynamically provisioned storage. Data-only suspension
removed the exact Pod; resume produced a new Pod UID and public bootstrap nonce
while preserving the marker and exact PVC/PV UIDs. Deletion independently observed
the replacement Pod, PVC, and PV gone. Run it with
`scripts/external-sandbox-e2e.sh`. Its standalone admission fixture and public
listener prove backend persistence; the separate real-core proof validates
credential bootstrap and RuntimeSession admission.
