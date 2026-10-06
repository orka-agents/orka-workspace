# Install and retire external providers

The platform installs the shared workspace API and ownership policy once.
Each provider installs its own configuration CRDs, controller ServiceAccount,
leader-election Lease, journal permissions, and backend access. Orka does not
install the native Sandbox or Substrate backend.

## Installation

The live development proof uses Kubernetes v1.37.0. Other versions are not yet
certified. The cluster must support `ValidatingAdmissionPolicy` and its CEL
authorizer. Use server-side apply for the embedded Pod schema:

```sh
kubectl apply --server-side -k config
```

Bind [core admission](../config/rbac/core-admission-role.yaml) to Orka and
[configuration](../config/rbac/operator-role.yaml) to your operators, in
addition to their ordinary API permissions. Every provider needs an exact
registration-scoped `provider-status` grant. A broad grant to a shared provider
ServiceAccount permits that identity to write other registrations' observations.

Build the selected provider from this repository root, publish an immutable
image digest, replace the example Deployment image, and install its bundle:

```sh
kubectl apply --server-side -k providers/fake/config
# Alternatively: providers/sandbox/config or providers/substrate/config.
```

See the [fake](../providers/fake/README.md),
[Sandbox](../providers/sandbox/README.md), and
[Substrate](../providers/substrate/README.md) deployment requirements. Substrate
also needs operator-managed native control credentials and reachable actor DNS.
Provider config and profile objects must retain their admitted UIDs and functional
specs. Replacing a registration's config requires a new registration.

Orka's integration branch enables the generic path with the workspace provider
API and ACP workspace dispatch. Enable its class-use and Task provenance
admission protections. New generic workspaces need no Sandbox/Substrate backend
flag. Provider configuration kinds are resolved through the Kubernetes REST mapper;
grant Orka read access to those installed kinds.

An ACP class declares its supervisor contract explicitly. For the fake provider,
a minimal example is:

```yaml
apiVersion: fake.workspace.orka.ai/v1alpha1
kind: FakePoolParameters
metadata:
  name: acp-example
spec: {}
---
apiVersion: workspace.orka.ai/v1alpha1
kind: ExecutionWorkspaceClass
metadata:
  name: acp-example
spec:
  mode: Interactive
  providerRef:
    name: fake
  parametersRef:
    group: fake.workspace.orka.ai
    kind: FakePoolParameters
    name: acp-example
  requiredFeatures: [acp.runtime.v2]
  allowedReuseScopes: [None]
  lifecycle:
    defaultOnDetach: Delete
    allowedOnDetach: [Delete]
    detachTimeout: 2m
    deletionPolicy:
      providerResources: Delete
      persistentVolumes: Delete
      checkpoints: Delete
```

Use your registration name and provider-owned profile reference for other
providers. An explicit ACP contract does not imply generic workspace-agent exec,
reset, or TLS support. Suspension additionally needs Session reuse, a DataOnly
profile, and bounded retention. Providers advertise only capabilities they prove.

Core's external workload defaults request CPU and memory before computing the
workload revision. They do not impose Kubernetes ephemeral-storage quotas on
external backends. The native Substrate provider rejects unsupported positive
resource requirements; it does not discard admitted limits during translation.

## Compatibility

These implementations use `workspace.orka.ai/v1alpha1` and
`orka.workspace.lifecycle.v1`. Install the shared API and provider binaries from
the same revision, and use the Orka integration revision that pins that shared
module. The older `v0.1.0-alpha.1` consumer does not implement this external
workload contract.

| Component | Supported boundary | Verification |
| --- | --- | --- |
| Kubernetes | v1.37.0 with CEL admission authorization | Scoped live fixtures; other versions unverified |
| Fake provider | Separate Pod runtime; `orka.harness.v2` | Two-replica ownership, actual Core Task/RuntimeSession, and exact credential/Pod retirement |
| Agent Sandbox | Unmodified upstream v1.0.3; dynamic PVC with Delete reclaim | Installed filesystem persistence and exact Pod/PVC/PV deletion |
| Agent Substrate | Native v0.1.0 at `fa6d949685a6318940a9a0195c867c864009b820`; gVisor; Data/Data/ColdBoot | Installed Data capture/export/import plus actual Core Task/RuntimeSession and exact native/credential retirement |
| Legacy allocations | Original in-tree owner only | Stock upgrade gate rejects all retained states before and after schema pruning; fresh startup follows exact cleanup |
| Pooled Substrate MCP Tools | Existing in-tree implementation | Retained migration boundary |
| Fiberd | No external provider yet | Upstream prerequisites remain unsatisfied |

## Startup and credentials

Core publishes a numbered workload containing public configuration. The provider
acknowledges the attachment before starting compute and reports the exact instance.
Core reads accessible Pods and storage, checks selecting network-policy permissions,
records an independent Pod or worker UID fence, and binds private credentials to
that instance. Only the authenticated supervisor probe can open RuntimePool
admission. A provider's Ready state cannot do so.

Replacement requires the previous exact instance to be stopped. Core observes
the recorded runtime Pod UID gone, or accepts the provider's current-generation
native-process termination observation for the exact core-authorized sequence
and identity. The native worker is infrastructure and may survive that process.
Core then rotates consumed credentials and publishes the next sequence with its
predecessor and retained-data proof. A replacement cancelled before startup can
retire without replacing its predecessor's verified startup binding. Kubernetes
CRD defaulting is accounted for in workload revision hashing.

## Drain, upgrade, and uninstall

Set the registration's lifecycle state to `Draining`. New workspaces are denied;
an existing workspace can continue only for its frozen session, slot, class, and
provider. Cold resume qualifies. `Disabled` denies continuation and allows cleanup.
Keep the controller and native backend running until all owned allocations retire.

Core closes runtime admission and completes exact authenticated drain before
publishing retirement. A lost Pod-backed supervisor can retire after Core is
quiescent and independently proves its persisted exact Pod UID is absent.
Provider status and infrastructure-worker absence cannot establish that exception
for a native process. The provider observes termination and records data
disposition. Core then removes its credentials and endpoint policies and releases
its finalizers. A missing response or changed identity leaves cleanup pending.
Provider journals remain until workspace finalization. Retained checkpoint
artifacts have their own references and must be released separately.

Drain and retire legacy allocations under the previous Orka release before
installing the new RuntimePool schema or replacing the controller. The removal
release has no in-tree ACP cleanup implementation. Its startup gate names every
old-shaped RuntimePool and every workspace bearing the old controller label,
including Ready, Failed, Suspended, and Deleted objects. It refuses startup until
their original owner removes them. Installing external controllers alongside the
previous release does not transfer ownership or authorize cutover. Do not copy
labels, adopt native IDs, or prune stored fields to transfer ownership.

Inventory retained native checkpoints, journals, templates, PVCs, and Actors as
well as public workspace objects. Keep the old release and native backend
available until their obligations are discharged. Remove the old
`acp.workspace.orka.ai` config/profile CRDs only after all references and objects
are gone. A schema update does not drain an allocation.

After owned workspaces and retained artifacts are gone, remove the provider
registration and deployment. Verify provider namespaces contain no outstanding
journals, anchors, compute, or retained storage. Remove provider-specific CRDs only
after their config/profile objects are no longer referenced. The shared bundle
stays installed while any other provider uses it.

Pooled Substrate MCP Tools still use the in-tree path through
`--substrate-mcp-tools-enabled`. Its Tool and SubstrateActorPool controllers,
authenticated control transport, and cleanup remain in Orka. This flag never
enables ACP workspace allocation. The old `--agent-sandbox-enabled`,
`--substrate-enabled`, and `--enable-fake-workspace-provider` flags are removed.
Remove them from controller arguments and use the separate providers for ACP.
Generic native worker observation uses read-only Pod permissions; the provider
owns worker compute and egress policies.

## Local verification

`make check` runs shared/provider tests, race checks, conformance, schema and
generation checks, and dependency checks. The separate-controller proof preserves
its scoped cluster and artifacts:

```sh
scripts/external-workspace-e2e.sh provider
ORKA_CORE_BUILD_RELEASED=1 scripts/external-workspace-e2e.sh core
```

The core mode builds the actual Orka supervisor and a deterministic ACP agent,
runs a Task through the independent provider, and checks runtime admission and
retirement. Default kind networking does not prove packet-level NetworkPolicy
enforcement. Sandbox filesystem persistence and installed native Substrate
Data export/import passed separate backend tests. Those fixtures construct core
admission and do not claim real-core credential bootstrap or RuntimeSession execution.

The Sandbox fixture runs with `scripts/external-sandbox-e2e.sh`. The native
fixture uses an isolated cluster with the backend's required certificate feature
gates; see [its setup and proof](../hack/external-substrate-e2e/README.md).
The additional `scripts/external-substrate-core-e2e.sh proof` runs an actual Task
through deployed Core and native provider controllers. It proves authenticated
Serving, RuntimeSession execution, and exact compute and credential retirement.
The [retained-resource upgrade proof](../hack/external-workspace-e2e/README.md)
uses the same stock Core image as the fake Task proof.
