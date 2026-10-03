# Fake workspace provider

This is a development status fixture. It models allocation, exact-instance stop,
deletion, attachment acknowledgement, and pool capacity. It does not launch an
image or provide ACP, exec, files, TLS, service endpoints, reset, or resume. Its
synthetic `.invalid` startup endpoint is evidence for contract tests only.

The controller uses the lifecycle implementation exercised by conformance.
Because the preserved shared v1alpha1 schema has no workload request, it derives
a fixed fixture request from the workspace and class binding. It never stores a
workload request or SDK startup evidence in shared status. Phase 2 adds the real
workload and secure-startup contract.

From `providers/`, run:

```sh
go test ./...
go run ./fake/cmd/orka-workspace-fake --conformance
```

The standalone conformance mode uses an isolated Kubernetes fake client. A new
driver reads the same ConfigMap records to exercise restart recovery. The
deployed controller uses an uncached Kubernetes client and persists those
records in the API server.

Each non-secret journal record is at most 256 KiB. The controller commits intent
and a random allocation/instance identity before progressing, and uses
resourceVersion compare-and-swap for every update. Retries retain the identity.
Stop and delete reject another instance's identity. Delete requires a stopped
instance, persists the deletion policy, then records data disposition. Deleted
records remain as tombstones owned by the ExecutionWorkspace, so Kubernetes
garbage collection removes them only after the workspace's finalizers finish.
The provider does not read attachment Secrets or store credentials in journals.

Install the shared CRDs and admission policies before this provider. Build the
image from the repository root with `docker build -f providers/fake/Dockerfile
-t orka-workspace-fake:dev .`, make the image available to your cluster, then run
`kubectl apply -k providers/fake/config`. The deployment has two replicas using
the separate `orka-workspace-fake.workspace.orka.ai` leader election Lease.
The included virtual-verb grant authorizes only the registration named `fake`.
Normal workspace progression requires matching core admission markers and the
current core-owned `Admitted` condition. Cleanup and attachment revocation can
proceed after admission expires. Core owns provider compatibility and readiness
conditions; this adapter publishes only capability, version, and heartbeat data.
