# Fake workspace provider

The development provider materializes `ExecutionWorkspace.spec.workload` as one
Kubernetes Pod using its admitted public template. It never reads or mounts
bootstrap credential Secrets. Orka core owns credential delivery and authenticated
runtime admission. The provider acknowledges attachment epochs before Pod startup.

Pod mode requires an explicit bootstrap port and `restartPolicy: Never`. Each
numbered request gets a new instance identity. The provider records creation
intent before creating the Pod, pins its API-assigned UID, and checks its realized
spec against the admitted template with Kubernetes defaults accounted for.
Startup evidence includes that exact Pod and its IP endpoint. It does not wait
for Pod Ready, since the supervisor may require core bootstrap first.

A missing Pod withdraws readiness. It cannot be recreated within the same
sequence. The controller waits for core's exact-instance `spec.retirement` authorization before stopping. Stop deletes only the recorded UID and waits to observe its absence.
Unresolved create outcomes and API outages cannot prove termination. Core must
publish the next sequence with the exact stopped predecessor before another Pod
can be created. The provider advertises ACP workload materialization and fixture
pool support; it does not advertise exec, files, TLS, or data suspension.

With `Runtime` omitted, the lifecycle uses a status simulation and a synthetic
`.invalid` endpoint. Only this simulation implements optional suspension with
non-secret lineage proofs. These tests do not prove real data preservation.

From `providers/`, run:

```sh
go test -race ./fake/...
go run ./fake/cmd/orka-workspace-fake --conformance
```

The standalone conformance mode runs allocation/deletion, sequence replacement,
and simulated suspension against isolated Kubernetes fake clients. Unit tests
also exercise Pod creation, exact UID/spec checks, lost responses, disappearance,
API outages, and attachment acknowledgement during failed startup.

The [live proof](../../hack/external-workspace-e2e/README.md) passed on
Kubernetes v1.37.0. Two replicas shared one leader Lease and passed all 16
ownership and exact-lifetime checks. A real Orka Task then completed sealed
credential bootstrap, authenticated supervisor admission, deterministic ACP
execution with RuntimeSession and prompt identities, persisted result, exact
Pod retirement, and complete attachment-Secret revocation. The default kind CNI
does not prove packet-level NetworkPolicy enforcement.

Journal records contain public configuration and progress, with a 256 KiB limit
per record and resourceVersion compare-and-swap. A protected journal-required
workspace annotation is committed before native effects. Losing the journal
after that marker blocks both recreation and cleanup, even before the first
allocation status update. Before advancing to a new
sequence, the provider archives the previous stopped record. Retired sequences
and the final deleted record remain as tombstones until the owning workspace's
finalizers finish. A new sequence cannot replace a deleted allocation. Cold
resume requires the exact retained-data proof from its stopped predecessor.

Install the shared CRDs and admission policies first. Build from the repository
root and make the image available to the development cluster:

```sh
docker build -f providers/fake/Dockerfile -t orka-workspace-fake:dev .
kubectl apply -k providers/fake/config
```

The deployment has two replicas sharing the
`orka-workspace-fake.workspace.orka.ai` leader-election Lease. Its virtual-verb
grant authorizes only the provider registration named `fake`. The controller uses
an uncached client for all journal operations. Normal progression requires
current core admission; cleanup and attachment revocation can proceed when that
admission becomes stale. Core owns provider compatibility and readiness
conditions, while the adapter publishes versions, capabilities and heartbeats.
