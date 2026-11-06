# Substrate provider

`orka-workspace-substrate` implements ACP workspace allocation against the
untouched Agent Substrate v1.0.0 control protocol (`substrate.ateapi.v1`,
`api/ateapi/v1/ateapi.proto`). The provider, protocol source, and conformance
baseline remain pinned to commit `b34fbee742906494b5eb4f1f36987176bf8770a4`
(`nativepb/upstream.env`). The copied Apache-licensed protocol retains its
upstream attribution in `nativepb/`. Regenerate or verify the bindings with
`PROTOC=<protoc 36.0> scripts/generate-substrate-proto.sh [generate|verify]`
(set `SUBSTRATE_SOURCE_DIR` to a checkout of the pin to avoid a download).
This deployment does not install Substrate or change an existing in-tree
allocation's owner. [ADR 0006](../../docs/adr/0006-agent-substrate-v1.md)
records the v1 contract changes.

Build from the repository root:

```sh
docker build -f providers/substrate/Dockerfile -t orka-workspace-substrate:dev .
kubectl apply -k providers/substrate/config
```

## Upgrading from Agent Substrate v0.1.0

Upstream cannot upgrade a v0.1.0 installation in place; v1.0.0 requires a fresh
install. This provider never adopts, migrates, or cleans up v0.1.0 state.
Journals and checkpoint catalogs written for v0.1.0 fail closed: workspaces
report `Provisioned=Unknown` with reason `LegacySubstrateV0State`, and every
lifecycle operation is refused. Before upgrading:

1. With the previous provider release, retire every v0.1.0 workspace (Delete)
   and delete every `ExecutionWorkspaceCheckpoint` it exported, until no
   `substrate-workspace-*`, `substrate-history-*`, `substrate-data-*` or
   `substrate-export-*` ConfigMaps remain.
2. Uninstall Agent Substrate v0.1.0 and install v1.0.0.
3. Upgrade this provider and Orka core together, since requests must carry the
   new egress allowlist instead of a NetworkPolicy.

## Installation

Install the shared workspace CRDs and ownership policies first.

**Control transport.** The shipped Deployment authenticates to ate-api with a
projected ServiceAccount token (audience `api.ate-system.svc`), so its principal
is `system:serviceaccount:orka-workspace-system:orka-workspace-substrate`. It
verifies ate-api with the `servicedns.podcert.ate.dev/identity`
ClusterTrustBundle. A client certificate/key pair (`--native-cert-file`,
`--native-key-file`) may replace the token; its principal is then its SPIFFE ID.
Agent Substrate's `ate-system` NetworkPolicy admits ate-api clients only from
namespaces labeled `ate.dev/api-access=true`; the shipped Namespace carries it.
Control always uses authenticated TLS. Credential values never enter workspace
requests, provider status, templates, journals, or logs. The binary reloads
bearer identity for every request and client certificates for every handshake.
Use a digest-pinned provider image. Two replicas share a provider-specific
leader Lease, and identity and journal reads bypass the manager cache.

**Authorization.** Agent Substrate v1 always enforces access policies. Grant the
provider principal least privilege: `editor` on every Atespace a
`SubstrateWorkspaceProfile` names, and global `viewer` (for `ListWorkers`,
`GetWorker` and `ListWorkerActorAssignments`). The provider does not call
`DrainWorker`, which needs global `owner`. With `kubectl ate` as an operator:

```yaml
# provider-atespace-policy.yaml (one per profile Atespace)
metadata:
  name: default
bindings:
- role: editor
  members:
  - user:system:serviceaccount:orka-workspace-system:orka-workspace-substrate
```

```sh
kubectl ate create access-policy -a <atespace> -f provider-atespace-policy.yaml
# Add a viewer binding for the same member to the global policy; `update`
# replaces the whole policy, so merge it into the current bindings first.
kubectl ate get access-policy --global -o yaml > global.yaml
kubectl ate update access-policy --global -f global.yaml
```

Do not make the provider a bootstrap owner. See upstream
`docs/guides/authorization.md`.

**Routing.** Actor DNS names no longer exist. The provider and Orka reach an
Actor through `atenet-router` over mTLS with the header
`ate-target-actor: <atespace>/<actor>`. The shipped Deployment presents a
`podidentity.podcert.ate.dev/identity` pod certificate as
`spiffe://cluster.local/ns/orka-workspace-system/sa/orka-workspace-substrate`.
Install Substrate with that SPIFFE ID, and Orka core's router client identity,
in `--ingress-allowed-spiffe-ids`; the router accepts no client otherwise.
Configure `--native-router-endpoint` (default
`https://atenet-router.ate-system.svc:443`), `--native-router-ca-file`, and
`--native-router-cert-file`/`--native-router-key-file` (one credential bundle
may serve both). Without router configuration the provider publishes only an
unconfigured status and serves no allocations.

**Egress.** Agent Substrate v1 removed direct egress. All Actor TCP egress
traverses the installation's egress gateway, denied unless the Actor's nested
`EgressPolicy` allows it; DNS for unlisted names answers `NXDOMAIN`. Orka admits
a host-name allowlist in `RuntimeWorkload.Egress` (`HTTP`, `HTTPS` or
`TLSPassthrough` rules with explicit ports; a lowercase FQDN or a leftmost-label
wildcard, never bare `*`), and must include every origin the supervisor needs,
such as its in-cluster provider-authentication proxy. The provider rejects a
request still carrying the Kubernetes `NetworkPolicy` shape, compiles the
allowlist into the Actor's `default` EgressPolicy before every Resume (updates
carry the observed policy's UID/version), re-verifies it on every Ready
observation, and installs deny-all and refuses to boot if compilation fails.
`HTTPS` rules are intercepted by the gateway. Only when an allowlist contains
one does the derived template project the `egress-mitm.ate.dev` and
`system-roots.ate.dev` trust bundles to
`/run/orka-substrate-egress-trust/trust-bundle.pem` and point `SSL_CERT_FILE`,
`SSL_CERT_DIR`, `REQUESTS_CA_BUNDLE`, `AWS_CA_BUNDLE`, `NODE_EXTRA_CA_CERTS`,
`CURL_CA_BUNDLE` and `GIT_SSL_CAINFO` at it; `HTTP` and `TLSPassthrough` need no
gateway CA. Upstream allows at most 32 environment variables per container, so a
runtime environment that cannot also hold those seven is refused. Admitted
runtimes cannot set those variables. The Orka supervisor builds each agent
child's environment explicitly, so intercepted HTTPS egress for agent children
also requires the supervisor to pass the trust variables through; until it does,
core admits only HTTP and TLS passthrough rules.

**Infrastructure.** The profile's infrastructure ActorTemplate must exist in
the profile Atespace, carry `resources.limits` (both CPU and memory; v1 requires
them), and select exactly one operator-owned `ate.dev/v1` WorkerPool by label.
Create it with `snapshotConfig.goldenSnapshotConfig.mode: GOLDEN_SNAPSHOT_MODE_DISABLED`
so no golden Actor runs its containers. Its containers never execute ACP work.
The source WorkerPool must use `sandboxClasses: [{name: gvisor}]` (or omit the
list), must not use the removed v1alpha1 `sandboxClass` field, and must leave
`template.resources` unset; it may run zero replicas. The provider pins the
source pool UID and selector, then copies its worker image, sandbox class
configuration, scheduling, labels and annotations into a private single-replica
WorkerPool for each instance. The private pool always names its SandboxConfig:
an omitted `configRef` is pinned to the source pool's resolved
`status.sandboxClasses[].configRef`, because the class default can move during an
upgrade. A source pool whose config is unresolved (no `Resolved=True` class
condition at the pool's current generation; Agent Substrate keeps the last
`configRef` while a removed default or deleting config fails to resolve),
switching (`pendingConfigRef`), or declared differently from what it resolved is refused. Its Pod template carries private allocation and
instance labels from birth, and the derived template's `workerSelector` adds
those labels. v1 workers report `--max-actors` (default 1000) as their Actor
count, so the provider instead sizes the private worker for exactly one Actor:
for Actor limits `(C, M)` the Pod limits and requests are `cpu=C`,
`memory=M+64Mi`, since a v1.0.0 gVisor worker keeps 64Mi for itself and reports
the rest as capacity. Before Resume the provider pins an `ACTIVE`, empty gVisor
worker whose capacity is exactly `(C, M)`, its registered process epoch, its
exact Pod UID, and an empty `ListWorkerActorAssignments`. It refuses missing or
foreign birth labels and never patches labels onto an executing worker. Observe
rechecks the pool spec, labels, recorded worker, capacity, sandbox class,
that the Actor runs under the recorded worker epoch, that the worker hosts only
this Actor, and the applied egress policy. Native worker and router ingress
remain operator-owned. Operators must avoid concurrent mutation of
adapter-owned resources.

## Workload translation

Orka supplies the immutable public supervisor request. The provider compiles it
into an immutable ActorTemplate with a native SystemInfo identity projection,
the egress trust bundle when the allowlist has HTTPS rules, a `/v2/health`
wakeup probe on port 80 (120s), and the
listen address adapted to port 80. Every server-defaulted field is set
explicitly (`preferredFidelity: VOLUMES`, golden snapshots `DISABLED`, the probe
timeout) so the stored template's digest equals the compiled digest. Actor size
comes from the admitted container limits, falling back per resource to the
infrastructure template's limits; both must end positive and exact (whole
millicores and bytes). CPU and memory requests require matching admitted limits
at least as large. Resources with requests but no limits, unsupported resource
names, and requests exceeding the limit fail before compute is created.
The pinned root overlay requires a permission repair before the requested
command starts. An explicit working directory is passed as a literal argument to
that initialization wrapper and selected before the exact command and arguments
execute. The backend has no ephemeral-volume primitive. Kubernetes `emptyDir`
declarations and mounts are rejected even without a quota, as is a required
read-only root filesystem. The dedicated durable workspace directory remains
supported through the DataOnly profile.
Admitted Pod scheduling constraints, including node selection, affinity,
tolerations, scheduler/runtime classes, priority, topology spread, scheduling
gates/groups, resource claims, Pod resource budgets, and OS selection, are
unsupported. Placement comes only from the exact operator WorkerPool and its
frozen specification. Core omits its generated Pod node selector from new native
intent before admission.
Environment values, commands, and arguments must already be resolved literals.
Kubernetes `$(NAME)` expansion and `$$` escaping are rejected before allocation
rather than copied with different native meaning. At most 32 environment
variables, including the seven trust variables, are supported.
Native active deadlines are unsupported and rejected before allocation. The
sealed-bootstrap nonce must be present once as a nonempty literal before any
compute is created.

Bootstrap and declared container ports must use TCP 80. A supplied
`ORKA_ACP_LISTEN_ADDRESS` must be literal `:80`. Kubernetes probes, lifecycle
hooks, termination grace periods, custom DNS or host namespaces, Pod identity
settings, readiness gates, service-link injection and container stream/restart
controls are unsupported. Digest images use the native cache, so omitted or
`IfNotPresent` image pull policy is supported; `Always` and `Never` are rejected.
DataOnly accepts only the single writable durable mount at
`/durable/orka-workspace`; other volume sources and additional mounts are rejected.
An omitted template namespace resolves downward identity to the request namespace.

Native sessions use supervisor defaults and SystemInfo identity. Pod namespace,
session-directory and configured Core MCP broker environment overrides are
rejected. Core omits these overrides, Kubernetes health/shutdown fields, and
declares port 80 before hashing fresh native intent.

The native process starts as UID/GID 0 regardless of image `USER`.
Requests must explicitly set effective `RunAsUser` and `RunAsGroup` to 0, either
on the supervisor container or inherited from the Pod security context. Every
supplied user/group constraint must match 0; non-root or privileged execution
requirements are rejected. Admitted capability add/drop lists are translated
without adding capabilities; absent lists use the backend defaults.
Other supplied security settings, including privilege escalation, seccomp,
AppArmor, SELinux, Windows options, proc mounts, filesystem groups, supplemental
groups, and sysctls, fail before allocation. Core publishes explicit UID/GID 0
and omits unsupported security settings from new native-process intent before
admission.

The provider advertises ACP allocation, `runtime.native-process`, verified
data-only suspension, `checkpoint.data`, `restore.cold`, and
`runtime.egress-allowlist` (it accepts only `RuntimeWorkload.Egress`, never a
NetworkPolicy). Pooled capacity, MCP
Service workloads, exec, files, and full-memory restore remain unsupported.
The `runtime.native-process` capability lets Core publish intent for a fresh
writable container filesystem with no Kubernetes scratch mounts and require exact
process startup evidence. Core freezes this choice before workload admission;
Pod-backed providers keep their own scratch-volume layout. Existing admitted
requests are never rewritten.

## Lifecycle

A workspace-owned ConfigMap persists bounded creation and retirement intent,
random never-reused Actor names, native UIDs, immutable template digests, the
applied egress policy UID and digest, exact worker placement and epoch, and
checkpoint provenance. A protected workspace marker binds the journal's exact
UID before native effects. Missing or changed journals close admission and block
cleanup. Previous numbered request tombstones remain until workspace
finalization. Native resources in worker namespaces use an exact namespace-local
ownership anchor. Journals remain until core finalizes the workspace.
An issued suspension is never replayed because of a transport error. Known native
`SUSPENDING` progress stays pending; a still-running or failed Actor reports an
unresolved outcome until exact snapshot evidence or authorized retirement settles it.

Infrastructure readiness reports the native Actor identity, exact worker Pod,
request sequence, the public sealed-bootstrap challenge hash, the router
endpoint, and `routerTarget` `<atespace>/<actor>`. The provider reads the
challenge through the router and rechecks native Actor version and worker
placement after receiving it. A seeded listener's 404 is accepted only for the
already journaled challenge and unchanged Actor/worker lifetime. Orka repeats
the process-bound challenge exchange through the router, delivers its own
credentials, and verifies the authenticated runtime boot. Attachment
acknowledgement precedes startup and never waits for runtime admission.

Every derived template is data-only with golden snapshots disabled, including
templates that do not permit user suspension, so a fresh Actor holds no
snapshot and its Resume is a cold boot; the provider verifies that after
`CreateActor` and before every Resume. Full process memory restore stays closed.
A suspend profile requires an interactive workspace, an allowed Suspend action,
bounded expiry, and the admitted mount and environment for
`/durable/orka-workspace`, with `ORKA_ACP_DURABLE_WORKSPACE_KEY=shared` so a fresh
session continues in the same retained directory. A workspace without a Session
keeps that layout, so a checkpoint restore into a Delete-on-detach Task works;
the bounded expiry covers any data it suspends.
A suspended count cap additionally requires maxLifetime and uses a persistent
ConfigMap reservation before capture. The provider verifies the exact
single-Actor worker hosts only this Actor, requests a `SYNCHRONOUS` suspension
once, and requires a new snapshot generation with fidelity `VOLUMES`, owner
`ACTOR`, a `COMPLETED` durable copy and the recorded template UID. It copies it
to a private Tag and fences the source Actor UID, version and snapshot before
and after `CreateTag` (v1 Tags no longer report a source Actor UID). The Tag must
be `ATESPACE`-scoped with a completed `VOLUMES` snapshot and the recorded
template UID. The provider deletes the exact private WorkerPool with foreground
propagation and the exact worker Pod with a UID precondition, then observes
exact worker Pod UID absence before deleting the source Actor or reporting
suspension. A snapshot alone is never termination evidence. The operator source
pool remains untouched. A worker replacement between preparation and native
scheduling never becomes startup evidence. Authorized retirement can separately
record and retire its exact UID only after the original Pod is absent and
private-pool ownership is verified; foreign labels, foreign assignments, or
changed placement block mutation.

Resume verifies the retained Tag, original template and immutable infrastructure,
including the frozen private worker specification. Generated allocation and
instance labels rotate. The worker image follows the source pool's current
image, because an Agent Substrate minor upgrade rolls WorkerPools to a new worker
image and the API server must never be newer than a worker: a data-only
(`VOLUMES`) resume cold boots anyway, so no process state crosses the change.
The new image must be pinned by digest (`repo@sha256:<64 hex>`), and sandbox
class, pinned SandboxConfig, scheduling, size, template labels, and the source
pool's own labels must remain identical; anything else fails closed. Each such
transition is appended to the journal's `workerImageProvenance` (from, to,
the previous private pool digest, time), bounded at 16 per lineage. A
`RestoreFrom` import applies the same rule against the checkpoint's recorded
pool specification and source pool labels (a catalog published before labels
were recorded restores only onto an identical pool specification). Do not patch the private `-workers` pools during an upgrade;
patch the operator pools, then let running workspaces move by eviction.
It creates a fresh Actor from the Tag under that original template (v1 seeds an
Actor only under the Tag's template), checks the Actor holds exactly the Tag's
completed `VOLUMES` snapshot, then uses native `UpdateActor` UID/version CAS to
select the newly compiled bootstrap template while still suspended. Its Resume
restores durable volumes and cold-boots. It applies the egress allowlist,
resumes once, checks a new worker and process challenge, and leaves credential
rotation and authenticated admission to Orka.
A lost boot or suspension response is recovered by observation; a possibly
accepted mutation is never replayed.

**Worker eviction.** When the exact fenced worker Pod begins graceful termination
(an upgrade roll, node drain, or Spot reclaim), Observe keeps reporting the
running instance with its unchanged startup evidence instead of withdrawing it,
so Orka core can close admission, drain the supervisor, and authorize a
synchronous `Retirement.Suspend` within the worker's 30-minute window. A worker
that is not already fenced is never adopted while terminating, and the suspend
path tolerates a Pod that is already deleting. An instance that is not suspended
in time crashes and fails as described below; its last checkpoint stays
retained.

**Runtime provenance.** Each data checkpoint records the Tag snapshot's
`sandbox_runtime` (class, schema version, attribute digest); a stamp from another
sandbox class fails the capture, and verification rejects a mismatching record.
A retried capture whose catalog was published before stamping keeps that
unstamped record; its Tag digest already pins the snapshot.
Agent Substrate runs every gVisor restore without memory under the worker's
default `runsc` and does not check cross-version compatibility. When a restore
crashes before its supervisor boots and the runtime stamped on the Actor's
worker assignment during that boot differs from the checkpoint's, the failure is
recorded with the distinct `Provisioned` reason `SandboxRuntimeRestoreFailed`
instead of `InstanceFailed`; the checkpoint stays available to explicit recovery
and nothing is replayed. The provider journals that assignment stamp when it
first observes it and never infers the runtime from the Worker's current
default, which can change within a registration epoch; without the stamp the
crash is reported as `InstanceFailed`. Explicit core Retirement.Suspend is honored
also while desiredState remains Ready during epoch rotation.
Actor creation intent is also journaled before its RPC. An issued create recovers
only by observing the Actor; absence without a recorded UID blocks allocation
and retirement rather than issuing another create.
A boot request that the native API refuses outright, for example with
`ResourceExhausted` while the private worker registers, is retried; any other
outcome stays recorded and is recovered by observation. An allocation without
process evidence after `--native-boot-timeout` (default 6m, the in-tree window)
fails. So does a journaled Actor that disappears or leaves `RUNNING` outside the
lifecycle. A failed allocation withdraws startup, reports the workspace `Failed`
and never restarts; its retained checkpoints remain available to
`recoverLastCheckpoint`. Physical cleanup still waits for Core's exact Stop (or
Suspend) authorization, which deletes the private WorkerPool and worker Pod and
removes any remaining Actor.

Deletion requires observed termination and Delete disposition for provider
resources, checkpoints, and storage. Cleanup removes recorded Tags, immutable
templates, and ownership anchors in order, every native delete carrying the
recorded UID as a `DeleteOptions` precondition (v1 deletes Actors in every
state; the Actor's egress policy goes with it), and records final disposition
before core may release the workspace. Retained in-tree MCP actor pools remain a
migration boundary: the shared workload contract currently has no MCP
service/pool request. Existing resources retire under their original owner.

## Checkpoints

`ExecutionWorkspaceCheckpoint` exports an existing verified Data artifact from
an idle suspended workspace. `recoverLastCheckpoint` explicitly selects already
verified retained data after failure or quarantine; export never interrupts or
retries uncertain work. A private index pins the exact public checkpoint UID and
selected artifact. Public status exposes only its digest, class binding,
completion time, and readiness. Private catalog owners use Kubernetes CAS to
retain the native Tag and immutable template across source deletion.
The export index has its own finalizer before publication or reference acquisition.
It survives foreground and namespace deletion until exact reference release and
native collection are accepted; a UID-bound receipt makes release retries safe.
An acquisition-intent marker is persisted before retaining the public reference.
If the selected catalog disappears before acquisition was ever issued, only the
exact index and checkpoint metadata can close. Legacy or issued acquisitions
remain closed on catalog loss.

An admitted `RestoreFrom` request must match the checkpoint UID and digest,
namespace, class and provider revisions, WorkerPool UID, and durable layout.
Import commits its own workspace reference before native creation, then cold
boots a fresh Actor with the new bootstrap template. Checkpoint deletion first
withdraws Ready, then freezes the exact pending workspace UIDs and workload
revisions in its protected index. It waits for those references to acquire their
own ownership; later restores cannot join the wait set or acquire the retiring
reference. Lost acquisition responses recover from the committed private owner,
even after public finalization. The last owner releases and observes exact native
Tag/template deletion; catalog tombstones remain through owner finalization and
protected export-index release.

## Verification

Run `cd providers && go test -race ./substrate/...` and `go vet ./substrate/...`.
The suite includes all shared lifecycle, replacement, and suspension conformance
variants against a simulated v1 control plane that applies upstream server
defaults, plus lost responses, journal loss, identity replacement, shared-worker
and foreign-assignment rejection, worker capacity/class/epoch changes, malformed
Tag provenance, non-`VOLUMES` snapshots, seeded fresh Actors, egress compilation,
drift repair and deny-all fallback, legacy NetworkPolicy requests, legacy v0.1.0
journals and catalogs, closed-bootstrap recovery, Ready-state suspension
rollover, checkpoint ownership transfers, source/public-reference deletion,
explicit failed-source recovery, private catalog loss, and separate selectors in
a shared WorkerPool. Unit tests cover simulated control operations only.

The installed native Data proof in `hack/external-substrate-e2e` passed against
an unmodified Agent Substrate v1.0.0 installation on 2026-11-06 (amd64 gVisor,
official `ate-setup` install): a private single-Actor gVisor worker, the
data-only golden-disabled template and the admitted egress allowlist existed
before every Resume; routed challenges, Actor UID/version and worker epoch
matched; synchronous `VOLUMES` capture preceded exact worker Pod and Actor
termination; an independent checkpoint survived source deletion; a cold import
into a fresh Actor read the original marker; and final owner deletion collected
native Tags and templates. The Core Task proof (last passed against v0.1.0 on
2026-10-05) is ported but needs an Orka core that admits `RuntimeWorkload.Egress`
and routes through `atenet-router`.
