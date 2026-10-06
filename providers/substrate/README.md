# Substrate provider

`orka-workspace-substrate` implements ACP workspace allocation against the
untouched Agent Substrate v0.1.0 control protocol. The provider, protocol source,
and conformance baseline remain pinned to commit
`fa6d949685a6318940a9a0195c867c864009b820`. The copied Apache-licensed protocol
retains its upstream attribution in `nativepb/`. This deployment does not install
Substrate or change an existing in-tree allocation's owner.

Build from the repository root:

```sh
docker build -f providers/substrate/Dockerfile -t orka-workspace-substrate:dev .
kubectl apply -k providers/substrate/config
```

Install the shared workspace CRDs and ownership policies first. Supply the
installation-owned `substrate-native-control` Secret with `ca.crt` and `token`,
or configure the binary with rotating client certificate/key files instead.
Control always uses authenticated TLS. Secret values never enter workspace
requests, provider status, templates, journals, or logs. The binary reloads bearer
identity for every request and client certificates for every TLS handshake.
Use a digest-pinned provider image. Two replicas share a provider-specific leader
Lease, and identity and journal reads bypass the manager cache.

Configure native direct egress with ateapi's transparent egress gateway disabled
and verify WorkerPool NetworkPolicy enforcement before explicitly setting
`--native-direct-egress=true` in an installation overlay. The shipped Deployment
keeps this acknowledgement false and cannot serve allocations until configured.
Configure the Actor DNS suffix so
`<actor>.<atespace>.<suffix>:80` reaches atenet-router directly from both provider
and Orka. No core routing flag or provider-specific transport is required. The
profile's infrastructure ActorTemplate must select exactly one operator-owned
Kubernetes WorkerPool. The provider pins that source pool UID and selector, then
copies its worker image, scheduling, resources, labels and annotations into a
private single-replica WorkerPool for each instance. The admitted Egress policy
exists before the private pool is created. Its Pod template carries private
allocation and instance labels, so each worker is confined from creation.
The provider pins an active, empty, single-Actor worker and its exact Pod UID
before native Resume. It refuses missing or foreign birth labels and never
patches labels onto an already executing worker. Observe rechecks the pool spec,
policy, labels, and recorded worker identity. The admitted policy must explicitly
select Egress alone. Required ingress isolation, ingress rules, and inactive
egress rules are rejected before allocation, including Kubernetes defaults that
require ingress isolation. Namespace-relative Pod peers are qualified with the
frozen runtime namespace when copied to the worker namespace; a missing runtime
namespace rejects such peers. Explicit namespace selectors remain unchanged.
Native worker and router ingress remain operator-owned: firewall management
traffic to authorized ateapi/atenet callers
and permit Orka's runtime routes. Worker ingress policies should select inherited
operator Pod template labels, since each private pool has a fresh name.
Operators must avoid additional policies that widen admitted worker egress and
concurrent mutation of adapter-owned resources. Native
Suspend/Resume/Delete do not have caller UID/version preconditions. The adapter's
ConfigMap CAS serializes its journal and does not add native operation fencing.

Orka supplies the immutable public supervisor request. The provider compiles it
into an immutable ActorTemplate with explicit gVisor isolation and a native
SystemInfo identity projection, resolves downward identity fields, and adapts the
listen address to the native router's port 80. The pinned root overlay requires
a permission repair before the requested command starts. An explicit working
directory is passed as a literal argument to that initialization wrapper and
selected before the exact command and arguments execute. The pinned backend has
no ephemeral-volume primitive. Kubernetes `emptyDir` declarations and mounts are
rejected even without a quota, as is a required read-only root filesystem.
The dedicated durable workspace directory remains supported through the DataOnly
profile. CPU and memory requests require matching admitted limits at least as
large; native scheduling reserves those full limits.
Resources with requests but no limits, unsupported resource names, and requests
exceeding the limit fail before compute is created.
Admitted Pod scheduling constraints, including node selection, affinity,
tolerations, scheduler/runtime classes, priority, topology spread, scheduling
gates/groups, resource claims, Pod resource budgets, and OS selection, are
unsupported. Placement comes only from the exact operator WorkerPool and its
frozen specification. Core omits its generated Pod node selector from new native
intent before admission.
Environment values, commands, and arguments must already be resolved literals.
Kubernetes `$(NAME)` expansion and `$$` escaping are rejected before allocation
rather than copied with different native meaning.
Native active deadlines are unsupported and rejected before allocation. The
sealed-bootstrap nonce must be present once as a nonempty literal before any
compute is created.

The pinned native process starts as UID/GID 0 regardless of image `USER`.
Explicit user/group constraints must match 0, and non-root or privileged execution
requirements are rejected. Admitted capability add/drop lists are translated
without adding capabilities; absent lists use the pinned backend defaults.
Other supplied security settings, including privilege escalation, seccomp,
AppArmor, SELinux, Windows options, proc mounts, filesystem groups, supplemental
groups, and sysctls, fail before allocation. Core publishes explicit UID/GID 0
and omits unsupported security settings from new native-process intent before
admission.

The provider advertises ACP allocation, `runtime.native-process`, verified
data-only suspension, `checkpoint.data`, and `restore.cold`. Pooled capacity, MCP
Service workloads, exec, files, TLS actor endpoints, and full-memory restore remain
unsupported.
The `runtime.native-process` capability lets Core publish intent for a fresh
writable container filesystem with no Kubernetes scratch mounts and require exact
process startup evidence. Core freezes this choice before workload admission;
Pod-backed providers keep their own scratch-volume layout. Existing admitted
requests are never rewritten.
Core publishes Egress-only intent for this capability. Native infrastructure and
router ingress remain the operator's responsibility described above.

A workspace-owned ConfigMap persists bounded creation and retirement intent,
random never-reused Actor names, native UIDs, immutable template digests, exact
worker placement, and checkpoint provenance. A protected workspace marker binds
the journal's exact UID before native effects. Missing or changed journals close
admission and block cleanup. Previous numbered request tombstones remain until
workspace finalization. Native resources in worker namespaces use an exact
namespace-local ownership anchor, which is removed after its network policy.
Journals remain until core finalizes the workspace.

Infrastructure readiness reports the native Actor identity, exact worker Pod,
request sequence, and public sealed-bootstrap challenge hash. The provider
rechecks native Actor version and worker placement after receiving the challenge.
A seeded listener's 404 is accepted only for the already journaled challenge and
unchanged Actor/worker lifetime. Orka repeats the process-bound challenge exchange,
delivers its own credentials, and verifies the authenticated runtime boot.
Attachment acknowledgement precedes startup and never waits for runtime admission.

Every derived template enforces Data/Data/ColdBoot, including templates that do
not permit user suspension. Full process memory restore stays closed. A suspend
profile requires interactive session reuse, an allowed Suspend action, bounded
expiry, and the admitted mount and environment for `/durable/orka-workspace`,
with `ORKA_ACP_DURABLE_WORKSPACE_KEY=shared` so a fresh session continues in the
same retained directory.
A suspended count cap additionally requires maxLifetime and uses a persistent
ConfigMap reservation before capture. The provider drains the exact single-Actor
worker, requests suspension once, verifies a new Data snapshot, copies it to a
private native Tag, and checks source UID/version/template before and after copy.
It deletes the exact private WorkerPool with foreground propagation and waits
for its finalizers and observed absence. It also observes exact worker Pod
termination before deleting the source Actor or reporting suspension. A snapshot
alone is never termination evidence. The operator source pool remains untouched.
A worker replacement between preparation and native scheduling never becomes
startup evidence. Authorized retirement can separately record and drain its
exact UID only after the original Pod is absent and private-pool ownership is
verified; foreign labels or placement block mutation.

Resume verifies the retained Tag, original template and immutable infrastructure,
including the frozen private worker specification. Only generated allocation
and instance labels may rotate; worker image, scheduling, resources, isolation,
and operator labels must remain identical.
It creates a fresh Actor using that original template, then uses native
UpdateActor UID/version CAS to select the newly compiled bootstrap template while
still suspended. It issues cold boot once, checks a new worker and process
challenge, and leaves credential rotation and authenticated admission to Orka.
A lost boot or suspension response is recovered by observation; a possibly
accepted mutation is never replayed. Explicit core Retirement.Suspend is honored
also while desiredState remains Ready during epoch rotation.
Actor creation intent is also journaled before its RPC. An issued create recovers
only by observing the Actor; absence without a recorded UID blocks allocation
and retirement rather than issuing another create.

Deletion requires observed termination and Delete disposition for provider
resources, checkpoints, and storage. Cleanup removes recorded Tags, immutable
templates, network policies, and ownership anchors in order, and records final
disposition before core may release the workspace. Retained in-tree MCP actor
pools remain a migration boundary: the shared workload contract currently has
no MCP service/pool request. Existing resources retire under their original owner.

`ExecutionWorkspaceCheckpoint` exports an existing verified Data artifact from
an idle suspended workspace. `recoverLastCheckpoint` explicitly selects already
verified retained data after failure or quarantine; export never interrupts or
retries uncertain work. A private index pins the exact public checkpoint UID and
selected artifact. Public status exposes only its digest, class binding,
completion time, and readiness. Private catalog owners use Kubernetes CAS to
retain the native Tag and immutable template across source deletion.

An admitted `RestoreFrom` request must match the checkpoint UID and digest,
namespace, class and provider revisions, WorkerPool UID, and durable layout.
Import commits its own workspace reference before native creation, then cold
boots a fresh Actor with the new bootstrap template. A pending public checkpoint
deletion waits for already published workspace references to acquire their own
ownership. Lost acquisition responses recover from the committed private owner,
even after public finalization. The last owner releases and observes exact native
Tag/template deletion; catalog tombstones remain through owner finalization.

Run `cd providers && go test -race ./substrate/...` and `go vet ./substrate/...`.
The suite includes all shared lifecycle, replacement, and suspension conformance
variants against simulated native control operations, plus lost responses,
journal loss, identity replacement, shared-worker rejection, malformed Tag
provenance, closed-bootstrap recovery, Ready-state suspension rollover, checkpoint
ownership transfers, source/public-reference deletion, explicit failed-source
recovery, private catalog loss, and separate selectors in a shared WorkerPool.
Unit tests cover simulated control operations. The installed native proof on
2026-10-05 separately verified real arm64 gVisor filesystem persistence through
Data capture, exact source Actor/worker/public workspace deletion, independent
checkpoint export/import, and a fresh process reading the original marker.
Actual Resume calls verified that admitted egress policies preceded private
worker Pod birth. Actor and worker UIDs and public challenge hashes rotated;
public checkpoint deletion preserved inherited Data, and final owner deletion
collected native Tags and runtime templates. Full-memory restoration remained
disabled.

The [native proof](../../hack/external-substrate-e2e/README.md) invokes the public
reconciler against the installed backend with constructed core admission and a
deterministic process challenge fixture. It proves native Data and ownership
semantics. It does not exercise the deployed provider controller, authenticated
ACP, or RuntimeSession admission. NetworkPolicy objects and exact selection are
verified; kind's default CNI does not prove packet enforcement.
