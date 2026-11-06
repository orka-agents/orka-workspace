# Installed native Substrate Data proof

This fixture runs the standalone provider against the official native backend
on an isolated repository-scoped kind cluster. It verifies a real filesystem
capture, independent checkpoint export, source deletion, and cold import into
a new Actor and process. It uses tag `external-substrate`; the shared workspace
and Sandbox test clusters are separate.

## Native pin and requirements

The backend is an unchanged checkout of
[agent-substrate/substrate](https://github.com/agent-substrate/substrate/tree/b34fbee742906494b5eb4f1f36987176bf8770a4)
at commit `b34fbee742906494b5eb4f1f36987176bf8770a4`, version `v1.0.0`
(`providers/substrate/nativepb/upstream.env`). Setup invokes its official
installer, `go run ./cmd/ate-setup deploy ate-system` with the kind profile
(`ATE_INSTALL_KIND=true`), `--sandbox-classes=gvisor` and egress credential
injection disabled. It builds every component from the checkout with `ko`.
Agent Substrate cannot upgrade a v0.1.0 install in place (upstream
`docs/guides/upgrade.md`): retire a cluster that ran the previous pin and use a
fresh dedicated cluster.

This fixture builds Linux images for the node architecture (amd64 or arm64)
with Go 1.27 and uses Kubernetes `v1.37.0`. Docker must have sufficient CPU,
memory, and disk space for the native control plane and gVisor workers. The
gVisor worker image is `cmd/ateworker-gvisor` from the pinned checkout; this
path does not use a Kubernetes RuntimeClass or require KVM.

[cluster.yaml](cluster.yaml) enables the backend's certificate API feature
gates. Setup enables `proxy_arp`/`proxy_ndp` only on the dedicated cluster's
nodes and creates the separately labeled `orka-external-substrate-registry` on
`127.0.0.1:5002`, which it passes as `KO_DOCKER_REPO`. It uses kindctl's scoped
kubeconfig and never invokes the upstream cluster creator, uses the shared
`kind-registry`, or replaces a shared registry.

Authorization is always enforced. Setup makes only the two proof client
identities bootstrap owners:
`spiffe://cluster.local/ns/external-substrate-e2e/sa/native-data-proof` and
`.../sa/native-core-proof`. Proof Jobs therefore run in the fixed
`external-substrate-e2e` namespace and operate on the run namespace. The
router (`--ingress-allowed-spiffe-ids`) admits those identities and the
deployed provider's
`spiffe://cluster.local/ns/orka-workspace-system/sa/orka-workspace-substrate`,
plus the Core proof's Orka manager,
`spiffe://cluster.local/ns/external-substrate-core/sa/orka-controller-manager`.
That manager runs in the fixed `external-substrate-core` namespace with a
projected pod certificate and routes sealed bootstrap, probes, and drain through
atenet-router; a retained earlier run must be reviewed and removed first.
`SUBSTRATE_E2E_EXTRA_INGRESS_SPIFFE_IDS` appends more identities. The Core proof grants the deployed provider only `editor`
on the run Atespace and global `viewer`.

No DNS or installation setting is patched. Actor ingress goes through
`atenet-router` with the `ate-target-actor` header, and all Actor egress goes
through the installed egress gateway under each Actor's `EgressPolicy`. Routed
reads retry only while the router route becomes available; native operations
and identity mismatches are never replayed.

## Run

Run every command from this repository. Scripts use `kindctl` from `PATH` by
default; set `KINDCTL_BIN=/path/to/kindctl` to choose another executable. Set
`SUBSTRATE_SOURCE_DIR` to a clean native checkout if it differs from the default.

```sh
pin=b34fbee742906494b5eb4f1f36987176bf8770a4
git clone --filter=blob:none --no-checkout https://github.com/agent-substrate/substrate.git /tmp/orka-external-substrate-native-pin-${pin}
git -C /tmp/orka-external-substrate-native-pin-${pin} checkout --detach ${pin}
bash scripts/external-substrate-e2e.sh preflight
bash scripts/external-substrate-e2e.sh install
bash scripts/external-substrate-e2e.sh proof
```

Skip cloning when an existing checkout passes the origin, exact commit, and
clean-tree checks. An existing dedicated cluster requires
`SUBSTRATE_REUSE_CLUSTER=1` for `install`. Standard `GOCACHE` and `GOMODCACHE`
environment overrides are inherited by all builds and the native installer.
Use separate cache directories when diagnosing host cache damage.

The proof creates a fresh namespace per run and installs shared/provider CRDs
on this dedicated backend cluster. It constructs admitted public workload
requests (with an empty, deny-all egress allowlist) and invokes the real public
reconciler from an in-cluster Job with native Pod certificates. It does not
install the Orka core or other providers. The operator source WorkerPool runs
no workers; private WorkerPools inherit its worker image and gVisor class, and
the provider sizes them for exactly one Actor. Before each actual native Resume
RPC, an independent check requires one private gVisor worker sized for one
Actor, birth-template identity labels, a data-only golden-disabled template, no
snapshot on a fresh Actor, the admitted egress allowlist, and unchanged source
WorkerPool UID and spec.

Results are written to `bin/external-substrate-proof-<UTC timestamp>/proof.log`
and `report.json`. The latest namespace is recorded in
`bin/external-substrate-proof-namespace`. For inspection, use:

```sh
"${KINDCTL_BIN:-kindctl}" kubectl --tag external-substrate get nodes -o wide
"${KINDCTL_BIN:-kindctl}" kubectl --tag external-substrate -n "$(cat bin/external-substrate-proof-namespace)" get pods,workerpools,executionworkspaces,executionworkspacecheckpoints
"${KINDCTL_BIN:-kindctl}" kubectl --tag external-substrate -n external-substrate-e2e get jobs
```

Failed Jobs are not retried. Their scoped resources remain available for exact
identity and journal inspection. A new `proof` invocation uses a fresh
namespace. Successful runs retire their tested allocations and verify native
Tag/template collection; the installation and namespace remain inspectable.

## Assertions and limits

The installed run against v1.0.0 on 2026-11-06 (amd64, dedicated kindctl
cluster) passed all assertions below. Its report and log are retained at
`bin/external-substrate-proof-20261106065356/`, recording exact process and
worker identities, router targets, challenge hashes, checkpoint UID/digest,
runtime/proof/worker image digests, and the native commit. The earlier v0.1.0
run is retained at `bin/external-substrate-proof-20261006010553/`.

The runtime writes a random marker only in the original process. Its imported
incarnation cannot initialize that marker. The proof checks:

- Real Data capture (`VOLUMES` fidelity, synchronous durability) completes
  before source Actor and exact worker Pod removal.
- An independent public checkpoint retains Data after source workspace deletion.
- Import by exact checkpoint UID and digest starts a fresh Actor and worker Pod
  and reads the original marker.
- The routed public challenge hash, Actor UID/version, exact worker UID and
  epoch, boot nonce, bootstrap nonce, and public key match the observed process;
  incarnation fields rotate across import.
- The inheriting workspace acquires durable ownership before boot. Removing the
  public checkpoint preserves inherited Data, while deleting the final owner
  collects private native artifacts.
- Native templates keep `VOLUMES` snapshots with golden snapshots disabled
  throughout; full-memory restoration remains disabled.

The deterministic runtime exercises native gVisor filesystem persistence and
the public process challenge protocol. It does not claim authenticated ACP
execution. Core admission is a constructed fixture; the core integration proof
covers that boundary separately. Egress enforcement is the installed egress
gateway's; this proof checks that the admitted allowlist was applied before
Resume, not packet-level behavior.

## Actual Core and deployed provider Task proof

The separate `core/` fixture uses the installed native backend, a deployed
standalone Substrate provider, the actual Orka controller and supervisor, and
the existing deterministic ACP agent. Its in-cluster client creates a typed
profile, class, Agent, and Task through their public APIs. It never invokes
provider lifecycle methods or writes workspace admission/status itself.

```sh
bash scripts/external-substrate-core-e2e.sh preflight
ORKA_CORE_BUILD_RELEASED=1 \
  ORKA_CORE_SOURCE=/path/to/orka \
  SUBSTRATE_WORKER_IMAGE=<pinned cmd/ateworker-gvisor image digest> \
  bash scripts/external-substrate-core-e2e.sh proof
```

This proof requires an Orka core that admits `RuntimeWorkload.Egress` (including
its provider-authentication proxy origin), consumes `NativeProcessEvidence.RouterTarget`,
and reaches the runtime through `atenet-router` with an allowlisted identity.

`KINDCTL_BIN`, `GOCACHE`, and `GOMODCACHE` overrides are inherited. The proof
requires the dedicated installation and registry described above. It freezes
both source trees before compilation and records source hashes, commit IDs, a
separate fixture hash, and built image digests in
`bin/external-substrate-core-<UTC timestamp>/`. Preflight requires at least four
CPU and 8 GiB of allocatable node memory and serving `ate-api-server`,
`ate-controller`, `atenet-router` and `atenet-egress`. Existing Core/provider
deployments cause a refusal so a later run cannot replace an earlier accepted
Task implicitly.

The provider keeps its shipped transport: a projected ServiceAccount token for
ate-api, with only the access policies above, and a rotating Pod certificate
for the router. The Task client uses a separate ServiceAccount with class-use
permission and passes the real fail-closed class, provenance, and authority
admission. A passing report requires current Core admission and provider
acknowledgement, exact native Actor UID/version and worker Pod, a private gVisor
worker sized for one Actor under the recorded epoch, the applied egress
allowlist, a routed startup endpoint with the exact Actor target, authenticated
Serving on the native supervisor port 80, an observed RuntimeSession UID and
prompt result, and exact Actor/private-pool/worker retirement. Terminal evidence
requires either public workspace absence or its exact UID, current generation,
saved allocation fence, and validated interactive cleanup disposition. The exact
Core RuntimePool must also be absent, with zero remaining attachment and pool
auth/provider credential Secrets.

Core consumes the one-time sealed challenge during credential delivery. The
client independently checks it through the router when still available and
reports whether that read preceded closure. A closed listener is accepted only
when the actual Core subsequently proves authenticated Serving for the recorded
exact instance and supervisor boot. Full-memory restoration stays gated. This
Task does not test checkpoint persistence; the Data fixture above proves that
path. Failures retain their accepted Task and evidence for inspection and are
not replayed; normal controllers may retire allocation resources.

The last installed run, `bin/external-substrate-core-20261006022705/report.json`,
was against v0.1.0 with Core commit `2599695ae06f5213d0411721347865888a0eff97`
and provider commit `63f33e3dd3e4ce30d1066a99f0ea8bd184bd44a1`.
