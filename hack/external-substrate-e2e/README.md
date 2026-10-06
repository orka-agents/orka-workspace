# Installed native Substrate Data proof

This fixture runs the standalone provider against the official native backend
on an isolated repository-scoped kind cluster. It verifies a real filesystem
capture, independent checkpoint export, source deletion, and cold import into
a new Actor and process. It uses tag `external-substrate`; the shared workspace
and Sandbox test clusters are separate.

## Native pin and requirements

The backend is an unchanged checkout of
[agent-substrate/substrate](https://github.com/agent-substrate/substrate/tree/fa6d949685a6318940a9a0195c867c864009b820)
at commit `fa6d949685a6318940a9a0195c867c864009b820`, version `v0.1.0`.
Setup invokes its official
[hack/install-ate-kind.sh](https://github.com/agent-substrate/substrate/blob/fa6d949685a6318940a9a0195c867c864009b820/hack/install-ate-kind.sh)
with `--deploy-ate-system`. Its bundled tool module pins `ko v0.19.1`.

This fixture builds Linux arm64 images with Go 1.27 and uses Kubernetes
`v1.37.0`. Docker must support arm64 containers and have sufficient CPU, memory,
and disk space for the native control plane and gVisor workers. The pinned
native arm64 gVisor archive is
`gs://gvisor/releases/nightly/2026-09-02/aarch64/gvisor.tar.zstd`, SHA-256
`a64916f9813ce7e4841a30480a599337f7dda07b421c6bf0123db2212aa7d1df`.
The backend downloads gVisor directly; this path does not use a Kubernetes
RuntimeClass or require KVM.

[cluster.yaml](cluster.yaml) enables the backend's certificate API feature
gates. Setup enables `proxy_arp` only on the dedicated cluster's node and creates
the separately labeled `orka-external-substrate-registry` on `127.0.0.1:5002`.
It uses kindctl's scoped kubeconfig and does not invoke the upstream cluster
creator or replace a shared registry.
The upstream DNS controller configures GKE kube-dns automatically. The fixture
adds its [documented CoreDNS stub integration](https://github.com/agent-substrate/substrate/blob/fa6d949685a6318940a9a0195c867c864009b820/cmd/atenet/internal/dns/README.md)
on this kind cluster, with exact ConfigMap UID/version tests and preservation
of existing Corefile rules. It waits for both CoreDNS replicas to run the new
Corefile before native execution. Public read verification retries DNS lookup
failures only; native operations and identity mismatches are never replayed.

## Run

Run every command from this repository. Set `KINDCTL_BIN` to your kindctl
executable and `SUBSTRATE_SOURCE_DIR` to a clean native checkout if they differ
from the script's defaults.

```sh
git clone --filter=blob:none --no-checkout https://github.com/agent-substrate/substrate.git /tmp/orka-external-substrate-native-pin-fa6d949685a6318940a9a0195c867c864009b820
git -C /tmp/orka-external-substrate-native-pin-fa6d949685a6318940a9a0195c867c864009b820 checkout --detach fa6d949685a6318940a9a0195c867c864009b820
bash scripts/external-substrate-e2e.sh preflight
bash scripts/external-substrate-e2e.sh install
bash scripts/external-substrate-e2e.sh proof
```

Skip cloning when an existing checkout passes the origin, exact commit, and
clean-tree checks. An existing dedicated cluster requires
`SUBSTRATE_REUSE_CLUSTER=1` for `install`. Standard `GOCACHE` and `GOMODCACHE`
environment overrides are inherited by all builds and the native bundled tool.
Use separate cache directories when diagnosing host cache damage.

The proof creates a fresh namespace per run and installs shared/provider CRDs
on this dedicated backend cluster. It constructs admitted public workload
requests and invokes the real public reconciler from an in-cluster Job with
native Pod certificates. It does not install the Orka core or other providers.
Its private WorkerPools inherit the operator's native worker image and Pod
template. Before each actual native Resume RPC, an independent check requires
one private worker, birth-template identity labels, an admitted egress policy
that predates Pod creation, and unchanged source WorkerPool UID and spec.

Results are written to `bin/external-substrate-proof-<UTC timestamp>/proof.log`
and `report.json`. The latest namespace is recorded in
`bin/external-substrate-proof-namespace`. For inspection, use:

```sh
"$KINDCTL_BIN" kubectl --tag external-substrate get nodes -o wide
"$KINDCTL_BIN" kubectl --tag external-substrate -n "$(cat bin/external-substrate-proof-namespace)" get pods,workerpools,executionworkspaces,executionworkspacecheckpoints
```

Failed Jobs are not retried. Their scoped resources remain available for exact
identity and journal inspection. A new `proof` invocation uses a fresh
namespace. Successful runs retire their tested allocations and verify native
Tag/template collection; the installation and namespace remain inspectable.

## Assertions and limits

The installed run on 2026-10-05 passed all assertions below. Its report and log
are retained at `bin/external-substrate-proof-20261006010553/`. The report
records exact process and worker identities, challenge hashes, checkpoint
UID/digest, runtime/proof/worker image digests, and the native commit.

The runtime writes a random marker only in the original process. Its imported
incarnation cannot initialize that marker. The proof checks:

- Real Data capture completes before source Actor and exact worker Pod removal.
- An independent public checkpoint retains Data after source workspace deletion.
- Import by exact checkpoint UID and digest starts a fresh Actor and worker Pod
  and reads the original marker.
- Direct public challenge hash, Actor UID/version, exact worker UID, boot nonce,
  bootstrap nonce, and public key match the observed process; incarnation fields
  rotate across import.
- The inheriting workspace acquires durable ownership before boot. Removing the
  public checkpoint preserves inherited Data, while deleting the final owner
  collects private native artifacts.
- Native templates retain Data/Data/ColdBoot settings throughout; full-memory
  restoration remains disabled.

The deterministic runtime exercises native gVisor filesystem persistence and
the public process challenge protocol. It does not claim authenticated ACP
execution. Core admission is a constructed fixture; the core integration proof
covers that boundary separately. NetworkPolicy objects and exact preboot worker
selection are checked, but kind's default CNI does not prove packet enforcement.
An enforcing production CNI and operator management-plane ingress rules remain
deployment requirements.
