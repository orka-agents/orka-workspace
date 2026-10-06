# External workspace local proof

Run from the repository root:

```sh
scripts/external-workspace-e2e.sh provider
```

The script creates or reuses this repository's `kindctl` cluster with tag
`external-workspace`. Every cluster command uses that tag and a scoped kubeconfig.
It preserves the cluster, frozen build source, images, and JSON proof report.
It does not alter the global kubeconfig or delete existing clusters.

The provider phase builds the separate fake controller, deploys two replicas,
and checks the shared leader-election Lease. Distinct ServiceAccounts prove
ownership admission accepts the selected provider and core writes while rejecting
ordinary HTTP writers lacking the virtual verb, foreign provider status, forged
core admission, attachment changes, controller routing changes, core condition
writes, and core finalizer removal. A running public listener Pod proves exact UID and
workload-revision reporting before credentials exist. Deletion waits for matching
core retirement authorization and verified physical absence.

This phase uses a fixture core identity to write the shared handoff. It does not
claim Orka runtime admission or RuntimeSession execution.

After the Orka implementation is released for building, prepare the real harness
v2 supervisor with the deterministic ACP stdio agent:

```sh
ORKA_CORE_BUILD_RELEASED=1 \
ORKA_CORE_SOURCE=/path/to/orka-worktree \
scripts/external-workspace-e2e.sh prepare-runtime
```

The fixture packaging follows Orka's existing `security-scan-e2e.sh` runtime:
the actual `orka-acp-runtime` and `orka-acp-exec-helper` binaries run a local
deterministic ACP agent in place of `node`. Bootstrap, authentication, process
isolation, RuntimeSession control, and fences remain the production supervisor's
responsibility. The agent supports initialize, authenticate, session creation,
loading, prompting, and cancellation without contacting a model or repository.
Its terminal response is `External workspace fixture completed.`

The prepared image is loaded into the same isolated cluster. This mode submits
no Tasks or RuntimeSessions. Run the complete proof after the core build is ready:

```sh
ORKA_CORE_BUILD_RELEASED=1 \
ORKA_CORE_SOURCE=/path/to/orka-worktree \
scripts/external-workspace-e2e.sh core
```

The core phase builds a frozen Orka controller, installs its CRDs and a local
self-signed admission certificate, and enables fail-closed class, provenance,
execution-authority, Agent, and attachment-Secret webhooks. The controller's
release manager role is bound in this isolated cluster. Fixture grants add fake
parameter reads and leader-election leases. The in-process fake adapter remains
disabled; the separate two-replica provider creates the runtime Pod.

The proof submits an Interactive class requiring `acp.runtime.v2` and a real
agent Task. It observes authenticated Serving and the actual Pod UID, session
UID, supervisor boot, profile digest, and prompt identity. Successful settlement
must revoke attachment token Secrets and retire the exact runtime Pod. JSON
reports contain only public identities. Generated credentials stay in a private
artifact directory and are never printed.

NetworkPolicy objects selecting the runtime Pod are checked. Packet enforcement
is not proved with kind's default CNI. The deterministic agent never contacts a
provider; its configured provider Service has no endpoints, so an unexpected
provider request fails locally.
