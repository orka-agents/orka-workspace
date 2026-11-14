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
(`ATE_INSTALL_KIND=true`), `--sandbox-classes=gvisor` and the bundled
Kubernetes Secrets egress credential provider
(`--credential-provider='{"name":"k8s.io"}'`). Its namespace policy is
installed empty and grants nothing; only the Core proof grants its own run (see
below). It builds every component from the checkout with `ko`.
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

After the installer, setup sets one router environment variable,
`OTEL_TRACES_SAMPLER=parentbased_always_on` on `ate-system/atenet-router`, and
waits for its rollout. This is a test-only setting: the v1.0.0 router gives
Envoy only a root `RandomSampling` ratio (1% by default) that replaces the
caller's sampling decision, so a sampled Orka trace reaches the Actor
unsampled and the ACP supervisor, which samples by parent, exports no spans.
`parentbased_always_on` maps to a ratio of 1 (upstream
`internal/serverboot/sampling.go`). Operators need the same setting for
end-to-end supervisor traces. Reapplying the unchanged value with
`SUBSTRATE_REUSE_CLUSTER=1` does not roll the router.

No DNS or other installation setting is patched. Actor ingress goes through
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
clean-tree checks. With `SUBSTRATE_CLONE=1` the scripts clone the pin
themselves when the checkout directory is absent; an existing directory is
never modified and must pass the same checks. `preflight-host` runs only the
host checks (tools, Docker, disk, native checkout), for a runner that has not
created the cluster yet; `preflight` also requires the dedicated cluster.
`diagnostics` writes redacted evidence to `bin/diagnostics-<UTC timestamp>/`;
the Data and Core proofs also write it to `<run dir>/diagnostics/` when they
fail. It holds the status (never the spec) of Tasks, RuntimePools,
ExecutionWorkspaces, classes, checkpoints and WorkerPools, Pod listings,
events, and the logs of ate-system, the provider, Core, the fixture proxy and
the proof Jobs. It never reads Secrets or Pod specs, never writes under
`private/`, and redacts bearer values, JWTs, PEM blocks and the fixture token.

`.github/workflows/substrate-live-e2e.yml` runs `preflight-host`, `install`,
the Data proof and the Core proof on a GitHub-hosted runner with
`SUBSTRATE_CLONE=1`, the kindctl vendored in the checked-out Orka core, and
that checkout as `ORKA_CORE_SOURCE` (`orka-ref` input, default `main`). It
runs weekly, on demand and on pull requests touching the providers, shared
APIs, these fixtures or scripts, and uploads `bin/` reports, logs and
diagnostics without `private/`, source snapshots or image contexts. Both
checkouts keep their repository basenames: kindctl derives the cluster name
from the directory, and a basename longer than 25 characters makes the
control-plane node name exceed 63 characters, which kind cannot register. An existing dedicated cluster requires
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
  bash scripts/external-substrate-core-e2e.sh proof
```

`SUBSTRATE_WORKER_IMAGE` defaults to the `workerImage` of the latest Data
report under `bin/` (the pinned `cmd/ateworker-gvisor` digest the Data proof
built on this installation); set it to override. It must be a
`localhost:5002/ateworker-gvisor-*@sha256:` reference.

This proof requires an Orka core that admits `RuntimeWorkload.Egress` (including
its provider-authentication proxy origin), consumes `NativeProcessEvidence.RouterTarget`,
reaches the runtime through `atenet-router` with an allowlisted identity,
injects the provider-proxy token at the egress gateway (Orka ADR 0045), drains
and suspends on worker eviction (ADR 0046), exports Substrate-routed ACP
traces (Orka #1061), and fits supervisor telemetry into the native 32-entry
environment limit, reporting it on the RuntimePool `TelemetryReady` condition.

`KINDCTL_BIN`, `GOCACHE`, and `GOMODCACHE` overrides are inherited. The proof
requires the dedicated installation and registry described above. It freezes
both source trees before compilation and records source hashes, commit IDs, a
separate fixture hash, and built image digests in
`bin/external-substrate-core-<UTC timestamp>/`. Preflight requires at least four
CPU (a GitHub-hosted runner has exactly four) and 8 GiB of allocatable node
memory and serving `ate-api-server`, `ate-controller`, `atenet-router`,
`atenet-egress` and `k8s-credential-provider`, plus the installer's
`otel-system` `opentelemetry-collector` and `jaeger` (see Tracing). Existing Core/provider
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
exact instance and supervisor boot. Full-memory restoration stays gated.
Failures retain their accepted Task and evidence for inspection and are not
replayed; normal controllers may retire allocation resources.

The run has two in-cluster Jobs, `external-substrate-core` (the single Task)
and `external-substrate-core-session` (the Session), started in that order
against the same Core, both with telemetry. The second starts only after the
first passed; traces are verified after both.

### Egress credential injection

`run.sh` grants the run Atespace (`external-substrate-core`, the profile
`templateRef` namespace) only Core's provider Secrets labeled
`orka.ai/runtime-pool-provider-credential: "true"` in its own namespace, in the
`ate-system/k8s-credential-provider-namespace-policy` ConfigMap, and restarts
the credential provider. The rendered provider Deployment adds
`--egress-credential-provider=k8s.io`; Core keeps its provider Secret in the
workload namespace, so no `--egress-credential-namespace` is needed. The
shipped `providers/substrate/config/deployment.yaml` keeps injection off.

The listener-less provider-proxy Service is replaced by a fixture origin
(`core/providerproxy`). It accepts a request only when `Authorization` is
`Bearer ` plus Core's provider token, which it reads from the same fixture
Secret Core does, and reports only counts per class (injected, Orka's public
placeholder, missing, other) and the agent's session report on port 8081,
which Core does not admit to runtime egress. The deterministic agent makes a
model request only when its prompt carries `orka-fixture:provider-probe`: one
Responses call through the supervisor's session provider proxy, which forwards
it upstream with the bootstrap placeholder bearer. The proof requires:

- Core's workload request carries exactly one `replaceHeaders` rule: HTTP, the
  provider-proxy host and port 8080, `Authorization`, prefix `Bearer `, and the
  `token` key of a RuntimePool-owned, labeled Secret in the run namespace; the
  request requires `runtime.egress-credential-injection`.
- The applied native `EgressPolicy` has exactly one rule with effects, an
  `http` rule for that host and port replacing `Authorization` with prefix
  `Bearer ` and `credentialUri`
  `ate-secret://k8s.io/default/<namespace>/<secret>/token`.
- Packet level: the origin received the prompt's request with Core's token and
  never a placeholder, missing or other bearer. The runtime never held the
  token, so only the gateway can have injected it.

### Tracing

Core runs with `--enable-telemetry`, `--runtime-telemetry-collector-egress`
and only
`OTEL_EXPORTER_OTLP_ENDPOINT=http://orka-otel-collector.<namespace>.svc.cluster.local:4317`
(gRPC), the smallest telemetry configuration. Core fits supervisor telemetry
into the 32 environment entries Substrate admits per container and reports the
outcome on the RuntimePool `TelemetryReady` condition. Every Core RuntimePool
(the single Task's and each Session turn's suspendable one) must report
`TelemetryReady=True/TelemetryProjected`; `False/NativeEnvironmentLimit` fails
the proof as soon as it is observed.

An `otel/opentelemetry-collector:0.111.0` (digest-pinned, mirrored into the
dedicated registry) logs spans with the debug exporter. `core/traces.py`
requires, for the single Task and for at least one Session turn, one trace in
which:

- Core's `acp.prompt` has a `substrate.router PUT` child: the routed, streamed
  ACP prompt request;
- the supervisor's `acp.supervisor.prompt` (`service.name=orka-acp-supervisor`,
  `orka.task.uid` of that Task) is a child of that request, either directly or
  through the `atenet-router-envoy` `ingress` span that is the request's
  child;
- `invoke_agent codex` is a child of `acp.supervisor.prompt` and
  `chat gpt-5.4` (the probe's model call) a child of `invoke_agent codex`.

With root sampling on, atenet-router's Envoy records an `ingress` span
between Core and the Actor and reports it to the installer's own trace
backend (`ate-system/ate-otel-config` points it at
`otel-system/opentelemetry-collector`, which feeds `otel-system/jaeger`), not
to the proof's collector. `run.sh` reads each candidate trace from that Jaeger
through the API server proxy, read-only, into `private/router-traces/`;
`traces.py` reads only `atenet-router-envoy` spans from it. Neither the
collector log nor those traces may contain the provider token or prompt text
(the fixture markers). The result is `traces.json` (span names, IDs and
services only) and the report's `tracing`.

### Session continuation across a worker eviction

The Session runs on the same Core and store as the single Task, with
telemetry: a suspendable native supervisor also uses two durable-workspace
entries (`ORKA_ACP_DURABLE_WORKSPACE_DIR` and `..._KEY`), and Core still fits
its supervisor telemetry (see Tracing). The Session Job:

1. Creates a `SuspendMode: DataOnly` profile and a class with reuse scope
   `Session`, `Suspend` (default) and `Delete` detach actions, a two-minute
   detach timeout and a one-hour maximum lifetime, and waits for `Ready`.
2. Turn 1 (`sessionRef.create`, `onDetach: Suspend`): the prompt makes the
   provider probe and then holds for 60 seconds
   (`orka-fixture:hold-seconds=60`). Once the prompt is accepted and its
   injected request arrived, the proof deletes the exact attested ateworker
   Pod with a UID precondition (Substrate gives workers a 3600-second grace
   period, so Core's eviction window is 30 minutes).
3. It requires the RuntimePool's `orka.ai/external-runtime-eviction` record to
   bind the workspace UID and the instance's allocation and instance IDs as
   suspendable; `WorkerEviction=True/EvictionDraining` with
   `AdmissionReady=False/AdmissionClosed` and lifecycle `Draining`; then
   `WorkerEviction=False/EvictionSuspended`; the turn `Succeeded` with a
   persisted result (the held prompt settled); the workspace `Suspended` with
   a `Stopped` allocation and retained data; and the evicted Actor, worker Pod
   and private WorkerPool gone. `EvictionDeadlineExceeded` or
   `EvictionNotSuspendable` fails the proof.
4. Turn 2 (`onDetach: Delete`) attaches the same workspace UID. Its workload
   fences the evicted instance (`previousInstance`) and resumes exactly the
   retained data reference; it runs on a new Actor UID, worker Pod, private
   pool and supervisor boot. The supervisor requires a committed durable
   checkpoint on resume (`expectDurableResume`), so success proves the
   workspace data survived. The agent's provider reports must be
   `mode=new;session=<rollout thread>` for turn 1 and
   `mode=resume;session=<same thread>` for turn 2: the agent accepts
   `session/resume` only when the Codex rollout Orka captured was restored.
   No process memory is restored (the template stays data-only).
5. Turn 2's `Delete` detach retires the workspace, native Actor, private pool,
   worker, Core RuntimePool and credential Secrets, with the same checks as the
   single Task.

### Limits

- Turn 1's supervisor is evicted while its prompt is held, and its prompt
  span is not always received: in one of the two 2026-11-14 runs it exported
  `acp.supervisor.session.create`, `acp.supervisor.prompt.lease` and
  `acp.supervisor.drain` but not `acp.supervisor.prompt` or its GenAI
  children. The proof therefore requires at least one complete Session turn
  and records both.
- The Envoy hop is read from the installer's Jaeger, which keeps traces in
  memory: a Jaeger restart between the run and the check loses it and fails
  the tracing assertion.
- Eviction is triggered by deleting the attested ateworker Pod. A node drain
  through the Eviction API and the `DisruptionTarget` and supervisor `SIGTERM`
  signal paths are not exercised separately, nor is
  `EvictionDeadlineExceeded` (the window is 30 minutes).
- Data survival is shown by the supervisor's required committed durable
  checkpoint and the restored Codex rollout, not by a separate marker file:
  read-intent Codex sessions cannot write the workspace.
- kind's default CNI does not enforce NetworkPolicy, and egress enforcement is
  the gateway's. The packet-level check covers one HTTP origin only.

### Last run

2026-11-14, amd64, a fresh dedicated kindctl cluster (16 CPU), Agent Substrate
v1.0.0 `b34fbee742906494b5eb4f1f36987176bf8770a4`, Orka core
`a11e5a267b3705293dfbdfb86380018922ebc12b` (a local branch merging two
unmerged Orka changes onto `main`: the router root-sampling e2e fix
`131fa4ba6` and the native telemetry environment fit `b66717282`), and this
repository at `1060ca0`. Install took 130 s, the Data proof 82 s
(`bin/external-substrate-proof-20261114215643/report.json`, all checks
passed), and the Core proof 216 s,
`bin/external-substrate-core-20261114215807/report.json`, with every check
passing:

- The single Task passed every check, including credential injection (one
  injected request; no placeholder, missing or other bearer). Its RuntimePool
  reported `TelemetryReady=True/TelemetryProjected`.
- Tracing passed for the single Task and both Session turns: each trace runs
  `acp.prompt` → `substrate.router PUT` → `atenet-router-envoy ingress` →
  `acp.supervisor.prompt` → `invoke_agent codex` → `chat gpt-5.4`. The
  supervisor also exported `gen_ai.client.operation.duration` and
  `gen_ai.invoke_agent.duration`. Neither the collector nor the router traces
  held the provider token or prompt text.
- The Session passed with telemetry on the same Core: the suspendable
  RuntimePool reported `TelemetryReady=True/TelemetryProjected` for both
  turns. The eviction record bound the exact instance (`source:
  WorkerTermination`, a 30-minute deadline, outcome `EvictionSuspended`);
  `EvictionDraining`, `AdmissionClosed` and `Draining` were observed, then
  `EvictionSuspended`. Turn 2 cold-resumed on a new Actor, worker Pod,
  private pool and supervisor boot with the agent reporting `mode=resume`
  for the rollout thread, and retirement was complete.

An earlier run the same day on the same sources (before the trace assertion
followed the router hop) is `bin/external-substrate-core-20261114214415/`.
It passed every other check; its supervisor spans arrived, but turn 1's
prompt span did not (see Limits).

The previous installed run, `bin/external-substrate-core-20261006022705/report.json`,
was against v0.1.0 with Core commit `2599695ae06f5213d0411721347865888a0eff97`
and provider commit `63f33e3dd3e4ce30d1066a99f0ea8bd184bd44a1`.
