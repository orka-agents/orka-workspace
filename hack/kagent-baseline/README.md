# kagent latency baseline

A measured kagent baseline for the Orka Substrate latency work, on the same
host: kagent `v1.0.1` with its Agent Substrate fork
(`github.com/kagent-dev/substrate`) `v1.0.0`, the same Substrate release Orka's
provider targets. One shared WorkerPool hosts one Actor per Session; kagent
suspends the Actor to a DATA snapshot after each turn and resumes it onto the
warm worker. A deterministic in-cluster OpenAI-compatible mock (`mockllm/`)
sleeps 2s and returns fixed text, like the Orka proof's deterministic ACP agent.
No external model or API is used, and its key is a placeholder.

## Install

Only public repositories, charts and images are used. Never run kagent's or
Substrate's kind scripts: they create or reuse the host's shared
`kind-registry`.

```sh
kindctl create --tag kagent-baseline --config hack/kagent-baseline/cluster.yaml --k8s-version v1.37.0
# proxy ARP/NDP on this cluster's node only, as Substrate's kind profile does
docker exec <node> sysctl -w net.ipv4.conf.all.proxy_arp=1
docker exec <node> sysctl -e -w net.ipv6.conf.all.proxy_ndp=1
curl -fsSL -o bin/kagent/kagent https://github.com/kagent-dev/kagent/releases/download/v1.0.1/kagent-linux-amd64   # check kagent-linux-amd64.sha256
kindctl exec --tag kagent-baseline -- sh -c 'OPENAI_API_KEY=placeholder-not-a-real-key XDG_CONFIG_HOME=$PWD/bin/kagent/xdg bin/kagent/kagent install --provider OpenAI'
```

`kagent install` installs the substrate CRDs, podcert, atelet and Substrate
charts (`oci://ghcr.io/kagent-dev/substrate/helm/*` `1.0.0`), the bundled
PostgreSQL, and the kagent charts (`oci://ghcr.io/kagent-dev/kagent/helm/*`
`1.0.1`) with a `kagent-default` WorkerPool (`ateworker-gvisor:v1.0.0`) and
Harness.

Then build and load the one local image (mock and Actor watcher) and apply the
baseline objects:

```sh
(cd hack/kagent-baseline && CGO_ENABLED=0 go build -o ../../bin/kagent/image/mockllm ./mockllm && \
  CGO_ENABLED=0 go build -o ../../bin/kagent/image/atewatch ./atewatch)
docker build -f hack/kagent-baseline/Dockerfile -t ai.orka.local/kagent-baseline-mock:dev bin/kagent/image
kindctl load --tag kagent-baseline ai.orka.local/kagent-baseline-mock:dev
kindctl kubectl --tag kagent-baseline apply -f hack/kagent-baseline/manifests.yaml
```

`manifests.yaml` holds the mock Deployment and Service, a `mock-llm`
ModelConfig (OpenAI chat completions at the mock), the `lat-baseline` Agent on
the `kagent-default` Harness, and `atewatch`: a read-only Pod that polls the
native ate-api every 100ms as kagent's bootstrap-owner client
(`ate-system/ate-client`) and prints every Actor state change. A
NetworkPolicy admits only that Pod to ate-api.

## Measure

```sh
KINDCTL_BIN=<kindctl> bash hack/kagent-baseline/run.sh 5
```

For each repetition: a cold first turn (Session create, then its first turn),
a follow-up once the Actor is `SUSPENDED`, and an immediate follow-up sent as
soon as the previous turn returned (recording whether kagent's quiescence
suspend was superseded). "Created" is the client clock just before the CLI
call; "prompt accepted" is the mock's server-side receipt of the turn's unique
marker; "done" is when the blocking invoke returns the completed task.
Results go to `bin/kagent-baseline-<UTC>/` (`samples.json`, `summary.md`,
`loadavg.log`).

The comparable Orka timestamp is the deterministic ACP agent's own
`session/prompt` receipt, which it reports in its reply and the Orka latency
harness reads back through Core's API (`clientAgentPromptReceivedMs`).
