# fiberd provider spike

This spike asks whether an Orka ACP workspace can run as a
[fiberd](https://github.com/helayoty/fiberd) fiber, and at what cost. It is a
throwaway feasibility probe, not a supported provider. It pins fiberd
`cddb385` (2026-10-14; first run `b3b5b47`) with the local `-gvisor-caps` patch,
and Orka `feat/workspace-out-of-tree` `d5d887e` plus the one-flag Core patch on
`spike/fiberd-provider`.

## Layout

| Path | Purpose |
| --- | --- |
| `providers/fiberd/cmd/orka-fiber-shim` | gVisor template: claims the fiber identity, writes the supervisor identity projection, bridges the endpoint, egress and Core URLs, then starts the supervisor |
| `providers/fiberd/cmd/orka-fiber-egress` | Home-side HTTP CONNECT proxy on each grant's run directory, with a host:port allowlist |
| `providers/fiberd/payload` | The Clone payload: identity, command, arguments and literal environment |
| `providers/fiberd` | Provider forked from the fake provider's reconcile flow, with fibers instead of Pods |
| `hack/fiberd-spike/check` | Stage 1 harness: clones supervisors and reads their sealed bootstrap challenges |
| `hack/fiberd-spike/core-proof`, `stage3.sh`, `core.sh`, `core-install.py` | Stage 3: actual, timed Core Task through the fiberd or Substrate provider |
| `hack/fiberd-spike/substrate.sh` | Substrate provider and fixture Core on the comparison cluster |
| `hack/fiberd-spike/measure.sh`, `measure/footprint.sh` | Timed Task batches, and one instance's memory, for either provider |
| `hack/fiberd-spike/spike.sh` | Driver: builds locally, runs images and a kind cluster on a remote Docker host |

## Run

```sh
hack/fiberd-spike/spike.sh build && hack/fiberd-spike/spike.sh image
hack/fiberd-spike/spike.sh up && hack/fiberd-spike/spike.sh home
hack/fiberd-spike/spike.sh stage1 -mode shared -workspaces 4 -probe 2
hack/fiberd-spike/spike.sh stage2
hack/fiberd-spike/spike.sh stage3
# Substrate comparison; ORKA_E2E_SOURCE is an Orka test/workspace-provider-e2e checkout.
hack/fiberd-spike/spike.sh substrate-up && hack/fiberd-spike/spike.sh substrate
hack/fiberd-spike/spike.sh measure fiberd 4 4 && hack/fiberd-spike/spike.sh measure substrate 4 4
hack/fiberd-spike/spike.sh footprint fiberd && hack/fiberd-spike/spike.sh footprint substrate
```

`SPIKE_HOST` (default `vm`) names the Docker host; nothing is built or loaded
into a local daemon. Substrate runs on its own cluster (`substrate-compare`),
created by Orka's `scripts/workspace-substrate-cluster.sh`.

## Results

Findings, the design review, benchmarks and code review outcomes are in
[docs/fiberd-evaluation.md](../../docs/fiberd-evaluation.md). Each run writes
its raw reports under `results/` in the remote work directory.
