# Architecture decisions

These records describe the extraction from Orka commit [`1cd16c88b43410e0ea46463f8a39b473d8e5250e`](https://github.com/orka-agents/orka/commit/1cd16c88b43410e0ea46463f8a39b473d8e5250e), inspected on October 2, 2026.

| Record | Decision |
| --- | --- |
| [0001](0001-ownership.md) | Shared fields and their writers |
| [0002](0002-startup-and-retirement.md) | Workload handoff, startup evidence, and cleanup order |
| [0003](0003-configuration-and-trust.md) | Typed configuration, virtual verbs, and operation journals |
| [0004](0004-compatibility.md) | Independent versions and supported combinations |

These records preserve the baseline decisions. See [implementation status](../implementation-status.md) for the completed workload handoff, separate providers, and live proofs, and [installation and retirement](../external-providers.md) for current deployment requirements. [The provider inventory](../provider-inventory.md) records the original extraction boundaries.

[ADR 0005](0005-persisted-workload-sequences.md) records numbered runtime requests, retained lineage, and exact-instance retirement authorization.

[ADR 0006](0006-agent-substrate-v1.md) records the Agent Substrate v1.0.0 contract: host-name egress allowlists, router-targeted startup evidence, single-Actor worker sizing, and retire-first handling of v0.1.0 state.
