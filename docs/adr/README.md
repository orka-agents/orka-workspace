# Architecture decisions

These records describe the extraction from Orka commit [`1cd16c88b43410e0ea46463f8a39b473d8e5250e`](https://github.com/orka-agents/orka/commit/1cd16c88b43410e0ea46463f8a39b473d8e5250e), inspected on October 2, 2026.

| Record | Decision |
| --- | --- |
| [0001](0001-ownership.md) | Shared fields and their writers |
| [0002](0002-startup-and-retirement.md) | Workload handoff, startup evidence, and cleanup order |
| [0003](0003-configuration-and-trust.md) | Typed configuration, virtual verbs, and operation journals |
| [0004](0004-compatibility.md) | Independent versions and supported combinations |

Phase 1 builds the shared packages and fake provider. It preserves the stored workspace schemas. The external RuntimePool handoff, production provider extraction, migration, and cluster proof described here remain later work. [The provider inventory](../provider-inventory.md) identifies those boundaries.

[ADR 0005](0005-persisted-workload-sequences.md) records numbered runtime requests, retained lineage, and exact-instance retirement authorization.
