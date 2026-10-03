# Contributing

Use the Go version and toolchain pinned in `go.mod`. This repository has two
modules. The root module contains shared APIs, the SDK, and conformance tests.
`providers/` contains provider implementations and their backend dependencies.

Run `make check` before sending a change. It builds and tests both modules,
checks formatting and `go vet`, checks dependency boundaries, and verifies
that committed CRDs and deepcopy methods match fresh generation.
`make generate manifests` updates generated files using controller-gen v0.20.0.
The generation tool is installed into the ignored `bin/` directory.

Shared code must not depend on Orka, provider implementations, or backend client
libraries. Providers must not import Orka or another provider. The boundary
check includes test imports and transitive dependencies.

CRD and hash fixtures under `testdata/baseline/` were captured from Orka commit
`1cd16c88b43410e0ea46463f8a39b473d8e5250e`. Keep them unchanged during extraction.
A later schema or hash change needs an explicit compatibility decision and a
migration plan before updating the fixtures.

Sign off commits with `git commit -s`. Keep patches focused and include the
verification needed to demonstrate the changed behavior. New providers must
pass the shared lifecycle conformance suite in their CI.
