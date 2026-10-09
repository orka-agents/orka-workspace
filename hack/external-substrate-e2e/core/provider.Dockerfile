FROM debian:trixie-slim@sha256:020c0d20b9880058cbe785a9db107156c3c75c2ac944a6aa7ab59f2add76a7bd
COPY --chmod=0555 orka-workspace-substrate /provider
USER 65532:65532
ENTRYPOINT ["/provider"]
