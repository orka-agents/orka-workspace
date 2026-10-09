FROM docker.io/library/debian:trixie-slim@sha256:020c0d20b9880058cbe785a9db107156c3c75c2ac944a6aa7ab59f2add76a7bd
COPY native-data-runtime /usr/local/bin/orka-acp-runtime
USER 0:0
ENTRYPOINT ["/usr/local/bin/orka-acp-runtime"]
