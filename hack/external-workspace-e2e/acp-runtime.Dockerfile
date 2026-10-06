FROM docker.io/library/debian:trixie-slim@sha256:020c0d20b9880058cbe785a9db107156c3c75c2ac944a6aa7ab59f2add76a7bd
LABEL ai.orka.test.fixture="external-workspace-harness-v2"
ENV HOME=/root ORKA_ACP_PROVIDER=codex PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
RUN mkdir -p /sessions /opt/codex-acp/dist && chmod 0711 /sessions
COPY orka-acp-runtime /usr/local/bin/orka-acp-runtime
COPY orka-acp-exec-helper /usr/local/bin/orka-acp-exec-helper
COPY acp-agent /usr/bin/node
USER 0:0
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/orka-acp-runtime"]
