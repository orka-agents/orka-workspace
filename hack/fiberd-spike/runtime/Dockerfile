# The Orka ACP supervisor image the spike's Core runtime pool names. It
# matches hack/external-workspace-e2e/acp-runtime.Dockerfile plus CA roots
# for egress. The fiberd home's gVisor rootfs is built from exactly this image.
FROM docker.io/library/debian:trixie-slim@sha256:020c0d20b9880058cbe785a9db107156c3c75c2ac944a6aa7ab59f2add76a7bd
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ENV HOME=/root ORKA_ACP_PROVIDER=codex PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
RUN mkdir -p /sessions /opt/codex-acp/dist && chmod 0711 /sessions
COPY --chmod=0555 bin/orka-acp-runtime bin/orka-acp-exec-helper /usr/local/bin/
COPY --chmod=0555 bin/acp-agent /usr/bin/node
USER 0:0
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/orka-acp-runtime"]
