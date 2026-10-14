# The spike's fiberd home image: fiberd's Kubernetes example image plus a
# gVisor rootfs holding the Orka ACP supervisor, the deterministic ACP agent
# fixture and orka-fiber-shim as the template, and the home-side egress proxy.
ARG FIBERD_IMAGE=fiberd:kind
FROM docker.io/library/debian:trixie-slim@sha256:020c0d20b9880058cbe785a9db107156c3c75c2ac944a6aa7ab59f2add76a7bd AS rootfs
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# The rootfs is read-only in every sandbox; /tmp is the sandbox's tmpfs.
RUN rm -rf /run /root && ln -s /tmp/run /run && ln -s /tmp/root /root && ln -s /tmp/sessions /sessions \
    && mkdir -p /host /fiberd/channel /opt/codex-acp/dist
COPY --chmod=0555 bin/orka-acp-runtime bin/orka-acp-exec-helper bin/orka-fiber-shim /usr/local/bin/
COPY --chmod=0555 bin/acp-agent /usr/bin/node

FROM ${FIBERD_IMAGE}
COPY --from=rootfs / /usr/share/orka-fiber/rootfs
COPY --chmod=0555 bin/orka-fiber-egress bin/fiberd-spike-check /usr/local/bin/
# The example controller runs `tini -- fiberd-k8s`; this earlier PATH entry
# starts the egress proxy beside the real fiberd-k8s.
COPY --chmod=0555 fiberd-k8s-wrapper.sh /usr/local/sbin/fiberd-k8s
