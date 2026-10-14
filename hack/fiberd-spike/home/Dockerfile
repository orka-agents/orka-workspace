# The spike's fiberd home image: fiberd's Kubernetes example image plus a
# gVisor rootfs made from the supervisor runtime image, with orka-fiber-shim
# as the template and the home-side egress proxy.
ARG FIBERD_IMAGE=fiberd:kind
ARG RUNTIME_IMAGE=orka-acp-runtime:spike
FROM ${RUNTIME_IMAGE} AS rootfs
# The rootfs is read-only in every sandbox; /tmp is the sandbox's tmpfs.
RUN rm -rf /run /root /sessions && ln -s /tmp/run /run \
    && ln -s /tmp/root /root && ln -s /tmp/sessions /sessions && mkdir -p /host /fiberd/channel
COPY --chmod=0555 bin/orka-fiber-shim /usr/local/bin/

FROM ${FIBERD_IMAGE}
COPY --from=rootfs / /usr/share/orka-fiber/rootfs
# A build cannot replace its own /etc/hosts, so link the copy here.
RUN ln -sf /tmp/run/hosts /usr/share/orka-fiber/rootfs/etc/hosts
COPY --chmod=0555 bin/orka-fiber-egress bin/fiberd-spike-check /usr/local/bin/
# The example controller runs `tini -- fiberd-k8s`; this earlier PATH entry
# starts the egress proxy beside the real fiberd-k8s.
COPY --chmod=0555 fiberd-k8s-wrapper.sh /usr/local/sbin/fiberd-k8s
