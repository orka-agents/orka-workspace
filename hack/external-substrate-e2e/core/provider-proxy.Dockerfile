FROM scratch
COPY --chmod=0555 fixture-provider-proxy /fixture-provider-proxy
USER 65532:65532
ENTRYPOINT ["/fixture-provider-proxy"]
