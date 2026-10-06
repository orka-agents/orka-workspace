FROM scratch
COPY pod-runtime /pod-runtime
USER 65532:65532
ENTRYPOINT ["/pod-runtime"]
