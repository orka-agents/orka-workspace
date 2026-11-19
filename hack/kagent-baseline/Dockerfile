# One local image for the kagent baseline: the deterministic OpenAI-compatible
# mock and the read-only ate-api Actor watcher. Binaries are built outside.
FROM scratch
COPY --chmod=0555 mockllm /mockllm
COPY --chmod=0555 atewatch /atewatch
USER 65532:65532
ENTRYPOINT ["/mockllm"]
