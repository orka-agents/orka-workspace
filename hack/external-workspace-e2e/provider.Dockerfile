FROM scratch
COPY orka-workspace-fake /orka-workspace-fake
USER 65532:65532
ENTRYPOINT ["/orka-workspace-fake"]
