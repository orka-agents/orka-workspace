FROM scratch
COPY orka-workspace-sandbox /orka-workspace-sandbox
USER 65532:65532
ENTRYPOINT ["/orka-workspace-sandbox"]
