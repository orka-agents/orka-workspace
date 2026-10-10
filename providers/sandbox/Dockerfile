FROM docker.io/library/golang:1.27.2@sha256:5bc7f572bbaa98885a3a1fd9c0aa76b59e3e14e8628bfc316bbfd0c701e4818c AS build
WORKDIR /src
COPY . .
WORKDIR /src/providers
RUN CGO_ENABLED=0 go build -trimpath -o /out/orka-workspace-sandbox ./sandbox/cmd/orka-workspace-sandbox

FROM scratch
COPY --from=build /out/orka-workspace-sandbox /orka-workspace-sandbox
USER 65532:65532
ENTRYPOINT ["/orka-workspace-sandbox"]
