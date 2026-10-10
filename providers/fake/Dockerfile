FROM docker.io/library/golang:1.27.2@sha256:5bc7f572bbaa98885a3a1fd9c0aa76b59e3e14e8628bfc316bbfd0c701e4818c AS build
WORKDIR /src
COPY . .
WORKDIR /src/providers
RUN CGO_ENABLED=0 go build -trimpath -o /out/orka-workspace-fake ./fake/cmd/orka-workspace-fake

FROM scratch
COPY --from=build /out/orka-workspace-fake /orka-workspace-fake
USER 65532:65532
ENTRYPOINT ["/orka-workspace-fake"]
