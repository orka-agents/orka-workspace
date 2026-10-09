FROM golang:1.27.0 AS build
WORKDIR /src
COPY . .
WORKDIR /src/providers
RUN CGO_ENABLED=0 go build -trimpath -o /out/orka-workspace-fake ./fake/cmd/orka-workspace-fake

FROM scratch
COPY --from=build /out/orka-workspace-fake /orka-workspace-fake
USER 65532:65532
ENTRYPOINT ["/orka-workspace-fake"]
