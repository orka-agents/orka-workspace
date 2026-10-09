FROM golang:1.27.0 AS build
WORKDIR /src
COPY . .
WORKDIR /src/providers
RUN CGO_ENABLED=0 go build -trimpath -o /out/orka-workspace-substrate ./substrate/cmd/orka-workspace-substrate

FROM scratch
COPY --from=build /out/orka-workspace-substrate /orka-workspace-substrate
USER 65532:65532
ENTRYPOINT ["/orka-workspace-substrate"]
