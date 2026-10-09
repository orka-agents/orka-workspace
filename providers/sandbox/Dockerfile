FROM golang:1.27.0 AS build
WORKDIR /src
COPY . .
WORKDIR /src/providers
RUN CGO_ENABLED=0 go build -trimpath -o /out/orka-workspace-sandbox ./sandbox/cmd/orka-workspace-sandbox

FROM scratch
COPY --from=build /out/orka-workspace-sandbox /orka-workspace-sandbox
USER 65532:65532
ENTRYPOINT ["/orka-workspace-sandbox"]
