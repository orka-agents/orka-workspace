module github.com/orka-agents/orka-workspace/fiberd-spike

go 1.27.0

toolchain go1.27.2

replace github.com/orka-agents/orka-workspace => ../..

replace github.com/orka-agents/orka-workspace/providers => ../../providers

require (
	github.com/helayoty/fiberd v0.1.1-0.20261013212911-b3b5b4735cff
	github.com/orka-agents/orka-workspace/providers v0.0.0
	google.golang.org/grpc v1.83.2
)

require (
	github.com/go-jose/go-jose/v4 v4.1.5 // indirect
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260819154853-08b0e4226688 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
