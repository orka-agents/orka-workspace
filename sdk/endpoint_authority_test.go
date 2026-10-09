// Copyright (c) 2026. MIT License - see LICENSE file for details.

package workspaceprovider

import (
	"reflect"
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	"github.com/orka-agents/orka-workspace/sdk/workspaceagent"
)

func TestAdvertisedEndpointAuthorityMatchesStartupAndClient(t *testing.T) {
	t.Parallel()
	for _, authority := range []struct {
		name, value string
		valid       bool
	}{
		{name: "DNS default port", value: "runtime.example", valid: true},
		{name: "minimum port", value: "runtime.example:1", valid: true},
		{name: "maximum port", value: "runtime.example:65535", valid: true},
		{name: "leading zero port", value: "runtime.example:00080", valid: true},
		{name: "IPv4", value: "192.0.2.1:443", valid: true},
		{name: "IPv6 default port", value: "[2001:db8::1]", valid: true},
		{name: "IPv6 explicit port", value: "[2001:db8::1]:65535", valid: true},
		{name: "zero port", value: "runtime.example:0"},
		{name: "out of range port", value: "runtime.example:65536"},
		{name: "overflow port", value: "runtime.example:999999999999999999999"},
		{name: "empty port", value: "runtime.example:"},
		{name: "IPv6 empty port", value: "[2001:db8::1]:"},
		{name: "text port", value: "runtime.example:http"},
		{name: "negative port", value: "runtime.example:-1"},
		{name: "signed port", value: "runtime.example:+1"},
		{name: "multiple ports", value: "runtime.example:443:444"},
		{name: "unbracketed IPv6", value: "2001:db8::1"},
		{name: "unclosed IPv6", value: "[2001:db8::1:443"},
		{name: "bracketed DNS", value: "[runtime.example]:443"},
		{name: "missing hostname", value: ":443"},
	} {
		for _, scheme := range []string{"http", "https", "tcp"} {
			t.Run(scheme+"/"+authority.name, func(t *testing.T) {
				request := workloadFixture()
				identity := InstanceIdentity{AllocationID: "allocation", InstanceID: "instance", RequestRevision: request.Revision}
				endpoint := scheme + "://" + authority.value + "/route%2Fprefix"
				workspace := &workspacev1alpha1.ExecutionWorkspace{Status: workspacev1alpha1.ExecutionWorkspaceStatus{
					Endpoints: []workspacev1alpha1.ExecutionWorkspaceEndpoint{{Name: "runtime", URL: endpoint}},
					Allocation: &AllocationObservation{Key: request.Key, Sequence: request.Sequence, State: AllocationReady, Identity: identity,
						Startup: &StartupEvidence{ContractVersion: LifecycleContractV1, Identity: identity, Endpoint: endpoint}},
				}}
				before := workspace.DeepCopy()
				if err := ValidateEndpoints(workspace.Status.Endpoints); (err == nil) != authority.valid {
					t.Fatalf("status endpoint authority accepted=%v, want %v: %v", err == nil, authority.valid, err)
				}
				// HTTP(S) startup and client endpoints share the same authority
				// requirements. Metadata additionally permits TCP routes.
				wantHTTP := authority.valid && scheme != "tcp"
				if err := ValidateStartup(request, *workspace.Status.Allocation); (err == nil) != wantHTTP {
					t.Fatalf("startup authority accepted=%v, want %v: %v", err == nil, wantHTTP, err)
				}
				if _, err := workspaceagent.NewClient(workspaceagent.ClientConfig{Endpoint: endpoint, AllowInsecure: true}); (err == nil) != wantHTTP {
					t.Fatalf("client authority accepted=%v, want %v: %v", err == nil, wantHTTP, err)
				}
				if !reflect.DeepEqual(workspace, before) {
					t.Fatal("validation rewrote advertised status or startup metadata")
				}
			})
		}
	}
}
