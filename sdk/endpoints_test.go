package workspaceprovider

import (
	"testing"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
)

func TestValidateEndpointsRequiresHostname(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, endpoint string
		valid          bool
	}{
		{name: "HTTP port only", endpoint: "http://:80"},
		{name: "HTTPS port only", endpoint: "https://:443"},
		{name: "TCP port only", endpoint: "tcp://:8080"},
		{name: "empty bracketed host", endpoint: "https://[]:443"},
		{name: "DNS host", endpoint: "https://runtime.example:443/v2", valid: true},
		{name: "IPv4 host", endpoint: "http://192.0.2.1:8080", valid: true},
		{name: "IPv6 host", endpoint: "tcp://[2001:db8::1]:8080", valid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateEndpoints([]workspacev1alpha1.ExecutionWorkspaceEndpoint{{Name: "runtime", URL: test.endpoint}})
			if (err == nil) != test.valid {
				t.Fatalf("ValidateEndpoints(%q) = %v, want valid %v", test.endpoint, err, test.valid)
			}
		})
	}
}

func TestValidateEndpointsRequiresUniqueNames(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		endpoints []workspacev1alpha1.ExecutionWorkspaceEndpoint
		valid     bool
	}{
		{name: "empty list", valid: true},
		{name: "empty name", endpoints: []workspacev1alpha1.ExecutionWorkspaceEndpoint{{URL: "https://runtime.example"}}},
		{name: "duplicate name", endpoints: []workspacev1alpha1.ExecutionWorkspaceEndpoint{{Name: "runtime", URL: "https://runtime.example"}, {Name: "runtime", URL: "https://other.example"}}},
		{name: "distinct names", endpoints: []workspacev1alpha1.ExecutionWorkspaceEndpoint{{Name: "runtime", URL: "https://runtime.example"}, {Name: "metrics", URL: "https://metrics.example"}}, valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateEndpoints(tc.endpoints)
			if (err == nil) != tc.valid {
				t.Fatalf("ValidateEndpoints() = %v, want valid %v", err, tc.valid)
			}
		})
	}
}
