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
