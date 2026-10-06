package workspaceprovider

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
)

// ValidateEndpoints rejects endpoint metadata that could disclose credentials or
// produce ambiguous routing. Adapters should call this before patching status;
// the CRD schema independently enforces credential-free URL shape, while this
// helper also validates optional port bounds.
func ValidateEndpoints(endpoints []workspacev1alpha1.ExecutionWorkspaceEndpoint) error {
	seen := make(map[string]bool, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint.Name == "" || seen[endpoint.Name] {
			return fmt.Errorf("workspace endpoints require unique, non-empty names")
		}
		seen[endpoint.Name] = true
		parsed, err := url.Parse(strings.TrimSpace(endpoint.URL))
		if err != nil || parsed.Hostname() == "" || strings.HasSuffix(parsed.Host, ":") {
			return fmt.Errorf("workspace endpoint %q is invalid", endpoint.Name)
		}
		if strings.Contains(parsed.Hostname(), ":") && !strings.HasPrefix(parsed.Host, "[") {
			return fmt.Errorf("workspace endpoint %q requires brackets around an IPv6 host", endpoint.Name)
		}
		switch parsed.Scheme {
		case "http", "https", "tcp":
		default:
			return fmt.Errorf("workspace endpoint %q uses unsupported scheme", endpoint.Name)
		}
		if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || strings.ContainsAny(parsed.Host, "@?#") {
			return fmt.Errorf("workspace endpoint %q contains credentials or ambiguous URL components", endpoint.Name)
		}
		if port := parsed.Port(); port != "" {
			number, err := strconv.Atoi(port)
			if err != nil || number < 1 || number > 65535 {
				return fmt.Errorf("workspace endpoint %q port must be between 1 and 65535", endpoint.Name)
			}
		}
	}
	return nil
}
