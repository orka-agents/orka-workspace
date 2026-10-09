// Copyright (c) 2026. MIT License - see LICENSE file for details.

package v1alpha1

import (
	"reflect"
	"strings"
	"testing"
)

func TestStartupEndpointUsesFrozenPublicBootstrapPort(t *testing.T) {
	for _, tc := range []struct {
		name, endpoint string
		bootstrap      int32
		valid          bool
	}{
		{"explicit HTTP", "http://runtime.example:8080", 8080, true},
		{"default HTTP", "http://runtime.example", 80, true},
		{"explicit default HTTP", "http://runtime.example:80", 80, true},
		{"default HTTPS", "https://runtime.example", 443, true},
		{"explicit default HTTPS", "https://runtime.example:443", 443, true},
		{"custom HTTPS", "https://runtime.example:8443", 8443, true},
		{"IPv6 default HTTP", "http://[2001:db8::1]", 80, true},
		{"IPv6 explicit HTTP", "http://[2001:db8::1]:8080", 8080, true},
		{"router path", "http://router.example/actor/route", 80, true},
		{"leading zero", "http://runtime.example:00080", 80, true},
		{"wrong explicit port", "http://runtime.example:8081", 8080, false},
		{"wrong HTTP default", "http://runtime.example", 8080, false},
		{"wrong HTTPS default", "https://runtime.example", 8080, false},
		{"wrong scheme default", "http://runtime.example", 443, false},
	} {
		for _, kind := range []string{"Pod", "native process", "endpoint-only lifecycle"} {
			t.Run(tc.name+"/"+kind, func(t *testing.T) {
				request := validationRuntimeRequest(t)
				request.Runtime.BootstrapPort = tc.bootstrap
				if kind == "native process" {
					request.Runtime.RequiredFeatures = []ExecutionWorkspaceFeature{WorkspaceFeatureNativeProcess}
				} else if kind == "endpoint-only lifecycle" {
					request.Runtime = nil
				}
				setValidationRevision(t, &request)
				identity := InstanceIdentity{AllocationID: "allocation", InstanceID: "instance", RequestRevision: request.Revision}
				startup := &StartupEvidence{ContractVersion: LifecycleContractV1, Identity: identity, Endpoint: tc.endpoint}
				pod := PodReference{Namespace: "runtimes", Name: "worker", UID: "pod-uid"}
				switch kind {
				case "Pod":
					startup.Pod = &pod
				case "native process":
					startup.Process = &NativeProcessEvidence{Namespace: "native", Name: "actor", UID: "actor-uid", Version: 1, Worker: pod, ChallengeSHA256: "sha256:" + strings.Repeat("d", 64)}
				}
				observation := AllocationObservation{Key: request.Key, Sequence: request.Sequence, Identity: identity, State: AllocationReady, Startup: startup}
				beforeRequest, beforeObservation := request.DeepCopy(), observation.DeepCopy()
				want := tc.valid || kind == "endpoint-only lifecycle"
				if err := ValidateStartup(request, observation); (err == nil) != want {
					t.Fatalf("startup accepted=%v, want %v: %v", err == nil, want, err)
				}
				if !reflect.DeepEqual(&request, beforeRequest) || !reflect.DeepEqual(&observation, beforeObservation) {
					t.Fatal("startup validation rewrote frozen request or evidence")
				}
			})
		}
	}
}
