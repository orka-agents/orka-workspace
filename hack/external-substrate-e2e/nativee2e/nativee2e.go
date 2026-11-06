// Copyright (c) 2026. MIT License - see LICENSE file for details.

// Package nativee2e holds Agent Substrate v1.0.0 fixtures shared by the
// isolated native proofs. It never runs against a cluster other than the
// dedicated kindctl --tag external-substrate cluster.
package nativee2e

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"time"

	provider "github.com/orka-agents/orka-workspace/providers/substrate"
	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Installed client identity files and endpoints for proof Jobs.
const (
	ServerCAFile     = "/native/server/trust-bundle.pem"
	ClientBundleFile = "/native/client/credential-bundle.pem"
	APIEndpoint      = "api.ate-system.svc:443"
	RouterEndpoint   = "https://atenet-router.ate-system.svc:443"
	// ProviderPrincipal is the shipped provider ServiceAccount's ate-api
	// principal when it authenticates with its projected token.
	ProviderPrincipal = "system:serviceaccount:orka-workspace-system:orka-workspace-substrate"
)

// ProofTransport authenticates a proof Job to ate-api and atenet-router with
// its pod certificate. Install allowlists that identity.
func ProofTransport() provider.Config {
	return provider.Config{APIEndpoint: APIEndpoint, CAFile: ServerCAFile, CertFile: ClientBundleFile, KeyFile: ClientBundleFile,
		RouterEndpoint: RouterEndpoint, RouterCAFile: ServerCAFile, RouterCertFile: ClientBundleFile, RouterKeyFile: ClientBundleFile}
}

// InfrastructureTemplate is the operator-owned base template. It never runs:
// golden snapshots are disabled and its size is the default Actor size.
func InfrastructureTemplate(atespace, image, poolLabel string) *pb.ActorTemplate {
	return &pb.ActorTemplate{Metadata: &pb.ResourceMetadata{Atespace: atespace, Name: "infrastructure"},
		Containers:     []*pb.Container{{Name: "runtime", Image: image}},
		WorkerSelector: &pb.Selector{MatchLabels: map[string]string{"orka.workspace.e2e/pool": poolLabel}},
		SnapshotConfig: &pb.SnapshotConfig{PreferredFidelity: pb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			GoldenSnapshotConfig: &pb.GoldenSnapshotConfig{Mode: pb.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED}},
		Resources: &pb.ResourceRequirements{Limits: &pb.Resources{CpuMillicores: 1000, MemoryBytes: 1 << 30}}}
}

// VerifyDataOnlyTemplate checks the v1 data-only gate on a provider template.
func VerifyDataOnlyTemplate(template *pb.ActorTemplate) error {
	config := template.GetSnapshotConfig()
	if template.GetMetadata().GetUid() == "" || config.GetPreferredFidelity() != pb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES || config.GetGoldenSnapshotConfig().GetMode() != pb.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED || template.GetGoldenEgressPolicy() != nil {
		return fmt.Errorf("native template is not data-only with golden snapshots disabled")
	}
	return nil
}

// VerifySingleActorWorker checks the worker is gVisor and sized for exactly
// the template-sized Actor, with the Actor running under its current epoch.
func VerifySingleActorWorker(worker *pb.Worker, template *pb.ActorTemplate, actor *pb.Actor) error {
	limits := template.GetResources().GetLimits()
	capacity := worker.GetStatus().GetCapacity()
	classes := worker.GetStatus().GetSandboxClasses()
	if len(classes) != 1 || classes[0].GetSandboxClass() != "gvisor" || capacity.GetCpuMillicores() != limits.GetCpuMillicores() || capacity.GetMemoryBytes() != limits.GetMemoryBytes() {
		return fmt.Errorf("native worker is not a gVisor worker sized for exactly one Actor")
	}
	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment.GetWorkerEpoch() <= 0 || assignment.GetWorkerEpoch() != worker.GetStatus().GetRegisteredEpoch() || assignment.GetSandboxRuntime().GetSandboxClass() != "gvisor" {
		return fmt.Errorf("native Actor does not run under the worker's registered gVisor epoch")
	}
	return nil
}

// VerifyEgressPolicy requires an explicit, bounded native allowlist.
func VerifyEgressPolicy(ctx context.Context, native pb.ControlClient, ref *pb.ObjectRef) (*pb.EgressPolicy, error) {
	policy, err := native.GetActorEgressPolicy(ctx, &pb.GetActorEgressPolicyRequest{Actor: ref, Name: "default"})
	if err != nil {
		return nil, fmt.Errorf("native Actor has no egress policy: %w", err)
	}
	for _, rule := range policy.GetRules() {
		hosts := append(append(slices.Clone(rule.GetHttp().GetHostnames()), rule.GetHttps().GetHostnames()...), rule.GetTlsPassthrough().GetHostnames()...)
		ports := rule.GetHttp().GetPorts()
		if rule.GetHttps() != nil {
			ports = rule.GetHttps().GetPorts()
		} else if rule.GetTlsPassthrough() != nil {
			ports = rule.GetTlsPassthrough().GetPorts()
		}
		if slices.Contains(hosts, "*") || ports.GetAll() != nil || len(ports.GetNumbers()) == 0 {
			return nil, fmt.Errorf("native Actor egress grants every name or port")
		}
	}
	return policy, nil
}

// GrantProviderAccess gives the provider principal least privilege: editor on
// the profile Atespace and global viewer for worker reads. The caller must be
// a bootstrap owner of the isolated installation.
func GrantProviderAccess(ctx context.Context, native pb.ControlClient, atespace, principal string) error {
	member := "user:" + principal
	_, err := native.CreateAtespaceAccessPolicy(ctx, &pb.CreateAtespaceAccessPolicyRequest{Atespace: &pb.ObjectRef{Name: atespace},
		AccessPolicy: &pb.AccessPolicy{Metadata: &pb.ResourceMetadata{Atespace: atespace, Name: "default"}, Bindings: []*pb.Binding{{Role: "editor", Members: []string{member}}}}})
	if err != nil {
		return fmt.Errorf("grant provider Atespace editor: %w", err)
	}
	for range 5 {
		global, err := native.GetGlobalAccessPolicy(ctx, &pb.GetGlobalAccessPolicyRequest{Name: "default"})
		if status.Code(err) == codes.NotFound {
			_, err = native.CreateGlobalAccessPolicy(ctx, &pb.CreateGlobalAccessPolicyRequest{AccessPolicy: &pb.AccessPolicy{Metadata: &pb.ResourceMetadata{Name: "default"}, Bindings: []*pb.Binding{{Role: "viewer", Members: []string{member}}}}})
			if status.Code(err) == codes.AlreadyExists {
				continue
			}
			return err
		}
		if err != nil {
			return err
		}
		for _, binding := range global.GetBindings() {
			if binding.GetRole() == "viewer" && slices.Contains(binding.GetMembers(), member) {
				return nil
			}
		}
		global.Bindings = append(global.Bindings, &pb.Binding{Role: "viewer", Members: []string{member}})
		_, err = native.UpdateGlobalAccessPolicy(ctx, &pb.UpdateGlobalAccessPolicyRequest{AccessPolicy: global})
		if status.Code(err) == codes.Aborted {
			continue
		}
		return err
	}
	return fmt.Errorf("global access policy changed concurrently")
}

// RouterGetJSON reads an Actor path through atenet-router over mTLS. The
// target header, never DNS, selects the Actor.
func RouterGetJSON(ctx context.Context, endpoint, target, path string, out any) (int, error) {
	roots := x509.NewCertPool()
	data, err := os.ReadFile(ServerCAFile)
	if err != nil || !roots.AppendCertsFromPEM(data) {
		return 0, fmt.Errorf("router trust bundle is unusable")
	}
	pair, err := tls.LoadX509KeyPair(ClientBundleFile, ClientBundleFile)
	if err != nil {
		return 0, fmt.Errorf("router client identity is unusable")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+path, nil)
	if err != nil {
		return 0, err
	}
	request.Header.Set("ate-target-actor", target)
	transport := &http.Transport{Proxy: nil, ForceAttemptHTTP2: true, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "atenet-router.ate-system.svc", Certificates: []tls.Certificate{pair}}}
	defer transport.CloseIdleConnections()
	response, err := (&http.Client{Transport: transport, Timeout: 15 * time.Second}).Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return response.StatusCode, fmt.Errorf("router fixture HTTP status %d", response.StatusCode)
	}
	return response.StatusCode, json.NewDecoder(io.LimitReader(response.Body, 65536)).Decode(out)
}
