package substrate

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"k8s.io/apimachinery/pkg/util/validation"
)

const UpstreamCommit = "fa6d949685a6318940a9a0195c867c864009b820"

// Config is installation-owned control transport. Workload credentials remain
// exclusively in core; these files authenticate the adapter to native control.
type Config struct {
	APIEndpoint, CAFile, CertFile, KeyFile, BearerTokenFile string
	ActorDNSSuffix                                          string
	DirectEgressEnabled                                     bool
}

func (c Config) Validate() error {
	if c.APIEndpoint == "" || c.CAFile == "" {
		return fmt.Errorf("native control requires an endpoint and trusted CA file")
	}
	if (c.CertFile == "") != (c.KeyFile == "") || (c.CertFile != "") == (c.BearerTokenFile != "") {
		return fmt.Errorf("native control requires either client certificate/key files or a bearer token file")
	}
	if len(validation.IsDNS1123Subdomain(c.ActorDNSSuffix)) != 0 {
		return fmt.Errorf("native Actor DNS suffix is invalid")
	}
	if !c.DirectEgressEnabled {
		return fmt.Errorf("native ACP requires operator-configured direct egress")
	}
	return nil
}

func Dial(c Config) (*grpc.ClientConn, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(c.CAFile)
	if err != nil {
		return nil, fmt.Errorf("cannot read native control CA file")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("native control CA file contains no certificates")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	if c.CertFile != "" {
		load := func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			pair, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("cannot load native client certificate/key pair")
			}
			return &pair, nil
		}
		if _, err := load(nil); err != nil {
			return nil, err
		}
		config.GetClientCertificate = load
	}
	options := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(config)), grpc.WithUnaryInterceptor(rpcDeadline)}
	if c.BearerTokenFile != "" {
		creds := bearerCredentials{path: c.BearerTokenFile}
		if _, err := creds.GetRequestMetadata(context.Background()); err != nil {
			return nil, err
		}
		options = append(options, grpc.WithPerRPCCredentials(creds))
	}
	return grpc.NewClient(c.APIEndpoint, options...)
}

func rpcDeadline(ctx context.Context, method string, req, reply any, conn *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	timeout := 30 * time.Second
	switch method {
	case pb.Control_ResumeActor_FullMethodName, pb.Control_SuspendActor_FullMethodName, pb.Control_DeleteActor_FullMethodName, pb.Control_DeleteActorTemplate_FullMethodName, pb.Control_CreateTag_FullMethodName, pb.Control_DeleteTag_FullMethodName:
		timeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return invoke(ctx, method, req, reply, conn, opts...)
}

type bearerCredentials struct{ path string }

func (c bearerCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	data, err := os.ReadFile(c.path)
	if err != nil {
		return nil, fmt.Errorf("cannot read native bearer token file")
	}
	token := strings.TrimSpace(string(data))
	if token == "" || strings.ContainsAny(token, "\r\n\t ") {
		return nil, fmt.Errorf("native bearer token file must contain one nonempty token")
	}
	return map[string]string{"authorization": "Bearer " + token}, nil
}
func (bearerCredentials) RequireTransportSecurity() bool { return true }
