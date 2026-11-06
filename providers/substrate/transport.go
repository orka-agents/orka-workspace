package substrate

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// UpstreamCommit pins Agent Substrate v1.0.0 for provider, protocol, and
// conformance. BackendAPIVersion is the exact native control API it speaks.
const (
	UpstreamCommit    = "b34fbee742906494b5eb4f1f36987176bf8770a4"
	BackendAPIVersion = "ateapi.v1.0.0"
)

// Config is installation-owned control and routing transport. Workload
// credentials remain exclusively in core; these files authenticate the adapter
// to native control and to the native ingress router.
type Config struct {
	APIEndpoint, CAFile, CertFile, KeyFile, BearerTokenFile string
	// RouterEndpoint is the native ingress router's HTTPS base URL. Every
	// request to an Actor is routed by an explicit target header, never DNS.
	RouterEndpoint string
	// RouterCAFile verifies the router's serving certificate.
	RouterCAFile string
	// RouterCertFile and RouterKeyFile hold the provider's rotating client
	// identity, which the router must allowlist. They may name one bundle.
	RouterCertFile, RouterKeyFile string
	// RouterServerName overrides the TLS server name; it defaults to the
	// router endpoint's host.
	RouterServerName string
	// BootTimeout bounds native creation and boot before first process
	// evidence. Zero selects defaultNativeBootTimeout.
	BootTimeout time.Duration
}

func (c Config) Validate() error {
	if c.APIEndpoint == "" || c.CAFile == "" {
		return fmt.Errorf("native control requires an endpoint and trusted CA file")
	}
	if (c.CertFile == "") != (c.KeyFile == "") || (c.CertFile != "") == (c.BearerTokenFile != "") {
		return fmt.Errorf("native control requires either client certificate/key files or a bearer token file")
	}
	if _, err := routerBaseURL(c.RouterEndpoint); err != nil {
		return err
	}
	if c.RouterCAFile == "" || c.RouterCertFile == "" || c.RouterKeyFile == "" {
		return fmt.Errorf("native routing requires a router CA file and a client certificate/key")
	}
	if c.BootTimeout < 0 {
		return fmt.Errorf("native boot timeout must not be negative")
	}
	return nil
}

// routerBaseURL accepts only an HTTPS origin without credentials, path, query
// or fragment, so the target header is the sole Actor selector.
func routerBaseURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.ForceQuery || strings.HasSuffix(u.Host, ":") {
		return nil, fmt.Errorf("native router endpoint must be an HTTPS origin without credentials, path, query or fragment")
	}
	u.Path = ""
	return u, nil
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
		load := rotatingClientCertificate(c.CertFile, c.KeyFile, "native client")
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

// rotatingClientCertificate rereads a projected certificate on every
// handshake. A pod-certificate credential bundle may supply both paths.
func rotatingClientCertificate(certFile, keyFile, purpose string) func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	return func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
		pair, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("cannot load %s certificate/key pair", purpose)
		}
		return &pair, nil
	}
}

// routerTLSConfig authenticates the router by its configured CA and presents
// the provider's rotating client identity, which the router allowlists.
func routerTLSConfig(c Config) (*tls.Config, error) {
	base, err := routerBaseURL(c.RouterEndpoint)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(c.RouterCAFile)
	if err != nil {
		return nil, fmt.Errorf("cannot read native router CA file")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("native router CA file contains no certificates")
	}
	serverName := c.RouterServerName
	if serverName == "" {
		serverName = base.Hostname()
	}
	load := rotatingClientCertificate(c.RouterCertFile, c.RouterKeyFile, "native router client")
	if _, err := load(nil); err != nil {
		return nil, err
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: serverName, GetClientCertificate: load}, nil
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
