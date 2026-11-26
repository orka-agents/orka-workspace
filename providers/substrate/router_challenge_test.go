package substrate

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// routerChallengeServer is a TLS router that requires a client certificate,
// serves the challenge for one target, and counts the connections it accepts.
type routerChallengeServer struct {
	*httptest.Server
	mu          sync.Mutex
	connections int
	targets     []string
	status      int
	body        string
}

func newRouterChallengeServer(t *testing.T) (*routerChallengeServer, Config) {
	t.Helper()
	s := &routerChallengeServer{status: http.StatusOK}
	challenge := bootstrapChallenge{Schema: "orka.harness.v2/sealed-bootstrap/v1", Nonce: "public-nonce", PublicKey: "k", BootNonce: "b"}
	challenge.Actor.Atespace, challenge.Actor.Name, challenge.Actor.UID = "native", "actor", "actor-uid"
	data, err := json.Marshal(challenge)
	if err != nil {
		t.Fatal(err)
	}
	s.body = string(data)
	s.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/credential-bootstrap" || len(r.TLS.PeerCertificates) == 0 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		s.mu.Lock()
		s.targets = append(s.targets, r.Header.Get(routerTargetHeader))
		status, body := s.status, s.body
		s.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	s.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			s.mu.Lock()
			s.connections++
			s.mu.Unlock()
		}
	}
	s.EnableHTTP2 = true
	s.TLS = &tls.Config{ClientAuth: tls.RequireAnyClientCert}
	s.StartTLS()
	t.Cleanup(s.Close)

	dir := t.TempDir()
	ca := filepath.Join(dir, "router-ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "provider"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, keyFile := filepath.Join(dir, "client.pem"), filepath.Join(dir, "client-key.pem")
	if err := os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	// httptest certificates are issued for example.com and loopback.
	return s, Config{RouterEndpoint: s.URL, RouterCAFile: ca, RouterCertFile: cert, RouterKeyFile: keyFile, RouterServerName: "example.com"}
}

func (s *routerChallengeServer) accepted() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connections
}

// One pass reads the challenge twice, for startup evidence and for the Ready
// observation. Both reads share one TLS connection; releasing the pass
// closes it, and the next pass handshakes again with freshly loaded TLS
// material.
func TestRouterChallengeReusesOneConnectionPerPass(t *testing.T) {
	server, config := newRouterChallengeServer(t)
	d := New(nil, nil, config)
	d.router = &routerChallengeGetter{config: config}
	for range 2 {
		challenge, err := d.routerChallenges().Get(t.Context(), config.RouterEndpoint, "native/actor")
		if err != nil {
			t.Fatal(err)
		}
		if challenge.Actor.UID != "actor-uid" || challenge.Nonce != "public-nonce" {
			t.Fatalf("challenge = %+v", challenge)
		}
	}
	if got := server.accepted(); got != 1 {
		t.Fatalf("two challenge reads in one pass opened %d connections, want 1", got)
	}
	d.release()
	if _, err := d.routerChallenges().Get(t.Context(), config.RouterEndpoint, "native/actor"); err != nil {
		t.Fatal(err)
	}
	if got := server.accepted(); got != 2 {
		t.Fatalf("a read after release reused a closed connection: %d connections", got)
	}
	d.release()
	next := New(nil, nil, config)
	defer next.release()
	if _, err := next.routerChallenges().Get(t.Context(), config.RouterEndpoint, "native/actor"); err != nil {
		t.Fatal(err)
	}
	if got := server.accepted(); got != 3 {
		t.Fatalf("a new pass reused another pass's connection: %d connections", got)
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	for _, target := range server.targets {
		if target != "native/actor" {
			t.Fatalf("a reused connection dropped the per-request target header: %q", target)
		}
	}
}

// Connection reuse changes no decoding rule: a closed listener, an error
// status and a malformed or oversized document still fail closed on a reused
// connection.
func TestRouterChallengeReuseKeepsFailClosedDecoding(t *testing.T) {
	server, config := newRouterChallengeServer(t)
	d := New(nil, nil, config)
	defer d.release()
	getter := d.routerChallenges()
	if _, err := getter.Get(t.Context(), config.RouterEndpoint, "native/actor"); err != nil {
		t.Fatal(err)
	}
	valid := server.body
	for name, change := range map[string]func(){
		"closed":   func() { server.status, server.body = http.StatusNotFound, "" },
		"status":   func() { server.status, server.body = http.StatusServiceUnavailable, valid },
		"unknown":  func() { server.status, server.body = http.StatusOK, `{"schema":"x","extra":true}` },
		"trailing": func() { server.status, server.body = http.StatusOK, valid+"{}" },
		"oversize": func() { server.status, server.body = http.StatusOK, `"`+string(make([]byte, 65537))+`"` },
	} {
		server.mu.Lock()
		change()
		server.mu.Unlock()
		_, err := getter.Get(t.Context(), config.RouterEndpoint, "native/actor")
		if err == nil {
			t.Fatalf("%s: a reused connection accepted an invalid challenge", name)
		}
		if name == "closed" && !errors.Is(err, errChallengeClosed) {
			t.Fatalf("closed listener: %v", err)
		}
	}
	// The TLS configuration is loaded once per pass: a pass whose router CA
	// is unreadable fails before any request.
	broken := config
	broken.RouterCAFile = filepath.Join(t.TempDir(), "missing.pem")
	if _, err := New(nil, nil, broken).routerChallenges().Get(t.Context(), broken.RouterEndpoint, "native/actor"); err == nil {
		t.Fatal("a pass without a readable router CA read the challenge")
	}
}
