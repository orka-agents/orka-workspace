// Copyright (c) 2026. MIT License - see LICENSE file for details.

// Command orka-fiber-shim is the fiberd gVisor template for an Orka ACP
// supervisor. It runs as the template sandbox's init and stays idle until
// fiberd checkpoints it. In every sandbox restored from that image it claims
// the fiber identity on fiberd's channel, writes the identity projection the
// supervisor binds into its sealed bootstrap challenge, bridges the fiber's
// unix endpoint to the supervisor's loopback listener and loopback egress to
// the home's proxy socket, then starts the supervisor from the Clone payload.
// The supervisor starts after the restore, so no key or credential of one
// fiber exists in the shared image or in another fiber.
package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/orka-agents/orka-workspace/providers/fiberd/payload"
)

const (
	specEnviron    = "/proc/gvisor/spec_environ"
	checkpointFile = "/proc/gvisor/checkpoint"
	poolSocket     = "/fiberd/channel/pool.sock"
	warmMarker     = "/host/warm.ready"
	recordMax      = 64 << 10
	claimFreshWait = 3 * time.Second
)

type options struct {
	supervisor  string
	upstream    string
	identityDir string
	egressSock  string
	egressAddr  string
	writable    string
	hostsFile   string
	dialWait    time.Duration
}

func main() {
	var o options
	flag.StringVar(&o.supervisor, "supervisor", "/usr/local/bin/orka-acp-runtime", "supervisor executable when the payload names no command")
	flag.StringVar(&o.upstream, "upstream", "127.0.0.1:80", "supervisor listener the fiber endpoint is bridged to")
	flag.StringVar(&o.identityDir, "identity-dir", "/run/orka-substrate-identity", "identity projection the supervisor reads")
	flag.StringVar(&o.egressSock, "egress-socket", "/host/egress.sock", "home egress proxy socket")
	flag.StringVar(&o.egressAddr, "egress-listen", "127.0.0.1:3128", "loopback address bridged to the egress socket")
	flag.StringVar(&o.writable, "writable", "/tmp/root,/tmp/run,/tmp/sessions", "tmpfs directories prepared before the template checkpoint")
	flag.StringVar(&o.hostsFile, "hosts-file", "/tmp/run/hosts", "writable hosts file the rootfs /etc/hosts links to")
	flag.DurationVar(&o.dialWait, "dial-wait", 15*time.Second, "how long a bridged connection waits for the supervisor listener")
	flag.Parse()
	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "orka-fiber-shim:", err)
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() > 0 {
			os.Exit(exit.ExitCode())
		}
		os.Exit(122)
	}
}

func run(o options) error {
	for _, dir := range strings.Split(o.writable, ",") {
		if dir = strings.TrimSpace(dir); dir != "" {
			if err := os.MkdirAll(dir, 0o711); err != nil {
				return err
			}
		}
	}
	if err := os.WriteFile(o.hostsFile, []byte(baseHosts), 0o644); err != nil {
		return err
	}
	f, err := os.OpenFile(warmMarker, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("warm marker: %w", err)
	}
	_ = f.Close()
	fmt.Fprintln(os.Stderr, "orka-fiber-shim: warm, waiting for the template checkpoint")
	var lastToken string
	for {
		restored, err := waitCheckpoint()
		if err != nil {
			return err
		}
		if !restored {
			continue
		}
		return serveFiber(o, &lastToken)
	}
}

// waitCheckpoint blocks in gVisor's checkpoint sync point and reports
// whether this process now runs in a restored sandbox.
func waitCheckpoint() (bool, error) {
	f, err := os.Open(checkpointFile)
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	buf := make([]byte, 32)
	for {
		n, err := f.Read(buf)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if n <= 0 {
			return false, nil
		}
		return strings.HasPrefix(string(buf[:n]), "restore"), nil
	}
}

type identity struct {
	kind     string
	fence    string
	endpoint string
	payload  []byte
	session  string
	conn     *net.UnixConn
}

// claim hands the restore's one-shot token to fiberd and reads the
// identity record, as libfiberzygote's fz_gvisor_identity does.
func claim(lastToken *string) (*identity, error) {
	var token string
	deadline := time.Now().Add(claimFreshWait)
	for {
		token = specToken()
		if token != "" && token != *lastToken {
			break
		}
		if time.Now().After(deadline) {
			return nil, errors.New("no fresh FIBERD_TOKEN in the spec environment")
		}
		time.Sleep(time.Millisecond)
	}
	*lastToken = token
	var conn *net.UnixConn
	var err error
	deadline = time.Now().Add(2 * time.Second)
	for {
		conn, err = net.DialUnix("unixpacket", nil, &net.UnixAddr{Name: poolSocket, Net: "unixpacket"})
		if err == nil || time.Now().After(deadline) || !(errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ENOENT)) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		return nil, fmt.Errorf("pool socket: %w", err)
	}
	if _, err := conn.Write(append(append([]byte{'t'}, token...), 0)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	record := make([]byte, recordMax)
	n, _, flags, _, err := conn.ReadMsgUnix(record, nil)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if n == 0 || flags&syscall.MSG_TRUNC != 0 {
		_ = conn.Close()
		return nil, errors.New("empty or truncated identity record")
	}
	id, err := parseIdentity(record[:n])
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	id.conn = conn
	return id, nil
}

func specToken() string {
	data, err := os.ReadFile(specEnviron)
	if err != nil {
		return ""
	}
	for _, entry := range strings.FieldsFunc(string(data), func(r rune) bool { return r == 0 || r == '\n' }) {
		if value, ok := strings.CutPrefix(entry, "FIBERD_TOKEN="); ok {
			return value
		}
	}
	return ""
}

// parseIdentity splits 'i' <generation> <kind> <fence> <endpoint>
// <payload-hex|-> <key|-> <cert|-> <caller|-> <session-hex|-> <seed-hex>,
// NUL-terminated.
func parseIdentity(rec []byte) (*identity, error) {
	if len(rec) < 2 || rec[0] != 'i' || rec[len(rec)-1] != 0 {
		return nil, errors.New("malformed identity record")
	}
	fields := strings.Split(string(rec[1:len(rec)-1]), "\x00")
	if len(fields) != 10 || fields[2] == "" || fields[3] == "" {
		return nil, fmt.Errorf("identity record has %d fields", len(fields))
	}
	id := &identity{kind: fields[1], fence: fields[2], endpoint: fields[3]}
	if fields[4] != "-" {
		p, err := hex.DecodeString(fields[4])
		if err != nil {
			return nil, fmt.Errorf("payload: %w", err)
		}
		id.payload = p
	}
	if fields[8] != "-" {
		session, err := hex.DecodeString(fields[8])
		if err != nil {
			return nil, fmt.Errorf("session: %w", err)
		}
		id.session = string(session)
	}
	if seed, err := hex.DecodeString(fields[9]); err != nil || len(seed) != 32 {
		return nil, errors.New("identity seed is not 32 bytes")
	}
	return id, nil
}

func (id *identity) ack() error {
	defer func() { _ = id.conn.Close() }()
	_, err := id.conn.Write(append(append([]byte{'a'}, id.fence...), 0))
	return err
}

func serveFiber(o options, lastToken *string) error {
	started := time.Now()
	id, err := claim(lastToken)
	if err != nil {
		return fmt.Errorf("identity: %w", err)
	}
	if id.kind != "clone" {
		return fmt.Errorf("unsupported fiber kind %q: parked supervisors are never resumed", id.kind)
	}
	if len(id.payload) == 0 {
		// fiberd's footprint probe restores one sandbox with no payload and
		// measures it serving. It gets an endpoint and no supervisor.
		return serveIdle(id)
	}
	launch, err := payload.Decode(id.payload)
	if err != nil {
		return fmt.Errorf("payload: %w", err)
	}
	// fiberd attests the session the fiber was cloned for; a named fiber
	// serves only the identity of that session.
	if id.session != "" && id.session != launch.Identity.Name {
		return fmt.Errorf("payload names %q, but fiberd cloned session %q", launch.Identity.Name, id.session)
	}
	if err := writeIdentity(o.identityDir, launch.Identity); err != nil {
		return err
	}
	env := supervisorEnv(launch.Env, o.egressAddr)
	// The supervisor's Core clients ignore proxy variables by design, so
	// every admitted URL host resolves to a loopback forwarder instead.
	if err := forwardURLHosts(env, o.hostsFile, o.egressSock); err != nil {
		return fmt.Errorf("url forwarders: %w", err)
	}
	upstream := o.upstream
	if listen := envValue(env, "ORKA_ACP_LISTEN_ADDRESS"); listen != "" {
		upstream = loopbackOf(listen)
	}
	egress, err := net.Listen("tcp", o.egressAddr)
	if err != nil {
		return fmt.Errorf("egress listener: %w", err)
	}
	go bridgeEgress(egress, o.egressSock)
	argv := append(append([]string{}, launch.Command...), launch.Args...)
	if len(launch.Command) == 0 {
		argv = append([]string{o.supervisor}, launch.Args...)
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = env
	cmd.Dir = "/"
	// A detached sandbox's stdio is discarded, so keep the supervisor's output
	// in the instance's own run directory for diagnosis.
	logPath := filepath.Join("/host", "supervisor-"+strings.ReplaceAll(id.fence, "/", "-")+".log")
	out := io.Writer(os.Stderr)
	if logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600); err == nil {
		defer func() { _ = logFile.Close() }()
		out = io.MultiWriter(os.Stderr, logFile)
	}
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start supervisor: %w", err)
	}
	_ = os.Remove(id.endpoint)
	ln, err := net.Listen("unix", id.endpoint)
	if err != nil {
		_ = cmd.Process.Kill()
		return fmt.Errorf("endpoint: %w", err)
	}
	if err := id.ack(); err != nil {
		_ = cmd.Process.Kill()
		return fmt.Errorf("ack: %w", err)
	}
	fmt.Fprintf(os.Stderr, "orka-fiber-shim: %s serving %s for %s/%s after %s\n", id.fence, id.endpoint,
		launch.Identity.Namespace, launch.Identity.Name, time.Since(started).Round(time.Microsecond))
	go bridgeEndpoint(ln, upstream, o.dialWait)
	if launch.Probe {
		go probe(id, launch, o, upstream)
	}
	err = cmd.Wait()
	fmt.Fprintf(out, "orka-fiber-shim: supervisor exited: %v\n", err)
	return err
}

func serveIdle(id *identity) error {
	_ = os.Remove(id.endpoint)
	ln, err := net.Listen("unix", id.endpoint)
	if err != nil {
		return fmt.Errorf("endpoint: %w", err)
	}
	if err := id.ack(); err != nil {
		return fmt.Errorf("ack: %w", err)
	}
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		_ = c.Close()
	}
}

func writeIdentity(dir string, id payload.Identity) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for name, value := range map[string]string{"atespace": id.Namespace, "name": id.Name, "uid": id.UID} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o444); err != nil {
			return err
		}
	}
	return nil
}

func supervisorEnv(admitted []string, egressAddr string) []string {
	env := append([]string{}, admitted...)
	set := func(name, value string) {
		if envValue(env, name) == "" {
			env = append(env, name+"="+value)
		}
	}
	set("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	set("HOME", "/root")
	// The supervisor requires a real session directory; the read-only rootfs
	// only links /sessions into the sandbox's tmpfs.
	set("ORKA_ACP_SESSION_BASE_DIR", "/tmp/sessions")
	proxy := "http://" + egressAddr
	for _, name := range []string{"HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy"} {
		set(name, proxy)
	}
	set("NO_PROXY", "localhost,127.0.0.1,::1")
	set("no_proxy", "localhost,127.0.0.1,::1")
	return env
}

func envValue(env []string, name string) string {
	value := ""
	for _, entry := range env {
		if v, ok := strings.CutPrefix(entry, name+"="); ok {
			value = v
		}
	}
	return value
}

func loopbackOf(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return listen
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

func bridgeEndpoint(ln net.Listener, upstream string, wait time.Duration) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go func() {
			deadline := time.Now().Add(wait)
			for {
				u, err := net.DialTimeout("tcp", upstream, time.Second)
				if err == nil {
					splice(c, u)
					return
				}
				if time.Now().After(deadline) {
					_ = c.Close()
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}
}

const baseHosts = "127.0.0.1\tlocalhost\n::1\tlocalhost ip6-localhost ip6-loopback\n"

// forwardURLHosts gives each non-loopback host named by an http(s) URL in env
// its own loopback address in the hosts file and listens there on the URL's
// port. Each connection is tunnelled with CONNECT through the home's egress
// proxy to the original host:port, so TLS server names stay unchanged.
func forwardURLHosts(env []string, hostsFile, egressSock string) error {
	hosts := map[string]string{}
	ports := map[string]map[string]bool{}
	for _, entry := range env {
		_, value, _ := strings.Cut(entry, "=")
		u, err := url.Parse(strings.TrimSpace(value))
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
			continue
		}
		host := u.Hostname()
		if host == "localhost" || net.ParseIP(host) != nil {
			continue
		}
		port := u.Port()
		if port == "" {
			port = map[string]string{"http": "80", "https": "443"}[u.Scheme]
		}
		if hosts[host] == "" {
			hosts[host] = fmt.Sprintf("127.0.1.%d", len(hosts)+1)
			ports[host] = map[string]bool{}
		}
		ports[host][port] = true
	}
	lines := baseHosts
	for host, ip := range hosts {
		lines += ip + "\t" + host + "\n"
		for port := range ports[host] {
			ln, err := net.Listen("tcp", net.JoinHostPort(ip, port))
			if err != nil {
				return err
			}
			go tunnel(ln, egressSock, net.JoinHostPort(host, port))
		}
	}
	return os.WriteFile(hostsFile, []byte(lines), 0o644)
}

func tunnel(ln net.Listener, sock, target string) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go func() {
			u, err := net.DialTimeout("unix", sock, 5*time.Second)
			if err != nil {
				_ = c.Close()
				return
			}
			if _, err := fmt.Fprintf(u, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target); err != nil {
				_ = c.Close()
				_ = u.Close()
				return
			}
			resp, err := http.ReadResponse(bufio.NewReader(&byteReader{u}), &http.Request{Method: http.MethodConnect})
			if err != nil || resp.StatusCode != http.StatusOK {
				fmt.Fprintf(os.Stderr, "orka-fiber-shim: CONNECT %s refused: %v %v\n", target, err, resp)
				_ = c.Close()
				_ = u.Close()
				return
			}
			splice(c, u)
		}()
	}
}

// byteReader keeps bufio from reading past the CONNECT response, so the
// tunnelled stream starts exactly after its blank line.
type byteReader struct{ c net.Conn }

func (r *byteReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return r.c.Read(p[:1])
}

func bridgeEgress(ln net.Listener, sock string) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go func() {
			u, err := net.Dial("unix", sock)
			if err != nil {
				_ = c.Close()
				return
			}
			splice(c, u)
		}()
	}
}

type halfCloser interface{ CloseWrite() error }

func splice(a, b net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	pipe := func(dst, src net.Conn) {
		defer wg.Done()
		_, _ = io.Copy(dst, src)
		if hc, ok := dst.(halfCloser); ok {
			_ = hc.CloseWrite()
		} else {
			_ = dst.Close()
		}
	}
	go pipe(a, b)
	go pipe(b, a)
	wg.Wait()
	_ = a.Close()
	_ = b.Close()
}

// probe records what this fiber can reach, for the spike report.
func probe(id *identity, launch payload.Launch, o options, upstream string) {
	result := map[string]any{"fence": id.fence, "identity": launch.Identity}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// The supervisor answers locally once booted.
	result["supervisorHealth"] = httpProbe(ctx, &http.Client{Timeout: 3 * time.Second}, "http://"+upstream+"/v2/health", 10*time.Second)
	d := net.Dialer{Timeout: 2 * time.Second}
	if c, err := d.DialContext(ctx, "tcp", "1.1.1.1:443"); err == nil {
		_ = c.Close()
		result["directEgress"] = "reachable"
	} else {
		result["directEgress"] = "blocked: " + err.Error()
	}
	if addrs, err := net.DefaultResolver.LookupHost(ctx, "example.com"); err == nil {
		result["dns"] = fmt.Sprintf("resolved %v", addrs)
	} else {
		result["dns"] = "blocked: " + err.Error()
	}
	proxyURL, _ := url.Parse("http://" + o.egressAddr)
	viaProxy := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
	result["proxyEgressAllowed"] = httpProbe(ctx, viaProxy, "https://example.com/", 0)
	result["proxyEgressDenied"] = httpProbe(ctx, viaProxy, "https://www.google.com/", 0)
	// Other fibers' endpoints and files in the grant's shared run directory.
	peers := map[string]string{}
	entries, _ := os.ReadDir("/host")
	for _, e := range entries {
		path := filepath.Join("/host", e.Name())
		if path == id.endpoint || !strings.HasSuffix(e.Name(), ".sock") || e.Name() == filepath.Base(o.egressSock) {
			continue
		}
		peer := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", path)
			}}}
		peers[e.Name()] = httpProbe(ctx, peer, "http://peer/v2/health", 0)
	}
	result["peerEndpoints"] = peers
	marker := filepath.Join("/host", "written-by-"+strings.ReplaceAll(id.fence, "/", "-"))
	if err := os.WriteFile(marker, []byte(id.fence), 0o644); err == nil {
		result["sharedRunDirWrite"] = "wrote " + marker
	} else {
		result["sharedRunDirWrite"] = "denied: " + err.Error()
	}
	data, _ := json.Marshal(result)
	fmt.Fprintf(os.Stderr, "orka-fiber-shim probe %s\n", data)
	_ = os.WriteFile(filepath.Join("/host", "probe-"+strings.ReplaceAll(id.fence, "/", "-")+".json"), data, 0o644)
}

func httpProbe(ctx context.Context, client *http.Client, target string, retryFor time.Duration) string {
	deadline := time.Now().Add(retryFor)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		resp, err := client.Do(req)
		if err == nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
			_ = resp.Body.Close()
			return fmt.Sprintf("%d %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		if time.Now().After(deadline) {
			return "error: " + err.Error()
		}
		time.Sleep(50 * time.Millisecond)
	}
}
