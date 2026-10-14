// Copyright (c) 2026. MIT License - see LICENSE file for details.

// Command fiberd-spike-check runs the Stage 1 fiberd feasibility checks from
// a Pod beside a fiberd home: it mints grants with the home's issuer key,
// clones Orka supervisors as gVisor fibers, reads each fiber's sealed
// bootstrap challenge through its endpoint, and releases them. It reports
// latency, challenge uniqueness and identity binding as JSON.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/helayoty/fiberd/pkg/consumer"
	"github.com/helayoty/fiberd/pkg/core"
	"github.com/helayoty/fiberd/pkg/endpoint"
	"github.com/helayoty/fiberd/pkg/grant"

	"github.com/orka-agents/orka-workspace/providers/fiberd/payload"
)

type options struct {
	target, nodeID, issuerKey, issuerURL string
	mode                                 string
	workspaces, warm, probe              int
	concurrency                          int
	named                                bool
	hold, settle                         time.Duration
}

type challenge struct {
	Schema    string `json:"schema"`
	Nonce     string `json:"nonce"`
	BootNonce string `json:"bootNonce"`
	PublicKey string `json:"publicKey"`
	Actor     struct {
		Atespace string `json:"atespace"`
		Name     string `json:"name"`
		UID      string `json:"uid"`
	} `json:"actor"`
}

type fiberResult struct {
	Workspace    string    `json:"workspace"`
	Grant        string    `json:"grant"`
	FiberID      string    `json:"fiberID,omitempty"`
	Endpoint     string    `json:"endpoint,omitempty"`
	Kind         string    `json:"kind,omitempty"`
	CloneMS      float64   `json:"cloneMs"`
	HealthMS     float64   `json:"cloneToHealthMs"`
	ChallengeMS  float64   `json:"cloneToChallengeMs"`
	Challenge    challenge `json:"challenge"`
	IdentityOK   bool      `json:"identityBound"`
	ReleasedGone bool      `json:"releasedEndpointRefuses"`
	Error        string    `json:"error,omitempty"`
}

type report struct {
	Mode             string        `json:"mode"`
	Workspaces       int           `json:"workspaces"`
	Warm             int           `json:"warm"`
	Named            bool          `json:"named"`
	Fibers           []fiberResult `json:"fibers"`
	UniqueChallenges bool          `json:"uniqueChallenges"`
	Clone            stats         `json:"cloneMs"`
	Health           stats         `json:"cloneToHealthMs"`
	Failures         int           `json:"failures"`
}

type stats struct {
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	Max float64 `json:"max"`
}

func main() {
	var o options
	flag.StringVar(&o.target, "target", "", "fiberd gRPC host:port")
	flag.StringVar(&o.nodeID, "node-id", "", "grant audience: the home's node id")
	flag.StringVar(&o.issuerKey, "issuer-key", "", "issuer private JWK")
	flag.StringVar(&o.issuerURL, "issuer", "", "issuer URL")
	flag.StringVar(&o.mode, "mode", "shared", "shared: one grant for every workspace; per-workspace: one grant each")
	flag.IntVar(&o.workspaces, "workspaces", 4, "supervisors to clone")
	flag.IntVar(&o.warm, "warm", 0, "fibers.warm for each grant")
	flag.IntVar(&o.probe, "probe", 0, "how many of the first fibers record isolation probes")
	flag.IntVar(&o.concurrency, "concurrency", 1, "clones in flight at once")
	flag.BoolVar(&o.named, "named", false, "clone named sessions instead of anonymous fibers")
	flag.DurationVar(&o.hold, "hold", 0, "keep every fiber running this long before release")
	flag.DurationVar(&o.settle, "settle", 0, "wait this long after the first clone, so a warm pool can fill")
	flag.Parse()
	r, err := run(o)
	data, _ := json.MarshalIndent(r, "", "  ")
	fmt.Println(string(data))
	if err != nil {
		fmt.Fprintln(os.Stderr, "fiberd-spike-check:", err)
		os.Exit(1)
	}
}

func run(o options) (*report, error) {
	key, err := grant.LoadKey(o.issuerKey)
	if err != nil {
		return nil, err
	}
	issuer := &grant.Issuer{Key: key, URL: o.issuerURL}
	conn, err := grpc.NewClient(o.target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c := consumer.New(conn)
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	run := time.Now().UTC().Format("150405")
	mint := func(uid string, max int) (string, error) {
		return issuer.Mint(core.Grant{
			UID: uid, Audience: o.nodeID, TemplateDigest: "sha256:orka-acp-supervisor", Tenant: "spike",
			FiberMax: max, FiberWarm: o.warm, LeaseExpiry: time.Now().Add(time.Hour),
			Policy: core.Policy{Isolation: core.Untrusted},
		})
	}
	tokens := make([]string, o.workspaces)
	grants := make([]string, o.workspaces)
	if o.mode == "shared" {
		uid := "spike-shared-" + run
		tok, err := mint(uid, o.workspaces)
		if err != nil {
			return nil, err
		}
		for i := range tokens {
			tokens[i], grants[i] = tok, uid
		}
	} else {
		for i := range tokens {
			uid := fmt.Sprintf("spike-ws%d-%s", i, run)
			if tokens[i], err = mint(uid, 1); err != nil {
				return nil, err
			}
			grants[i] = uid
		}
	}

	r := &report{Mode: o.mode, Workspaces: o.workspaces, Warm: o.warm, Named: o.named}
	r.Fibers = make([]fiberResult, o.workspaces)
	sem := make(chan struct{}, max(1, o.concurrency))
	var wg sync.WaitGroup
	for i := range o.workspaces {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			r.Fibers[i] = cloneOne(ctx, c, tokens[i], grants[i], i, o)
		}(i)
		// Shared grants warm on their first admission; measure that alone.
		if i == 0 {
			wg.Wait()
			time.Sleep(o.settle)
		}
	}
	wg.Wait()

	seen := map[string]bool{}
	r.UniqueChallenges = true
	var clones, healths []float64
	for _, f := range r.Fibers {
		if f.Error != "" {
			r.Failures++
			continue
		}
		key := f.Challenge.PublicKey + "/" + f.Challenge.BootNonce
		if seen[key] || f.Challenge.PublicKey == "" {
			r.UniqueChallenges = false
		}
		seen[key] = true
		clones = append(clones, f.CloneMS)
		healths = append(healths, f.HealthMS)
	}
	r.Clone, r.Health = summarize(clones), summarize(healths)

	if o.hold > 0 {
		fmt.Fprintf(os.Stderr, "HOLD %d fibers for %s\n", len(r.Fibers)-r.Failures, o.hold)
		time.Sleep(o.hold)
	}
	for i := range r.Fibers {
		f := &r.Fibers[i]
		if f.FiberID == "" {
			continue
		}
		if err := c.Release(ctx, f.FiberID, true); err != nil {
			f.Error = strings.TrimSpace(f.Error + " release: " + err.Error())
			r.Failures++
			continue
		}
		_, err := get(ctx, f.Endpoint, "/v2/health", 2*time.Second)
		f.ReleasedGone = err != nil
	}
	if r.Failures > 0 || !r.UniqueChallenges {
		return r, fmt.Errorf("%d failures, unique challenges %v", r.Failures, r.UniqueChallenges)
	}
	return r, nil
}

func cloneOne(ctx context.Context, c *consumer.Client, token, grantUID string, i int, o options) fiberResult {
	res := fiberResult{Workspace: fmt.Sprintf("ws-%d", i), Grant: grantUID}
	nonce := randomHex(32)
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	id := payload.Identity{Namespace: "spike", Name: res.Workspace, UID: randomHex(16)}
	data, err := payload.Encode(payload.Launch{
		Version: payload.Version, Identity: id, Probe: i < o.probe,
		Env: []string{
			"ORKA_ACP_CREDENTIAL_BOOTSTRAP_NONCE=" + nonce,
			"ORKA_ACP_CREDENTIAL_BOOTSTRAP_PUBLIC_KEY=" + base64.RawURLEncoding.EncodeToString(pub),
			"ORKA_ACP_LISTEN_ADDRESS=127.0.0.1:80",
			"ORKA_ACP_PROVIDER=codex",
		},
	})
	if err != nil {
		res.Error = err.Error()
		return res
	}
	session := ""
	if o.named {
		session = res.Workspace
	}
	start := time.Now()
	f, err := c.Clone(ctx, token, session, 60*time.Second, data)
	res.CloneMS = ms(time.Since(start))
	if err != nil {
		res.Error = "clone: " + err.Error()
		return res
	}
	res.FiberID, res.Endpoint, res.Kind = f.ID, f.Endpoint, fmt.Sprint(f.Kind)
	if _, err := get(ctx, f.Endpoint, "/v2/health", 30*time.Second); err != nil {
		res.Error = "health: " + err.Error()
		return res
	}
	res.HealthMS = ms(time.Since(start))
	body, err := get(ctx, f.Endpoint, "/v2/credential-bootstrap", 10*time.Second)
	if err != nil {
		res.Error = "challenge: " + err.Error()
		return res
	}
	res.ChallengeMS = ms(time.Since(start))
	if err := json.Unmarshal(body, &res.Challenge); err != nil {
		res.Error = "challenge: " + err.Error()
		return res
	}
	ch := res.Challenge
	res.IdentityOK = ch.Nonce == nonce && ch.Actor.Atespace == id.Namespace && ch.Actor.Name == id.Name && ch.Actor.UID == id.UID
	if !res.IdentityOK {
		res.Error = "challenge does not bind the cloned identity"
	}
	return res
}

func get(ctx context.Context, ep, path string, retryFor time.Duration) ([]byte, error) {
	tr := &http.Transport{DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) { return endpoint.Dial(ctx, ep) }}
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	deadline := time.Now().Add(retryFor)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://fiber"+path, nil)
		resp, err := client.Do(req)
		if err == nil {
			body, rerr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if rerr == nil && resp.StatusCode == http.StatusOK {
				return body, nil
			}
			err = fmt.Errorf("%d %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func summarize(values []float64) stats {
	if len(values) == 0 {
		return stats{}
	}
	s := slices.Clone(values)
	slices.Sort(s)
	at := func(q float64) float64 { return s[min(len(s)-1, int(q*float64(len(s))))] }
	return stats{P50: at(0.5), P90: at(0.9), Max: s[len(s)-1]}
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
