// Copyright (c) 2026. MIT License - see LICENSE file for details.

package fiberd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/helayoty/fiberd/pkg/consumer"
	"github.com/helayoty/fiberd/pkg/core"
	"github.com/helayoty/fiberd/pkg/grant"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// HomeConfig names one fiberd home and the issuer key the provider mints its
// per-instance grants with.
type HomeConfig struct {
	// Pod is the home Pod. Its IP serves the Fibers API unless Target is set.
	Pod types.NamespacedName
	// Target overrides the Fibers API address.
	Target string
	// NodeID is the home's grant audience.
	NodeID string
	// IssuerKeyFile and IssuerURL mint grants the home verifies.
	IssuerKeyFile, IssuerURL string
	// TemplateDigest names the home's supervisor template in every grant.
	TemplateDigest string
	// SupervisorImages are the images the home's rootfs was built from.
	SupervisorImages []string
	// Capabilities are the home's -gvisor-caps, as CAP_ names.
	Capabilities []string
	// Lease is each grant's lease. A running fiber ends when it lapses.
	Lease time.Duration
}

// Home is the fiberd Fibers backend over the plaintext gRPC API.
type Home struct {
	cfg    HomeConfig
	reader client.Reader
	issuer *grant.Issuer

	mu     sync.Mutex
	api    *consumer.Client
	target string
}

var _ Fibers = (*Home)(nil)

// NewHome dials the home. reader must be uncached; Home reads the home Pod
// on every call so a replaced home is never mistaken for the recorded one.
func NewHome(ctx context.Context, reader client.Reader, cfg HomeConfig) (*Home, error) {
	key, err := grant.LoadKey(cfg.IssuerKeyFile)
	if err != nil {
		return nil, err
	}
	h := &Home{cfg: cfg, reader: reader, issuer: &grant.Issuer{Key: key, URL: cfg.IssuerURL}}
	if cfg.Lease <= 0 {
		h.cfg.Lease = 24 * time.Hour
	}
	if _, err := h.client(ctx); err != nil {
		return nil, err
	}
	return h, nil
}

// client returns the API client for the current home. Without a configured
// Target it follows the home Pod's IP, so a replaced home is dialed anew;
// instances recorded on the old home are fenced by its Pod UID.
func (h *Home) client(ctx context.Context) (*consumer.Client, error) {
	target := h.cfg.Target
	if target == "" {
		pod, err := h.pod(ctx)
		if err != nil {
			return nil, err
		}
		target = net.JoinHostPort(pod.Status.PodIP, "8484")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.api != nil && h.target == target {
		return h.api, nil
	}
	api, err := consumer.Dial(ctx, target)
	if err != nil {
		return nil, err
	}
	if h.api != nil {
		_ = h.api.Close()
	}
	h.api, h.target = api, target
	return api, nil
}

func (h *Home) pod(ctx context.Context) (*corev1.Pod, error) {
	pod := &corev1.Pod{}
	if err := h.reader.Get(ctx, h.cfg.Pod, pod); err != nil {
		return nil, err
	}
	if pod.DeletionTimestamp != nil || net.ParseIP(pod.Status.PodIP) == nil {
		return nil, fmt.Errorf("fiberd home %s is not serving", h.cfg.Pod)
	}
	return pod, nil
}

func (h *Home) Home(ctx context.Context) (workspaceprovider.PodReference, map[string]string, error) {
	pod, err := h.pod(ctx)
	if err != nil {
		return workspaceprovider.PodReference{}, nil, err
	}
	return workspaceprovider.PodReference{Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}, pod.Labels, nil
}

func (h *Home) Images() []string { return h.cfg.SupervisorImages }

func (h *Home) Capabilities() []string { return h.cfg.Capabilities }

func (h *Home) Clone(ctx context.Context, g FiberGrant, launch []byte) (Fiber, error) {
	// One fiber per grant: fibers of one grant share a run directory that
	// each can read, write and connect into. Pressure never parks a
	// supervisor, whose memory holds credentials.
	token, err := h.issuer.Mint(core.Grant{
		UID: g.UID, Audience: h.cfg.NodeID, TemplateDigest: h.cfg.TemplateDigest, Tenant: g.Tenant,
		FiberMax: 1, LeaseExpiry: time.Now().Add(h.cfg.Lease),
		Policy: core.Policy{Isolation: core.Untrusted, PSISomeAvg10Shed: 100, PSISomeAvg10Park: 100},
	})
	if err != nil {
		return Fiber{}, err
	}
	api, err := h.client(ctx)
	if err != nil {
		return Fiber{}, err
	}
	f, err := api.Clone(ctx, token, g.Session, 30*time.Second, launch)
	if err != nil {
		return Fiber{}, err
	}
	endpoint, err := url.Parse(f.Endpoint)
	if err != nil || endpoint.Scheme != "tcp" || endpoint.Host == "" {
		return Fiber{}, fmt.Errorf("fiber endpoint %q is not a relayed tcp endpoint", f.Endpoint)
	}
	return Fiber{ID: f.ID, Relay: endpoint.Host, Epoch: int64(f.Fence.Epoch)}, nil
}

func (h *Home) Release(ctx context.Context, fiberID string) error {
	api, err := h.client(ctx)
	if err != nil {
		return err
	}
	if err := api.Release(ctx, fiberID, true); err != nil && !consumer.NotFound(err) {
		return err
	}
	return nil
}

// Running reads the grant from Watch. The home sends one status per admitted
// grant at once and again every status interval, but no batch boundary or
// tombstone: a grant is absent only once a whole batch went by without it,
// seen as a repeated grant, or when the home reports no grant at all.
func (h *Home) Running(ctx context.Context, grantUID string) (int, error) {
	api, err := h.client(ctx)
	if err != nil {
		return 0, err
	}
	wctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	running, complete, seen := -1, false, map[string]bool{}
	err = api.Watch(wctx, func(s consumer.Status) {
		switch {
		case s.GrantUID == grantUID:
			running = int(s.Running)
			cancel()
		case seen[s.GrantUID]:
			complete = true
			cancel()
		default:
			seen[s.GrantUID] = true
		}
	})
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	if err != nil {
		return 0, err
	}
	if running >= 0 {
		return running, nil
	}
	if complete || len(seen) == 0 {
		return 0, nil
	}
	return 0, fmt.Errorf("fiberd home did not report grant %s within one status batch", grantUID)
}

func (h *Home) Challenge(ctx context.Context, relay string) (BootstrapChallenge, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+relay+"/v2/credential-bootstrap", nil)
	if err != nil {
		return BootstrapChallenge{}, err
	}
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	c := &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := c.Do(request)
	if err != nil {
		return BootstrapChallenge{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == http.StatusNotFound {
		return BootstrapChallenge{}, ErrChallengeClosed
	}
	if response.StatusCode != http.StatusOK {
		return BootstrapChallenge{}, fmt.Errorf("supervisor bootstrap challenge is unavailable: %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 65537))
	if err != nil || len(data) > 65536 {
		return BootstrapChallenge{}, fmt.Errorf("supervisor challenge size is invalid")
	}
	var challenge BootstrapChallenge
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&challenge); err != nil {
		return BootstrapChallenge{}, fmt.Errorf("supervisor challenge is invalid")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return BootstrapChallenge{}, fmt.Errorf("supervisor challenge has trailing data")
	}
	return challenge, nil
}
