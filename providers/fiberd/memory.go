// Copyright (c) 2026. MIT License - see LICENSE file for details.

package fiberd

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"

	"github.com/orka-agents/orka-workspace/providers/fiberd/payload"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
)

// MemoryFibers is an in-memory home for lifecycle conformance. Each fiber's
// supervisor answers its challenge from the launch payload, as the real
// template does after a restore.
type MemoryFibers struct {
	mu       sync.Mutex
	HomePod  workspaceprovider.PodReference
	Labels   map[string]string
	Served   []string
	epoch    int64
	seq      int
	grants   map[string]*memoryFiber
	byRelay  map[string]*memoryFiber
	Releases int
}

type memoryFiber struct {
	id        string
	relay     string
	running   bool
	challenge BootstrapChallenge
}

var _ Fibers = (*MemoryFibers)(nil)

// NewMemoryFibers returns a home serving image.
func NewMemoryFibers(image string) *MemoryFibers {
	return &MemoryFibers{
		HomePod: workspaceprovider.PodReference{Namespace: "fiberd-home", Name: "home", UID: "home-uid"},
		Labels:  map[string]string{"fiberd.io/grant": "home"}, Served: []string{image},
		epoch: 1, grants: map[string]*memoryFiber{}, byRelay: map[string]*memoryFiber{},
	}
}

func (m *MemoryFibers) Clone(_ context.Context, g FiberGrant, launch []byte) (Fiber, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if f := m.grants[g.UID]; f != nil && f.running {
		return Fiber{ID: f.id, Relay: f.relay, Epoch: m.epoch}, nil
	}
	l, err := payload.Decode(launch)
	if err != nil {
		return Fiber{}, err
	}
	m.seq++
	f := &memoryFiber{id: fmt.Sprintf("%s/%d/%d", g.UID, m.epoch, m.seq), relay: fmt.Sprintf("10.0.0.1:%d", 30000+m.seq), running: true}
	f.challenge.Schema = sealedBootstrapSchema
	for _, entry := range l.Env {
		if value, ok := strings.CutPrefix(entry, bootstrapNonceEnv+"="); ok {
			f.challenge.Nonce = value
		}
	}
	f.challenge.BootNonce, f.challenge.PublicKey = random32(), random32()
	f.challenge.Actor.Atespace, f.challenge.Actor.Name, f.challenge.Actor.UID = l.Identity.Namespace, l.Identity.Name, l.Identity.UID
	m.grants[g.UID], m.byRelay[f.relay] = f, f
	return Fiber{ID: f.id, Relay: f.relay, Epoch: m.epoch}, nil
}

func (m *MemoryFibers) Release(_ context.Context, fiberID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, f := range m.grants {
		if f.id == fiberID && f.running {
			f.running = false
			m.Releases++
		}
	}
	return nil
}

func (m *MemoryFibers) Running(_ context.Context, grantUID string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if f := m.grants[grantUID]; f != nil && f.running {
		return 1, nil
	}
	return 0, nil
}

func (m *MemoryFibers) Challenge(_ context.Context, relay string) (BootstrapChallenge, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f := m.byRelay[relay]
	if f == nil || !f.running {
		return BootstrapChallenge{}, fmt.Errorf("connection refused")
	}
	return f.challenge, nil
}

func (m *MemoryFibers) Home(context.Context) (workspaceprovider.PodReference, map[string]string, error) {
	return m.HomePod, m.Labels, nil
}

func (m *MemoryFibers) Images() []string { return m.Served }

func random32() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}
