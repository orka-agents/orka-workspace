// Copyright (c) 2026. MIT License - see LICENSE file for details.

// Package payload is the Clone payload the fiberd provider hands its gVisor
// template. It carries the admitted public supervisor launch: the native
// identity Orka binds bootstrap credentials to, plus the container command,
// arguments and environment. It never carries a credential.
package payload

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	// Version is the only payload version the template accepts.
	Version = 1
	// MaxBytes is fiberd's Clone payload limit.
	MaxBytes = 4096
)

// Identity is the native process identity the supervisor reads from its
// identity projection and binds into its sealed bootstrap challenge.
type Identity struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
}

// Launch is one supervisor incarnation's public launch definition.
type Launch struct {
	Version  int      `json:"v"`
	Identity Identity `json:"identity"`
	Command  []string `json:"command,omitempty"`
	Args     []string `json:"args,omitempty"`
	Env      []string `json:"env,omitempty"`
	// Probe asks the template to record isolation and egress diagnostics.
	Probe bool `json:"probe,omitempty"`
}

func (l Launch) Validate() error {
	if l.Version != Version {
		return fmt.Errorf("unsupported payload version %d", l.Version)
	}
	if strings.TrimSpace(l.Identity.Namespace) == "" || strings.TrimSpace(l.Identity.Name) == "" || strings.TrimSpace(l.Identity.UID) == "" {
		return errors.New("payload identity is incomplete")
	}
	for _, entry := range l.Env {
		if name, _, ok := strings.Cut(entry, "="); !ok || name == "" {
			return fmt.Errorf("invalid environment entry %q", entry)
		}
	}
	return nil
}

// Encode returns JSON, gzipped when plain JSON exceeds MaxBytes.
func Encode(l Launch) ([]byte, error) {
	if err := l.Validate(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(l)
	if err != nil {
		return nil, err
	}
	if len(data) <= MaxBytes {
		return data, nil
	}
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, err
	}
	if _, err := zw.Write(data); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	if buf.Len() > MaxBytes {
		return nil, fmt.Errorf("launch payload is %d bytes compressed, limit %d", buf.Len(), MaxBytes)
	}
	return buf.Bytes(), nil
}

// Decode accepts plain or gzipped JSON and rejects unknown fields.
func Decode(data []byte) (Launch, error) {
	if len(data) > MaxBytes {
		return Launch{}, fmt.Errorf("payload is %d bytes, limit %d", len(data), MaxBytes)
	}
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		zr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return Launch{}, err
		}
		plain, err := io.ReadAll(io.LimitReader(zr, 1<<20))
		if err != nil {
			return Launch{}, err
		}
		data = plain
	}
	var l Launch
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&l); err != nil {
		return Launch{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Launch{}, errors.New("payload has trailing data")
	}
	return l, l.Validate()
}
