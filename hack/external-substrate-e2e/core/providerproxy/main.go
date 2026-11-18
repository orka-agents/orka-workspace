// Copyright (c) 2026. MIT License - see LICENSE file for details.

// The fixture provider-auth proxy stands in for Orka's provider proxy origin
// in the Core proof. It accepts a request only when its Authorization header
// carries Core's provider token, which the runtime never holds: the Agent
// Substrate egress gateway must have replaced the runtime's public
// placeholder. It never logs or returns header values, only counts by class
// and the bounded session mode/ID report of the deterministic agent.
package main

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

// placeholder is Orka's public ProviderTokenEgressInjectionPlaceholder.
const placeholder = "orka-egress-gateway-injects-provider-credential"

var sessionReport = regexp.MustCompile(`^mode=(new|load|resume);session=[A-Za-z0-9-]{1,128}$`)

type observations struct {
	mu sync.Mutex
	// Injected counts requests bearing exactly Core's provider token.
	Injected int `json:"injected"`
	// Refused counts injected requests that were not the agent's Responses
	// call (POST /v1/responses) and so were answered 401.
	Refused int `json:"refused"`
	// Placeholder counts requests that still carried the runtime placeholder.
	Placeholder int `json:"placeholder"`
	Missing     int `json:"missing"`
	Other       int `json:"other"`
	// Sessions lists the agent's session reports on accepted requests.
	Sessions []string `json:"sessions"`
}

func classify(header string, token []byte) string {
	if header == "" {
		return "missing"
	}
	value, ok := strings.CutPrefix(header, "Bearer ")
	switch {
	case ok && subtle.ConstantTimeCompare([]byte(value), token) == 1:
		return "injected"
	case ok && value == placeholder:
		return "placeholder"
	default:
		return "other"
	}
}

func main() {
	raw, err := os.ReadFile(os.Getenv("TOKEN_FILE"))
	token := bytes.TrimSpace(raw)
	if err != nil || len(token) == 0 {
		log.Fatal("TOKEN_FILE names no provider token")
	}
	seen := &observations{Sessions: []string{}}
	api := seen.handler(token)
	observe := http.NewServeMux()
	observe.HandleFunc("/observations", func(w http.ResponseWriter, _ *http.Request) {
		seen.mu.Lock()
		defer seen.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(seen)
	})
	observe.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	go func() {
		log.Fatal((&http.Server{Addr: ":8081", Handler: observe, ReadHeaderTimeout: 10 * time.Second}).ListenAndServe())
	}()
	log.Fatal((&http.Server{Addr: ":8080", Handler: api, ReadHeaderTimeout: 10 * time.Second}).ListenAndServe())
}

// handler serves the provider origin. Only the agent's Responses call with
// Core's token is answered 200 and records its session report.
func (seen *observations) handler(token []byte) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		class := classify(r.Header.Get("Authorization"), token)
		var body struct {
			Model    string            `json:"model"`
			Metadata map[string]string `json:"metadata"`
		}
		_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body)
		accepted := class == "injected" && r.Method == http.MethodPost && r.URL.Path == "/v1/responses"
		seen.mu.Lock()
		switch class {
		case "injected":
			seen.Injected++
			if !accepted {
				seen.Refused++
			} else if report := body.Metadata["orka_fixture"]; sessionReport.MatchString(report) && len(seen.Sessions) < 32 {
				seen.Sessions = append(seen.Sessions, report)
			}
		case "placeholder":
			seen.Placeholder++
		case "missing":
			seen.Missing++
		default:
			seen.Other++
		}
		seen.mu.Unlock()
		log.Printf("provider request method=%s path=%s authorization=%s", r.Method, r.URL.Path, class)
		w.Header().Set("Content-Type", "application/json")
		if !accepted {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"message":"fixture provider proxy refused the request","type":"invalid_request_error"}}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "resp_orka_fixture", "object": "response", "status": "completed", "model": body.Model,
			"output": []any{map[string]any{"type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "fixture"}}}}})
	})
}
