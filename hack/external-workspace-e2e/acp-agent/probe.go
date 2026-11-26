// Copyright (c) 2026. MIT License - see LICENSE file for details.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Opt-in prompt markers. Only a Task whose prompt carries them changes the
// fixture's behavior, so every other proof keeps making no network calls.
const (
	// providerProbeMarker makes one model request through the supervisor's
	// session provider proxy, the path a real Codex CLI uses.
	providerProbeMarker = "orka-fixture:provider-probe"
	// holdMarkerPrefix keeps the prompt open for the given seconds after the
	// probe, so a proof can act on the worker while a prompt is accepted.
	holdMarkerPrefix = "orka-fixture:hold-seconds="
	maxHoldSeconds   = 300
)

var holdMarker = regexp.MustCompile(regexp.QuoteMeta(holdMarkerPrefix) + `([0-9]{1,3})\b`)

// promptText joins the text blocks of a session/prompt request.
func promptText(line []byte) string {
	var request struct {
		Params struct {
			Prompt []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"prompt"`
		} `json:"params"`
	}
	if json.Unmarshal(line, &request) != nil {
		return ""
	}
	var text strings.Builder
	for _, block := range request.Params.Prompt {
		if block.Type == "text" {
			text.WriteString(block.Text)
			text.WriteByte('\n')
		}
	}
	return text.String()
}

// holdDuration returns the requested hold, bounded to maxHoldSeconds.
func holdDuration(prompt string) time.Duration {
	match := holdMarker.FindStringSubmatch(prompt)
	if match == nil {
		return 0
	}
	seconds, err := strconv.Atoi(match[1])
	if err != nil || seconds <= 0 {
		return 0
	}
	return time.Duration(min(seconds, maxHoldSeconds)) * time.Second
}

// codexProvider reads the model and session provider-proxy base URL that the
// supervisor projects into CODEX_CONFIG.
func codexProvider(config string) (model, baseURL string, err error) {
	var parsed struct {
		Model          string `json:"model"`
		ModelProvider  string `json:"model_provider"`
		ModelProviders map[string]struct {
			BaseURL string `json:"base_url"`
		} `json:"model_providers"`
	}
	if err := json.Unmarshal([]byte(config), &parsed); err != nil {
		return "", "", fmt.Errorf("CODEX_CONFIG is not JSON")
	}
	provider, ok := parsed.ModelProviders[parsed.ModelProvider]
	if parsed.Model == "" || !ok {
		return "", "", fmt.Errorf("CODEX_CONFIG names no loopback session provider proxy")
	}
	parsedURL, err := url.Parse(provider.BaseURL)
	if err != nil || parsedURL.Scheme != "http" || parsedURL.User != nil || parsedURL.Hostname() != "127.0.0.1" || parsedURL.Port() == "" {
		return "", "", fmt.Errorf("CODEX_CONFIG names no loopback session provider proxy")
	}
	return parsed.Model, strings.TrimRight(provider.BaseURL, "/"), nil
}

// providerProbe sends one Responses request with the session's proxy
// credential. The report names only the session mode and ID; the upstream
// fixture records it so a proof can tell a resumed session from a new one.
// A non-2xx response is an error, so the agent fails the turn.
func providerProbe(mode, sessionID string) (int, error) {
	model, baseURL, err := codexProvider(os.Getenv("CODEX_CONFIG"))
	if err != nil {
		return 0, err
	}
	credential := os.Getenv("CODEX_API_KEY")
	if credential == "" {
		return 0, fmt.Errorf("CODEX_API_KEY is unset")
	}
	body, err := json.Marshal(map[string]any{"model": model, "input": "Orka external workspace fixture provider probe.", "stream": false,
		"metadata": map[string]string{"orka_fixture": "mode=" + mode + ";session=" + sessionID}})
	if err != nil {
		return 0, err
	}
	request, err := http.NewRequest(http.MethodPost, baseURL+"/responses", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	request.Header.Set("Authorization", "Bearer "+credential)
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{Proxy: nil}}
	response, err := client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return response.StatusCode, fmt.Errorf("provider probe HTTP %d", response.StatusCode)
	}
	return response.StatusCode, nil
}
