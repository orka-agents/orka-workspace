// Copyright (c) 2026. MIT License - see LICENSE file for details.

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNativeSessionRestoredRequiresTheRollout(t *testing.T) {
	home := t.TempDir()
	if !nativeSessionRestored("", fixtureThreadID) {
		t.Fatal("without a CODEX_HOME the fixture must accept a resume")
	}
	if nativeSessionRestored(home, fixtureThreadID) {
		t.Fatal("a resume must fail when the session's rollout is missing")
	}
	t.Setenv("CODEX_HOME", home)
	if !publishRollout() {
		t.Fatal("publishRollout did not write the rollout")
	}
	if !nativeSessionRestored(home, fixtureThreadID) {
		t.Fatal("a resume must succeed once the rollout is restored")
	}
	for _, id := range []string{"", "external-workspace-1", "../" + fixtureThreadID, "*", "01a10020-*", fixtureThreadID[len(fixtureThreadID)-12:], fixtureThreadID[9:], "57-44-" + fixtureThreadID} {
		if nativeSessionRestored(home, id) {
			t.Fatalf("session %q must not resume", id)
		}
	}
	empty := filepath.Join(home, "sessions", "2026", "10", "03", "rollout-2026-10-03T00-00-00-empty-session.jsonl")
	if err := os.MkdirAll(filepath.Dir(empty), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if nativeSessionRestored(home, "empty-session") {
		t.Fatal("an empty rollout must not count as a restored session")
	}
}

func TestPromptMarkers(t *testing.T) {
	line := []byte(`{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"s","prompt":[{"type":"text","text":"run orka-fixture:provider-probe orka-fixture:hold-seconds=45"},{"type":"image","text":"orka-fixture:hold-seconds=9"}]}}`)
	prompt := promptText(line)
	if !strings.Contains(prompt, providerProbeMarker) || holdDuration(prompt) != 45*time.Second {
		t.Fatalf("markers were not parsed from text blocks: %q", prompt)
	}
	if holdDuration("orka-fixture:hold-seconds=999") != maxHoldSeconds*time.Second || holdDuration("orka-fixture:hold-seconds=0") != 0 || holdDuration("no marker") != 0 {
		t.Fatal("hold must be bounded and opt-in")
	}
	if promptText([]byte(`not json`)) != "" {
		t.Fatal("a malformed prompt carries no markers")
	}
}

func TestCodexProviderRequiresTheLoopbackSessionProxy(t *testing.T) {
	model, base, err := codexProvider(`{"model":"gpt-5.4","model_provider":"orka","model_providers":{"orka":{"base_url":"http://127.0.0.1:4567/_orka/provider/r/v1/"}}}`)
	if err != nil || model != "gpt-5.4" || base != "http://127.0.0.1:4567/_orka/provider/r/v1" {
		t.Fatalf("codexProvider = %q %q %v", model, base, err)
	}
	for _, config := range []string{``, `{}`, `{"model":"m","model_provider":"orka","model_providers":{"orka":{"base_url":"http://example.com"}}}`, `{"model_provider":"orka","model_providers":{"orka":{"base_url":"http://127.0.0.1:1"}}}`} {
		if _, _, err := codexProvider(config); err == nil {
			t.Fatalf("config %q must be refused", config)
		}
	}
}

func TestProviderProbeSendsTheSessionCredentialAndMode(t *testing.T) {
	var got struct {
		authorization string
		body          map[string]any
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.authorization = r.Header.Get("Authorization")
		if r.URL.Path != "/_orka/provider/r/v1/responses" || json.NewDecoder(r.Body).Decode(&got.body) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	t.Setenv("CODEX_CONFIG", `{"model":"gpt-5.4","model_provider":"orka","model_providers":{"orka":{"base_url":"`+server.URL+`/_orka/provider/r/v1"}}}`)
	t.Setenv("CODEX_API_KEY", "session-credential")
	code, err := providerProbe("resume", fixtureThreadID)
	if err != nil || code != http.StatusOK {
		t.Fatalf("probe = %d %v", code, err)
	}
	metadata, _ := got.body["metadata"].(map[string]any)
	if got.authorization != "Bearer session-credential" || got.body["model"] != "gpt-5.4" || metadata["orka_fixture"] != "mode=resume;session="+fixtureThreadID {
		t.Fatalf("probe request = %q %v", got.authorization, got.body)
	}
}
