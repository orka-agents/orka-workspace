// Copyright (c) 2026. MIT License - see LICENSE file for details.

// The deterministic stdio agent follows the existing Orka security-scan ACP
// fixture protocol. It makes no model, repository, or external network calls.
//
// Orka captures a native Codex session before a Session suspend and restores
// it only through session/resume, so the agent advertises
// sessionCapabilities.resume and, when it runs with a CODEX_HOME, publishes a
// real Codex 0.160.0 rollout (testdata/rollout.jsonl, sessionkit's "basic"
// compatibility fixture, MIT) whose UUIDv7 thread ID is the ACP session ID.
package main

import (
	"bufio"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// fixtureThreadID is the thread of the embedded Codex rollout.
const fixtureThreadID = "01a10020-1222-76e3-977d-d5165792ae72"

//go:embed testdata/rollout.jsonl
var fixtureRollout []byte

// publishRollout writes the embedded rollout under CODEX_HOME at Codex's
// canonical sessions/YYYY/MM/DD path and reports whether it did.
func publishRollout() bool {
	home := os.Getenv("CODEX_HOME")
	if home == "" {
		return false
	}
	dir := filepath.Join(home, "sessions", "2026", "10", "02")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return false
	}
	return os.WriteFile(filepath.Join(dir, "rollout-2026-10-02T21-57-44-"+fixtureThreadID+".jsonl"), fixtureRollout, 0o600) == nil
}

// safeSessionID accepts only the letters, digits and hyphens of a session ID, so
// the ID can be neither a path nor a glob pattern.
func safeSessionID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, r := range id {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' {
			return false
		}
	}
	return true
}

// nativeSessionRestored reports whether a session can be resumed. Without a
// CODEX_HOME the fixture keeps no native session, so any ID is accepted as
// before. With one, the session's rollout must be present under
// sessions/YYYY/MM/DD, as Codex requires: a resume that restored nothing must
// not succeed.
func nativeSessionRestored(home, sessionID string) bool {
	if home == "" {
		return true
	}
	if !safeSessionID(sessionID) {
		return false
	}
	matches, err := filepath.Glob(filepath.Join(home, "sessions", "*", "*", "*", "rollout-*-"+sessionID+".jsonl"))
	if err != nil {
		return false
	}
	for _, match := range matches {
		if info, err := os.Stat(match); err == nil && info.Mode().IsRegular() && info.Size() > 0 {
			return true
		}
	}
	return false
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64<<10), 2<<20)
	writer := bufio.NewWriter(os.Stdout)
	write := func(value any) {
		if err := json.NewEncoder(writer).Encode(value); err != nil {
			os.Exit(2)
		}
		if err := writer.Flush(); err != nil {
			os.Exit(2)
		}
	}
	var sequence uint64
	sessionID := ""
	for scanner.Scan() {
		var message struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &message); err != nil {
			os.Exit(2)
		}
		result := map[string]any{}
		switch message.Method {
		case "initialize":
			result = map[string]any{"protocolVersion": 1, "agentInfo": map[string]any{"name": "external-workspace-fixture", "version": "1"}, "agentCapabilities": map[string]any{"loadSession": true, "sessionCapabilities": map[string]any{"resume": map[string]any{}}, "mcpCapabilities": map[string]any{"http": true}}, "authMethods": []any{map[string]any{"id": "api-key", "name": "Fixture API key"}}}
		case "authenticate":
		case "session/new":
			sequence++
			sessionID = fmt.Sprintf("external-workspace-%d", sequence)
			if sequence == 1 && publishRollout() {
				sessionID = fixtureThreadID
			}
			result["sessionId"] = sessionID
		case "session/load", "session/resume":
			var params struct {
				Params struct {
					SessionID string `json:"sessionId"`
				} `json:"params"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &params); err != nil {
				os.Exit(2)
			}
			if !nativeSessionRestored(os.Getenv("CODEX_HOME"), params.Params.SessionID) {
				if len(message.ID) != 0 {
					write(map[string]any{"jsonrpc": "2.0", "id": message.ID, "error": map[string]any{"code": -32002, "message": "session not found", "data": map[string]any{"sessionId": params.Params.SessionID}}})
				}
				continue
			}
			sessionID = params.Params.SessionID
		case "session/prompt":
			// Leave time for the independent proof client to observe Serving
			// and verify the selected Pod before terminal settlement retires it.
			time.Sleep(2 * time.Second)
			write(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{"sessionId": sessionID, "update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "External workspace fixture completed."}}}})
			result["stopReason"] = "end_turn"
		case "session/cancel":
			continue
		default:
			if len(message.ID) != 0 {
				write(map[string]any{"jsonrpc": "2.0", "id": message.ID, "error": map[string]any{"code": -32601, "message": "unsupported fixture method"}})
			}
			continue
		}
		if len(message.ID) != 0 {
			write(map[string]any{"jsonrpc": "2.0", "id": message.ID, "result": result})
		}
	}
	if scanner.Err() != nil {
		os.Exit(2)
	}
}
