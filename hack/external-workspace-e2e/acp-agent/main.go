// Copyright (c) 2026. MIT License - see LICENSE file for details.

// The deterministic stdio agent follows the existing Orka security-scan ACP
// fixture protocol. It makes no model, repository, or external network calls.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

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
			result = map[string]any{"protocolVersion": 1, "agentInfo": map[string]any{"name": "external-workspace-fixture", "version": "1"}, "agentCapabilities": map[string]any{"loadSession": true, "mcpCapabilities": map[string]any{"http": true}}, "authMethods": []any{map[string]any{"id": "api-key", "name": "Fixture API key"}}}
		case "authenticate":
		case "session/new":
			sequence++
			sessionID = fmt.Sprintf("external-workspace-%d", sequence)
			result["sessionId"] = sessionID
		case "session/load":
			var params struct {
				Params struct {
					SessionID string `json:"sessionId"`
				} `json:"params"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &params); err != nil {
				os.Exit(2)
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
