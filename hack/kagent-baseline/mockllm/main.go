// Copyright (c) 2026. MIT License - see LICENSE file for details.

// mockllm is a deterministic OpenAI-compatible chat completions endpoint for
// the kagent latency baseline. Like the Orka proof's deterministic ACP agent,
// it makes no model call: every request waits MOCK_DELAY (default 2s) and
// returns fixed text. It logs one JSON line when a request arrives, carrying
// the latency marker found in the last user message, so "prompt accepted" is
// timed server-side. Authorization headers are never read or logged.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"time"
)

var marker = regexp.MustCompile(`LATMARK-[A-Za-z0-9-]+`)

type message struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
}

type request struct {
	Model    string    `json:"model"`
	Stream   bool      `json:"stream"`
	Messages []message `json:"messages"`
}

func text(content any) string {
	switch value := content.(type) {
	case string:
		return value
	case []any:
		out := ""
		for _, part := range value {
			if m, ok := part.(map[string]any); ok {
				if s, ok := m["text"].(string); ok {
					out += s
				}
			}
		}
		return out
	}
	return ""
}

func emit(fields map[string]any) {
	fields["time"] = time.Now().UTC().Format(time.RFC3339Nano)
	encoded, _ := json.Marshal(fields)
	fmt.Println(string(encoded))
}

func main() {
	delay := 2 * time.Second
	if raw := os.Getenv("MOCK_DELAY"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			log.Fatal(err)
		}
		delay = parsed
	}
	reply := "kagent baseline fixture completed."
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		received := time.Now()
		var req request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		found := ""
		for i := len(req.Messages) - 1; i >= 0; i-- {
			if req.Messages[i].Role == "user" {
				found = marker.FindString(text(req.Messages[i].Content))
				break
			}
		}
		emit(map[string]any{"msg": "mock request received", "marker": found, "stream": req.Stream, "messages": len(req.Messages)})
		time.Sleep(delay - time.Since(received))
		id := fmt.Sprintf("chatcmpl-%d", received.UnixNano())
		created := received.Unix()
		if req.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			flusher, _ := w.(http.Flusher)
			for _, chunk := range []map[string]any{
				{"id": id, "object": "chat.completion.chunk", "created": created, "model": req.Model,
					"choices": []any{map[string]any{"index": 0, "delta": map[string]any{"role": "assistant", "content": reply}}}},
				{"id": id, "object": "chat.completion.chunk", "created": created, "model": req.Model,
					"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"}},
					"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}},
			} {
				encoded, _ := json.Marshal(chunk)
				fmt.Fprintf(w, "data: %s\n\n", encoded)
				if flusher != nil {
					flusher.Flush()
				}
			}
			fmt.Fprint(w, "data: [DONE]\n\n")
		} else {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "object": "chat.completion", "created": created, "model": req.Model,
				"choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": reply}, "finish_reason": "stop"}},
				"usage":   map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})
		}
		emit(map[string]any{"msg": "mock response sent", "marker": found})
	})
	log.Fatal(http.ListenAndServe(":8080", mux))
}
