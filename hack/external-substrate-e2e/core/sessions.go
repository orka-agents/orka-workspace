// Copyright (c) 2026. MIT License - see LICENSE file for details.

package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// serviceAccountTokenPath is the Job's own projected identity. Core's API
// authenticates it and requires it to be bound to the run namespace.
const serviceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"

// cleanupSessions deletes, through Core's public API, every Session that a
// Task in the run namespace references. A session member Task keeps its
// frozen authority until Session cleanup records its runtime cleanup receipt,
// so core/cleanup.sh runs this before deleting the Tasks. The token is read
// per request and never printed.
func cleanupSessions(ctx context.Context, c client.Client, namespace, endpoint string) (map[string]any, error) {
	tasks := &unstructured.UnstructuredList{}
	tasks.SetAPIVersion("core.orka.ai/v1alpha1")
	tasks.SetKind("TaskList")
	if err := c.List(ctx, tasks, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	names := map[string]bool{}
	for i := range tasks.Items {
		if name := str(&tasks.Items[i], "spec", "sessionRef", "name"); name != "" {
			names[name] = true
		}
	}
	sessions := make([]string, 0, len(names))
	for name := range names {
		sessions = append(sessions, name)
	}
	sort.Strings(sessions)
	results := map[string]int{}
	httpClient := &http.Client{Timeout: 30 * time.Second}
	for _, name := range sessions {
		target := strings.TrimRight(endpoint, "/") + "/api/v1/sessions/" + url.PathEscape(name) + "?namespace=" + url.QueryEscape(namespace)
		for attempt := 1; ; attempt++ {
			token, err := os.ReadFile(serviceAccountTokenPath)
			if err != nil {
				return nil, fmt.Errorf("read the Job's ServiceAccount token: %w", err)
			}
			request, err := http.NewRequestWithContext(ctx, http.MethodDelete, target, nil)
			if err != nil {
				return nil, err
			}
			request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
			response, err := httpClient.Do(request)
			code := 0
			if err == nil {
				code = response.StatusCode
				_ = response.Body.Close()
			}
			fmt.Printf("DELETE session %s attempt %d -> %d\n", name, attempt, code)
			// 409: the Session still has active or unsettled work; settlement
			// is asynchronous, so retry for a bounded time.
			if err == nil && (code == http.StatusNoContent || code == http.StatusNotFound) {
				results[name] = code
				break
			}
			if attempt >= 60 || (err == nil && code != http.StatusConflict && code < 500) {
				return nil, fmt.Errorf("delete Session %s: status %d, %v", name, code, err)
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(5 * time.Second):
			}
		}
	}
	return map[string]any{"namespace": namespace, "sessions": results}, nil
}
