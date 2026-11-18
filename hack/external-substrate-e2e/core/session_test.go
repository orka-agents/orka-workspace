// Copyright (c) 2026. MIT License - see LICENSE file for details.

package main

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func runtimePoolWithConditions(conditions ...any) *unstructured.Unstructured {
	pool := &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"name": "acp-ws-codex"}}}
	if len(conditions) != 0 {
		pool.Object["status"] = map[string]any{"conditions": conditions}
	}
	return pool
}

func TestObserveTelemetryReadyReplacesTheLastObservation(t *testing.T) {
	var seen telemetryReady
	projected := map[string]any{"type": "TelemetryReady", "status": "True", "reason": "TelemetryProjected", "message": "configured"}
	if err := observeTelemetryReady(runtimePoolWithConditions(projected), &seen); err != nil {
		t.Fatal(err)
	}
	if err := requireTelemetryReady(seen, "pool"); err != nil {
		t.Fatalf("projected condition rejected: %v", err)
	}
	// A later poll without the condition must not keep the earlier True.
	if err := observeTelemetryReady(runtimePoolWithConditions(map[string]any{"type": "AdmissionReady", "status": "True"}), &seen); err != nil {
		t.Fatal(err)
	}
	if err := requireTelemetryReady(seen, "pool"); err == nil {
		t.Fatalf("absent condition kept an earlier observation: %+v", seen)
	}
	if seen.RuntimePool != "acp-ws-codex" {
		t.Fatalf("observation lost the RuntimePool name: %+v", seen)
	}
	limited := map[string]any{"type": "TelemetryReady", "status": "False", "reason": "NativeEnvironmentLimit", "message": "no room"}
	if err := observeTelemetryReady(runtimePoolWithConditions(limited), &seen); err == nil {
		t.Fatal("NativeEnvironmentLimit was accepted")
	}
}
