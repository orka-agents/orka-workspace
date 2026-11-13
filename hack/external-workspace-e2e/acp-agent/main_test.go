// Copyright (c) 2026. MIT License - see LICENSE file for details.

package main

import (
	"os"
	"path/filepath"
	"testing"
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
