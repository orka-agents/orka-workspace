// Copyright (c) 2026. MIT License - see LICENSE file for details.

package main

import "testing"

func TestClassifyAcceptsOnlyTheInjectedToken(t *testing.T) {
	token := []byte("core-provider-token")
	for header, want := range map[string]string{
		"":                           "missing",
		"Bearer core-provider-token": "injected",
		"Bearer " + placeholder:      "placeholder",
		"Bearer core-provider-toke":  "other",
		"bearer core-provider-token": "other",
		"core-provider-token":        "other",
	} {
		if got := classify(header, token); got != want {
			t.Fatalf("classify(%q) = %s, want %s", header, got, want)
		}
	}
	if !sessionReport.MatchString("mode=resume;session=01a10020-1222-76e3-977d-d5165792ae72") || sessionReport.MatchString("mode=resume;session=../x") {
		t.Fatal("session report validation differs")
	}
}
