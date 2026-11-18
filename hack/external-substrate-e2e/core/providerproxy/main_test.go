// Copyright (c) 2026. MIT License - see LICENSE file for details.

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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

func TestHandlerRecordsSessionsOnlyForTheAcceptedResponsesCall(t *testing.T) {
	token := []byte("core-provider-token")
	seen := &observations{Sessions: []string{}}
	handler := seen.handler(token)
	send := func(method, path, authorization string) int {
		body := `{"model":"gpt-5.4","metadata":{"orka_fixture":"mode=new;session=s-1"}}`
		request := httptest.NewRequest(method, path, strings.NewReader(body))
		request.Header.Set("Authorization", authorization)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder.Code
	}
	if code := send(http.MethodPost, "/v1/responses", "Bearer core-provider-token"); code != http.StatusOK {
		t.Fatalf("accepted call = %d, want 200", code)
	}
	if code := send(http.MethodPost, "/v1/chat/completions", "Bearer core-provider-token"); code != http.StatusUnauthorized {
		t.Fatalf("wrong path = %d, want 401", code)
	}
	if code := send(http.MethodGet, "/v1/responses", "Bearer core-provider-token"); code != http.StatusUnauthorized {
		t.Fatalf("wrong method = %d, want 401", code)
	}
	if code := send(http.MethodPost, "/v1/responses", "Bearer "+placeholder); code != http.StatusUnauthorized {
		t.Fatalf("placeholder = %d, want 401", code)
	}
	if seen.Injected != 3 || seen.Refused != 2 || seen.Placeholder != 1 || len(seen.Sessions) != 1 || seen.Sessions[0] != "mode=new;session=s-1" {
		t.Fatalf("observations = %+v", seen)
	}
}
