// Copyright (c) 2026. MIT License - see LICENSE file for details.

package workspaceagent

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestClientOperationIDsAreEscapedOnceWithEndpointPrefix(t *testing.T) {
	for _, prefix := range []string{"", "/gateway", "/gateway/", "/gateway%2Ftenant/", "/gateway%25token/"} {
		t.Run("prefix="+prefix, func(t *testing.T) {
			t.Parallel()
			base := strings.TrimSuffix(prefix, "/")
			type receivedRequest struct {
				method, uri, host, query, operationID string
			}
			received := make(chan receivedRequest, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost && r.URL.EscapedPath() == base+ExecPath {
					var request ExecRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						http.Error(w, "invalid operation request", http.StatusBadRequest)
						return
					}
					received <- receivedRequest{r.Method, r.RequestURI, r.Host, r.URL.RawQuery, request.OperationID}
					_ = json.NewEncoder(w).Encode(ExecResponse{Versioned: NewVersioned(), OperationID: request.OperationID, State: OperationStateRunning})
					return
				}
				escapedID, found := strings.CutPrefix(r.URL.EscapedPath(), base+ExecStatusPrefix)
				if !found {
					http.NotFound(w, r)
					return
				}
				if r.Method == http.MethodPost {
					escapedID, found = strings.CutSuffix(escapedID, "/cancel")
					if !found {
						http.NotFound(w, r)
						return
					}
				}
				id, err := url.PathUnescape(escapedID)
				if err != nil {
					http.Error(w, "invalid operation ID", http.StatusBadRequest)
					return
				}
				received <- receivedRequest{r.Method, r.RequestURI, r.Host, r.URL.RawQuery, id}
				if r.Method == http.MethodPost {
					_ = json.NewEncoder(w).Encode(CancelResponse{Versioned: NewVersioned(), OperationID: id})
				} else {
					_ = json.NewEncoder(w).Encode(ExecResponse{Versioned: NewVersioned(), OperationID: id})
				}
			}))
			defer server.Close()
			c, err := NewClient(ClientConfig{Endpoint: server.URL + prefix, AllowInsecure: true})
			if err != nil {
				t.Fatal(err)
			}
			serverURL, err := url.Parse(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			credentials := AttachmentCredentials{Bearer: "attachment-token", WorkspaceUID: "workspace-uid", Epoch: 1}
			for _, id := range []string{"op/segment", "op%2Fencoded", "op?query=1#fragment", "操作/δοκιμή%", "//foreign.invalid/path", "https://foreign.invalid/path?x=1", " op ", "\top\n", "\u00a0op\u00a0", " \t\n"} {
				request := ExecRequest{OperationID: id, Command: []string{"true"}}
				response, err := c.Exec(t.Context(), credentials, request)
				if err != nil {
					t.Fatal(err)
				}
				if response.OperationID != id || request.OperationID != id || request.ProtocolVersion != "" {
					t.Fatalf("Exec did not preserve operation ID or input request: response=%#v request=%#v", response, request)
				}
				created := <-received
				if created.method != http.MethodPost || created.uri != base+ExecPath || created.operationID != id || created.host != serverURL.Host || created.query != "" {
					t.Fatalf("Exec request = %#v; want original operation %q", created, id)
				}
				for _, method := range []string{http.MethodGet, http.MethodPost} {
					t.Run(method+"/"+id, func(t *testing.T) {
						expectedURI := base + ExecStatusPrefix + url.PathEscape(id)
						var addressedID string
						if method == http.MethodPost {
							response, err := c.Cancel(t.Context(), credentials, id)
							if err != nil {
								t.Fatal(err)
							}
							addressedID = response.OperationID
							expectedURI += "/cancel"
						} else {
							response, err := c.ExecStatus(t.Context(), credentials, id)
							if err != nil {
								t.Fatal(err)
							}
							addressedID = response.OperationID
						}
						request := <-received
						if addressedID != id {
							t.Fatalf("operation endpoint addressed %q instead of %q", addressedID, id)
						}
						if request.method != method || request.uri != expectedURI || request.operationID != id || request.host != serverURL.Host || request.query != "" {
							t.Fatalf("request = %#v; want exact URI %q, host %q and operation %q", request, expectedURI, serverURL.Host, id)
						}
					})
				}
			}
		})
	}
}
