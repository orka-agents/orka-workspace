package substrate

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNativeAuthenticationSelectionAndRouterConfiguration(t *testing.T) {
	config := Config{APIEndpoint: "api.native.svc:443", CAFile: "ca.crt", BearerTokenFile: "token", RouterEndpoint: "https://atenet-router.ate-system.svc:443", RouterCAFile: "router-ca.pem", RouterCertFile: "identity.pem", RouterKeyFile: "identity.pem"}
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"plaintext", "noIdentity", "bothIdentities", "halfPair", "cleartextRouter", "routerPath", "routerCredentials", "routerQuery", "noRouter", "noRouterCA", "noRouterIdentity", "negativeBoot"} {
		t.Run(change, func(t *testing.T) {
			c := config
			switch change {
			case "plaintext":
				c.CAFile = ""
			case "noIdentity":
				c.BearerTokenFile = ""
			case "bothIdentities":
				c.CertFile = "client.crt"
				c.KeyFile = "client.key"
			case "halfPair":
				c.CertFile = "client.crt"
			case "cleartextRouter":
				c.RouterEndpoint = "http://atenet-router.ate-system.svc"
			case "routerPath":
				c.RouterEndpoint = "https://atenet-router.ate-system.svc/actors"
			case "routerCredentials":
				c.RouterEndpoint = "https://user:secret@atenet-router.ate-system.svc"
			case "routerQuery":
				c.RouterEndpoint = "https://atenet-router.ate-system.svc?actor=x"
			case "noRouter":
				c.RouterEndpoint = ""
			case "noRouterCA":
				c.RouterCAFile = ""
			case "noRouterIdentity":
				c.RouterKeyFile = ""
			case "negativeBoot":
				c.BootTimeout = -1
			}
			if c.Validate() == nil {
				t.Fatal("unsafe native transport admitted")
			}
		})
	}
}
func TestBearerIdentityReloadsProjectedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity")
	credentials := bearerCredentials{path: path}
	for _, value := range []string{"first-test-token", "rotated-test-token"} {
		if err := os.WriteFile(path, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
		metadata, err := credentials.GetRequestMetadata(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if metadata["authorization"] != "Bearer "+value {
			t.Fatal("native identity did not reload on rotation")
		}
	}
	for _, value := range []string{"", "embedded token", "embedded\nline"} {
		if err := os.WriteFile(path, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := credentials.GetRequestMetadata(t.Context()); err == nil {
			t.Fatal("malformed native identity accepted")
		}
	}
	if !credentials.RequireTransportSecurity() {
		t.Fatal("native control identity accepted without TLS")
	}
}
