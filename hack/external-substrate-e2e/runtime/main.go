// Copyright (c) 2026. MIT License - see LICENSE file for details.

// A deterministic process fixture for native Data capture and cold import.
// It publishes a real per-process public challenge but does not execute ACP.
package main

import (
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		log.Fatal(err)
	}
	boot := make([]byte, 32)
	if _, err := rand.Read(boot); err != nil {
		log.Fatal(err)
	}
	readIdentity := func(name string) string {
		data, err := os.ReadFile(filepath.Join("/run/orka-substrate-identity", name))
		if err != nil {
			log.Fatal(err)
		}
		return strings.TrimSpace(string(data))
	}
	challenge := struct {
		Schema    string `json:"schema"`
		Nonce     string `json:"nonce"`
		BootNonce string `json:"bootNonce"`
		PublicKey string `json:"publicKey"`
		Actor     struct {
			Atespace string `json:"atespace"`
			Name     string `json:"name"`
			UID      string `json:"uid"`
		} `json:"actor"`
	}{Schema: "orka.harness.v2/sealed-bootstrap/v1", Nonce: os.Getenv("ORKA_ACP_CREDENTIAL_BOOTSTRAP_NONCE"),
		BootNonce: base64.RawURLEncoding.EncodeToString(boot), PublicKey: base64.RawURLEncoding.EncodeToString(key.PublicKey().Bytes())}
	challenge.Actor.Atespace = readIdentity("atespace")
	challenge.Actor.Name = readIdentity("name")
	challenge.Actor.UID = readIdentity("uid")
	markerPath := filepath.Join(os.Getenv("ORKA_ACP_DURABLE_WORKSPACE_DIR"), "shared", "live-marker")
	if os.Getenv("ORKA_ACP_CREDENTIAL_BOOTSTRAP_NONCE") == os.Getenv("SUBSTRATE_E2E_INITIALIZE_NONCE") {
		if err := os.MkdirAll(filepath.Dir(markerPath), 0700); err != nil {
			log.Fatal(err)
		}
		file, err := os.OpenFile(markerPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			log.Fatal(err)
		}
		if _, err := file.WriteString(os.Getenv("SUBSTRATE_E2E_MARKER")); err != nil {
			log.Fatal(err)
		}
		if err := file.Sync(); err != nil {
			log.Fatal(err)
		}
		if err := file.Close(); err != nil {
			log.Fatal(err)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/v2/credential-bootstrap", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(challenge)
	})
	mux.HandleFunc("/proof/data", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		data, err := os.ReadFile(markerPath)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"marker": string(data), "bootNonce": challenge.BootNonce})
	})
	log.Fatal(http.ListenAndServe(":80", mux))
}
