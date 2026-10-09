// Copyright (c) 2026. MIT License - see LICENSE file for details.

// This listener proves provider materialization before credential bootstrap.
// Full RuntimeSession proof uses Orka's actual ACP supervisor instead.
package main

import (
	"log"
	"net/http"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"fixture":"external-workspace","credentials":"unseeded"}`))
	})
	log.Fatal(http.ListenAndServe(":8080", mux))
}
