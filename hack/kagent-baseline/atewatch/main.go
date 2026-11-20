// Copyright (c) 2026. MIT License - see LICENSE file for details.

// atewatch prints one JSON line whenever an Actor of the watched install
// changes state or worker, polling the native ate-api read API every
// WATCH_INTERVAL (default 100ms). It runs in-cluster as kagent's bootstrap
// owner client (system:serviceaccount:ate-system:ate-client) with a projected
// token, and reports only public identities and states.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	pb "github.com/orka-agents/orka-workspace/providers/substrate/nativepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// tokenCredentials reads the projected token per request, so rotation is
// honored and the token is never logged.
type tokenCredentials struct{ path string }

func (t tokenCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	token, err := os.ReadFile(t.path)
	if err != nil {
		return nil, err
	}
	return map[string]string{"authorization": "Bearer " + strings.TrimSpace(string(token))}, nil
}

func (tokenCredentials) RequireTransportSecurity() bool { return true }

func dial(endpoint, caFile, tokenFile string) (*grpc.ClientConn, error) {
	ca, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("CA file has no certificates")
	}
	return grpc.NewClient(endpoint, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots})),
		grpc.WithPerRPCCredentials(tokenCredentials{path: tokenFile}))
}

type actorView struct {
	State  string `json:"state"`
	Worker string `json:"worker"`
	UID    string `json:"uid"`
}

func emit(fields map[string]any) {
	fields["time"] = time.Now().UTC().Format(time.RFC3339Nano)
	encoded, _ := json.Marshal(fields)
	fmt.Println(string(encoded))
}

func main() {
	interval := 100 * time.Millisecond
	if raw := os.Getenv("WATCH_INTERVAL"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		interval = parsed
	}
	conn, err := dial(os.Getenv("ATE_API_ENDPOINT"), os.Getenv("ATE_API_CA_FILE"), os.Getenv("ATE_API_TOKEN_FILE"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial ate-api:", err)
		os.Exit(1)
	}
	defer conn.Close()
	control := pb.NewControlClient(conn)
	seen := map[string]actorView{}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		spaces, err := control.ListAtespaces(ctx, &pb.ListAtespacesRequest{Options: &pb.ListOptions{PageSize: 1000}})
		if err != nil {
			emit(map[string]any{"msg": "list atespaces failed", "error": err.Error()})
			cancel()
			time.Sleep(time.Second)
			continue
		}
		current := map[string]bool{}
		for _, space := range spaces.GetAtespaces() {
			actors, err := control.ListActors(ctx, &pb.ListActorsRequest{Atespace: space.GetMetadata().GetName(), Options: &pb.ListOptions{PageSize: 1000}})
			if err != nil {
				emit(map[string]any{"msg": "list actors failed", "atespace": space.GetMetadata().GetName(), "error": err.Error()})
				continue
			}
			for _, actor := range actors.GetActors() {
				key := actor.GetMetadata().GetAtespace() + "/" + actor.GetMetadata().GetName()
				view := actorView{State: actor.GetStatus().GetState().String(), Worker: actor.GetStatus().GetWorkerAssignment().GetWorkerPod(), UID: actor.GetMetadata().GetUid()}
				current[key] = true
				if seen[key] != view {
					seen[key] = view
					emit(map[string]any{"msg": "actor", "actor": key, "uid": view.UID, "state": view.State, "workerPod": view.Worker})
				}
			}
		}
		for key := range seen {
			if !current[key] {
				delete(seen, key)
				emit(map[string]any{"msg": "actor", "actor": key, "state": "ABSENT"})
			}
		}
		cancel()
		time.Sleep(interval)
	}
}
