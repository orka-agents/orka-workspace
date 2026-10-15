// Copyright (c) 2026. MIT License - see LICENSE file for details.

package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// The socket must be connectable by fibers from the moment it is bound,
// without a chmod that could follow a path a fiber replaced.
func TestEgressSocketIsWorldWritableAtBind(t *testing.T) {
	// A relative path keeps the socket within unix path length limits.
	t.Chdir(t.TempDir())
	path := "egress.sock"
	ln, err := listenWorldWritable(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o666 {
		t.Fatalf("egress socket mode is %v, want a socket with 0666", info.Mode())
	}
}

// proxy serves handle on a loopback listener, and upstream echoes back.
func proxy(t *testing.T) (proxyAddr, upstreamAddr string) {
	t.Helper()
	upstream, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = upstream.Close() })
	go func() {
		for {
			c, err := upstream.Accept()
			if err != nil {
				return
			}
			go func() { defer func() { _ = c.Close() }(); _, _ = io.Copy(c, c) }()
		}
	}()
	front, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = front.Close() })
	allowed := map[string]bool{upstream.Addr().String(): true}
	go serve(front, "test", allowed)
	return front.Addr().String(), upstream.Addr().String()
}

// Bytes a client sends right behind its CONNECT request belong to the tunnel.
func TestEgressForwardsTunnelBytesSentWithTheRequest(t *testing.T) {
	addr, target := proxy(t)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := fmt.Fprintf(c, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\nhello", target, target); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT: %v %v", resp, err)
	}
	got := make([]byte, 5)
	if _, err := io.ReadFull(br, got); err != nil || string(got) != "hello" {
		t.Fatalf("tunnel echoed %q, %v", got, err)
	}
}

// A request header past the limit is refused before the proxy reads it all.
func TestEgressRefusesOversizedRequestHeaders(t *testing.T) {
	addr, target := proxy(t)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	go func() {
		_, _ = fmt.Fprintf(c, "CONNECT %s HTTP/1.1\r\nHost: %s\r\nX-Pad: %s\r\n\r\n", target, target, strings.Repeat("a", 1<<20))
	}()
	resp, err := http.ReadResponse(bufio.NewReader(c), &http.Request{Method: http.MethodConnect})
	if err != nil || resp.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("oversized CONNECT got %v, %v; want 431", resp, err)
	}
}
