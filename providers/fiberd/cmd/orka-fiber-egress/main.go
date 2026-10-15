// Copyright (c) 2026. MIT License - see LICENSE file for details.

// Command orka-fiber-egress runs beside fiberd in a home Pod. gVisor fibers
// have no network, but they can connect to host unix sockets under their
// grant's run directory. This daemon serves an HTTP CONNECT proxy on
// <run-dir>/<grant>/egress.sock for every grant, enforcing an allowlist of
// host:port destinations, and dials out from the home Pod's network.
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	runDir := flag.String("run-dir", "/var/lib/fiberd/run", "fiberd's run directory, one subdirectory per grant")
	name := flag.String("socket", "egress.sock", "socket name in each grant directory")
	allow := flag.String("allow", "", "comma-separated host:port destinations; a host may start with *. and a port may be *; empty allows none")
	flag.Parse()
	allowed := map[string]bool{}
	for _, entry := range strings.Split(*allow, ",") {
		if entry = strings.TrimSpace(entry); entry != "" {
			allowed[strings.ToLower(entry)] = true
		}
	}
	serving := map[string]net.Listener{}
	var mu sync.Mutex
	for {
		entries, _ := os.ReadDir(*runDir)
		live := map[string]bool{}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			dir := filepath.Join(*runDir, e.Name())
			live[dir] = true
			mu.Lock()
			_, ok := serving[dir]
			mu.Unlock()
			if ok {
				continue
			}
			path := filepath.Join(dir, *name)
			_ = os.Remove(path)
			ln, err := listenWorldWritable(path)
			if err != nil {
				continue
			}
			mu.Lock()
			serving[dir] = ln
			mu.Unlock()
			log.Printf("serving %s", path)
			go serve(ln, e.Name(), allowed)
		}
		mu.Lock()
		for dir, ln := range serving {
			if !live[dir] {
				_ = ln.Close()
				delete(serving, dir)
			}
		}
		mu.Unlock()
		time.Sleep(200 * time.Millisecond)
	}
}

// listenWorldWritable binds a unix socket that fibers, which run as other
// users, can connect to. The grant directory is fiber-writable, so the mode
// is set by bind itself: a later chmod of the path could follow a symlink a
// fiber swapped in and change a file outside the grant. This process
// creates no other files, so changing its umask briefly is safe.
func listenWorldWritable(path string) (net.Listener, error) {
	old := syscall.Umask(0o111)
	defer syscall.Umask(old)
	return net.Listen("unix", path)
}

// permitted matches host:port against exact entries, "*.suffix" hosts and
// "*" ports.
func permitted(allowed map[string]bool, target string) bool {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return false
	}
	for entry := range allowed {
		h, p, err := net.SplitHostPort(entry)
		if err != nil || (p != "*" && p != port) {
			continue
		}
		if h == host || (strings.HasPrefix(h, "*.") && strings.HasSuffix(host, h[1:])) {
			return true
		}
	}
	return false
}

func serve(ln net.Listener, grant string, allowed map[string]bool) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go handle(c, grant, allowed)
	}
}

// maxHeaderBytes bounds a CONNECT request. One home serves every grant, so
// a fiber must not make it allocate without bound before the allowlist runs.
const maxHeaderBytes = 16 << 10

func handle(c net.Conn, grant string, allowed map[string]bool) {
	defer func() { _ = c.Close() }()
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	header := &io.LimitedReader{R: c, N: maxHeaderBytes}
	br := bufio.NewReader(header)
	req, err := http.ReadRequest(br)
	if err != nil {
		if header.N == 0 {
			log.Printf("grant %s: refused a request header over %d bytes", grant, maxHeaderBytes)
			_, _ = fmt.Fprint(c, "HTTP/1.1 431 Request Header Fields Too Large\r\nContent-Length: 0\r\n\r\n")
		}
		return
	}
	_ = c.SetReadDeadline(time.Time{})
	// Tunnel bytes sent with the request are already buffered; the rest of
	// the tunnel is not bounded by the header limit.
	buffered, _ := br.Peek(br.Buffered())
	in := io.MultiReader(bytes.NewReader(bytes.Clone(buffered)), c)
	if req.Method != http.MethodConnect {
		log.Printf("grant %s: refused %s %s", grant, req.Method, req.URL)
		_, _ = fmt.Fprint(c, "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n")
		return
	}
	target := strings.ToLower(req.Host)
	if !permitted(allowed, target) {
		log.Printf("grant %s: denied CONNECT %s", grant, target)
		_, _ = fmt.Fprint(c, "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
		return
	}
	u, err := net.DialTimeout("tcp", target, 10*time.Second)
	if err != nil {
		log.Printf("grant %s: CONNECT %s: %v", grant, target, err)
		_, _ = fmt.Fprint(c, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
		return
	}
	defer func() { _ = u.Close() }()
	log.Printf("grant %s: CONNECT %s", grant, target)
	if _, err := fmt.Fprint(c, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(u, in)
		if tc, ok := u.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(c, u)
		if uc, ok := c.(*net.UnixConn); ok {
			_ = uc.CloseWrite()
		}
	}()
	wg.Wait()
}
