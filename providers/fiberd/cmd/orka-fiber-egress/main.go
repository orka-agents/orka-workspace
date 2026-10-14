// Copyright (c) 2026. MIT License - see LICENSE file for details.

// Command orka-fiber-egress runs beside fiberd in a home Pod. gVisor fibers
// have no network, but they can connect to host unix sockets under their
// grant's run directory. This daemon serves an HTTP CONNECT proxy on
// <run-dir>/<grant>/egress.sock for every grant, enforcing an allowlist of
// host:port destinations, and dials out from the home Pod's network.
package main

import (
	"bufio"
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
	"time"
)

func main() {
	runDir := flag.String("run-dir", "/var/lib/fiberd/run", "fiberd's run directory, one subdirectory per grant")
	name := flag.String("socket", "egress.sock", "socket name in each grant directory")
	allow := flag.String("allow", "", "comma-separated host:port destinations; empty allows none")
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
			ln, err := net.Listen("unix", path)
			if err != nil {
				continue
			}
			_ = os.Chmod(path, 0o666)
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

func serve(ln net.Listener, grant string, allowed map[string]bool) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go handle(c, grant, allowed)
	}
}

func handle(c net.Conn, grant string, allowed map[string]bool) {
	defer func() { _ = c.Close() }()
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(c)
	req, err := http.ReadRequest(br)
	if err != nil {
		return
	}
	_ = c.SetReadDeadline(time.Time{})
	if req.Method != http.MethodConnect {
		log.Printf("grant %s: refused %s %s", grant, req.Method, req.URL)
		_, _ = fmt.Fprint(c, "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n")
		return
	}
	target := strings.ToLower(req.Host)
	if !allowed[target] {
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
		_, _ = io.Copy(u, br)
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
