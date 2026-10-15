// Copyright (c) 2026. MIT License - see LICENSE file for details.

package main

import (
	"os"
	"testing"
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
