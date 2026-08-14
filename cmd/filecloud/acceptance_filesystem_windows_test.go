//go:build windows

package main

import "testing"

func requireAcceptanceFilesystem(t *testing.T, worktree, platform, filesystem string) {
	t.Helper()
	if platform != "windows" || filesystem != "ntfs" {
		t.Fatalf("Windows acceptance process received %q/%q", platform, filesystem)
	}
	root, err := openWorktreeRoot(worktree, requireNTFS)
	if err != nil {
		t.Fatalf("Windows/NTFS acceptance requires local fixed NTFS: %v", err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}
}
