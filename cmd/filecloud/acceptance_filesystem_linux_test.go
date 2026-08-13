//go:build linux

package main

import "testing"

func requireAcceptanceFilesystem(t *testing.T, worktree, platform, filesystem string) {
	t.Helper()
	if platform != "linux" || filesystem != "ext4" {
		t.Fatalf("Linux acceptance process received %q/%q", platform, filesystem)
	}
	requireLinuxExt4(t, worktree)
}
