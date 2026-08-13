//go:build darwin

package main

import "testing"

func requireAcceptanceFilesystem(t *testing.T, worktree, platform, filesystem string) {
	t.Helper()
	if platform != "darwin" || filesystem != "apfs" {
		t.Fatalf("Darwin acceptance process received %q/%q", platform, filesystem)
	}
	requireMacOSAPFS(t, worktree)
}
