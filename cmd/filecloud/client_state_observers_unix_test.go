//go:build !windows

package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	libraryapi "github.com/mingming-cn/filecloud/internal/library"
)

func TestClientStateObserversCanonicalizeWorktreeAlias(t *testing.T) {
	var config libraryapi.Config
	environment := newLibraryCLIEnvironment(t, config)
	realParent := t.TempDir()
	realWorktree := filepath.Join(realParent, "worktree")
	if err := os.Mkdir(realWorktree, 0o700); err != nil {
		t.Fatalf("create real worktree: %v", err)
	}
	aliasParent := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(realParent, aliasParent); err != nil {
		t.Fatalf("create worktree alias: %v", err)
	}
	aliasWorktree := filepath.Join(aliasParent, "worktree")
	clientDir := t.TempDir()
	if err := runTest(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID,
		aliasWorktree, testClientDeviceID), strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("bind aliased worktree: %v", err)
	}

	if err := os.WriteFile(filepath.Join(aliasWorktree, "published.txt"), []byte("published"), 0o600); err != nil {
		t.Fatalf("write published file through alias: %v", err)
	}
	if err := syncTestWorktree(t, clientDir, aliasWorktree); err != nil {
		t.Fatalf("sync published file through alias: %v", err)
	}
	assertTestConverged(t, environment, clientDir, aliasWorktree)
	captured := captureTestBinding(t, clientDir, aliasWorktree)
	canonicalWorktree := canonicalTestWorktree(t, aliasWorktree)
	if captured.binding.Worktree != canonicalWorktree {
		t.Fatalf("captured binding worktree=%q, want %q", captured.binding.Worktree, canonicalWorktree)
	}
	if token := readTestAccessToken(t, clientDir, aliasWorktree); string(token) != environment.token {
		t.Fatal("observed access token match=false, want true")
	}

	if err := os.WriteFile(filepath.Join(aliasWorktree, "pending.txt"), []byte("pending"), 0o600); err != nil {
		t.Fatalf("write pending file through alias: %v", err)
	}
	stopErr := errors.New("stop before head publication")
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", clientDir, "--worktree", aliasWorktree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{
			checkFilesystem: func(*os.File) error { return nil },
			beforeHeadCAS:   func() error { return stopErr },
		})
	if !errors.Is(err, stopErr) {
		t.Fatalf("sync before Head publication error=%v, want %v", err, stopErr)
	}
	readTestPendingPublication(t, clientDir, aliasWorktree)
}
