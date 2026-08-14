//go:build darwin

package main

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestMain(m *testing.M) {
	tempDir, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve test temp directory: %v\n", err)
		os.Exit(1)
	}
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		fmt.Fprintf(os.Stderr, "set test temp directory: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func platformTestTempDir(t *testing.T) string {
	t.Helper()
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
	var token [2]byte
	if _, err := rand.Read(token[:]); err != nil {
		t.Fatal(err)
	}
	slots := len(alphabet) * len(alphabet)
	start := (int(token[0])<<8 | int(token[1])) % slots
	for offset := range slots {
		slot := (start + offset) % slots
		directory := filepath.Join("/private/tmp", ".f"+string([]byte{
			alphabet[slot/len(alphabet)], alphabet[slot%len(alphabet)],
		}))
		if err := os.Mkdir(directory, 0o700); errors.Is(err, os.ErrExist) {
			continue
		} else if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := os.RemoveAll(directory); err != nil {
				t.Errorf("remove test directory %q: %v", directory, err)
			}
		})
		return directory
	}
	t.Fatal("no short Darwin test directory is available")
	return ""
}

func requireAcceptanceFilesystem(t *testing.T, worktree, platform, filesystem string) {
	t.Helper()
	if platform != "darwin" || filesystem != "apfs" {
		t.Fatalf("Darwin acceptance process received %q/%q", platform, filesystem)
	}
	requireMacOSAPFS(t, worktree)
}
