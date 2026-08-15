//go:build linux || darwin

package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckReadyRejectsReplacedDataDirectoryLock(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := Init(t.Context(), dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	store, err := OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenForServe: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	}()

	lockPath := filepath.Join(dataDir, _lockName)
	if err := os.Rename(lockPath, lockPath+".replaced"); err != nil {
		t.Fatalf("rename held lock: %v", err)
	}
	if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
		t.Fatalf("replace held lock: %v", err)
	}

	err = store.CheckReady(t.Context())
	if err == nil || !strings.Contains(err.Error(), "lock file was replaced") {
		t.Fatalf("CheckReady error = %v, want replaced lock", err)
	}
	if _, err := OpenGarbageCollector(t.Context(), dataDir); err == nil || !strings.Contains(err.Error(), "locked by another process") {
		t.Fatalf("OpenGarbageCollector after lock replacement error = %v, want lock conflict", err)
	}
}
