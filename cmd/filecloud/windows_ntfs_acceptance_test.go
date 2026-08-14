//go:build windows

package main

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// TestWindowsNTFSAcceptanceMatrix is an explicit real-host gate. It deliberately
// does not run in ordinary Windows test invocations because its child suite
// creates worktrees and injects process failures on the verified NTFS volume.
func TestWindowsNTFSAcceptanceMatrix(t *testing.T) {
	if os.Getenv("FILECLOUD_PLATFORM_MATRIX_CHILD") == "1" {
		t.Skip("Windows/NTFS acceptance child suite does not recurse")
	}
	if os.Getenv("FILECLOUD_RUN_1B_NTFS") != "1" {
		t.Skip("set FILECLOUD_RUN_1B_NTFS=1 to run the Windows/NTFS acceptance matrix")
	}
	root, err := os.MkdirTemp(".", ".windows-ntfs-matrix-")
	if err != nil {
		t.Fatal(err)
	}
	root, err = filepath.Abs(root)
	if err != nil {
		t.Fatal(errors.Join(err, os.RemoveAll(root)))
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("remove NTFS matrix directory: %v", err)
		}
	})
	opened, err := openWorktreeRoot(root, requireNTFS)
	if err != nil {
		t.Fatalf("Windows/NTFS acceptance requires local fixed NTFS: %v", err)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
	runPlatformMatrix(t, root, platformMatrixScenarios(), "windows", "ntfs", []string{
		"FILECLOUD_RUN_1A=",
		"FILECLOUD_RUN_1B_APFS=",
		"FILECLOUD_RUN_1B_NTFS=1",
		"FILECLOUD_WINDOWS_NTFS_ROOT=" + root,
	})
	t.Logf("platform=%s filesystem=ntfs worktree=%s", runtime.GOOS, root)
}
