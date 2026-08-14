//go:build linux

package main

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
)

func TestLinuxExt4AcceptanceMatrix(t *testing.T) {
	if os.Getenv("FILECLOUD_PLATFORM_MATRIX_CHILD") == "1" {
		t.Skip("Linux/ext4 acceptance child suite does not recurse")
	}
	if os.Getenv("FILECLOUD_RUN_1A") != "1" {
		t.Skip("set FILECLOUD_RUN_1A=1 to run the Linux/ext4 acceptance matrix")
	}
	ext4Temp, err := os.MkdirTemp(".", ".linux-ext4-matrix-")
	if err != nil {
		t.Fatal(err)
	}
	ext4Temp, err = filepath.Abs(ext4Temp)
	if err != nil {
		t.Fatal(errors.Join(err, os.RemoveAll(ext4Temp)))
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(ext4Temp); err != nil {
			t.Errorf("remove Linux/ext4 matrix directory: %v", err)
		}
	})
	requireLinuxExt4(t, ext4Temp)

	runPlatformMatrix(t, ext4Temp, platformMatrixScenarios(), "linux", "ext4", []string{
		"FILECLOUD_RUN_1A=1",
		"FILECLOUD_RUN_1B_APFS=",
		"FILECLOUD_RUN_1B_NTFS=",
		"FILECLOUD_LINUX_EXT4_ROOT=" + ext4Temp,
	})
}

func requireLinuxExt4(t *testing.T, worktree string) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Fatalf("Linux/ext4 acceptance requires Linux/ext4, got %s", runtime.GOOS)
	}
	root, err := openWorktreeRoot(worktree, requireSupportedFilesystem)
	if err != nil {
		t.Fatalf("Linux/ext4 acceptance requires an ext-family worktree: %v", err)
	}
	defer func() {
		if err := root.Close(); err != nil {
			t.Errorf("close ext4 worktree: %v", err)
		}
	}()
	filesystem, err := mountedFilesystemForDirectory(root.directory)
	if err != nil {
		t.Fatalf("identify worktree filesystem: %v", err)
	}
	var info syscall.Statfs_t
	if err := syscall.Fstatfs(int(root.directory.Fd()), &info); err != nil {
		t.Fatalf("inspect ext4 worktree: %v", err)
	}
	t.Logf("platform=%s filesystem=%s magic=0x%x worktree=%s", runtime.GOOS, filesystem, uint64(info.Type), worktree)
}
