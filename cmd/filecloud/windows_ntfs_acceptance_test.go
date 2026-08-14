//go:build windows

package main

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/mingming-cn/filecloud/internal/acceptance"
	"github.com/mingming-cn/filecloud/internal/fscompat"
	"golang.org/x/sys/windows"
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
	t.Logf("verified local NTFS worktree=%s", root)
}

// TestWindowsNTFSPrimitives exercises the handle-relative boundary. It is
// gated because it needs a real local fixed NTFS volume and can be rejected by
// host policy before the test body runs.
func TestWindowsNTFSMatrixWiresFullScenarioSet(t *testing.T) {
	required := map[string]bool{}
	for _, scenario := range platformMatrixScenarios() {
		required[scenario.test] = true
	}
	for _, test := range []string{
		"TestPlatformCorrectnessLoop",
		"TestLibraryBindDoubleEmptyConvergesAndUnbindIsLocalOnly",
		"TestScanRegularFileRetriesConcurrentRewrite",
		"TestFSActionSubprocessCrashMatrix",
		"TestPublicBindSubprocessCrashMatrix",
		"TestPublicSyncSubprocessCrashMatrix",
		"TestLibrarySyncInterrupted100MiBUploadSendsOnlyMissingBlocks",
		"TestLibrarySyncStructuralConflictsPreserveCompleteLocalObject",
	} {
		if !required[test] {
			t.Fatalf("Windows NTFS matrix omitted %s", test)
		}
	}
}

func TestWindowsNTFSPrimitives(t *testing.T) {
	if os.Getenv("FILECLOUD_RUN_1B_NTFS") != "1" {
		t.Skip("set FILECLOUD_RUN_1B_NTFS=1 to run the Windows/NTFS primitive spike")
	}
	path := acceptance.Root()
	if path == "" {
		path = "."
	}
	path, err := os.MkdirTemp(path, ".windows-ntfs-primitives-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("remove NTFS primitive directory: %v", err)
		}
	})
	root, err := openWorktreeRoot(path, requireNTFS)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := os.WriteFile(filepath.Join(path, "source"), []byte("source"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "target"), []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := renameNoReplace(int(root.directory.Fd()), "source", int(root.directory.Fd()), "target"); !errors.Is(err, windows.ERROR_FILE_EXISTS) {
		t.Fatalf("no-replace rename error=%v, want existing target", err)
	}
	if data, err := os.ReadFile(filepath.Join(path, "target")); err != nil || string(data) != "target" {
		t.Fatalf("no-replace rename changed target=%q err=%v", data, err)
	}
	if err := os.Symlink("target", filepath.Join(path, "link")); err == nil {
		if _, err := fscompat.Openat(int(root.directory.Fd()), "link", fscompat.O_RDONLY|fscompat.O_NOFOLLOW, 0); err == nil {
			t.Fatal("handle-relative no-follow opened a reparse point")
		}
	}
	name, err := windows.UTF16PtrFromString(filepath.Join(path, "source"))
	if err != nil {
		t.Fatal(err)
	}
	held, err := windows.CreateFile(name, windows.GENERIC_READ, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, nil,
		windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(held)
	if err := renameNoReplace(int(root.directory.Fd()), "source", int(root.directory.Fd()), "renamed"); !errors.Is(err, windows.ERROR_SHARING_VIOLATION) {
		t.Fatalf("occupied source rename error=%v, want sharing violation", err)
	}
	if data, err := os.ReadFile(filepath.Join(path, "source")); err != nil || string(data) != "source" {
		t.Fatalf("occupied rename did not preserve source=%q err=%v", data, err)
	}
	if err := fscompat.SyncDirectory(int(root.directory.Fd())); err != nil {
		t.Fatalf("flush NTFS parent directory: %v", err)
	}
	line, err := acceptance.Encode(acceptance.Attestation{
		Kind: "filesystem-primitives", Scenario: "Windows NTFS primitives", Platform: runtime.GOOS, Filesystem: "ntfs",
		NoFollow: true, StableFileIdentity: true, NoReplaceRename: true, SameDirectoryRename: true,
		DirectorySync: true, OccupiedRenamePreserved: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(line)
}
