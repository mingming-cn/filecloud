//go:build windows

package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
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
	if err := os.Symlink("target", filepath.Join(path, "link")); err != nil {
		t.Fatalf("create NTFS reparse-point fixture: %v", err)
	}
	if fd, err := fscompat.Openat(int(root.directory.Fd()), "link", fscompat.O_RDONLY|fscompat.O_NOFOLLOW, 0); err == nil {
		fscompat.Close(fd)
		t.Fatal("handle-relative no-follow opened a reparse point")
	}
	if err := os.WriteFile(filepath.Join(path, "identity-source"), []byte("identity"), 0o600); err != nil {
		t.Fatal(err)
	}
	var before fscompat.Stat_t
	if err := fscompat.Fstatat(int(root.directory.Fd()), "identity-source", &before, fscompat.AT_SYMLINK_NOFOLLOW); err != nil {
		t.Fatal(err)
	}
	if err := renameNoReplace(int(root.directory.Fd()), "identity-source", int(root.directory.Fd()), "identity-target"); err != nil {
		t.Fatalf("same-directory no-replace rename: %v", err)
	}
	var after fscompat.Stat_t
	if err := fscompat.Fstatat(int(root.directory.Fd()), "identity-target", &after, fscompat.AT_SYMLINK_NOFOLLOW); err != nil ||
		before.Dev != after.Dev || before.Ino != after.Ino {
		t.Fatalf("renamed file identity changed: before=%d/%d after=%d/%d err=%v", before.Dev, before.Ino, after.Dev, after.Ino, err)
	}
	if err := os.Link(filepath.Join(path, "identity-target"), filepath.Join(path, "target")); !errors.Is(err, os.ErrExist) {
		t.Fatalf("no-replace hard-link error=%v, want exists", err)
	}
	if data, err := os.ReadFile(filepath.Join(path, "target")); err != nil || string(data) != "target" {
		t.Fatalf("hard-link publication replaced target=%q err=%v", data, err)
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
	verifyOccupiedRenameRecovery(t, path)
	lockPath := filepath.Join(path, "binding.lock")
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer lockFile.Close()
	if err := fscompat.Flock(int(lockFile.Fd()), fscompat.LOCK_EX|fscompat.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestWindowsNTFSLockHelper$")
	command.Env = append(os.Environ(), "FILECLOUD_NTFS_LOCK_HELPER=1", "FILECLOUD_NTFS_LOCK_PATH="+lockPath)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("verify cross-process NTFS lock: %v\n%s", err, output)
	}
	if err := fscompat.SyncDirectory(int(root.directory.Fd())); err != nil {
		t.Fatalf("flush NTFS parent directory: %v", err)
	}
	line, err := acceptance.Encode(acceptance.Attestation{
		Kind: "filesystem-primitives", Scenario: "Windows NTFS primitives", Platform: runtime.GOOS, Filesystem: "ntfs",
		NoFollow: true, StableFileIdentity: true, NoReplaceRename: true, NoReplaceLink: true, SameDirectoryRename: true,
		DirectorySync: true, CrossProcessLock: true, OccupiedRenamePreserved: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(line)
}

func verifyOccupiedRenameRecovery(t *testing.T, parent string) {
	t.Helper()
	ctx := context.Background()
	clientDir, err := os.MkdirTemp(parent, ".occupied-client-")
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := os.MkdirTemp(parent, ".occupied-worktree-")
	if err != nil {
		t.Fatal(err)
	}
	db, err := initializeClientDB(ctx, clientDir, syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	root, err := openWorktreeRoot(worktree, requireNTFS)
	if err != nil {
		t.Fatal(err)
	}
	if err := bindFSJournalRoot(ctx, db, worktree, root); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktree, "source"), []byte("user-content"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stat fscompat.Stat_t
	if err := fscompat.Fstatat(int(root.directory.Fd()), "source", &stat, fscompat.AT_SYMLINK_NOFOLLOW); err != nil {
		t.Fatal(err)
	}
	action := fsAction{Worktree: worktree, ActionID: "1234567890abcdef1234567890abcdef", Order: 1,
		Phase: fsPhasePreBase, Op: fsOpRename, Parent: "", ParentDevice: root.device, ParentInode: root.inode,
		Source: "source", Target: "target", ExpectedKind: "File", ExpectedDevice: stat.Dev, ExpectedInode: stat.Ino,
		State: fsStateIntent}
	if err := insertFSActionIntent(ctx, db, action); err != nil {
		t.Fatal(err)
	}
	name, err := windows.UTF16PtrFromString(filepath.Join(worktree, "source"))
	if err != nil {
		t.Fatal(err)
	}
	held, err := windows.CreateFile(name, windows.GENERIC_READ, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, nil,
		windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := recoverFSActions(ctx, db, worktree, root, nil); !errors.Is(err, windows.ERROR_SHARING_VIOLATION) {
		windows.CloseHandle(held)
		t.Fatalf("occupied journal recovery error=%v, want sharing violation", err)
	}
	if data, err := os.ReadFile(filepath.Join(worktree, "source")); err != nil || string(data) != "user-content" {
		windows.CloseHandle(held)
		t.Fatalf("occupied journal recovery changed source=%q err=%v", data, err)
	}
	var state string
	if err := db.QueryRowContext(ctx, "SELECT state FROM fs_actions WHERE action_id=?", action.ActionID).Scan(&state); err != nil || state != fsStateIntent {
		windows.CloseHandle(held)
		t.Fatalf("occupied journal state=%q err=%v, want intent", state, err)
	}
	if err := windows.CloseHandle(held); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(root.Close(), db.Close()); err != nil {
		t.Fatal(err)
	}
	db, err = openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root, err = openWorktreeRoot(worktree, requireNTFS)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := recoverFSActions(ctx, db, worktree, root, nil); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(worktree, "target")); err != nil || string(data) != "user-content" {
		t.Fatalf("restarted journal recovery target=%q err=%v", data, err)
	}
	if _, err := os.Stat(filepath.Join(worktree, "source")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restarted journal recovery retained source: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT state FROM fs_actions WHERE action_id=?", action.ActionID).Scan(&state); err != nil || state != fsStateCompleted {
		t.Fatalf("restarted journal state=%q err=%v, want completed", state, err)
	}
}

func TestWindowsNTFSLockHelper(t *testing.T) {
	if os.Getenv("FILECLOUD_NTFS_LOCK_HELPER") != "1" {
		t.Skip("NTFS lock helper only")
	}
	file, err := os.OpenFile(os.Getenv("FILECLOUD_NTFS_LOCK_PATH"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	err = fscompat.Flock(int(file.Fd()), fscompat.LOCK_EX|fscompat.LOCK_NB)
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return
	}
	if err != nil {
		t.Fatalf("unexpected NTFS lock error: %v", err)
	}
	t.Fatal("acquired an NTFS lock held by another process")
}
