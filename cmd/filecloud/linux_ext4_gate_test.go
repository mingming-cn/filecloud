//go:build linux

package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"

	"github.com/mingming-cn/filecloud/internal/acceptance"
	"golang.org/x/sys/unix"
)

func TestCrossPlatformAcceptanceMatrix(t *testing.T) {
	if os.Getenv("FILECLOUD_PLATFORM_MATRIX_CHILD") == "1" {
		t.Skip("cross-platform acceptance child suite does not recurse")
	}
	if os.Getenv("FILECLOUD_RUN_1B") != "1" {
		t.Skip("set FILECLOUD_RUN_1B=1 to run the cross-platform acceptance matrix")
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

	runPlatformMatrix(t, ext4Temp, platformMatrixScenarios(), "linux", "ext4", platformMatrixEnvironment())
}

func TestLinuxExt4Primitives(t *testing.T) {
	if os.Getenv("FILECLOUD_RUN_1B") != "1" {
		t.Skip("set FILECLOUD_RUN_1B=1 to run the filesystem primitive contract")
	}
	rootPath := acceptance.Root()
	if rootPath == "" {
		rootPath = "."
	}
	rootPath, err := os.MkdirTemp(rootPath, ".linux-ext4-primitives-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(rootPath); err != nil {
			t.Errorf("remove ext4 primitive directory: %v", err)
		}
	})
	requireLinuxExt4(t, rootPath)
	casefoldLookup := "case-sensitive-distinct"
	if !testFilesystemCaseSensitive(t, rootPath) {
		casefoldLookup = "case-insensitive-alias"
	}
	root, err := openWorktreeRoot(rootPath, requireSupportedFilesystem)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()

	if err := os.Symlink("missing", filepath.Join(rootPath, "link")); err != nil {
		t.Fatal(err)
	}
	if fd, err := unix.Openat(int(root.directory.Fd()), "link", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0); err == nil {
		unix.Close(fd)
		t.Fatal("O_NOFOLLOW opened an ext4 symlink target")
	}

	if err := os.WriteFile(filepath.Join(rootPath, "source"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	held, err := os.OpenFile(filepath.Join(rootPath, "source"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	var before unix.Stat_t
	if err := unix.Fstat(int(held.Fd()), &before); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootPath, "occupied"), []byte("occupied"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := renameNoReplace(int(root.directory.Fd()), "source", int(root.directory.Fd()), "occupied"); !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("no-replace rename error=%v, want EEXIST", err)
	}
	if data, err := os.ReadFile(filepath.Join(rootPath, "occupied")); err != nil || string(data) != "occupied" {
		t.Fatalf("occupied target=%q err=%v", data, err)
	}
	if err := renameNoReplace(int(root.directory.Fd()), "source", int(root.directory.Fd()), "old-name"); err != nil {
		t.Fatalf("same-directory no-replace rename: %v", err)
	}
	var renamed unix.Stat_t
	if err := unix.Fstatat(int(root.directory.Fd()), "old-name", &renamed, unix.AT_SYMLINK_NOFOLLOW); err != nil ||
		before.Dev != renamed.Dev || before.Ino != renamed.Ino {
		t.Fatalf("renamed path identity changed: before=%d/%d after=%d/%d err=%v",
			before.Dev, before.Ino, renamed.Dev, renamed.Ino, err)
	}
	if err := os.WriteFile(filepath.Join(rootPath, "link-source"), []byte("link-source"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootPath, "link-target"), []byte("link-target"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(rootPath, "link-source"), filepath.Join(rootPath, "link-target")); !errors.Is(err, os.ErrExist) {
		t.Fatalf("no-replace hard-link error=%v, want exists", err)
	}
	if err := unix.Unlinkat(int(root.directory.Fd()), "old-name", 0); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootPath, "source"), []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := held.WriteAt([]byte("OLD"), 0); err != nil {
		t.Fatal(err)
	}
	if err := held.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := root.directory.Sync(); err != nil {
		t.Fatalf("sync ext4 parent directory: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(rootPath, "source")); err != nil || string(data) != "new" {
		t.Fatalf("replacement path changed through old fd: data=%q err=%v", data, err)
	}
	var detached unix.Stat_t
	if err := unix.Fstat(int(held.Fd()), &detached); err != nil || detached.Ino != before.Ino || detached.Nlink != 0 {
		t.Fatalf("old fd detached identity=%d nlink=%d err=%v", detached.Ino, detached.Nlink, err)
	}

	lockPath := filepath.Join(rootPath, "binding.lock")
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer lockFile.Close()
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestLinuxExt4LockHelper$")
	command.Env = append(os.Environ(), "FILECLOUD_EXT4_LOCK_HELPER=1", "FILECLOUD_EXT4_LOCK_PATH="+lockPath)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("verify cross-process ext4 flock: %v\n%s", err, output)
	}

	emitPlatformAttestation(t, platformAttestation{
		Kind: "filesystem-primitives", Scenario: "filesystem primitives", Platform: runtime.GOOS, Filesystem: "ext4",
		NoFollow: true, StableFileIdentity: true, NoReplaceRename: true, NoReplaceLink: true, CasefoldLookup: casefoldLookup,
		SameDirectoryRename: true, DirectorySync: true, CrossProcessLock: true, OldFDWritesDetached: true,
		Warning: "a writer holding an old file descriptor can modify the detached old inode after checkout; the active path remains the checked-out inode",
	})
}

func TestLinuxExt4LockHelper(t *testing.T) {
	if os.Getenv("FILECLOUD_EXT4_LOCK_HELPER") != "1" {
		t.Skip("ext4 lock helper only")
	}
	file, err := os.OpenFile(os.Getenv("FILECLOUD_EXT4_LOCK_PATH"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
		return
	}
	if err != nil {
		t.Fatalf("unexpected ext4 flock error: %v", err)
	}
	t.Fatal("acquired an ext4 lock held by another process")
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
