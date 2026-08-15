//go:build windows

package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"

	"golang.org/x/sys/windows"
)

// Windows does not support exec.Cmd.ExtraFiles. Reopen the tracked pathname in
// the helper before the journal operation; Go opens it with delete sharing, so
// the test still exercises an old handle across the subsequent rename.
func attachTestHeldFile(_ *exec.Cmd, file *os.File) { _ = file.Close() }

func testCanInheritHeldFile() bool                   { return false }
func testCanRenameDirectoryWithHeldDescendant() bool { return false }

func openTestHeldFile(path string, write bool) (*os.File, error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	access := uint32(windows.GENERIC_READ)
	if write {
		access |= windows.GENERIC_WRITE
	}
	handle, err := windows.CreateFile(name, access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil,
		windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(handle), path), nil
}

func openTestHeldConflictFile() (*os.File, error) {
	worktree, relative := os.Getenv("FILECLOUD_PUBLIC_CRASH_WORKTREE"), os.Getenv("FILECLOUD_PUBLIC_MUTATION_PATH")
	if worktree == "" || relative == "" {
		return nil, errors.New("Windows held conflict path is absent")
	}
	return openTestHeldFile(filepath.Join(worktree, filepath.FromSlash(relative)), true)
}
