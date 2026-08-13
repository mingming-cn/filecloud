//go:build linux

package main

import (
	"errors"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

var _fsActionOpenat2 = unix.Openat2

func openActionParent(root *openedWorktree, relative string) (int, error) {
	fd, err := _fsActionOpenat2(int(root.directory.Fd()), relative, &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV,
	})
	if errors.Is(err, syscall.ENOSYS) {
		return openFSActionParentFallback(root, relative)
	}
	return fd, err
}

func renameNoReplace(sourceParent int, source string, targetParent int, target string) error {
	return unix.Renameat2(sourceParent, source, targetParent, target, unix.RENAME_NOREPLACE)
}

func filesystemMtimeNS(value time.Time) int64 {
	return value.UnixNano()
}

func setFileMtime(file *os.File, value time.Time) error {
	timestamp := unix.NsecToTimespec(value.UnixNano())
	return unix.UtimesNanoAt(int(file.Fd()), "", []unix.Timespec{timestamp, timestamp}, unix.AT_EMPTY_PATH)
}
