//go:build darwin

package main

import (
	"os"
	"time"

	"golang.org/x/sys/unix"
)

func openActionParent(root *openedWorktree, relative string) (int, error) {
	return openFSActionParentFallback(root, relative)
}

func renameNoReplace(sourceParent int, source string, targetParent int, target string) error {
	return unix.RenameatxNp(sourceParent, source, targetParent, target, unix.RENAME_EXCL)
}

func filesystemMtimeNS(value time.Time) int64 {
	return value.Truncate(time.Microsecond).UnixNano()
}

func setFileMtime(file *os.File, value time.Time) error {
	timestamp := unix.NsecToTimeval(filesystemMtimeNS(value))
	return unix.Futimes(int(file.Fd()), []unix.Timeval{timestamp, timestamp})
}
