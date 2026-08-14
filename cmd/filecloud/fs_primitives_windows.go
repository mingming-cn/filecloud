//go:build windows

package main

import (
	"errors"
	"os"
	"time"
	"unsafe"

	"github.com/mingming-cn/filecloud/internal/fscompat"
	"golang.org/x/sys/windows"
)

type _fileRenameInformation struct {
	ReplaceIfExists uint32
	RootDirectory   windows.Handle
	FileNameLength  uint32
	FileName        [1]uint16
}

func openActionParent(root *openedWorktree, relative string) (int, error) {
	return openFSActionParentFallback(root, relative)
}

// renameNoReplace uses the verified source and target parent handles. A zero
// ReplaceIfExists is the NT no-replace operation; sharing violations are returned
// unchanged so the journal retains the source for a later recovery attempt.
func renameNoReplace(sourceParent int, source string, targetParent int, target string) error {
	fd, err := fscompat.Openat(sourceParent, source, fscompat.O_RDONLY|fscompat.O_NOFOLLOW|fscompat.O_DELETE, 0)
	if err != nil {
		return err
	}
	defer fscompat.Close(fd)
	name, err := windows.UTF16FromString(target)
	if err != nil {
		return err
	}
	name = name[:len(name)-1]
	var prototype _fileRenameInformation
	size := int(unsafe.Offsetof(prototype.FileName)) + len(name)*2
	buffer := make([]byte, size)
	info := (*_fileRenameInformation)(unsafe.Pointer(&buffer[0]))
	info.RootDirectory = windows.Handle(targetParent)
	info.FileNameLength = uint32(len(name) * 2)
	copy(unsafe.Slice(&info.FileName[0], len(name)), name)
	var status windows.IO_STATUS_BLOCK
	err = windows.NtSetInformationFile(windows.Handle(fd), &status, &buffer[0], uint32(len(buffer)), windows.FileRenameInformation)
	if errors.Is(err, windows.STATUS_OBJECT_NAME_COLLISION) || errors.Is(err, windows.STATUS_OBJECT_NAME_EXISTS) {
		return fscompat.EEXIST
	}
	return fscompat.NormalizeError(err)
}

func filesystemMtimeNS(value time.Time) int64 { return value.UnixNano() }
func setFileMtime(file *os.File, value time.Time) error {
	stamp := windows.NsecToFiletime(value.UnixNano())
	return windows.SetFileTime(windows.Handle(file.Fd()), nil, &stamp, &stamp)
}
