//go:build windows

package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

const _requiredNTFSVolumeFlags = windows.FILE_SUPPORTS_REPARSE_POINTS |
	windows.FILE_SUPPORTS_HARD_LINKS | windows.FILE_PERSISTENT_ACLS | windows.FILE_UNICODE_ON_DISK

func requireSupportedFilesystem(directory *os.File) error {
	return requireNTFS(directory)
}

// requireNTFS accepts only a local fixed NTFS volume with the primitives used by
// the client journal. A failed capability query is a refusal, never a fallback
// to path-based filesystem operations.
func requireNTFS(directory *os.File) error {
	if directory == nil {
		return errors.New("worktree directory is not open")
	}
	root, err := volumeRoot(directory.Name())
	if err != nil {
		return fmt.Errorf("identify worktree volume: %w", err)
	}
	if windows.GetDriveType(root) != windows.DRIVE_FIXED {
		return errors.New("unsupported worktree volume; local fixed NTFS is required")
	}
	var serial, maximum uint32
	var flags uint32
	filesystem := make([]uint16, 32)
	if err := windows.GetVolumeInformationByHandle(windows.Handle(directory.Fd()), nil, 0, &serial, &maximum, &flags,
		&filesystem[0], uint32(len(filesystem))); err != nil {
		return fmt.Errorf("inspect NTFS volume: %w", err)
	}
	name := windows.UTF16ToString(filesystem)
	if !strings.EqualFold(name, "NTFS") {
		return fmt.Errorf("unsupported worktree filesystem %q; local NTFS is required", name)
	}
	if flags&_requiredNTFSVolumeFlags != _requiredNTFSVolumeFlags {
		return fmt.Errorf("unsupported NTFS capabilities 0x%x; reparse points, hard links, persistent ACLs, and Unicode are required", flags)
	}
	var caseInfo uint32
	if err := windows.GetFileInformationByHandleEx(windows.Handle(directory.Fd()), windows.FileCaseSensitiveInfo,
		(*byte)(unsafe.Pointer(&caseInfo)), uint32(unsafe.Sizeof(caseInfo))); err != nil {
		return fmt.Errorf("inspect NTFS case-sensitivity: %w", err)
	}
	if caseInfo&windows.FILE_CS_FLAG_CASE_SENSITIVE_DIR != 0 {
		return errors.New("case-sensitive NTFS worktree directories are unsupported")
	}
	return nil
}

func volumeRoot(path string) (*uint16, error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	root := make([]uint16, windows.MAX_PATH)
	if err := windows.GetVolumePathName(name, &root[0], uint32(len(root))); err != nil {
		return nil, err
	}
	return &root[0], nil
}
