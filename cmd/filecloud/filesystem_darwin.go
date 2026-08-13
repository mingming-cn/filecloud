//go:build darwin

package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	_volCapIntFlock      = uint32(0x00000200)
	_volCapIntRenameExcl = uint32(0x00080000)
	_volumeCapabilities  = _volCapIntFlock | _volCapIntRenameExcl
)

func requireSupportedFilesystem(directory *os.File) error {
	return requireAPFS(directory)
}

func requireAPFS(directory *os.File) error {
	filesystem, local, err := apfsFilesystemForDirectory(directory)
	if err != nil {
		return fmt.Errorf("inspect worktree filesystem: %w", err)
	}
	if filesystem != "apfs" || !local {
		return fmt.Errorf("unsupported worktree filesystem %s (local=%t); local macOS APFS is required", filesystem, local)
	}
	supported, valid, err := volumeInterfaceCapabilities(directory)
	if err != nil {
		return fmt.Errorf("inspect APFS capabilities: %w", err)
	}
	if valid&_volumeCapabilities != _volumeCapabilities || supported&_volumeCapabilities != _volumeCapabilities {
		return fmt.Errorf("unsupported APFS capabilities supported=0x%x valid=0x%x; flock and exclusive rename are required",
			supported, valid)
	}
	return nil
}

func mountedFilesystemForDirectory(directory *os.File) (string, error) {
	filesystem, _, err := apfsFilesystemForDirectory(directory)
	return filesystem, err
}

func volumeInterfaceCapabilities(directory *os.File) (supported, valid uint32, retErr error) {
	attributes := unix.Attrlist{
		Bitmapcount: 5,
		Volattr:     unix.ATTR_VOL_INFO | unix.ATTR_VOL_CAPABILITIES,
	}
	var buffer [36]byte
	_, _, errno := unix.Syscall6(unix.SYS_FGETATTRLIST, directory.Fd(),
		uintptr(unsafe.Pointer(&attributes)), uintptr(unsafe.Pointer(&buffer[0])), uintptr(len(buffer)), 0, 0)
	runtime.KeepAlive(directory)
	if errno != 0 {
		return 0, 0, errno
	}
	if length := binary.LittleEndian.Uint32(buffer[:4]); length != uint32(len(buffer)) {
		return 0, 0, errors.New("APFS capability response has unexpected length")
	}
	return binary.LittleEndian.Uint32(buffer[8:12]), binary.LittleEndian.Uint32(buffer[24:28]), nil
}

func apfsFilesystemForDirectory(directory *os.File) (string, bool, error) {
	var info unix.Statfs_t
	if err := unix.Fstatfs(int(directory.Fd()), &info); err != nil {
		return "", false, err
	}
	filesystem := string(bytes.TrimRight(info.Fstypename[:], "\x00"))
	return filesystem, info.Flags&unix.MNT_LOCAL != 0, nil
}
