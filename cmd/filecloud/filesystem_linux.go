//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

const _ext4Magic = 0xef53

func requireSupportedFilesystem(directory *os.File) error {
	return requireExt4(directory)
}

var validateWorktreeDirectoryHandle = func(int) error { return nil }

func requireExt4(directory *os.File) error {
	var info syscall.Statfs_t
	if err := syscall.Fstatfs(int(directory.Fd()), &info); err != nil {
		return fmt.Errorf("inspect worktree filesystem: %w", err)
	}
	filesystem, err := mountedFilesystemForDirectory(directory)
	if err != nil {
		return fmt.Errorf("identify worktree filesystem: %w", err)
	}
	if err := validateSupportedFilesystemPolicy("linux", filesystem, true); err != nil {
		return err
	}
	if uint64(info.Type) != _ext4Magic {
		return fmt.Errorf("ext4 worktree has unexpected filesystem type 0x%x", uint64(info.Type))
	}
	return nil
}

func mountedFilesystemForDirectory(directory *os.File) (filesystem string, retErr error) {
	var stat syscall.Stat_t
	if err := syscall.Fstat(int(directory.Fd()), &stat); err != nil {
		return "", fmt.Errorf("inspect worktree device: %w", err)
	}
	mountinfo, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return "", fmt.Errorf("open mountinfo: %w", err)
	}
	defer func() {
		retErr = errors.Join(retErr, mountinfo.Close())
	}()
	return mountedFilesystem(mountinfo, directory.Name(), unix.Major(uint64(stat.Dev)), unix.Minor(uint64(stat.Dev)))
}
