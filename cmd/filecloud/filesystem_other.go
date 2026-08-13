//go:build !linux && !darwin

package main

import (
	"errors"
	"os"
)

func requireSupportedFilesystem(*os.File) error {
	return errors.New("unsupported worktree platform; Linux/ext4 or macOS/APFS is required")
}
