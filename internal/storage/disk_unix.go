//go:build linux || darwin

package storage

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func diskUsage(path string) (uint64, uint64, error) {
	var status unix.Statfs_t
	if err := unix.Statfs(path, &status); err != nil {
		return 0, 0, fmt.Errorf("stat filesystem: %w", err)
	}
	blockSize := uint64(status.Bsize)
	return uint64(status.Bavail) * blockSize, uint64(status.Blocks) * blockSize, nil
}
