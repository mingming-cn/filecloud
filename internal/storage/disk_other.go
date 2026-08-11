//go:build !linux && !darwin && !windows

package storage

import "errors"

func diskUsage(string) (uint64, uint64, error) {
	return 0, 0, errors.New("filesystem capacity is unavailable")
}
