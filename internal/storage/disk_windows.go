//go:build windows

package storage

import "golang.org/x/sys/windows"

func diskUsage(path string) (uint64, uint64, error) {
	encoded, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, 0, err
	}
	var free, total, ignored uint64
	if err := windows.GetDiskFreeSpaceEx(encoded, &free, &total, &ignored); err != nil {
		return 0, 0, err
	}
	return free, total, nil
}
