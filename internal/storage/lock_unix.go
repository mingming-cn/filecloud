//go:build linux || darwin

package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

type dataLock struct {
	file *os.File
}

func openDataLock(dataDir string, create bool) (*dataLock, error) {
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	file, err := os.OpenFile(filepath.Join(dataDir, _lockName), flags, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open data-directory lock: %w", err)
	}
	return &dataLock{file: file}, nil
}

func (l *dataLock) exclusive() error {
	if err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return errors.New("data directory is locked by another process")
		}
		return fmt.Errorf("lock data directory: %w", err)
	}
	return nil
}

func (l *dataLock) shared() error {
	if err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("downgrade data-directory lock: %w", err)
	}
	return nil
}

func (l *dataLock) Close() error {
	return errors.Join(
		syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN),
		l.file.Close(),
	)
}
