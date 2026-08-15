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
	directory *os.File
	lockFile  *os.File
	lockPath  string
}

func openDataLock(dataDir string, create bool) (*dataLock, error) {
	lockPath := filepath.Join(dataDir, _lockName)
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	lockFile, err := os.OpenFile(lockPath, flags, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open data-directory lock: %w", err)
	}
	directory, err := os.Open(dataDir)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open data directory for locking: %w", err), lockFile.Close())
	}
	return &dataLock{directory: directory, lockFile: lockFile, lockPath: lockPath}, nil
}

func (l *dataLock) exclusive() error {
	if err := syscall.Flock(int(l.directory.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return errors.New("data directory is locked by another process")
		}
		return fmt.Errorf("lock data directory: %w", err)
	}
	return l.check()
}

func (l *dataLock) shared() error {
	if err := syscall.Flock(int(l.directory.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("downgrade data-directory lock: %w", err)
	}
	return l.check()
}

func (l *dataLock) check() error {
	held, err := l.lockFile.Stat()
	if err != nil {
		return fmt.Errorf("stat held lock: %w", err)
	}
	current, err := os.Stat(l.lockPath)
	if err != nil {
		return fmt.Errorf("stat current lock: %w", err)
	}
	if !os.SameFile(held, current) {
		return errors.New("data-directory lock file was replaced")
	}
	return nil
}

func (l *dataLock) Close() error {
	return errors.Join(
		syscall.Flock(int(l.directory.Fd()), syscall.LOCK_UN),
		l.directory.Close(),
		l.lockFile.Close(),
	)
}
