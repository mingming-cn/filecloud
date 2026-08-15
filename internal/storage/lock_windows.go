//go:build windows

package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

// dataLock uses byte-range locks because Windows does not provide flock.
// The first byte is the long-lived reader/writer lock. The second byte serializes
// the exclusive-to-shared transition performed after migrations.
type dataLock struct {
	file      *os.File
	lockPath  string
	lockHeld  bool
	guardHeld bool
}

func openDataLock(dataDir string, create bool) (*dataLock, error) {
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	lockPath := filepath.Join(dataDir, _lockName)
	file, err := os.OpenFile(lockPath, flags, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open data-directory lock: %w", err)
	}
	return &dataLock{file: file, lockPath: lockPath}, nil
}

func (l *dataLock) exclusive() error {
	if err := lockByte(l.file, 1, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY); err != nil {
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return errors.New("data directory is locked by another process")
		}
		return fmt.Errorf("lock data-directory transition guard: %w", err)
	}
	l.guardHeld = true
	if err := lockByte(l.file, 0, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY); err != nil {
		unlockErr := unlockByte(l.file, 1)
		if unlockErr == nil {
			l.guardHeld = false
		}
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return errors.Join(errors.New("data directory is locked by another process"), unlockErr)
		}
		return errors.Join(fmt.Errorf("lock data directory: %w", err), unlockErr)
	}
	l.lockHeld = true
	return l.check()
}

func (l *dataLock) shared() error {
	// Hold the guard while changing modes so another process cannot observe an
	// unlocked migration boundary. A shared LockFileEx lock permits other
	// serving processes but excludes administrators requesting exclusive access.
	if !l.guardHeld {
		return errors.New("data-directory lock was not exclusively held")
	}
	if err := unlockByte(l.file, 0); err != nil {
		return fmt.Errorf("unlock exclusive data-directory lock: %w", err)
	}
	l.lockHeld = false
	if err := lockByte(l.file, 0, windows.LOCKFILE_FAIL_IMMEDIATELY); err != nil {
		return fmt.Errorf("downgrade data-directory lock: %w", err)
	}
	l.lockHeld = true
	if err := l.check(); err != nil {
		return err
	}
	if err := unlockByte(l.file, 1); err != nil {
		return fmt.Errorf("unlock data-directory transition guard: %w", err)
	}
	l.guardHeld = false
	return nil
}

func (l *dataLock) check() error {
	held, err := l.file.Stat()
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
	var lockErr, guardErr error
	if l.lockHeld {
		lockErr = unlockByte(l.file, 0)
		if lockErr == nil {
			l.lockHeld = false
		}
	}
	if l.guardHeld {
		guardErr = unlockByte(l.file, 1)
		if guardErr == nil {
			l.guardHeld = false
		}
	}
	return errors.Join(lockErr, guardErr, l.file.Close())
}

func lockByte(file *os.File, offset uint32, flags uint32) error {
	return windows.LockFileEx(windows.Handle(file.Fd()), flags, 0, 1, 0, &windows.Overlapped{Offset: offset})
}

func unlockByte(file *os.File, offset uint32) error {
	return windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, &windows.Overlapped{Offset: offset})
}
