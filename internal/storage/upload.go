package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	_defaultUploadGlobalConcurrency = 8
	_defaultUploadUserConcurrency   = 2
	_defaultUploadRequestTimeout    = time.Minute
	_defaultUploadBudgetBytes       = int64(10 << 30)
	_defaultUploadBudgetWindow      = time.Hour
	_minimumFreeDiskBytes           = uint64(1 << 30)
)

var (
	// ErrUploadRateLimited reports an exhausted upload concurrency or byte budget.
	ErrUploadRateLimited = errors.New("upload rate limited")
	// ErrUploadUnavailable reports insufficient free disk space for a new object.
	ErrUploadUnavailable = errors.New("upload unavailable")
)

// UploadConfig bounds object PUT resource consumption.
type UploadConfig struct {
	GlobalConcurrency int
	UserConcurrency   int
	RequestTimeout    time.Duration
	BudgetBytes       int64
	BudgetWindow      time.Duration
	Now               func() time.Time
	DiskUsage         func(string) (free, total uint64, err error)
}

type uploadState struct {
	config   UploadConfig
	global   int
	users    map[string]int
	reserved uint64
}

// DefaultUploadConfig returns the production upload limits.
func DefaultUploadConfig() UploadConfig {
	return UploadConfig{
		GlobalConcurrency: _defaultUploadGlobalConcurrency,
		UserConcurrency:   _defaultUploadUserConcurrency,
		RequestTimeout:    _defaultUploadRequestTimeout,
		BudgetBytes:       _defaultUploadBudgetBytes,
		BudgetWindow:      _defaultUploadBudgetWindow,
		Now:               time.Now,
		DiskUsage:         diskUsage,
	}
}

func normalizeUploadConfig(config UploadConfig) (UploadConfig, error) {
	defaults := DefaultUploadConfig()
	if config.GlobalConcurrency == 0 {
		config.GlobalConcurrency = defaults.GlobalConcurrency
	}
	if config.UserConcurrency == 0 {
		config.UserConcurrency = defaults.UserConcurrency
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = defaults.RequestTimeout
	}
	if config.BudgetBytes == 0 {
		config.BudgetBytes = defaults.BudgetBytes
	}
	if config.BudgetWindow == 0 {
		config.BudgetWindow = defaults.BudgetWindow
	}
	if config.Now == nil {
		config.Now = defaults.Now
	}
	if config.DiskUsage == nil {
		config.DiskUsage = defaults.DiskUsage
	}
	if config.GlobalConcurrency < 1 || config.UserConcurrency < 1 || config.RequestTimeout <= 0 || config.BudgetBytes < 1 || config.BudgetWindow <= 0 {
		return UploadConfig{}, errors.New("upload limits must be positive")
	}
	return config, nil
}

// ConfigureUpload applies server startup upload limits and returns their normalized form.
func (s *Store) ConfigureUpload(config UploadConfig) (UploadConfig, error) {
	config, err := normalizeUploadConfig(config)
	if err != nil {
		return UploadConfig{}, err
	}
	s.uploadMu.Lock()
	defer s.uploadMu.Unlock()
	if s.upload.global != 0 || len(s.upload.users) != 0 || s.upload.reserved != 0 {
		return UploadConfig{}, errors.New("cannot change upload limits while uploads are active")
	}
	s.upload.config = config
	return config, nil
}

// AcquireObjectUpload obtains the non-blocking global and per-user PUT slots.
func (s *Store) AcquireObjectUpload(ownerUserID string) (func(), error) {
	s.uploadMu.Lock()
	defer s.uploadMu.Unlock()
	if s.upload.global >= s.upload.config.GlobalConcurrency || s.upload.users[ownerUserID] >= s.upload.config.UserConcurrency {
		return nil, ErrUploadRateLimited
	}
	s.upload.global++
	s.upload.users[ownerUserID]++
	var once sync.Once
	return func() {
		once.Do(func() {
			s.uploadMu.Lock()
			defer s.uploadMu.Unlock()
			s.upload.global--
			if s.upload.users[ownerUserID] == 1 {
				delete(s.upload.users, ownerUserID)
			} else {
				s.upload.users[ownerUserID]--
			}
		})
	}, nil
}

// CheckObjectUpload admits a metadata object before its body is read.
func (s *Store) CheckObjectUpload(ctx context.Context, ownerUserID, libraryID, kind, objectID string) error {
	exists, err := s.HasObject(ctx, ownerUserID, libraryID, kind, objectID)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return s.reserveUploadBytes(0)
}

// ReserveBlockUpload reserves temporary disk bytes before a block body is read.
func (s *Store) ReserveBlockUpload(ctx context.Context, ownerUserID, libraryID, objectID string, size int64) (func(), error) {
	if size < 1 {
		return nil, errors.New("invalid block reservation size")
	}
	exists, err := s.HasObject(ctx, ownerUserID, libraryID, "blocks", objectID)
	if err != nil {
		return nil, err
	}
	if exists {
		return func() {}, nil
	}
	if err := s.reserveUploadBytes(uint64(size)); err != nil {
		return nil, err
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			s.uploadMu.Lock()
			s.upload.reserved -= uint64(size)
			s.uploadMu.Unlock()
		})
	}, nil
}

func (s *Store) reserveUploadBytes(size uint64) error {
	s.uploadMu.Lock()
	defer s.uploadMu.Unlock()
	free, total, err := s.upload.config.DiskUsage(s.objectsDir)
	if err != nil {
		return fmt.Errorf("read available disk space: %w", err)
	}
	minimum := max(total/20, _minimumFreeDiskBytes)
	if free < minimum || s.upload.reserved > free-minimum || size > free-minimum-s.upload.reserved {
		return ErrUploadUnavailable
	}
	s.upload.reserved += size
	return nil
}

func (s *Store) reserveUploadBudget(ctx context.Context, ownerUserID string, bytes int64) (func() error, error) {
	s.uploadMu.Lock()
	defer s.uploadMu.Unlock()
	config := s.upload.config
	now := config.Now().UTC()
	cutoff := now.Add(-config.BudgetWindow).UnixNano()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin upload usage: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM upload_charges WHERE user_id = ? AND accepted_at <= ?", ownerUserID, cutoff); err != nil {
		return nil, fmt.Errorf("prune upload usage: %w", err)
	}
	var used int64
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(SUM(bytes), 0) FROM upload_charges WHERE user_id = ? AND accepted_at > ?", ownerUserID, cutoff).Scan(&used); err != nil {
		return nil, fmt.Errorf("read upload usage: %w", err)
	}
	if bytes > config.BudgetBytes-used {
		return nil, ErrUploadRateLimited
	}
	result, err := tx.ExecContext(ctx, "INSERT INTO upload_charges(user_id, accepted_at, bytes) VALUES (?, ?, ?)", ownerUserID, now.UnixNano(), bytes)
	if err != nil {
		return nil, fmt.Errorf("record upload usage: %w", err)
	}
	chargeID, err := result.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("read upload charge: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit upload usage: %w", err)
	}
	return func() error {
		_, err := s.db.ExecContext(context.WithoutCancel(ctx), "DELETE FROM upload_charges WHERE id = ?", chargeID)
		if err != nil {
			return fmt.Errorf("release upload usage: %w", err)
		}
		return nil
	}, nil
}
