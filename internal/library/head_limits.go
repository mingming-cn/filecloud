package library

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"
)

const (
	_defaultHeadValidationGlobalConcurrency = 2
	_defaultHeadValidationTimeout           = 2 * time.Minute
)

// HeadValidationConfig bounds Head graph validation resource consumption.
type HeadValidationConfig struct {
	GlobalConcurrency    int
	RequestTimeout       time.Duration
	MaxSnapshotDepth     int
	MaxTraversalContexts int
	MaxCommitDepth       int
	MaxIntroducedCommits int
	MaxValidatedObjects  int
}

// DefaultHeadValidationConfig returns the production Head validation limits.
func DefaultHeadValidationConfig() HeadValidationConfig {
	return HeadValidationConfig{
		GlobalConcurrency:    _defaultHeadValidationGlobalConcurrency,
		RequestTimeout:       _defaultHeadValidationTimeout,
		MaxSnapshotDepth:     _maxSnapshotDepth,
		MaxTraversalContexts: _maxSnapshotContexts,
		MaxCommitDepth:       _maxCommitDepth,
		MaxIntroducedCommits: _maxIntroducedCommits,
		MaxValidatedObjects:  _maxValidatedObjects,
	}
}

func normalizeHeadValidationConfig(config HeadValidationConfig) (HeadValidationConfig, error) {
	defaults := DefaultHeadValidationConfig()
	if config.GlobalConcurrency == 0 {
		config.GlobalConcurrency = defaults.GlobalConcurrency
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = defaults.RequestTimeout
	}
	if config.MaxSnapshotDepth == 0 {
		config.MaxSnapshotDepth = defaults.MaxSnapshotDepth
	}
	if config.MaxTraversalContexts == 0 {
		config.MaxTraversalContexts = defaults.MaxTraversalContexts
	}
	if config.MaxCommitDepth == 0 {
		config.MaxCommitDepth = defaults.MaxCommitDepth
	}
	if config.MaxIntroducedCommits == 0 {
		config.MaxIntroducedCommits = defaults.MaxIntroducedCommits
	}
	if config.MaxValidatedObjects == 0 {
		config.MaxValidatedObjects = defaults.MaxValidatedObjects
	}
	if config.GlobalConcurrency < 1 || config.RequestTimeout <= 0 || config.MaxSnapshotDepth < 1 ||
		config.MaxTraversalContexts < 1 || config.MaxCommitDepth < 1 || config.MaxIntroducedCommits < 1 || config.MaxValidatedObjects < 1 {
		return HeadValidationConfig{}, errors.New("head validation limits must be positive")
	}
	if config.MaxSnapshotDepth > defaults.MaxSnapshotDepth || config.MaxTraversalContexts > defaults.MaxTraversalContexts ||
		config.MaxCommitDepth > defaults.MaxCommitDepth || config.MaxIntroducedCommits > defaults.MaxIntroducedCommits ||
		config.MaxValidatedObjects > defaults.MaxValidatedObjects {
		return HeadValidationConfig{}, errors.New("head validation limits exceed protocol maximum")
	}
	return config, nil
}

type headValidationKey struct {
	ownerID, libraryID string
}

type headValidationLimiter struct {
	mu          sync.Mutex
	globalLimit int
	global      int
	libraries   map[headValidationKey]struct{}
}

func newHeadValidationLimiter(globalLimit int) (*headValidationLimiter, error) {
	if globalLimit < 1 {
		return nil, errors.New("head validation concurrency limit must be positive")
	}
	return &headValidationLimiter{globalLimit: globalLimit, libraries: make(map[headValidationKey]struct{})}, nil
}

func (l *headValidationLimiter) tryAcquire(ownerID, libraryID string) (func(), bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := headValidationKey{ownerID: ownerID, libraryID: libraryID}
	if l.global >= l.globalLimit {
		return nil, false
	}
	if _, exists := l.libraries[key]; exists {
		return nil, false
	}
	l.global++
	l.libraries[key] = struct{}{}
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.global--
			delete(l.libraries, key)
		})
	}, true
}

func (h *handler) admitHeadValidation(w http.ResponseWriter, r *http.Request, ownerID, libraryID string) (*http.Request, func(), bool) {
	releaseSlot, ok := h.headLimiter.tryAcquire(ownerID, libraryID)
	if !ok {
		w.Header().Set("Retry-After", "1")
		h.writeError(w, http.StatusTooManyRequests, 4000, "head validation busy")
		return nil, nil, false
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.headValidation.RequestTimeout)
	var once sync.Once
	return r.WithContext(ctx), func() {
		once.Do(func() {
			cancel()
			releaseSlot()
		})
	}, true
}
