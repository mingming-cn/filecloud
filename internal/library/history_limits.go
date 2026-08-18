package library

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"
)

const (
	_defaultHistoryGlobalConcurrency = 2
	_defaultHistoryUserConcurrency   = 1
	_defaultHistoryTimeout           = 2 * time.Minute
)

// HistoryConfig bounds one mainline history traversal.
type HistoryConfig struct {
	GlobalConcurrency int
	UserConcurrency   int
	RequestTimeout    time.Duration
}

// DefaultHistoryConfig returns the production history traversal limits.
func DefaultHistoryConfig() HistoryConfig {
	return HistoryConfig{
		GlobalConcurrency: _defaultHistoryGlobalConcurrency,
		UserConcurrency:   _defaultHistoryUserConcurrency,
		RequestTimeout:    _defaultHistoryTimeout,
	}
}

func normalizeHistoryConfig(config HistoryConfig) (HistoryConfig, error) {
	defaults := DefaultHistoryConfig()
	if config.GlobalConcurrency == 0 {
		config.GlobalConcurrency = defaults.GlobalConcurrency
	}
	if config.UserConcurrency == 0 {
		config.UserConcurrency = defaults.UserConcurrency
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = defaults.RequestTimeout
	}
	if config.GlobalConcurrency < 1 || config.UserConcurrency < 1 || config.RequestTimeout <= 0 {
		return HistoryConfig{}, errors.New("history limits must be positive")
	}
	return config, nil
}

type historyLimiter struct {
	mu          sync.Mutex
	globalLimit int
	userLimit   int
	global      int
	users       map[string]int
}

func newHistoryLimiter(globalLimit, userLimit int) (*historyLimiter, error) {
	if globalLimit < 1 || userLimit < 1 {
		return nil, errors.New("history concurrency limits must be positive")
	}
	return &historyLimiter{globalLimit: globalLimit, userLimit: userLimit, users: make(map[string]int)}, nil
}

func (l *historyLimiter) tryAcquire(userID string) (func(), bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.global >= l.globalLimit || l.users[userID] >= l.userLimit {
		return nil, false
	}
	l.global++
	l.users[userID]++
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.global--
			l.users[userID]--
			if l.users[userID] == 0 {
				delete(l.users, userID)
			}
		})
	}, true
}

func (h *handler) admitHistory(w http.ResponseWriter, r *http.Request, userID string) (*http.Request, func(), bool) {
	releaseSlot, ok := h.historyLimiter.tryAcquire(userID)
	if !ok {
		w.Header().Set("Retry-After", "1")
		h.writeError(w, http.StatusTooManyRequests, 4000, "history traversal rate limited")
		return nil, nil, false
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.history.RequestTimeout)
	var once sync.Once
	return r.WithContext(ctx), func() {
		once.Do(func() {
			cancel()
			releaseSlot()
		})
	}, true
}
