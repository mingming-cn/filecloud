// Package opslog writes structured operational events without serializing error details.
package opslog

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"strings"
	"time"
)

const (
	_categoryCanceled    = "canceled"
	_categoryConflict    = "conflict"
	_categoryInternal    = "internal"
	_categoryInvalid     = "invalid_input"
	_categoryLocked      = "lock_conflict"
	_categoryTimeout     = "timeout"
	_categoryUnavailable = "unavailable"
)

type record struct {
	Time          string `json:"time"`
	Level         string `json:"level"`
	Command       string `json:"command"`
	Library       string `json:"library"`
	Phase         string `json:"phase"`
	ErrorCategory string `json:"error_category"`
}

// Info writes a successful operational event.
func Info(logger *log.Logger, command, library, phase string) {
	write(logger, record{
		Time: time.Now().UTC().Format(time.RFC3339Nano), Level: "info", Command: command,
		Library: library, Phase: phase, ErrorCategory: "none",
	})
}

// Error writes an operational failure without including the original error text.
func Error(logger *log.Logger, command, library, phase string, err error) {
	write(logger, record{
		Time: time.Now().UTC().Format(time.RFC3339Nano), Level: "error", Command: command,
		Library: library, Phase: phase, ErrorCategory: category(err),
	})
}

// RedactedWriter converts free-form component output into redacted error events.
func RedactedWriter(logger *log.Logger, command, library, phase string) io.Writer {
	return redactedWriter{logger: logger, command: command, library: library, phase: phase}
}

type redactedWriter struct {
	logger                  *log.Logger
	command, library, phase string
}

func (w redactedWriter) Write(data []byte) (int, error) {
	Error(w.logger, w.command, w.library, w.phase, errors.New(string(data)))
	return len(data), nil
}

func write(logger *log.Logger, value record) {
	if logger == nil {
		logger = log.Default()
	}
	data, err := json.Marshal(value)
	if err != nil {
		return
	}
	logger.Print(string(data))
}

func category(err error) string {
	if err == nil {
		return _categoryInternal
	}
	switch {
	case errors.Is(err, context.Canceled):
		return _categoryCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return _categoryTimeout
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "locked by another process"), strings.Contains(message, "already running"):
		return _categoryLocked
	case strings.Contains(message, "unavailable"), strings.Contains(message, "broken pipe"), strings.Contains(message, "closed"):
		return _categoryUnavailable
	case strings.Contains(message, "conflict"), strings.Contains(message, "changed before"):
		return _categoryConflict
	case strings.Contains(message, "invalid"), strings.Contains(message, "usage:"):
		return _categoryInvalid
	default:
		return _categoryInternal
	}
}
