// Package health provides process liveness and storage readiness endpoints.
package health

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/mingming-cn/filecloud/internal/opslog"
)

const (
	_successBody     = "{\"RetCode\":0,\"Message\":\"success\"}\n"
	_unavailableBody = "{\"RetCode\":5001,\"Message\":\"storage unavailable\"}\n"
)

type readinessChecker interface {
	CheckReady(context.Context) error
}

// NewHandler returns the health endpoint handler.
func NewHandler(checker readinessChecker, logger *log.Logger) http.Handler {
	if logger == nil {
		logger = log.Default()
	}
	writeResponse := func(w http.ResponseWriter, status int, body string) {
		if err := writeJSON(w, status, body); err != nil {
			opslog.Error(logger, "serve", "", "write_health_response", err)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, _successBody)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if checker == nil || checker.CheckReady(r.Context()) != nil {
			writeResponse(w, http.StatusServiceUnavailable, _unavailableBody)
			return
		}
		writeResponse(w, http.StatusOK, _successBody)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "default-src 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		mux.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, body string) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write([]byte(body)); err != nil {
		return fmt.Errorf("write JSON body: %w", err)
	}
	return nil
}
