// Package health provides process liveness and storage readiness endpoints.
package health

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
)

const (
	_successBody     = "{\"RetCode\":0,\"Message\":\"success\"}\n"
	_unavailableBody = "{\"RetCode\":5001,\"Message\":\"storage unavailable\"}\n"
)

// NewHandler returns the health endpoint handler.
func NewHandler(db *sql.DB, objectsDir string, logger *log.Logger) http.Handler {
	if logger == nil {
		logger = log.Default()
	}
	writeResponse := func(w http.ResponseWriter, status int, body string) {
		if err := writeJSON(w, status, body); err != nil {
			logger.Printf("write health response: %v", err)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, _successBody)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := ready(r.Context(), db, objectsDir); err != nil {
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

func ready(ctx context.Context, db *sql.DB, objectsDir string) error {
	var version int
	if err := db.QueryRowContext(ctx,
		"SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1").Scan(&version); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	probe, err := os.CreateTemp(objectsDir, ".ready-*")
	if err != nil {
		return fmt.Errorf("create object storage probe: %w", err)
	}
	name := probe.Name()
	if err := probe.Close(); err != nil {
		return errors.Join(fmt.Errorf("close object storage probe: %w", err), removeProbe(name))
	}
	return removeProbe(name)
}

func removeProbe(name string) error {
	if err := os.Remove(name); err != nil {
		return fmt.Errorf("remove object storage probe: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body string) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write([]byte(body)); err != nil {
		return fmt.Errorf("write JSON body: %w", err)
	}
	return nil
}
