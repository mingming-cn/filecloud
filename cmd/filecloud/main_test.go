package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRunInitAndArguments(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	var stdout, stderr bytes.Buffer
	if err := run(t.Context(), []string{"init", "--data-dir", dataDir}, &stdout, &stderr); err != nil {
		t.Fatalf("run init: %v", err)
	}
	if err := run(t.Context(), []string{"init", "--data-dir", dataDir}, &stdout, &stderr); err != nil {
		t.Fatalf("run init again: %v", err)
	}

	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{name: "no command", want: "usage: filecloud"},
		{name: "unknown command", args: []string{"unknown"}, want: `unknown command "unknown"`},
		{name: "init missing data directory", args: []string{"init"}, want: "usage: filecloud init"},
		{name: "serve missing data directory", args: []string{"serve"}, want: "usage: filecloud serve"},
		{name: "unexpected positional argument", args: []string{"init", "--data-dir", dataDir, "extra"}, want: "usage: filecloud init"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := run(t.Context(), test.args, io.Discard, io.Discard)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("run(%q) error = %v, want %q", test.args, err, test.want)
			}
		})
	}
}

func TestDefaultListenIsLoopback(t *testing.T) {
	if _defaultListen != "127.0.0.1:8080" {
		t.Fatalf("default listen = %q, want loopback", _defaultListen)
	}
}

func TestServeHealthLockConflictAndShutdown(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := run(t.Context(), []string{"init", "--data-dir", dataDir}, io.Discard, io.Discard); err != nil {
		t.Fatalf("init: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	addressOutput := newLineWriter()
	var stderr bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"serve", "--data-dir", dataDir, "--listen", "127.0.0.1:0"}, addressOutput, &stderr)
	}()

	var line string
	select {
	case line = <-addressOutput.lines:
	case err := <-done:
		t.Fatalf("serve exited before listening: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for serve to listen")
	}
	address := strings.TrimPrefix(strings.TrimSpace(line), "listening on ")
	if address == strings.TrimSpace(line) || !strings.HasPrefix(address, "127.0.0.1:") {
		t.Fatalf("listen output = %q", line)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Get("http://" + address + "/healthz")
	if err != nil {
		t.Fatalf("GET healthz: %v", err)
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		t.Fatalf("read healthz response: %v", err)
	}
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), `"RetCode":0`) {
		t.Fatalf("healthz = %d %q", response.StatusCode, body)
	}

	conflictErr := run(t.Context(), []string{
		"serve", "--data-dir", dataDir, "--listen", "127.0.0.1:0",
	}, io.Discard, io.Discard)
	if conflictErr == nil || !strings.Contains(conflictErr.Error(), "locked by another process") {
		t.Fatalf("second serve error = %v, want lock conflict", conflictErr)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve shutdown: %v; stderr: %s", err, stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for serve shutdown")
	}
}

func TestServeHandlesListenOutputError(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := run(t.Context(), []string{"init", "--data-dir", dataDir}, io.Discard, io.Discard); err != nil {
		t.Fatalf("init: %v", err)
	}

	err := run(t.Context(), []string{
		"serve", "--data-dir", dataDir, "--listen", "127.0.0.1:0",
	}, failingWriter{}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "write listen address") {
		t.Fatalf("serve error = %v, want output error", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	addressOutput := newLineWriter()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{
			"serve", "--data-dir", dataDir, "--listen", "127.0.0.1:0",
		}, addressOutput, io.Discard)
	}()
	select {
	case <-addressOutput.lines:
		cancel()
	case err := <-done:
		t.Fatalf("serve did not release resources after output error: %v", err)
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("timed out waiting for restarted serve")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown restarted serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out shutting down restarted serve")
	}
}

type lineWriter struct {
	lines chan string
	once  sync.Once
}

func newLineWriter() *lineWriter {
	return &lineWriter{lines: make(chan string, 1)}
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.once.Do(func() {
		w.lines <- string(p)
	})
	return len(p), nil
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("output unavailable")
}
