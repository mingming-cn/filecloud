package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mingming-cn/filecloud/internal/auth"
	"github.com/mingming-cn/filecloud/internal/storage"
)

func TestUserCommandsReadPasswordFromStdinWithoutDisclosure(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := storage.Init(t.Context(), dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	var stdout, stderr bytes.Buffer
	args := []string{"user", "add", "--data-dir", dataDir, "--username", "A\u030Alice", "--password-stdin"}
	if err := run(t.Context(), args, strings.NewReader("first password\n"), &stdout, &stderr); err != nil {
		t.Fatalf("user add: %v", err)
	}
	if !strings.Contains(stdout.String(), "Ålice") {
		t.Fatalf("user add output = %q, want NFC username", stdout.String())
	}
	for _, output := range []string{stdout.String(), stderr.String()} {
		if strings.Contains(output, "first password") || strings.Contains(output, "$argon2") {
			t.Fatalf("user command disclosed password material: %q", output)
		}
	}

	err := run(t.Context(), []string{"user", "add", "--data-dir", dataDir, "--username", "åLICE", "--password-stdin"},
		strings.NewReader("other password\n"), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "username already exists") {
		t.Fatalf("canonical duplicate error = %v", err)
	}

	stdout.Reset()
	if err := run(t.Context(), []string{"user", "reset-password", "--data-dir", dataDir, "--username", "ÅLICE", "--password-stdin"},
		strings.NewReader("replacement password\n"), &stdout, &stderr); err != nil {
		t.Fatalf("reset-password: %v", err)
	}
	if strings.Contains(stdout.String()+stderr.String(), "replacement password") || strings.Contains(stdout.String()+stderr.String(), "$argon2") {
		t.Fatal("reset-password disclosed password material")
	}
}

func TestLoginAndLogoutCommands(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := storage.Init(t.Context(), dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	store, err := storage.OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenForServe: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	params := auth.Params{Memory: 64, Iterations: 1, Parallelism: 1, SaltLength: 8, KeyLength: 16}
	hash, err := auth.HashPassword([]byte("password"), params, bytes.NewReader(bytes.Repeat([]byte{1}, 32)))
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if err := store.CreateUser(t.Context(), storage.User{ID: "user-id", Username: "alice", PasswordHash: hash}, time.Now()); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	handler, err := auth.NewHandler(store, log.New(io.Discard, "", 0), auth.HandlerConfig{
		Params: params,
		Random: bytes.NewReader(bytes.Repeat([]byte{2}, 256)),
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	var output bytes.Buffer
	if err := run(t.Context(), []string{"login", "--server", server.URL, "--username", "alice", "--device-name", "laptop", "--password-stdin"},
		strings.NewReader("password\n"), &output, io.Discard); err != nil {
		t.Fatalf("login: %v", err)
	}
	var response struct {
		Session struct{ AccessToken string }
	}
	if err := json.Unmarshal(output.Bytes(), &response); err != nil || response.Session.AccessToken == "" {
		t.Fatalf("login output = %q, decode error = %v", output.String(), err)
	}
	if err := run(t.Context(), []string{"logout", "--server", server.URL, "--token-stdin"},
		strings.NewReader(response.Session.AccessToken+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("logout: %v", err)
	}
	if err := run(t.Context(), []string{"logout", "--server", server.URL, "--token-stdin"},
		strings.NewReader(response.Session.AccessToken+"\n"), io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("reused logout error = %v, want 401", err)
	}
}

func TestSessionResponseCloseErrorsAreReturned(t *testing.T) {
	closeErr := errors.New("close unavailable")
	if err := closeResponseError("login", closeErr); !errors.Is(err, closeErr) || !strings.Contains(err.Error(), "close login response") {
		t.Fatalf("closeResponseError = %v", err)
	}
	if err := closeResponseError("logout", nil); err != nil {
		t.Fatalf("nil closeResponseError = %v", err)
	}
}

func TestServerURLRequiresHTTPSExceptLoopback(t *testing.T) {
	for _, accepted := range []string{"https://example.com", "http://127.0.0.1:8080", "http://[::1]:8080", "http://localhost:8080"} {
		if _, err := validateServerURL(accepted); err != nil {
			t.Errorf("validateServerURL(%q): %v", accepted, err)
		}
	}
	for _, rejected := range []string{"http://example.com", "ftp://localhost", "https://user@example.com", "https://example.com/path"} {
		if _, err := validateServerURL(rejected); err == nil {
			t.Errorf("validateServerURL(%q) unexpectedly succeeded", rejected)
		}
	}
}
