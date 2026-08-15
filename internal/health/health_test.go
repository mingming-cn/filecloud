package health

import (
	"bytes"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mingming-cn/filecloud/internal/storage"
)

func TestHealthzDoesNotAccessStorage(t *testing.T) {
	handler := NewHandler(nil, log.New(&bytes.Buffer{}, "", 0))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	assertResponse(t, response, http.StatusOK, _successBody)
}

func TestReadyzChecksDatabaseAndObjectStorage(t *testing.T) {
	t.Run("ready", func(t *testing.T) {
		store, _ := openStore(t)
		defer closeStore(t, store)

		response := serveRequest(NewHandler(store, nil), "/readyz")
		assertResponse(t, response, http.StatusOK, _successBody)
	})

	t.Run("database unavailable", func(t *testing.T) {
		store, dataDir := openStore(t)
		defer closeStore(t, store)
		if err := store.DB().Close(); err != nil {
			t.Fatalf("close database: %v", err)
		}

		response := serveRequest(NewHandler(store, nil), "/readyz")
		assertUnavailable(t, response, filepath.Join(dataDir, "metadata.db"))
	})

	t.Run("object storage unavailable", func(t *testing.T) {
		store, _ := openStore(t)
		defer closeStore(t, store)
		if err := os.RemoveAll(store.ObjectsDir()); err != nil {
			t.Fatalf("remove objects directory: %v", err)
		}

		response := serveRequest(NewHandler(store, nil), "/readyz")
		assertUnavailable(t, response, store.ObjectsDir())
	})
}

func TestHandlerLogsResponseWriteErrors(t *testing.T) {
	var logs bytes.Buffer
	handler := NewHandler(nil, log.New(&logs, "", 0))
	response := &failingResponseWriter{header: make(http.Header)}
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if !strings.Contains(logs.String(), `"phase":"write_health_response"`) ||
		!strings.Contains(logs.String(), `"error_category":"unavailable"`) || strings.Contains(logs.String(), "broken pipe") {
		t.Fatalf("log = %q, want redacted response write category", logs.String())
	}
}

func assertResponse(t *testing.T, response *httptest.ResponseRecorder, status int, body string) {
	t.Helper()
	if response.Code != status {
		t.Errorf("status = %d, want %d", response.Code, status)
	}
	if response.Body.String() != body {
		t.Errorf("body = %q, want %q", response.Body.String(), body)
	}
	for header, want := range map[string]string{
		"Cache-Control":           "no-store",
		"Content-Security-Policy": "default-src 'none'",
		"Content-Type":            "application/json",
		"X-Content-Type-Options":  "nosniff",
		"X-Frame-Options":         "DENY",
	} {
		if got := response.Header().Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
}

func assertUnavailable(t *testing.T, response *httptest.ResponseRecorder, unavailablePath string) {
	t.Helper()
	assertResponse(t, response, http.StatusServiceUnavailable, _unavailableBody)
	body := response.Body.String()
	for _, secret := range []string{"sqlite", "schema_migrations", unavailablePath} {
		if strings.Contains(body, secret) {
			t.Errorf("unavailable response leaks %q: %q", secret, body)
		}
	}
}

func serveRequest(handler http.Handler, path string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	return response
}

func openStore(t *testing.T) (*storage.Store, string) {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := storage.Init(t.Context(), dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	store, err := storage.OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenForServe: %v", err)
	}
	return store, dataDir
}

func closeStore(t *testing.T, store *storage.Store) {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Errorf("close store: %v", err)
	}
}

type failingResponseWriter struct {
	header http.Header
}

func (w *failingResponseWriter) Header() http.Header {
	return w.header
}

func (*failingResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("broken pipe")
}

func (*failingResponseWriter) WriteHeader(int) {}
