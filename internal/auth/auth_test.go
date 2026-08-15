package auth

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mingming-cn/filecloud/internal/storage"
)

func testParams() Params {
	return Params{Memory: 64, Iterations: 1, Parallelism: 1, SaltLength: 8, KeyLength: 16}
}

func TestSessionRawHTTPHeadersAndEmptyLogout(t *testing.T) {
	handler, store, _ := newTestHandler(t, nil, nil)
	defer closeTestStore(t, store)
	server := httptest.NewServer(handler)
	defer server.Close()

	loginBody := `{"Username":"alice","Password":"correct password","DeviceName":"raw-http"}`
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/v1/sessions", strings.NewReader(loginBody))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var login struct {
		RetCode int
		Message string
		Session struct{ AccessToken string }
	}
	decodeErr := json.NewDecoder(response.Body).Decode(&login)
	closeErr := response.Body.Close()
	if decodeErr != nil || closeErr != nil || response.StatusCode != http.StatusOK ||
		response.Header.Get("Cache-Control") != "no-store" || login.RetCode != 0 || login.Message != "success" || login.Session.AccessToken == "" {
		t.Fatalf("raw login = status=%d headers=%v retcode=%d message=%q token_present=%v decode=%v close=%v",
			response.StatusCode, response.Header, login.RetCode, login.Message, login.Session.AccessToken != "", decodeErr, closeErr)
	}

	logout, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, server.URL+"/v1/sessions/current", nil)
	if err != nil {
		t.Fatal(err)
	}
	logout.Header.Set("Authorization", "Bearer "+login.Session.AccessToken)
	response, err = server.Client().Do(logout)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(response.Body)
	closeErr = response.Body.Close()
	if readErr != nil || closeErr != nil || response.StatusCode != http.StatusNoContent || len(data) != 0 {
		t.Fatalf("raw logout = status=%d body=%q read=%v close=%v", response.StatusCode, data, readErr, closeErr)
	}

	response, err = server.Client().Do(logout.Clone(t.Context()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			t.Errorf("close revoked-token response: %v", err)
		}
	}()
	var failure struct {
		RetCode int
		Message string
	}
	if err := json.NewDecoder(response.Body).Decode(&failure); err != nil || response.StatusCode != http.StatusUnauthorized ||
		response.Header.Get("WWW-Authenticate") != "Bearer" || failure.RetCode != 1001 || failure.Message == "" {
		t.Fatalf("raw revoked token = status=%d headers=%v body=%+v err=%v", response.StatusCode, response.Header, failure, err)
	}
}

func TestCreateAndDeleteSession(t *testing.T) {
	handler, store, now := newTestHandler(t, nil, nil)
	defer closeTestStore(t, store)

	response := request(handler, http.MethodPost, "/v1/sessions",
		`{"Username":"ALICE","Password":"correct password","DeviceName":"laptop"}`, "")
	if response.Code != http.StatusOK {
		t.Fatalf("login status = %d, body = %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	var envelope struct {
		RetCode int
		Message string
		Session struct {
			AccessToken string
			ExpiresAt   string
			UserID      string `json:"UserId"`
		}
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode login response: %v", err)
	}
	if envelope.RetCode != 0 || envelope.Message != "success" || envelope.Session.UserID != "user-id" || envelope.Session.AccessToken == "" {
		t.Fatalf("login response = %+v", envelope)
	}
	if envelope.Session.ExpiresAt != now.Add(_sessionLifetime).Format(time.RFC3339) {
		t.Fatalf("ExpiresAt = %q", envelope.Session.ExpiresAt)
	}

	var persisted []byte
	if err := store.DB().QueryRow("SELECT token_hash FROM access_tokens").Scan(&persisted); err != nil {
		t.Fatalf("read persisted token: %v", err)
	}
	wantHash := sha256.Sum256([]byte(envelope.Session.AccessToken))
	if !bytes.Equal(persisted, wantHash[:]) || bytes.Contains(persisted, []byte(envelope.Session.AccessToken)) {
		t.Fatal("database did not persist only the binary token hash")
	}

	response = request(handler, http.MethodDelete, "/v1/sessions/current", "", envelope.Session.AccessToken)
	if response.Code != http.StatusNoContent || response.Body.Len() != 0 {
		t.Fatalf("first logout = %d %q, want 204 empty", response.Code, response.Body.String())
	}
	response = request(handler, http.MethodDelete, "/v1/sessions/current", "", envelope.Session.AccessToken)
	assertUnauthorized(t, response)
}

func TestLoginUnknownUserAndWrongPasswordAreUniform(t *testing.T) {
	handler, store, _ := newTestHandler(t, nil, nil)
	defer closeTestStore(t, store)

	wrong := request(handler, http.MethodPost, "/v1/sessions",
		`{"Username":"alice","Password":"wrong","DeviceName":"laptop"}`, "")
	unknown := request(handler, http.MethodPost, "/v1/sessions",
		`{"Username":"nobody","Password":"wrong","DeviceName":"laptop"}`, "")
	assertUnauthorized(t, wrong)
	assertUnauthorized(t, unknown)
	if wrong.Body.String() != unknown.Body.String() {
		t.Fatalf("401 bodies differ: wrong=%q unknown=%q", wrong.Body.String(), unknown.Body.String())
	}
}

func TestLoginRejectsStoredHashParametersThatDifferFromHandlerConfig(t *testing.T) {
	tests := []struct {
		name   string
		params Params
	}{
		{name: "memory", params: Params{Memory: 72, Iterations: 1, Parallelism: 1, SaltLength: 8, KeyLength: 16}},
		{name: "iterations", params: Params{Memory: 64, Iterations: 2, Parallelism: 1, SaltLength: 8, KeyLength: 16}},
		{name: "parallelism", params: Params{Memory: 64, Iterations: 1, Parallelism: 2, SaltLength: 8, KeyLength: 16}},
		{name: "salt length", params: Params{Memory: 64, Iterations: 1, Parallelism: 1, SaltLength: 9, KeyLength: 16}},
		{name: "key length", params: Params{Memory: 64, Iterations: 1, Parallelism: 1, SaltLength: 8, KeyLength: 17}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			handler, store, _ := newTestHandler(t, nil, log.New(&logs, "", 0))
			defer closeTestStore(t, store)
			hash, err := HashPassword([]byte("correct password"), test.params, bytes.NewReader(bytes.Repeat([]byte{4}, 32)))
			if err != nil {
				t.Fatalf("HashPassword: %v", err)
			}
			if _, err := store.DB().Exec("UPDATE users SET password_hash = ? WHERE id = 'user-id'", hash); err != nil {
				t.Fatalf("replace password hash: %v", err)
			}

			response := request(handler, http.MethodPost, "/v1/sessions",
				`{"Username":"alice","Password":"correct password","DeviceName":"laptop"}`, "")
			if response.Code != http.StatusInternalServerError || !strings.Contains(logs.String(), `"phase":"verify_password_hash"`) ||
				!strings.Contains(logs.String(), `"error_category":"internal"`) || strings.Contains(logs.String(), "password hash parameters") {
				t.Fatalf("mismatched %s response = %d %q, logs = %q; want redacted integrity category",
					test.name, response.Code, response.Body.String(), logs.String())
			}
		})
	}
}

func TestCreateSessionRejectsContentTypeBodyAndJSONBeforeKDF(t *testing.T) {
	limiter, err := newKDFLimiter(1, 1, 1)
	if err != nil {
		t.Fatalf("newKDFLimiter: %v", err)
	}
	release, ok := limiter.tryAcquire("occupied-source", "occupied-user")
	if !ok {
		t.Fatal("failed to saturate KDF limiter")
	}
	defer release()
	handler, store, _ := newTestHandler(t, limiter, nil)
	defer closeTestStore(t, store)

	missingContentType := requestWithHeaders(handler, http.MethodPost, "/v1/sessions",
		`{"Username":"alice","Password":"correct password","DeviceName":"x"}`, "", "192.0.2.1:1234", nil)
	if missingContentType.Code != http.StatusBadRequest {
		t.Fatalf("missing Content-Type = %d %q", missingContentType.Code, missingContentType.Body.String())
	}
	wrongContentType := requestWithHeaders(handler, http.MethodPost, "/v1/sessions", `{}`, "", "192.0.2.1:1234",
		map[string]string{"Content-Type": "text/plain"})
	if wrongContentType.Code != http.StatusBadRequest {
		t.Fatalf("wrong Content-Type = %d %q", wrongContentType.Code, wrongContentType.Body.String())
	}

	oversize := request(handler, http.MethodPost, "/v1/sessions", strings.Repeat("x", _maxRequestBody+1), "")
	if oversize.Code != http.StatusRequestEntityTooLarge || !strings.Contains(oversize.Body.String(), `"RetCode":3005`) {
		t.Fatalf("oversize response = %d %q", oversize.Code, oversize.Body.String())
	}

	invalidBodies := []string{
		`{"Username":"alice","Password":"correct password","DeviceName":"x","Extra":"x"}`,
		`{"Username":"alice","Username":"alice","Password":"correct password","DeviceName":"x"}`,
		`{"Username":"alice","Password":"correct password","DeviceName":"x"} {}`,
		`{"Username":"alice","Password":1,"DeviceName":"x"}`,
		`{"Username":"alice","Password":"correct password"}`,
	}
	for _, body := range invalidBodies {
		response := request(handler, http.MethodPost, "/v1/sessions", body, "")
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), `"RetCode":1000`) {
			t.Errorf("invalid body %q = %d %q", body, response.Code, response.Body.String())
		}
	}
}

func TestCreateSessionEachKDFCapacityReturnsImmediately(t *testing.T) {
	tests := []struct {
		name             string
		limits           [3]int
		occupiedSource   string
		occupiedUsername string
		requestSource    string
		requestUsername  string
		headers          map[string]string
	}{
		{
			name: "global", limits: [3]int{1, 2, 2}, occupiedSource: "198.51.100.1", occupiedUsername: "occupied",
			requestSource: "192.0.2.1:1234", requestUsername: "alice",
		},
		{
			name: "direct source IP", limits: [3]int{2, 1, 2}, occupiedSource: "192.0.2.1", occupiedUsername: "occupied",
			requestSource: "192.0.2.1:1234", requestUsername: "alice", headers: map[string]string{"X-Forwarded-For": "203.0.113.9"},
		},
		{
			name: "canonical username", limits: [3]int{2, 2, 1}, occupiedSource: "198.51.100.1", occupiedUsername: "alice",
			requestSource: "192.0.2.1:1234", requestUsername: "ALICE",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limiter, err := newKDFLimiter(test.limits[0], test.limits[1], test.limits[2])
			if err != nil {
				t.Fatalf("newKDFLimiter: %v", err)
			}
			release, ok := limiter.tryAcquire(test.occupiedSource, test.occupiedUsername)
			if !ok {
				t.Fatal("failed to occupy KDF limiter")
			}
			handler, store, _ := newTestHandler(t, limiter, nil)
			defer closeTestStore(t, store)

			body := `{"Username":"` + test.requestUsername + `","Password":"correct password","DeviceName":"laptop"}`
			headers := map[string]string{"Content-Type": "application/json"}
			for name, value := range test.headers {
				headers[name] = value
			}
			started := time.Now()
			response := requestWithHeaders(handler, http.MethodPost, "/v1/sessions", body, "", test.requestSource, headers)
			if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
				t.Fatalf("saturated login queued for %v", elapsed)
			}
			if response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") != "1" ||
				!strings.Contains(response.Body.String(), `"RetCode":4000`) {
				t.Fatalf("saturated response = %d headers=%v body=%q", response.Code, response.Header(), response.Body.String())
			}

			release()
			response = requestWithHeaders(handler, http.MethodPost, "/v1/sessions", body, "", test.requestSource, headers)
			if response.Code != http.StatusOK {
				t.Fatalf("released limiter response = %d %q", response.Code, response.Body.String())
			}
		})
	}
}

func TestCreateSessionReleasesLimiterOnStorageError(t *testing.T) {
	limiter, err := newKDFLimiter(1, 1, 1)
	if err != nil {
		t.Fatalf("newKDFLimiter: %v", err)
	}
	handler, store, _ := newTestHandler(t, limiter, nil)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for range 2 {
		response := request(handler, http.MethodPost, "/v1/sessions",
			`{"Username":"alice","Password":"correct password","DeviceName":"laptop"}`, "")
		if response.Code != http.StatusInternalServerError {
			t.Fatalf("storage error response = %d %q, want 500 not leaked limiter 429", response.Code, response.Body.String())
		}
	}
}

func TestPasswordHashParametersAndStrictVerification(t *testing.T) {
	params := testParams()
	hash, err := HashPassword([]byte("password"), params, bytes.NewReader(bytes.Repeat([]byte{7}, 32)))
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if !strings.HasPrefix(hash, "$argon2id$v=19$m=64,t=1,p=1$") {
		t.Fatalf("hash format = %q", hash)
	}
	valid, err := VerifyPassword([]byte("password"), hash, params)
	if err != nil || !valid {
		t.Fatalf("correct VerifyPassword = %v, %v", valid, err)
	}
	valid, err = VerifyPassword([]byte("wrong"), hash, params)
	if err != nil || valid {
		t.Fatalf("wrong VerifyPassword = %v, %v", valid, err)
	}

	invalidParams := []Params{
		{},
		{Memory: 7, Iterations: 1, Parallelism: 1, SaltLength: 8, KeyLength: 16},
		{Memory: _maxArgonMemory + 1, Iterations: 1, Parallelism: 1, SaltLength: 8, KeyLength: 16},
		{Memory: 64, Iterations: _maxArgonIterations + 1, Parallelism: 1, SaltLength: 8, KeyLength: 16},
		{Memory: 64, Iterations: 1, Parallelism: _maxArgonParallelism + 1, SaltLength: 8, KeyLength: 16},
		{Memory: 64, Iterations: 1, Parallelism: 1, SaltLength: _maxArgonSaltLength + 1, KeyLength: 16},
		{Memory: 64, Iterations: 1, Parallelism: 1, SaltLength: 8, KeyLength: _maxArgonKeyLength + 1},
	}
	for _, params := range invalidParams {
		if _, err := HashPassword([]byte("password"), params, bytes.NewReader(nil)); !errors.Is(err, _errInvalidParams) {
			t.Errorf("HashPassword(%+v) error = %v, want invalid parameters", params, err)
		}
	}

	malformed := []string{
		strings.Replace(hash, "v=19", "v=19x", 1),
		strings.Replace(hash, "m=64", "m=64x", 1),
		strings.Replace(hash, "m=64", "m=064", 1),
		strings.Replace(hash, "t=1", "t=11", 1),
		strings.Replace(hash, "p=1", "p=17", 1),
		strings.Replace(hash, "m=64", "m=262145", 1),
		"$argon2id$v=19$m=64,t=1,p=1$" + strings.Repeat("A", 1000) + "$AAAA",
		"$argon2id$v=19$m=64,t=1,p=1$AAAAAAAAAAA$" + strings.Repeat("A", 1000),
		strings.Replace(hash, "$BwcH", "$Bw\ncH", 1),
		hash + "$trailing",
	}
	for _, encoded := range malformed {
		if valid, err := VerifyPassword([]byte("password"), encoded, params); !errors.Is(err, _errInvalidHash) || valid {
			t.Errorf("VerifyPassword(%q) = %v, %v; want false, %v", encoded, valid, err, _errInvalidHash)
		}
	}
}

func TestDeleteSessionTokenFailuresAreUniform(t *testing.T) {
	handler, store, now := newTestHandler(t, nil, nil)
	defer closeTestStore(t, store)

	expiredToken := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, _accessTokenBytes))
	expiredHash := sha256.Sum256([]byte(expiredToken))
	if err := store.CreateSession(t.Context(), "user-id", expiredHash, "old", now.Add(-2*time.Hour), now.Add(-time.Hour)); err != nil {
		t.Fatalf("CreateSession expired: %v", err)
	}
	missingToken := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{2}, _accessTokenBytes))
	for _, token := range []string{
		"",
		"short",
		strings.Repeat("a", 43),
		strings.Repeat("A", 42) + "*",
		strings.Repeat("A", 43) + "=",
		missingToken,
		expiredToken,
	} {
		response := request(handler, http.MethodDelete, "/v1/sessions/current", "", token)
		assertUnauthorized(t, response)
	}
}

func TestJSONWriteErrorsAreLogged(t *testing.T) {
	var logs bytes.Buffer
	h := &handler{logger: log.New(&logs, "", 0)}
	h.writeError(failingResponseWriter{header: make(http.Header)}, http.StatusBadRequest, 1000, "invalid request")
	if !strings.Contains(logs.String(), `"phase":"write_session_response"`) ||
		!strings.Contains(logs.String(), `"error_category":"unavailable"`) || strings.Contains(logs.String(), "output unavailable") {
		t.Fatalf("logger output = %q", logs.String())
	}
}

func TestNewHandlerRejectsInvalidConstruction(t *testing.T) {
	if _, err := newKDFLimiter(0, 1, 1); err == nil {
		t.Fatal("zero KDF limit unexpectedly accepted")
	}
	if _, err := NewHandler(nil, nil, HandlerConfig{}); err == nil {
		t.Fatal("nil store unexpectedly accepted")
	}
}

func newTestHandler(t *testing.T, limiter *kdfLimiter, logger *log.Logger) (http.Handler, *storage.Store, time.Time) {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := storage.Init(t.Context(), dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	store, err := storage.OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenForServe: %v", err)
	}
	params := testParams()
	hash, err := HashPassword([]byte("correct password"), params, bytes.NewReader(bytes.Repeat([]byte{3}, 32)))
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	if err := store.CreateUser(t.Context(), storage.User{ID: "user-id", Username: "Alice", PasswordHash: hash}, now); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	random := bytes.NewReader(bytes.Repeat([]byte{9}, 1024))
	handler, err := NewHandler(store, logger, HandlerConfig{
		Params: params, Random: random, Now: func() time.Time { return now }, limiter: limiter,
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler, store, now
}

func request(handler http.Handler, method, path, body, token string) *httptest.ResponseRecorder {
	headers := make(map[string]string)
	if method == http.MethodPost {
		headers["Content-Type"] = "application/json"
	}
	return requestWithHeaders(handler, method, path, body, token, "192.0.2.1:1234", headers)
}

func requestWithHeaders(handler http.Handler, method, path, body, token, remoteAddr string, headers map[string]string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.RemoteAddr = remoteAddr
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	handler.ServeHTTP(recorder, req)
	return recorder
}

func assertUnauthorized(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Code != http.StatusUnauthorized || response.Header().Get("WWW-Authenticate") != "Bearer" ||
		response.Body.String() != "{\"RetCode\":1001,\"Message\":\"authentication failed\"}\n" {
		t.Fatalf("unauthorized response = %d headers=%v body=%q", response.Code, response.Header(), response.Body.String())
	}
}

func closeTestStore(t *testing.T, store *storage.Store) {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

type failingResponseWriter struct {
	header http.Header
}

func (w failingResponseWriter) Header() http.Header {
	return w.header
}

func (failingResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("output unavailable")
}

func (failingResponseWriter) WriteHeader(int) {}
