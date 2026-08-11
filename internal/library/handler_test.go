package library

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mingming-cn/filecloud/internal/storage"
)

const (
	_ownerID      = "12345678-9abc-4def-8123-456789abcdef"
	_otherID      = "22345678-9abc-4def-8123-456789abcdef"
	_ownerToken   = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE"
	_otherToken   = "AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI"
	_expiredToken = "AwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM"
	_revokedToken = "BAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQ"
)

func TestCreateReplayAndReadLibrary(t *testing.T) {
	handler, store, now := newTestHandler(t)
	defer closeStore(t, store)
	id := "01234567-89ab-4def-8123-456789abcdef"

	response := serve(handler, http.MethodPut, "/v1/libraries/"+id, `{"Name":"A\u030Angstrom"}`, _ownerToken)
	assertStatusCode(t, response, http.StatusCreated, 0)
	var created libraryEnvelope
	decode(t, response, &created)
	if created.Library.LibraryID != id || created.Library.OwnerUserID != _ownerID || created.Library.Name != "Ångstrom" || created.Library.HeadCommitID != nil ||
		created.Library.ETag != `"head-version-0"` || created.Library.CreatedAt != now.Truncate(time.Second).Format(time.RFC3339) ||
		response.Header().Get("ETag") != `"head-version-0"` {
		t.Fatalf("created library = %+v, headers = %v", created.Library, response.Header())
	}

	response = serve(handler, http.MethodPut, "/v1/libraries/"+id, `{"Name":"Ångstrom"}`, _ownerToken)
	assertStatusCode(t, response, http.StatusOK, 0)
	response = serve(handler, http.MethodPut, "/v1/libraries/"+id, `{"Name":"different"}`, _ownerToken)
	assertStatusCode(t, response, http.StatusConflict, 3001)

	response = serve(handler, http.MethodGet, "/v1/libraries/"+id, "", _ownerToken)
	assertStatusCode(t, response, http.StatusOK, 0)
	if response.Header().Get("ETag") != `"head-version-0"` {
		t.Fatalf("GET ETag = %q", response.Header().Get("ETag"))
	}
}

func TestCreateLibraryRejectsInvalidInput(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	validID := "01234567-89ab-4def-8123-456789abcdef"
	tests := []struct {
		name   string
		path   string
		body   string
		status int
		code   int
	}{
		{name: "uppercase UUID", path: "/v1/libraries/01234567-89AB-4def-8123-456789abcdef", body: `{"Name":"x"}`, status: 400, code: 1000},
		{name: "invalid UUID variant", path: "/v1/libraries/01234567-89ab-4def-7123-456789abcdef", body: `{"Name":"x"}`, status: 400, code: 1000},
		{name: "empty name", path: "/v1/libraries/" + validID, body: `{"Name":""}`, status: 400, code: 1000},
		{name: "too many characters", path: "/v1/libraries/" + validID, body: `{"Name":"` + strings.Repeat("a", 129) + `"}`, status: 400, code: 1000},
		{name: "unknown field", path: "/v1/libraries/" + validID, body: `{"Name":"x","Extra":1}`, status: 400, code: 1000},
		{name: "duplicate field", path: "/v1/libraries/" + validID, body: `{"Name":"x","Name":"y"}`, status: 400, code: 1000},
		{name: "trailing JSON", path: "/v1/libraries/" + validID, body: `{"Name":"x"}{}`, status: 400, code: 1000},
		{name: "unpaired high surrogate", path: "/v1/libraries/" + validID, body: `{"Name":"\ud800"}`, status: 400, code: 1000},
		{name: "unpaired low surrogate", path: "/v1/libraries/" + validID, body: `{"Name":"\udc00"}`, status: 400, code: 1000},
		{name: "oversize", path: "/v1/libraries/" + validID, body: strings.Repeat("x", _maxRequestBody+1), status: 413, code: 3005},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := serve(handler, http.MethodPut, test.path, test.body, _ownerToken)
			assertStatusCode(t, response, test.status, test.code)
		})
	}

	response := serve(handler, http.MethodPut, "/v1/libraries/00000000-0000-4000-8000-000000000009", `{"Name":"\ud83d\ude00"}`, _ownerToken)
	assertStatusCode(t, response, 201, 0)

	response = serve(handler, http.MethodPut, "/v1/libraries/"+validID, `{"Name":"same"}`, _ownerToken)
	assertStatusCode(t, response, 201, 0)
	response = serve(handler, http.MethodPut, "/v1/libraries/00000000-0000-4000-8000-000000000001", `{"Name":"same"}`, _ownerToken)
	assertStatusCode(t, response, 409, 3000)
}

func TestConcurrentLibraryCreatesPreserveIdempotencyAndUniqueness(t *testing.T) {
	tests := []struct {
		name     string
		requests [2]createRequest
		want     [2]createResult
	}{
		{
			name: "same id and name",
			requests: [2]createRequest{
				{ID: "00000000-0000-4000-8000-000000000001", Name: "same"},
				{ID: "00000000-0000-4000-8000-000000000001", Name: "same"},
			},
			want: [2]createResult{{Status: 200}, {Status: 201}},
		},
		{
			name: "same id and different name",
			requests: [2]createRequest{
				{ID: "00000000-0000-4000-8000-000000000001", Name: "first"},
				{ID: "00000000-0000-4000-8000-000000000001", Name: "second"},
			},
			want: [2]createResult{{Status: 201}, {Status: 409, RetCode: 3001}},
		},
		{
			name: "different id and same canonical name",
			requests: [2]createRequest{
				{ID: "00000000-0000-4000-8000-000000000001", Name: "A\\u030A"},
				{ID: "00000000-0000-4000-8000-000000000002", Name: "Å"},
			},
			want: [2]createResult{{Status: 201}, {Status: 409, RetCode: 3000}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, store, _ := newTestHandler(t)
			defer closeStore(t, store)
			got := createLibrariesConcurrently(handler, test.requests)
			if got != test.want {
				t.Fatalf("concurrent creates = %+v, want %+v", got, test.want)
			}
		})
	}
}

func TestListLibrariesUsesStableOpaqueExpiringPagination(t *testing.T) {
	handler, store, now := newTestHandler(t)
	defer closeStore(t, store)
	for index, id := range []string{
		"00000000-0000-4000-8000-000000000003",
		"00000000-0000-4000-8000-000000000001",
		"00000000-0000-4000-8000-000000000002",
	} {
		response := serve(handler, http.MethodPut, "/v1/libraries/"+id, `{"Name":"library-`+string(rune('a'+index))+`"}`, _ownerToken)
		assertStatusCode(t, response, 201, 0)
	}

	response := serve(handler, http.MethodGet, "/v1/libraries?PageSize=1", "", _ownerToken)
	assertStatusCode(t, response, 200, 0)
	var first listEnvelope
	decode(t, response, &first)
	if len(first.Libraries) != 1 || first.Libraries[0].LibraryID != "00000000-0000-4000-8000-000000000001" || first.NextPageToken == "" {
		t.Fatalf("first page = %+v", first)
	}
	decodedToken, err := base64.RawURLEncoding.Strict().DecodeString(first.NextPageToken)
	if err != nil {
		t.Fatalf("decode opaque page token: %v", err)
	}
	if bytes.Contains(decodedToken, []byte(first.Libraries[0].LibraryID)) || bytes.Contains(decodedToken, []byte("owner")) {
		t.Fatalf("page token exposes plaintext cursor: %q", decodedToken)
	}
	foreignToken := serve(handler, http.MethodGet,
		"/v1/libraries?PageToken="+first.NextPageToken, "", _otherToken)
	assertStatusCode(t, foreignToken, 400, 1000)
	invalidToken := serve(handler, http.MethodGet, "/v1/libraries?PageToken=invalid", "", _otherToken)
	assertStatusCode(t, invalidToken, 400, 1000)
	if foreignToken.Body.String() != invalidToken.Body.String() {
		t.Fatalf("foreign and invalid page token responses differ: %q vs %q",
			foreignToken.Body.String(), invalidToken.Body.String())
	}

	response = serve(handler, http.MethodGet, "/v1/libraries?PageSize=2&PageToken="+first.NextPageToken, "", _ownerToken)
	assertStatusCode(t, response, 200, 0)
	var second listEnvelope
	decode(t, response, &second)
	if len(second.Libraries) != 2 || second.Libraries[0].LibraryID != "00000000-0000-4000-8000-000000000002" || second.NextPageToken != "" {
		t.Fatalf("second page = %+v", second)
	}

	tamperedBytes := bytes.Clone(decodedToken)
	tamperedBytes[len(tamperedBytes)-1] ^= 1
	tampered := base64.RawURLEncoding.EncodeToString(tamperedBytes)
	for _, path := range []string{
		"/v1/libraries?PageSize=",
		"/v1/libraries?PageToken=",
		"/v1/libraries?PageSize=0",
		"/v1/libraries?PageSize=501",
		"/v1/libraries?PageSize=x",
		"/v1/libraries?PageToken=invalid",
		"/v1/libraries?PageToken=" + tampered,
	} {
		assertStatusCode(t, serve(handler, http.MethodGet, path, "", _ownerToken), 400, 1000)
	}

	expiring, err := NewHandler(store, log.New(io.Discard, "", 0), Config{
		Now:          func() time.Time { return now.Add(_pageTokenLifetime + time.Second) },
		PageTokenKey: bytes.Repeat([]byte{7}, 32),
	})
	if err != nil {
		t.Fatalf("NewHandler expiring: %v", err)
	}
	assertStatusCode(t, serve(expiring, http.MethodGet, "/v1/libraries?PageToken="+first.NextPageToken, "", _ownerToken), 400, 1000)
}

func TestLibraryAuthenticationAndOwnerIsolationAreUniform(t *testing.T) {
	handler, store, now := newTestHandler(t)
	defer closeStore(t, store)
	id := "01234567-89ab-4def-8123-456789abcdef"
	assertStatusCode(t, serve(handler, http.MethodPut, "/v1/libraries/"+id, `{"Name":"private"}`, _ownerToken), 201, 0)

	missing := serve(handler, http.MethodGet, "/v1/libraries/00000000-0000-4000-8000-000000000001", "", _otherToken)
	foreign := serve(handler, http.MethodGet, "/v1/libraries/"+id, "", _otherToken)
	assertStatusCode(t, missing, 404, 2000)
	assertStatusCode(t, foreign, 404, 2000)
	if missing.Body.String() != foreign.Body.String() {
		t.Fatalf("missing and foreign responses differ: %q vs %q", missing.Body.String(), foreign.Body.String())
	}

	response := serve(handler, http.MethodPut, "/v1/libraries/"+id, `{"Name":"other private"}`, _otherToken)
	assertStatusCode(t, response, 201, 0)
	response = serve(handler, http.MethodGet, "/v1/libraries/"+id, "", _otherToken)
	assertStatusCode(t, response, 200, 0)
	var other libraryEnvelope
	decode(t, response, &other)
	if other.Library.Name != "other private" {
		t.Fatalf("same LibraryId across owners returned %+v", other.Library)
	}

	expiredHash := sha256.Sum256([]byte(_expiredToken))
	if err := store.CreateSession(t.Context(), _ownerID, expiredHash, "expired", now.Add(-2*time.Hour), now.Add(-time.Hour)); err != nil {
		t.Fatalf("CreateSession expired: %v", err)
	}
	revokedHash := sha256.Sum256([]byte(_revokedToken))
	if err := store.CreateSession(t.Context(), _ownerID, revokedHash, "revoked", now.Add(-time.Hour), now.Add(time.Hour)); err != nil {
		t.Fatalf("CreateSession revoked: %v", err)
	}
	if revoked, err := store.RevokeSession(t.Context(), revokedHash, now); err != nil || !revoked {
		t.Fatalf("RevokeSession = %v, %v", revoked, err)
	}
	for _, token := range []string{"", "invalid", base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32)), _expiredToken, _revokedToken} {
		response := serve(handler, http.MethodGet, "/v1/libraries/"+id, "", token)
		assertStatusCode(t, response, 401, 1001)
		if response.Header().Get("WWW-Authenticate") != "Bearer" {
			t.Fatalf("WWW-Authenticate = %q", response.Header().Get("WWW-Authenticate"))
		}
	}
}

type createRequest struct {
	ID   string
	Name string
}

type createResult struct {
	Status  int
	RetCode int
}

func createLibrariesConcurrently(handler http.Handler, requests [2]createRequest) [2]createResult {
	start := make(chan struct{})
	results := make(chan createResult, len(requests))
	var wait sync.WaitGroup
	for _, request := range requests {
		wait.Go(func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					results <- createResult{Status: 500, RetCode: -1}
				}
			}()
			<-start
			response := serve(handler, http.MethodPut, "/v1/libraries/"+request.ID,
				`{"Name":"`+request.Name+`"}`, _ownerToken)
			var envelope struct{ RetCode int }
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
				results <- createResult{Status: response.Code, RetCode: -1}
				return
			}
			results <- createResult{Status: response.Code, RetCode: envelope.RetCode}
		})
	}
	close(start)
	wait.Wait()
	close(results)

	got := make([]createResult, 0, len(requests))
	for result := range results {
		got = append(got, result)
	}
	slices.SortFunc(got, func(a, b createResult) int {
		return cmp.Or(cmp.Compare(a.Status, b.Status), cmp.Compare(a.RetCode, b.RetCode))
	})
	return [2]createResult(got)
}

type libraryEnvelope struct {
	RetCode int
	Message string
	Library libraryResponse
}

type listEnvelope struct {
	RetCode       int
	Message       string
	Libraries     []libraryResponse
	NextPageToken string
}

func newTestHandler(t *testing.T) (http.Handler, *storage.Store, time.Time) {
	return newTestHandlerWithConfig(t, Config{})
}

func newTestHandlerWithConfig(t *testing.T, config Config) (http.Handler, *storage.Store, time.Time) {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := storage.Init(t.Context(), dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	store, err := storage.OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenForServe: %v", err)
	}
	now := time.Date(2026, 2, 3, 4, 5, 6, 789, time.UTC)
	for _, user := range []storage.User{
		{ID: _ownerID, Username: "alice", PasswordHash: "hash"},
		{ID: _otherID, Username: "bob", PasswordHash: "hash"},
	} {
		if err := store.CreateUser(t.Context(), user, now); err != nil {
			t.Fatalf("CreateUser: %v", err)
		}
	}
	for token, userID := range map[string]string{_ownerToken: _ownerID, _otherToken: _otherID} {
		digest := sha256.Sum256([]byte(token))
		if err := store.CreateSession(t.Context(), userID, digest, "device", now, now.Add(time.Hour)); err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
	}
	if config.Now == nil {
		config.Now = func() time.Time { return now }
	}
	if config.PageTokenKey == nil {
		config.PageTokenKey = bytes.Repeat([]byte{7}, 32)
	}
	handler, err := NewHandler(store, log.New(io.Discard, "", 0), config)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler, store, now
}

func serve(handler http.Handler, method, path, body, token string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	handler.ServeHTTP(response, request)
	return response
}

func assertStatusCode(t *testing.T, response *httptest.ResponseRecorder, status, code int) {
	t.Helper()
	var envelope struct{ RetCode int }
	if response.Body.Len() > 0 {
		if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
			t.Fatalf("decode response %d %q: %v", response.Code, response.Body.String(), err)
		}
	}
	if response.Code != status || envelope.RetCode != code {
		t.Fatalf("response = %d/%d %q, want %d/%d", response.Code, envelope.RetCode, response.Body.String(), status, code)
	}
}

func decode(t *testing.T, response *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.Unmarshal(response.Body.Bytes(), target); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

func ExampleConfig() {
	config := Config{Now: func() time.Time { return time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC) }}
	fmt.Println(config.Now().UTC().Format(time.RFC3339))
	// Output: 2026-02-03T04:05:06Z
}

func closeStore(t *testing.T, store *storage.Store) {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
