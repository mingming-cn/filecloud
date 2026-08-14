package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mingming-cn/filecloud/internal/fscompat"
	libraryapi "github.com/mingming-cn/filecloud/internal/library"
	"github.com/mingming-cn/filecloud/internal/object"
	"github.com/mingming-cn/filecloud/internal/storage"
	_ "modernc.org/sqlite"
)

const (
	testClientUserID    = "01234567-89ab-4def-8123-456789abcdef"
	testClientLibraryID = "11111111-2222-4333-8444-555555555555"
	testClientDeviceID  = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
	testOtherDeviceID   = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
)

func TestCheckRemoteObjectsRejectsInvalidResponses(t *testing.T) {
	requestedID := object.ID([]byte("requested"))
	unexpectedID := object.ID([]byte("unexpected"))
	tests := []struct {
		name, response string
	}{
		{"missing structure", `{"RetCode":0,"Message":"success"}`},
		{"wrong field type", `{"RetCode":0,"Message":"success","MissingObjects":"bad"}`},
		{"unexpected id", `{"RetCode":0,"Message":"success","MissingObjects":[{"ObjectId":"` + unexpectedID + `","ObjectType":"Block"}]}`},
		{"unexpected type", `{"RetCode":0,"Message":"success","MissingObjects":[{"ObjectId":"` + requestedID + `","ObjectType":"File"}]}`},
		{"duplicate", `{"RetCode":0,"Message":"success","MissingObjects":[{"ObjectId":"` + requestedID + `","ObjectType":"Block"},{"ObjectId":"` + requestedID + `","ObjectType":"Block"}]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, test.response)
			}))
			defer server.Close()
			options := bindOptions{base: mustServerURL(t, server.URL), libraryID: testClientLibraryID, token: []byte("token")}
			_, err := checkRemoteObjects(t.Context(), options, []clientObjectReference{{ObjectID: requestedID, ObjectType: "Block"}})
			if err == nil || !strings.Contains(err.Error(), "invalid object checks response") {
				t.Fatalf("invalid response error=%v", err)
			}
		})
	}
}

func TestLibraryClientIgnoresOptionalResponseFields(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := httptest.NewRecorder()
		environment.handler.ServeHTTP(response, r)
		for name, values := range response.Header() {
			for _, value := range values {
				w.Header().Add(name, value)
			}
		}
		data := response.Body.Bytes()
		if response.Code == http.StatusOK && strings.HasSuffix(r.URL.Path, "/head") {
			var envelope map[string]any
			if err := json.Unmarshal(data, &envelope); err != nil {
				t.Errorf("decode Head fixture: %v", err)
				return
			}
			envelope["OptionalFutureField"] = true
			head, ok := envelope["Head"].(map[string]any)
			if !ok {
				t.Errorf("Head fixture shape = %#v", envelope["Head"])
				return
			}
			head["OptionalFutureField"] = "ignored"
			var err error
			data, err = json.Marshal(envelope)
			if err != nil {
				t.Errorf("encode Head fixture: %v", err)
				return
			}
		}
		w.WriteHeader(response.Code)
		if _, err := w.Write(data); err != nil {
			t.Errorf("write Head fixture: %v", err)
		}
	}))
	defer server.Close()

	head, err := getRemoteHead(t.Context(), mustServerURL(t, server.URL), testClientLibraryID, []byte(environment.token))
	if err != nil || head.CommitID != nil || head.ETag != `"head-version-0"` {
		t.Fatalf("Head with optional fields = %+v, %v", head, err)
	}
}

func TestCheckRemoteObjectsBatchesAt1000(t *testing.T) {
	var batchSizes []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct{ Objects []clientObjectReference }
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode object checks request: %v", err)
			return
		}
		batchSizes = append(batchSizes, len(request.Objects))
		w.Header().Set("Content-Type", "application/json")
		if _, err := io.WriteString(w, `{"RetCode":0,"Message":"success","MissingObjects":[],"OptionalFutureField":true}`); err != nil {
			t.Errorf("write object checks response: %v", err)
		}
	}))
	defer server.Close()
	references := make([]clientObjectReference, 1001)
	for index := range references {
		references[index] = clientObjectReference{ObjectID: object.ID([]byte(fmt.Sprintf("object-%d", index))), ObjectType: "Block"}
	}
	options := bindOptions{base: mustServerURL(t, server.URL), libraryID: testClientLibraryID, token: []byte("token")}
	missing, err := checkRemoteObjects(t.Context(), options, references)
	if err != nil || len(missing) != 0 || len(batchSizes) != 2 || batchSizes[0] != 1000 || batchSizes[1] != 1 {
		t.Fatalf("object check batches=%v missing=%v err=%v", batchSizes, missing, err)
	}
}

func TestUpdateRemoteHeadRetriesExplicitTransientResponses(t *testing.T) {
	commitID := strings.Repeat("a", 64)
	attempts := atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read attempt %d: %v", attempt, err)
			return
		}
		var request struct {
			CommitID string `json:"CommitId"`
		}
		if err := json.Unmarshal(body, &request); err != nil || request.CommitID != commitID ||
			r.Header.Get("If-Match") != `"head-version-0"` {
			t.Errorf("attempt %d request=%q If-Match=%q err=%v", attempt, body, r.Header.Get("If-Match"), err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch attempt {
		case 1:
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			if _, err := io.WriteString(w, `{"RetCode":4000,"Message":"head validation busy"}`); err != nil {
				t.Errorf("write busy response: %v", err)
			}
		case 2:
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable)
			if _, err := io.WriteString(w, `{"RetCode":5001,"Message":"head validation unavailable"}`); err != nil {
				t.Errorf("write unavailable response: %v", err)
			}
		default:
			response, err := json.Marshal(struct {
				RetCode int
				Message string
				Head    remoteHead
			}{RetCode: 0, Message: "success", Head: remoteHead{CommitID: &commitID, ETag: `"head-version-1"`}})
			if err != nil {
				t.Errorf("marshal success response: %v", err)
				return
			}
			if _, err := w.Write(response); err != nil {
				t.Errorf("write success response: %v", err)
			}
		}
	}))
	defer server.Close()

	head, conflict, err := updateRemoteHead(t.Context(), mustServerURL(t, server.URL), testClientLibraryID,
		[]byte("token"), `"head-version-0"`, commitID)
	if err != nil || conflict || attempts.Load() != 3 || head.CommitID == nil || *head.CommitID != commitID || head.ETag != `"head-version-1"` {
		t.Fatalf("update Head = %+v conflict=%v attempts=%d err=%v", head, conflict, attempts.Load(), err)
	}
}

func TestDoClientRequestReturnsTransportErrorWithNoResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack response: %v", err)
			return
		}
		if err := connection.Close(); err != nil {
			t.Errorf("close hijacked response: %v", err)
		}
	}))
	defer server.Close()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	status, data, etag, err := doClientRequest(request)
	if err == nil || status != 0 || data != nil || etag != "" {
		t.Fatalf("transport failure = status=%d data=%q etag=%q err=%v", status, data, etag, err)
	}
}

func TestUpdateRemoteHeadDoesNotRetryUnknownNetworkResult(t *testing.T) {
	attempts := atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack Head response: %v", err)
			return
		}
		if err := connection.Close(); err != nil {
			t.Errorf("close hijacked Head response: %v", err)
		}
	}))
	defer server.Close()

	_, _, err := updateRemoteHead(t.Context(), mustServerURL(t, server.URL), testClientLibraryID,
		[]byte("token"), `"head-version-0"`, strings.Repeat("a", 64))
	if err == nil || attempts.Load() != 1 {
		t.Fatalf("unknown Head result attempts=%d err=%v", attempts.Load(), err)
	}
}

func TestTransientRetryDelayAndCancellation(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	for _, test := range []struct {
		name       string
		retryAfter string
		want       time.Duration
	}{
		{name: "seconds", retryAfter: "2", want: 2 * time.Second},
		{name: "date", retryAfter: now.Add(3 * time.Second).Format(http.TimeFormat), want: 3 * time.Second},
		{name: "past date", retryAfter: now.Add(-time.Second).Format(http.TimeFormat), want: 0},
		{name: "bounded date", retryAfter: now.Add(time.Hour).Format(http.TimeFormat), want: 30 * time.Second},
		{name: "invalid default", retryAfter: "invalid", want: time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := transientRetryDelay(test.retryAfter, now); got != test.want {
				t.Fatalf("transientRetryDelay(%q)=%s want=%s", test.retryAfter, got, test.want)
			}
		})
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := waitTransientRetry(ctx, "30", now); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled retry wait error=%v", err)
	}
}

func TestLibraryBindDoubleEmptyConvergesAndUnbindIsLocalOnly(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	allowTestFilesystem(t)
	clientDir := filepath.Join(t.TempDir(), "client")
	worktree := filepath.Join(t.TempDir(), "worktree")
	if err := os.Mkdir(worktree, 0o700); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	if err := runTest(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID),
		strings.NewReader(environment.token+"\n"), &output, io.Discard); err != nil {
		t.Fatalf("library bind: %v", err)
	}
	if !strings.Contains(output.String(), "library bound") {
		t.Fatalf("bind output = %q", output.String())
	}

	head, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if err != nil || head.CommitID == nil {
		t.Fatalf("get initialized Head: head=%+v err=%v", head, err)
	}
	binding := readTestBinding(t, clientDir, worktree)
	if binding.SyncBase != *head.CommitID || binding.HeadETag != head.ETag {
		t.Fatalf("binding Base/ETag = %s/%s, Head = %s/%s", binding.SyncBase, binding.HeadETag, *head.CommitID, head.ETag)
	}
	emptyBytes, emptyID, err := object.Canonicalize("directories", []byte(`{"Entries":[],"Type":"Directory","Version":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if binding.SyncBaseRoot != emptyID {
		t.Fatalf("Sync Base Root = %s, want %s", binding.SyncBaseRoot, emptyID)
	}
	commitBytes := getTestObject(t, environment.server.URL, environment.token, "commits", *head.CommitID)
	commit, err := object.VerifyCommit(commitBytes, *head.CommitID)
	if err != nil {
		t.Fatalf("verify Head commit: %v", err)
	}
	if commit.AuthorUserID != testClientUserID || commit.DeviceID != testClientDeviceID || commit.Message != "sync" || commit.Root != emptyID || len(commit.Parents) != 0 {
		t.Fatalf("initial commit = %+v", commit)
	}
	if got := getTestObject(t, environment.server.URL, environment.token, "directories", emptyID); !bytes.Equal(got, emptyBytes) {
		t.Fatalf("empty Directory bytes = %q, want %q", got, emptyBytes)
	}
	info, err := os.Stat(filepath.Join(clientDir, _clientDatabaseName))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("client database mode = %v, err=%v", info.Mode().Perm(), err)
	}

	output.Reset()
	if err := runTest(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID),
		strings.NewReader(environment.token+"\n"), &output, io.Discard); err != nil {
		t.Fatalf("idempotent bind: %v", err)
	}
	if !strings.Contains(output.String(), "already bound") {
		t.Fatalf("idempotent output = %q", output.String())
	}
	assertPlatformConverged(t, "double-empty binding", environment, clientDir, worktree, nil)

	before := *head.CommitID
	if err := runTest(t.Context(), []string{"library", "unbind", "--client-dir", clientDir, "--worktree", worktree},
		strings.NewReader(""), io.Discard, io.Discard); err != nil {
		t.Fatalf("library unbind: %v", err)
	}
	if err := runTest(t.Context(), []string{"library", "unbind", "--client-dir", clientDir, "--worktree", worktree},
		strings.NewReader(""), io.Discard, io.Discard); err != nil {
		t.Fatalf("idempotent library unbind: %v", err)
	}
	after, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if err != nil || after.CommitID == nil || *after.CommitID != before {
		t.Fatalf("Head changed by unbind: before=%s after=%+v err=%v", before, after, err)
	}
	if _, err := os.Stat(worktree); err != nil {
		t.Fatalf("unbind removed worktree: %v", err)
	}
}

func TestLibraryBindConcurrentInitializationAdoptsWinner(t *testing.T) {
	var arrivals atomic.Int32
	release := make(chan struct{})
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	allowTestFilesystem(t)

	type result struct {
		clientDir string
		worktree  string
		err       error
	}
	results := make(chan result, 2)
	var wait sync.WaitGroup
	for _, deviceID := range []string{testClientDeviceID, testOtherDeviceID} {
		clientDir := filepath.Join(t.TempDir(), "client")
		worktree := filepath.Join(t.TempDir(), "worktree")
		if err := os.Mkdir(worktree, 0o700); err != nil {
			t.Fatal(err)
		}
		wait.Go(func() {
			item := result{clientDir: clientDir, worktree: worktree}
			defer func() {
				if recovered := recover(); recovered != nil {
					item.err = fmt.Errorf("concurrent bind panic: %v", recovered)
				}
				results <- item
			}()
			item.err = runLibraryWithConfig(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, deviceID)[1:],
				strings.NewReader(environment.token+"\n"), io.Discard, io.Discard, libraryClientConfig{
					checkFilesystem: func(*os.File) error { return nil },
					beforeHeadCAS: func() error {
						if arrivals.Add(1) == 2 {
							close(release)
						}
						select {
						case <-release:
							return nil
						case <-t.Context().Done():
							return t.Context().Err()
						}
					},
				})
		})
	}
	first, second := <-results, <-results
	wait.Wait()
	for _, item := range []result{first, second} {
		if item.err != nil {
			t.Fatalf("concurrent bind: %v", item.err)
		}
	}
	firstBinding := readTestBinding(t, first.clientDir, first.worktree)
	secondBinding := readTestBinding(t, second.clientDir, second.worktree)
	if firstBinding.SyncBase != secondBinding.SyncBase || firstBinding.SyncBaseRoot != secondBinding.SyncBaseRoot {
		t.Fatalf("clients diverged: first=%+v second=%+v", firstBinding, secondBinding)
	}
	commitBytes := getTestObject(t, environment.server.URL, environment.token, "commits", firstBinding.SyncBase)
	commit, err := object.VerifyCommit(commitBytes, firstBinding.SyncBase)
	if err != nil || len(commit.Parents) != 0 {
		t.Fatalf("winner is not an initial commit: %+v err=%v", commit, err)
	}
	assertPlatformConverged(t, "concurrent initialization first client", environment, first.clientDir, first.worktree, nil)
	assertPlatformConverged(t, "concurrent initialization second client", environment, second.clientDir, second.worktree, nil)
}

func TestLibraryBindRejectsUnsupportedOrNonEmptyAndBindingConflicts(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	clientDir := filepath.Join(t.TempDir(), "client")
	worktree := filepath.Join(t.TempDir(), "worktree")
	if err := os.Mkdir(worktree, 0o700); err != nil {
		t.Fatal(err)
	}

	err := runLibraryWithConfig(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID)[1:],
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard, libraryClientConfig{
			checkFilesystem: func(*os.File) error { return errors.New("unsupported test filesystem") },
		})
	if err == nil || !strings.Contains(err.Error(), "unsupported test filesystem") {
		t.Fatalf("unsupported filesystem error = %v", err)
	}
	if _, err := os.Stat(clientDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsupported bind wrote client state: %v", err)
	}

	allowTestFilesystem(t)
	if err := os.WriteFile(filepath.Join(worktree, "local.txt"), []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	err = runTest(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "issue #8") {
		t.Fatalf("non-empty local error = %v", err)
	}
	if err := os.Remove(filepath.Join(worktree, "local.txt")); err != nil {
		t.Fatal(err)
	}
	if err := runTest(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("initial bind: %v", err)
	}

	otherWorktree := filepath.Join(t.TempDir(), "other")
	if err := os.Mkdir(otherWorktree, 0o700); err != nil {
		t.Fatal(err)
	}
	err = runTest(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, otherWorktree, testOtherDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "unbind it first") {
		t.Fatalf("same library new path error = %v", err)
	}

	otherLibraryID := "22222222-3333-4444-8555-666666666666"
	if _, _, err := environment.store.CreateLibrary(t.Context(), storage.Library{ID: otherLibraryID, OwnerUserID: testClientUserID, Name: "Other"}, time.Now()); err != nil {
		t.Fatalf("CreateLibrary(other): %v", err)
	}
	err = runTest(t.Context(), bindArgs(clientDir, environment.server.URL, otherLibraryID, worktree, testClientDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "already bound to another library") {
		t.Fatalf("same worktree new library error = %v", err)
	}

	freshClient := filepath.Join(t.TempDir(), "fresh-client")
	if err := runTest(t.Context(), bindArgs(freshClient, environment.server.URL, testClientLibraryID, otherWorktree, testOtherDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("remote checkout bind: %v", err)
	}
	if binding := readTestBinding(t, freshClient, otherWorktree); binding.SyncBase == "" {
		t.Fatal("remote checkout did not establish Sync Base")
	}
	assertPlatformConverged(t, "binding conflict original client", environment, clientDir, worktree, nil)
	assertPlatformConverged(t, "binding conflict remote checkout", environment, freshClient, otherWorktree, nil)
}

func TestLibraryBindRevalidatesBeforeHeadCASAndUnbindCancelsIntent(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	clientDir, worktree := newClientPaths(t)
	localPath := filepath.Join(worktree, "appeared.txt")
	args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID)
	err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
		libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, beforeHeadCAS: func() error {
			return os.WriteFile(localPath, []byte("local"), 0o600)
		}})
	if err == nil || !strings.Contains(err.Error(), "worktree changed during bind") {
		t.Fatalf("CAS barrier error = %v", err)
	}
	head, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if err != nil || head.CommitID != nil {
		t.Fatalf("Head published after worktree changed: head=%+v err=%v", head, err)
	}
	assertIntentCount(t, clientDir, 1)
	config := libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}
	otherWorktree := filepath.Join(t.TempDir(), "other-worktree")
	if err := os.Mkdir(otherWorktree, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", clientDir, "--worktree", otherWorktree},
		strings.NewReader(""), io.Discard, io.Discard, config); err != nil {
		t.Fatalf("unbind other worktree: %v", err)
	}
	assertIntentCount(t, clientDir, 1)
	if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", clientDir, "--worktree", worktree},
		strings.NewReader(""), io.Discard, io.Discard, config); err != nil {
		t.Fatalf("cancel pending bind: %v", err)
	}
	assertNoIntent(t, clientDir)
	if got, err := os.ReadFile(localPath); err != nil || string(got) != "local" {
		t.Fatalf("unbind changed worktree: data=%q err=%v", got, err)
	}
	if err := os.Remove(localPath); err != nil {
		t.Fatal(err)
	}
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("bind after cancel: %v", err)
	}
}

func TestLibraryClientConfigAndStateDurability(t *testing.T) {
	config := normalizeLibraryClientConfig(libraryClientConfig{})
	if config.checkFilesystem == nil || config.now == nil || config.syncFile == nil || config.syncDirectory == nil {
		t.Fatalf("zero config not normalized: %+v", config)
	}

	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	clientDir, worktree := newClientPaths(t)
	var synced []string
	config = libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, syncDirectory: func(path string) error {
		synced = append(synced, path)
		return syncDirectory(path)
	}}
	args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID)
	if err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard, config); err != nil {
		t.Fatal(err)
	}
	want := []string{filepath.Dir(clientDir), clientDir, clientDir, clientDir}
	if strings.Join(synced, "|") != strings.Join(want, "|") {
		t.Fatalf("synced directories = %v, want %v", synced, want)
	}
	synced = nil
	if err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard, config); err != nil {
		t.Fatalf("repeat open: %v", err)
	}
	want = []string{filepath.Dir(clientDir), clientDir, clientDir, clientDir}
	if strings.Join(synced, "|") != strings.Join(want, "|") {
		t.Fatalf("repeat open synced directories = %v, want %v", synced, want)
	}

	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var secureDelete int
	if err := db.QueryRow("PRAGMA secure_delete").Scan(&secureDelete); err != nil || secureDelete != 1 {
		t.Fatalf("secure_delete = %d, err=%v", secureDelete, err)
	}
	for table, forbidden := range map[string][]string{
		"bindings":     {"server_identity"},
		"bind_intents": {"access_token", "expected_head", "status"},
	} {
		rows, err := db.Query("PRAGMA table_info(" + table + ")")
		if err != nil {
			t.Fatal(err)
		}
		columns := map[string]bool{}
		for rows.Next() {
			var cid, notNull, primaryKey int
			var name, columnType string
			var defaultValue any
			if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
				t.Fatal(err)
			}
			columns[name] = true
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
		for _, name := range forbidden {
			if columns[name] {
				t.Errorf("%s still has forbidden column %s", table, name)
			}
		}
	}
}

func TestLibraryBindRemotePreflightFailuresCreateNoClientState(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	allowTestFilesystem(t)
	for _, test := range []struct {
		name, libraryID, token string
	}{
		{"missing", "33333333-4444-4555-8666-777777777777", environment.token},
		{"unauthorized", testClientLibraryID, "invalid-token"},
	} {
		t.Run(test.name, func(t *testing.T) {
			clientDir, worktree := newClientPaths(t)
			err := runTest(t.Context(), bindArgs(clientDir, environment.server.URL, test.libraryID, worktree, testClientDeviceID),
				strings.NewReader(test.token+"\n"), io.Discard, io.Discard)
			if err == nil {
				t.Fatal("expected remote preflight failure")
			}
			if _, statErr := os.Stat(clientDir); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("remote preflight created client state: %v", statErr)
			}
		})
	}
}

type panicReader struct{}

func (panicReader) Read([]byte) (int, error) { panic("token reader called") }

func TestLibraryBindCanceledBeforeTokenRead(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := runLibraryWithConfig(ctx, []string{"bind"}, panicReader{}, io.Discard, io.Discard, libraryClientConfig{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled bind error = %v", err)
	}
}

func TestLibraryBindRejectsServerWithoutOwnerBeforeUpload(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	var mutations atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/libraries/"+testClientLibraryID {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"RetCode":0,"Message":"success","Library":{"LibraryId":"`+testClientLibraryID+`"}}`)
			return
		}
		if r.Method == http.MethodPut {
			mutations.Add(1)
		}
		environment.handler.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	clientDir, worktree := newClientPaths(t)
	err := runTest(t.Context(), bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testClientDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "incompatible server") {
		t.Fatalf("missing OwnerUserId error = %v", err)
	}
	if mutations.Load() != 0 {
		t.Fatalf("uploaded %d objects without OwnerUserId", mutations.Load())
	}
	if _, statErr := os.Stat(clientDir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("missing owner created client state: %v", statErr)
	}
}

func TestLibraryBindResolvesCommittedHeadAfterServerReturns500(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{AfterHeadUpdate: func() error {
		return errors.New("response lost")
	}})
	clientDir, worktree := newClientPaths(t)
	var output, errorsOutput bytes.Buffer
	if err := runTest(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID),
		strings.NewReader(environment.token+"\n"), &output, &errorsOutput); err != nil {
		t.Fatalf("bind after unknown CAS result: %v", err)
	}
	if strings.Contains(output.String()+errorsOutput.String(), environment.token) {
		t.Fatal("token leaked to CLI output")
	}
	assertNoIntent(t, clientDir)
}

func TestLibraryBindPreservesPublishErrorWhenHeadRemainsEmpty(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/head") {
			http.Error(w, "publish failed", http.StatusInternalServerError)
			return
		}
		environment.handler.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	clientDir, worktree := newClientPaths(t)
	err := runTest(t.Context(), bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testClientDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "publish library Head failed") || !strings.Contains(err.Error(), "remained empty") {
		t.Fatalf("publish failure = %v", err)
	}
	head, headErr := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if headErr != nil || head.CommitID != nil {
		t.Fatalf("failed publication changed Head: head=%+v err=%v", head, headErr)
	}
}

func TestLibraryBindRetainsIntentWhenPostCASHeadGetFails(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	var published, failed atomic.Bool
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/head") && published.Load() && failed.CompareAndSwap(false, true) {
			http.Error(w, "unavailable", http.StatusInternalServerError)
			return
		}
		environment.handler.ServeHTTP(w, r)
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/head") {
			published.Store(true)
		}
	}))
	defer proxy.Close()
	clientDir, worktree := newClientPaths(t)
	args := bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testClientDeviceID)
	err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "resolve library Head") {
		t.Fatalf("post-CAS GET failure = %v", err)
	}
	assertIntentCount(t, clientDir, 1)
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("recover after post-CAS GET failure: %v", err)
	}
	assertNoIntent(t, clientDir)
}

func TestLibraryBindRetainsIntentAndRecoversAfterFinalizeFailure(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	clientDir, worktree := newClientPaths(t)
	args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID)
	err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
		libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, now: time.Now,
			beforeFinalize: func() error { return errors.New("injected finalize failure") }})
	if err == nil || !strings.Contains(err.Error(), "finalize") {
		t.Fatalf("finalize failure = %v", err)
	}
	assertIntentCount(t, clientDir, 1)
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("recover pending bind: %v", err)
	}
	assertNoIntent(t, clientDir)
	if string(readTestAccessToken(t, clientDir, worktree)) != environment.token {
		t.Fatal("binding did not retain access token")
	}
	for _, name := range append([]string{_clientDatabaseName}, bindingLockNames(worktree, environment.server.URL, testClientLibraryID)...) {
		info, err := os.Stat(filepath.Join(clientDir, name))
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode = %v, err=%v", name, info.Mode().Perm(), err)
		}
	}
}

func TestLibraryBindPendingIntentRequiresSameParameters(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	clientDir, worktree := newClientPaths(t)
	args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID)
	err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
		libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, now: time.Now,
			beforeFinalize: func() error { return errors.New("stop") }})
	if err == nil {
		t.Fatal("expected injected finalize failure")
	}
	otherArgs := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
	err = runTest(t.Context(), otherArgs, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "different bind parameters") {
		t.Fatalf("different pending parameters error = %v", err)
	}
	assertIntentCount(t, clientDir, 1)
}

func TestLibraryBindRejectsTamperedPendingIntentBeforePublication(t *testing.T) {
	for _, test := range []struct {
		name   string
		tamper func(*sql.DB) error
	}{
		{"id", func(db *sql.DB) error {
			_, err := db.Exec("UPDATE bind_intents SET candidate_commit = ?", strings.Repeat("0", 64))
			return err
		}},
		{"data", func(db *sql.DB) error {
			_, err := db.Exec("UPDATE bind_intents SET candidate_data = ?", []byte("{}"))
			return err
		}},
		{"root", func(db *sql.DB) error {
			_, err := db.Exec("UPDATE bind_intents SET candidate_root = ?", strings.Repeat("0", 64))
			return err
		}},
		{"owner", func(db *sql.DB) error {
			_, err := db.Exec("UPDATE bind_intents SET user_id = ?", "22222222-3333-4444-8555-666666666666")
			return err
		}},
		{"device", func(db *sql.DB) error {
			_, err := db.Exec("UPDATE bind_intents SET device_id = ?", testOtherDeviceID)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
			clientDir, worktree := newClientPaths(t)
			args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID)
			err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
				libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, beforeHeadCAS: func() error { return errors.New("stop") }})
			if err == nil {
				t.Fatal("expected setup failure")
			}
			db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			if err := errors.Join(test.tamper(db), db.Close()); err != nil {
				t.Fatal(err)
			}
			err = runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "pending bind intent is corrupt") {
				t.Fatalf("tampered intent error = %v", err)
			}
			head, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
			if err != nil || head.CommitID != nil {
				t.Fatalf("tampered intent changed Head: head=%+v err=%v", head, err)
			}
		})
	}
}

func TestWritableClientTransactionsReserveWriteLockAtBegin(t *testing.T) {
	clientDir := t.TempDir()
	firstDB, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer firstDB.Close()
	secondDB, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	defer secondDB.Close()

	first, err := firstDB.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		tx  *sql.Tx
		err error
	}
	started := make(chan struct{})
	finished := make(chan result, 1)
	go func() {
		close(started)
		tx, err := secondDB.BeginTx(t.Context(), nil)
		finished <- result{tx: tx, err: err}
	}()
	<-started
	select {
	case got := <-finished:
		if got.tx != nil {
			_ = got.tx.Rollback()
		}
		_ = first.Rollback()
		t.Fatalf("second writable transaction did not wait: %v", got.err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := first.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-finished:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if err := got.tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second writable transaction did not start after release")
	}
}

func TestLibraryBindUnrelatedBindingsRunInParallel(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	otherLibraryID := "22222222-3333-4444-8555-666666666666"
	if _, _, err := environment.store.CreateLibrary(t.Context(), storage.Library{ID: otherLibraryID, OwnerUserID: testClientUserID, Name: "Other"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	clientDir := filepath.Join(t.TempDir(), "client")
	_, firstWorktree := newClientPaths(t)
	_, secondWorktree := newClientPaths(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var arrivals atomic.Int32
	release := make(chan struct{})
	results := make(chan error, 2)
	for _, item := range []struct{ libraryID, worktree, deviceID string }{
		{testClientLibraryID, firstWorktree, testClientDeviceID},
		{otherLibraryID, secondWorktree, testOtherDeviceID},
	} {
		go func(item struct{ libraryID, worktree, deviceID string }) {
			results <- runLibraryWithConfig(ctx, bindArgs(clientDir, environment.server.URL, item.libraryID, item.worktree, item.deviceID)[1:],
				strings.NewReader(environment.token+"\n"), io.Discard, io.Discard, libraryClientConfig{
					checkFilesystem: func(*os.File) error { return nil },
					afterLock: func() {
						if arrivals.Add(1) == 2 {
							close(release)
						}
						select {
						case <-release:
						case <-ctx.Done():
						}
					},
				})
		}(item)
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("parallel bind: %v", err)
		}
	}
}

func TestLibraryBindFailedIntentDoesNotBlockUnrelatedBinding(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	otherLibraryID := "22222222-3333-4444-8555-666666666666"
	if _, _, err := environment.store.CreateLibrary(t.Context(), storage.Library{ID: otherLibraryID, OwnerUserID: testClientUserID, Name: "Other"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	clientDir := filepath.Join(t.TempDir(), "client")
	_, firstWorktree := newClientPaths(t)
	_, secondWorktree := newClientPaths(t)
	err := runLibraryWithConfig(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, firstWorktree, testClientDeviceID)[1:],
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard, libraryClientConfig{
			checkFilesystem: func(*os.File) error { return nil }, beforeHeadCAS: func() error { return errors.New("stop") },
		})
	if err == nil {
		t.Fatal("expected first bind to retain an intent")
	}
	if err := runTest(t.Context(), bindArgs(clientDir, environment.server.URL, otherLibraryID, secondWorktree, testOtherDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("unrelated bind blocked by failed intent: %v", err)
	}
	assertIntentCount(t, clientDir, 1)
}

func TestLibraryBindClientLockPreventsSecondRemotePublication(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	otherLibraryID := "22222222-3333-4444-8555-666666666666"
	if _, _, err := environment.store.CreateLibrary(t.Context(), storage.Library{ID: otherLibraryID, OwnerUserID: testClientUserID, Name: "Other"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	clientDir, worktree := newClientPaths(t)
	locked, release := make(chan struct{}), make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- runLibraryWithConfig(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID)[1:],
			strings.NewReader(environment.token+"\n"), io.Discard, io.Discard, libraryClientConfig{
				checkFilesystem: func(*os.File) error { return nil }, now: time.Now,
				afterLock: func() { close(locked); <-release },
			})
	}()
	<-locked
	secondDone, attempting := make(chan error, 1), make(chan struct{})
	go func() {
		secondDone <- runLibraryWithConfig(t.Context(), bindArgs(clientDir, environment.server.URL, otherLibraryID, worktree, testOtherDeviceID)[1:],
			strings.NewReader(environment.token+"\n"), io.Discard, io.Discard, libraryClientConfig{
				checkFilesystem: func(*os.File) error { return nil }, now: time.Now, beforeFlock: func() { close(attempting) },
			})
	}()
	<-attempting
	select {
	case err := <-secondDone:
		t.Fatalf("second bind passed flock before release: %v", err)
	default:
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first bind: %v", err)
	}
	if err := <-secondDone; err == nil || !strings.Contains(err.Error(), "already bound to another library") {
		t.Fatalf("second bind after first finalize = %v", err)
	}
	head, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), otherLibraryID, []byte(environment.token))
	if err != nil || head.CommitID != nil {
		t.Fatalf("second library was published: head=%+v err=%v", head, err)
	}
}

func TestLibraryBindRejectsNoncanonicalPendingWinner(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	clientDir, worktree := newClientPaths(t)
	args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID)
	base := mustServerURL(t, environment.server.URL)
	head, err := getRemoteHead(t.Context(), base, testClientLibraryID, []byte(environment.token))
	if err != nil {
		t.Fatal(err)
	}
	_, emptyRoot, err := object.Canonicalize("directories", []byte(`{"Entries":[],"Type":"Directory","Version":1}`))
	if err != nil {
		t.Fatal(err)
	}
	options := bindOptions{clientDir: clientDir, serverURL: environment.server.URL, libraryID: testClientLibraryID,
		worktree: worktree, deviceID: testClientDeviceID, base: base, token: []byte(environment.token)}
	intent, err := newBindIntent(options, testClientUserID, head, emptyRoot, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(clientDir, 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if err := saveBindIntent(t.Context(), db, *intent); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	binding := readTestIntent(t, clientDir)
	otherRootBytes, otherRoot, err := object.Canonicalize("directories", []byte(`{"Entries":[{"Id":"`+binding.CandidateRoot+`","ModifiedAt":"2026-01-01T00:00:00Z","Name":"x","Type":"Directory"}],"Type":"Directory","Version":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := environment.store.PutObject(t.Context(), testClientUserID, testClientLibraryID, "directories", otherRoot, bytes.NewReader(otherRootBytes)); err != nil {
		t.Fatal(err)
	}
	commitInput := strings.Replace(string(binding.CandidateData), binding.CandidateRoot, otherRoot, 1)
	commitBytes, commitID, err := object.Canonicalize("commits", []byte(commitInput))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := environment.store.PutObject(t.Context(), testClientUserID, testClientLibraryID, "commits", commitID, bytes.NewReader(commitBytes)); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.store.UpdateLibraryHead(t.Context(), testClientUserID, testClientLibraryID, nil, 0, commitID, nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	err = runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "conflicts with pending") {
		t.Fatalf("noncanonical winner error = %v", err)
	}
	assertIntentCount(t, clientDir, 1)
	config := libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}
	if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", clientDir, "--worktree", worktree},
		strings.NewReader(""), io.Discard, io.Discard, config); err != nil {
		t.Fatalf("cancel conflicting intent: %v", err)
	}
	assertNoIntent(t, clientDir)
	head, err = getRemoteHead(t.Context(), base, testClientLibraryID, []byte(environment.token))
	if err != nil || head.CommitID == nil || *head.CommitID != commitID {
		t.Fatalf("unbind changed conflicting remote Head: head=%+v err=%v", head, err)
	}
	entries, err := os.ReadDir(worktree)
	if err != nil || len(entries) != 0 {
		t.Fatalf("unbind changed worktree: entries=%v err=%v", entries, err)
	}
	if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", clientDir, "--worktree", worktree},
		strings.NewReader(""), io.Discard, io.Discard, config); err != nil {
		t.Fatalf("repeat unbind after cancel: %v", err)
	}
}

func TestLibraryUnbindWaitsForClientLockAndRemainsLocalOnly(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	clientDir, worktree := newClientPaths(t)
	if err := runTest(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	headBefore, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if err != nil {
		t.Fatal(err)
	}
	config := libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}
	lock, err := lockBinding(t.Context(), clientDir, worktree, environment.server.URL, testClientLibraryID, normalizeLibraryClientConfig(config))
	if err != nil {
		t.Fatal(err)
	}
	done, attempting := make(chan error, 1), make(chan struct{})
	go func() {
		done <- runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", clientDir, "--worktree", worktree},
			strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{
				checkFilesystem: func(*os.File) error { return nil }, beforeFlock: func() { close(attempting) },
			})
	}()
	<-attempting
	select {
	case err := <-done:
		t.Fatalf("unbind passed held client lock: %v", err)
	default:
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	headAfter, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if err != nil || headAfter.CommitID == nil || headBefore.CommitID == nil || *headAfter.CommitID != *headBefore.CommitID {
		t.Fatalf("unbind changed remote Head: before=%+v after=%+v err=%v", headBefore, headAfter, err)
	}
}

func TestLibraryBindRejectsWorktreeReplacementBeforeCAS(t *testing.T) {
	for _, replacement := range []string{"directory", "symlink"} {
		t.Run(replacement, func(t *testing.T) {
			environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
			clientDir, worktree := newClientPaths(t)
			original := worktree + ".original"
			err := runLibraryWithConfig(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID)[1:],
				strings.NewReader(environment.token+"\n"), io.Discard, io.Discard, libraryClientConfig{
					checkFilesystem: func(*os.File) error { return nil },
					beforeHeadCAS: func() error {
						if err := os.Rename(worktree, original); err != nil {
							return err
						}
						if err := os.WriteFile(filepath.Join(original, "late.txt"), []byte("late"), 0o600); err != nil {
							return err
						}
						if replacement == "symlink" {
							return os.Symlink(original, worktree)
						}
						return os.Mkdir(worktree, 0o700)
					},
				})
			if err == nil || !strings.Contains(err.Error(), "worktree changed during bind") {
				t.Fatalf("replacement error = %v", err)
			}
			head, headErr := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
			if headErr != nil || head.CommitID != nil {
				t.Fatalf("Head published after replacement: head=%+v err=%v", head, headErr)
			}
		})
	}
}

func TestLibraryBindRejectsFileAddedThroughOpenedWorktree(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	clientDir, worktree := newClientPaths(t)
	err := runLibraryWithConfig(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID)[1:],
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard, libraryClientConfig{
			checkFilesystem: func(*os.File) error { return nil },
			beforeHeadCAS: func() error {
				return os.WriteFile(filepath.Join(worktree, "late.txt"), []byte("late"), 0o600)
			},
		})
	if err == nil || !strings.Contains(err.Error(), "worktree changed during bind") {
		t.Fatalf("opened worktree mutation error = %v", err)
	}
	head, headErr := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if headErr != nil || head.CommitID != nil {
		t.Fatalf("Head published after mutation: head=%+v err=%v", head, headErr)
	}
}

func TestLockClientDirAlreadyCanceledHasNoSideEffects(t *testing.T) {
	clientDir := filepath.Join(t.TempDir(), "client")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := lockClientKeys(ctx, clientDir, []string{lockName("test-state")}, normalizeLibraryClientConfig(libraryClientConfig{})); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled lock error = %v", err)
	}
	if _, err := os.Stat(clientDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled lock created client directory: %v", err)
	}
}

func TestLockClientDirRejectsUnsupportedFilesystemBeforeCreatingLock(t *testing.T) {
	clientDir := filepath.Join(t.TempDir(), "client")
	checked := false
	_, err := lockClientKeys(t.Context(), clientDir, []string{lockName("test-state")}, libraryClientConfig{
		checkFilesystem: func(directory *os.File) error {
			checked = directory.Fd() != ^uintptr(0)
			return errors.New("unsupported client filesystem")
		},
		syncDirectory: syncDirectory,
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported client filesystem") || !checked {
		t.Fatalf("client filesystem check: checked=%t err=%v", checked, err)
	}
	matches, globErr := filepath.Glob(filepath.Join(clientDir, "lock-*"))
	if globErr != nil || len(matches) != 0 {
		t.Fatalf("rejected client filesystem locks=%v err=%v", matches, globErr)
	}
}

func TestLockClientDirCancellationClosesWaiter(t *testing.T) {
	clientDir := filepath.Join(t.TempDir(), "client")
	config := normalizeLibraryClientConfig(libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }})
	held, err := lockClientKeys(t.Context(), clientDir, []string{lockName("test-state")}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()

	ctx, cancel := context.WithCancel(t.Context())
	attempting := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := lockClientKeys(ctx, clientDir, []string{lockName("test-state")}, libraryClientConfig{
			syncDirectory: syncDirectory,
			beforeFlock:   func() { close(attempting) },
		})
		done <- err
	}()
	<-attempting
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled lock error = %v", err)
	}
}

func TestClientInitializationRetriesDirectorySync(t *testing.T) {
	clientDir := filepath.Join(t.TempDir(), "client")
	calls := 0
	config := normalizeLibraryClientConfig(libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, syncDirectory: func(path string) error {
		calls++
		if calls == 1 {
			return errors.New("injected sync failure")
		}
		return syncDirectory(path)
	}})
	if _, err := lockClientKeys(t.Context(), clientDir, []string{lockName("test-state")}, config); err == nil || !strings.Contains(err.Error(), "injected sync failure") {
		t.Fatalf("first sync error = %v", err)
	}
	lock, err := lockClientKeys(t.Context(), clientDir, []string{lockName("test-state")}, config)
	if err != nil {
		t.Fatalf("retry lock: %v", err)
	}
	defer lock.Close()
	db, err := initializeClientDB(t.Context(), clientDir, config.syncDirectory)
	if err != nil {
		t.Fatalf("retry initialize: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if calls < 4 {
		t.Fatalf("sync calls after retry = %d, want parent, lock, and database retries", calls)
	}
}

func TestLockClientDirRetriesAllDurabilitySteps(t *testing.T) {
	clientDir := filepath.Join(t.TempDir(), "client")
	fileSyncs, clientDirSyncs := 0, 0
	config := normalizeLibraryClientConfig(libraryClientConfig{
		checkFilesystem: func(*os.File) error { return nil },
		syncFile: func(file *os.File) error {
			fileSyncs++
			return file.Sync()
		},
		syncDirectory: func(path string) error {
			if path == clientDir {
				clientDirSyncs++
				if clientDirSyncs == 1 {
					return errors.New("injected client directory sync failure")
				}
			}
			return syncDirectory(path)
		},
	})
	if _, err := lockClientKeys(t.Context(), clientDir, []string{lockName("test-state")}, config); err == nil || !strings.Contains(err.Error(), "injected client directory sync failure") {
		t.Fatalf("first sync error = %v", err)
	}
	lock, err := lockClientKeys(t.Context(), clientDir, []string{lockName("test-state")}, config)
	if err != nil {
		t.Fatalf("retry lock: %v", err)
	}
	defer lock.Close()
	if fileSyncs != 2 || clientDirSyncs != 2 {
		t.Fatalf("retry durability calls: file=%d clientDir=%d, want 2 each", fileSyncs, clientDirSyncs)
	}
}

func TestMountedFilesystemIdentifiesExactHeldMount(t *testing.T) {
	mountinfo := strings.Join([]string{
		"24 1 8:1 / / rw,relatime - ext3 /dev/root rw",
		"25 24 8:1 /project /workspace rw,relatime - ext4 /dev/root rw",
		`26 25 8:2 / /workspace/nested\040mount rw,relatime - tmpfs tmpfs rw`,
	}, "\n")
	for _, test := range []struct {
		name       string
		path       string
		major      uint32
		minor      uint32
		filesystem string
		wantErr    bool
	}{
		{name: "exact ext4 bind mount", path: "/workspace/project", major: 8, minor: 1, filesystem: "ext4"},
		{name: "root ext3", path: "/other", major: 8, minor: 1, filesystem: "ext3"},
		{name: "escaped nested mount", path: "/workspace/nested mount/file", major: 8, minor: 2, filesystem: "tmpfs"},
		{name: "held device must match", path: "/workspace/project", major: 8, minor: 2, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			filesystem, err := mountedFilesystem(strings.NewReader(mountinfo), test.path, test.major, test.minor)
			if (err != nil) != test.wantErr || filesystem != test.filesystem {
				t.Fatalf("mountedFilesystem(%q, %d:%d) = %q, %v", test.path, test.major, test.minor, filesystem, err)
			}
		})
	}
}

func TestMountinfoPathRejectsInvalidEscapes(t *testing.T) {
	for _, value := range []string{`/truncated\04`, `/invalid\041escape`, `/non-octal\xyz`} {
		if result, err := unescapeMountinfoPath(value); err == nil {
			t.Fatalf("unescapeMountinfoPath(%q)=%q, want error", value, result)
		}
	}
}

func TestBindClientFilesystemCheckPrecedesRemoteAndStateCreation(t *testing.T) {
	worktree := filepath.Join(t.TempDir(), "worktree")
	if err := os.Mkdir(worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	stateParent := t.TempDir()
	clientDir := filepath.Join(stateParent, "client")
	requests := atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	checkedState := false
	config := libraryClientConfig{checkFilesystem: func(file *os.File) error {
		if file.Name() == worktree {
			return nil
		}
		checkedState = true
		return errors.New("unsupported client filesystem")
	}}
	err := runLibraryWithConfig(t.Context(), bindArgs(clientDir, server.URL, testClientLibraryID, worktree, testClientDeviceID)[1:],
		strings.NewReader("token\n"), io.Discard, io.Discard, config)
	if err == nil || !strings.Contains(err.Error(), "unsupported client filesystem") || !checkedState {
		t.Fatalf("client filesystem error: checked=%t err=%v", checkedState, err)
	}
	if requests.Load() != 0 {
		t.Fatalf("remote requests before client filesystem rejection = %d", requests.Load())
	}
	if _, err := os.Stat(clientDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected client filesystem created state directory: %v", err)
	}
}

func TestOpenEmptyWorktreeFilesystemCheckUsesHeldFDAndPrecedesRemote(t *testing.T) {
	worktree := filepath.Join(t.TempDir(), "worktree")
	if err := os.Mkdir(worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	requests := atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	checked := false
	config := libraryClientConfig{checkFilesystem: func(file *os.File) error {
		var stat fscompat.Stat_t
		if err := fscompat.Fstat(int(file.Fd()), &stat); err != nil {
			t.Fatalf("filesystem seam did not receive a held fd: %v", err)
		}
		checked = true
		return errors.New("unsupported test filesystem")
	}}
	err := runLibraryWithConfig(t.Context(), bindArgs(filepath.Join(t.TempDir(), "client"), server.URL, testClientLibraryID, worktree, testClientDeviceID)[1:],
		strings.NewReader("token\n"), io.Discard, io.Discard, config)
	if err == nil || !strings.Contains(err.Error(), "unsupported test filesystem") {
		t.Fatalf("filesystem error = %v", err)
	}
	if !checked || requests.Load() != 0 {
		t.Fatalf("filesystem checked=%v, remote requests=%d", checked, requests.Load())
	}
}

func TestLibraryCommandUsageLocksExplicitFlags(t *testing.T) {
	var help bytes.Buffer
	err := runTest(t.Context(), []string{"library", "bind", "--help"}, strings.NewReader(""), io.Discard, &help)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("bind --help error = %v", err)
	}
	for _, required := range []string{"client-dir", "server", "library-id", "worktree", "device-id", "token-stdin"} {
		if !strings.Contains(help.String(), required) {
			t.Errorf("bind help missing %q: %s", required, help.String())
		}
	}
	if err := runTest(t.Context(), []string{"library", "unbind"}, strings.NewReader(""), io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "--client-dir") {
		t.Fatalf("unbind usage error = %v", err)
	}
}

type libraryCLIEnvironment struct {
	store   *storage.Store
	handler http.Handler
	server  *httptest.Server
	token   string
}

func newLibraryCLIEnvironment(t *testing.T, config libraryapi.Config) libraryCLIEnvironment {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "server")
	if err := storage.Init(t.Context(), dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	store, err := storage.OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenForServe: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	now := time.Now().UTC()
	if err := store.CreateUser(t.Context(), storage.User{ID: testClientUserID, Username: "alice", PasswordHash: "unused"}, now); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	token := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	if err := store.CreateSession(t.Context(), testClientUserID, sha256.Sum256([]byte(token)), "test", now, now.Add(time.Hour)); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, _, err := store.CreateLibrary(t.Context(), storage.Library{ID: testClientLibraryID, OwnerUserID: testClientUserID, Name: "Test"}, now); err != nil {
		t.Fatalf("CreateLibrary: %v", err)
	}
	handler, err := libraryapi.NewHandler(store, log.New(io.Discard, "", 0), config)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return libraryCLIEnvironment{store: store, handler: handler, server: server, token: token}
}

func bindArgs(clientDir, server, libraryID, worktree, deviceID string) []string {
	return []string{"library", "bind", "--client-dir", clientDir, "--server", server, "--library-id", libraryID,
		"--worktree", worktree, "--device-id", deviceID, "--token-stdin"}
}

func runTest(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	return runLibraryWithConfig(ctx, args[1:], stdin, stdout, stderr, libraryClientConfig{
		checkFilesystem: func(*os.File) error { return nil }, now: time.Now,
	})
}

func allowTestFilesystem(*testing.T) {}

func mustServerURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := validateServerURL(raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func newClientPaths(t *testing.T) (string, string) {
	t.Helper()
	clientDir := filepath.Join(t.TempDir(), "client")
	worktree := filepath.Join(t.TempDir(), "worktree")
	if err := os.Mkdir(worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	return clientDir, worktree
}

func assertNoIntent(t *testing.T, clientDir string) { assertIntentCount(t, clientDir, 0) }

func assertIntentCount(t *testing.T, clientDir string, want int) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(clientDir, _clientDatabaseName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM bind_intents").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("pending intent count = %d, want %d", count, want)
	}
}

func readTestIntent(t *testing.T, clientDir string) bindIntent {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(clientDir, _clientDatabaseName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var intent bindIntent
	err = db.QueryRow(`SELECT server_url, library_id, worktree, user_id, device_id, expected_etag,
		candidate_commit, candidate_root, candidate_data FROM bind_intents LIMIT 1`).Scan(
		&intent.ServerURL, &intent.LibraryID, &intent.Worktree, &intent.UserID, &intent.DeviceID, &intent.ExpectedETag,
		&intent.CandidateCommit, &intent.CandidateRoot, &intent.CandidateData)
	if err != nil {
		t.Fatalf("read intent: intent=%+v err=%v", intent, err)
	}
	return intent
}

func readTestBinding(t *testing.T, clientDir, worktree string) clientBinding {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(clientDir, _clientDatabaseName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	canonical, err := canonicalExistingPath(worktree)
	if err != nil {
		t.Fatal(err)
	}
	var binding clientBinding
	var accessToken []byte
	err = db.QueryRow(`SELECT server_url, library_id, worktree, user_id, device_id, sync_base_commit, sync_base_root, head_etag, access_token FROM bindings WHERE worktree = ?`, canonical).
		Scan(&binding.ServerURL, &binding.LibraryID, &binding.Worktree, &binding.UserID, &binding.DeviceID, &binding.SyncBase, &binding.SyncBaseRoot, &binding.HeadETag, &accessToken)
	if err != nil {
		t.Fatalf("read binding: %v", err)
	}
	return binding
}

func readTestAccessToken(t *testing.T, clientDir, worktree string) []byte {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(clientDir, _clientDatabaseName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var token []byte
	if err := db.QueryRow("SELECT access_token FROM bindings WHERE worktree = ?", worktree).Scan(&token); err != nil {
		t.Fatalf("read binding token: %v", err)
	}
	return token
}

func getTestObject(t *testing.T, serverURL, token, kind, id string) []byte {
	t.Helper()
	base := mustServerURL(t, serverURL)
	request, err := authenticatedRequest(t.Context(), "GET", base.JoinPath("v1/libraries", testClientLibraryID, "objects", kind, id).String(), []byte(token), nil)
	if err != nil {
		t.Fatal(err)
	}
	status, data, _, err := doClientRequest(request)
	if err != nil || status != 200 {
		t.Fatalf("GET %s/%s: status=%d err=%v", kind, id, status, err)
	}
	return data
}
