package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mingming-cn/filecloud/internal/library"
	"github.com/mingming-cn/filecloud/internal/object"
	"github.com/mingming-cn/filecloud/internal/storage"
)

func TestLibraryHistoryListUsesBindingReadOnlyAndPrintsCompleteCommitIDs(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, library.Config{})
	clientDir, worktree := newClientPaths(t)
	if err := os.MkdirAll(worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	bind := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID)
	if err := runTest(t.Context(), bind, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("bind: %v", err)
	}
	binding := readTestBinding(t, clientDir, worktree)
	current, err := environment.store.GetLibrary(t.Context(), testClientUserID, testClientLibraryID)
	if err != nil || current.HeadCommitID == nil {
		t.Fatalf("current library = %+v/%v", current, err)
	}
	root := binding.SyncBaseRoot
	parent := *current.HeadCommitID
	for range 2 {
		data, id, err := canonicalCommit(testClientUserID, testClientDeviceID, root, []string{parent}, func() time.Time {
			return time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := environment.store.PutObject(t.Context(), testClientUserID, testClientLibraryID, "commits", id, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		if _, err := environment.store.UpdateLibraryHead(t.Context(), testClientUserID, testClientLibraryID, current.HeadCommitID,
			current.HeadVersion, id, nil, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		current, err = environment.store.GetLibrary(t.Context(), testClientUserID, testClientLibraryID)
		if err != nil || current.HeadCommitID == nil {
			t.Fatalf("updated library = %+v/%v", current, err)
		}
		parent = id
	}
	beforeDB, err := os.ReadFile(filepath.Join(clientDir, _clientDatabaseName))
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	args := []string{"library", "history", "list", "--client-dir", clientDir, "--worktree", worktree, "--page-size", "2"}
	if err := runTest(t.Context(), args, strings.NewReader(""), &output, io.Discard); err != nil {
		t.Fatalf("history list: %v", err)
	}
	afterDB, err := os.ReadFile(filepath.Join(clientDir, _clientDatabaseName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(beforeDB, afterDB) {
		t.Fatal("history list modified client database")
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[0], parent+" ") || len(lines[0]) < 64 || !strings.Contains(lines[0], "parents=1") || !strings.Contains(lines[0], "message=\"sync\"") {
		t.Fatalf("history output = %q", output.String())
	}
	if strings.Contains(output.String(), parent[:12]) && !strings.Contains(output.String(), parent+" ") {
		t.Fatal("history output used a short commit id")
	}
}

func TestLibraryHistoryInspectCommitIsReadOnlyAndRejectsInvalidIDsLocally(t *testing.T) {
	fixture := newHistoryInspectFixture(t)
	beforeClient := captureHistoryInspectClientState(t, fixture.clientDir)
	beforeWorktree := captureHistoryInspectWorktree(t, fixture.worktree)

	var output bytes.Buffer
	args := []string{"library", "history", "inspect", "--client-dir", fixture.clientDir, "--worktree", fixture.worktree, "--commit", fixture.commitID}
	if err := runTest(t.Context(), args, strings.NewReader(""), &output, io.Discard); err != nil {
		t.Fatalf("history inspect Commit: %v", err)
	}
	for _, field := range []string{
		"CommitId=" + fixture.commitID,
		"Role=mainline",
		"MainlineCommitId=" + fixture.commitID,
		"AuthorUserId=" + testClientUserID,
		"CreatedAt=",
		"DeviceId=" + testClientDeviceID,
		"Message=\"sync\"",
		"Parents=",
		"Root=" + fixture.rootID,
	} {
		if !strings.Contains(output.String(), field) {
			t.Fatalf("history inspect output missing %q: %q", field, output.String())
		}
	}
	requests := fixture.requests()
	if len(requests) != 1 || requests[0] != http.MethodGet+" /v1/libraries/"+testClientLibraryID+"/history/"+fixture.commitID {
		t.Fatalf("Commit-only inspect requests = %v", requests)
	}

	for _, id := range []string{fixture.commitID[:12], fixture.commitID[:63], fixture.commitID + "0", strings.ToUpper(fixture.commitID), strings.Repeat("g", 64)} {
		err := runTest(t.Context(), []string{"library", "history", "inspect", "--client-dir", fixture.clientDir,
			"--worktree", fixture.worktree, "--commit", id}, strings.NewReader(""), io.Discard, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "commit must be a complete 64-character lowercase object ID") {
			t.Fatalf("invalid CommitId %q error = %v", id, err)
		}
	}
	if got := fixture.requests(); !slices.Equal(got, requests) {
		t.Fatalf("locally rejected CommitIds made requests: before=%v after=%v", requests, got)
	}
	fixture.assertUnchanged(t, beforeClient, beforeWorktree)
}

func TestLibraryHistoryInspectFileAndDirectoryPagesWithoutBlocks(t *testing.T) {
	fixture := newHistoryInspectFixture(t)
	beforeClient := captureHistoryInspectClientState(t, fixture.clientDir)
	beforeWorktree := captureHistoryInspectWorktree(t, fixture.worktree)

	var fileOutput bytes.Buffer
	fileArgs := []string{"library", "history", "inspect", "--client-dir", fixture.clientDir, "--worktree", fixture.worktree,
		"--commit", fixture.commitID, "--path", "docs/readme.txt"}
	if err := runTest(t.Context(), fileArgs, strings.NewReader(""), &fileOutput, io.Discard); err != nil {
		t.Fatalf("inspect historical file: %v", err)
	}
	for _, field := range []string{"Path=docs/readme.txt", "Type=File", "FileId=", "Size=7", "ModifiedAt=", "Blocks=1"} {
		if !strings.Contains(fileOutput.String(), field) {
			t.Fatalf("file inspect output missing %q: %q", field, fileOutput.String())
		}
	}

	var firstPage bytes.Buffer
	rootArgs := []string{"library", "history", "inspect", "--client-dir", fixture.clientDir, "--worktree", fixture.worktree,
		"--commit", fixture.commitID, "--path", ".", "--page-size", "1"}
	if err := runTest(t.Context(), rootArgs, strings.NewReader(""), &firstPage, io.Discard); err != nil {
		t.Fatalf("inspect historical root page 1: %v", err)
	}
	if !strings.Contains(firstPage.String(), "Type=Directory") || !strings.Contains(firstPage.String(), "DirectoryId="+fixture.rootID) ||
		!strings.Contains(firstPage.String(), "Entry name=a.txt type=File") || strings.Contains(firstPage.String(), "readme.txt") {
		t.Fatalf("root page 1 output = %q", firstPage.String())
	}
	token := outputValue(firstPage.String(), "next_page_token=")
	if token == "" {
		t.Fatalf("root page 1 omitted token: %q", firstPage.String())
	}

	var secondPage bytes.Buffer
	if err := runTest(t.Context(), append(rootArgs, "--page-token", token), strings.NewReader(""), &secondPage, io.Discard); err != nil {
		t.Fatalf("inspect historical root page 2: %v", err)
	}
	if !strings.Contains(secondPage.String(), "Entry name=docs type=Directory") || strings.Contains(secondPage.String(), "readme.txt") {
		t.Fatalf("root page 2 output = %q", secondPage.String())
	}

	const pathCanary = "docs/path-canary"
	var diagnostics bytes.Buffer
	err := runTest(t.Context(), []string{"library", "history", "inspect", "--client-dir", fixture.clientDir, "--worktree", fixture.worktree,
		"--commit", fixture.commitID, "--path", pathCanary, "--page-token", token}, strings.NewReader(""), io.Discard, &diagnostics)
	if err == nil {
		t.Fatal("cross-path page token unexpectedly succeeded")
	}
	combinedDiagnostics := err.Error() + diagnostics.String() + fixture.serverLogs.String()
	for label, canary := range map[string]string{
		"path": pathCanary, "page token": token, "access token": fixture.environment.token, "authorization header": "Authorization",
	} {
		if strings.Contains(combinedDiagnostics, canary) {
			t.Fatalf("history inspect diagnostics exposed the %s canary", label)
		}
	}
	tamperedBytes, decodeErr := base64.RawURLEncoding.Strict().DecodeString(token)
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	tamperedBytes[len(tamperedBytes)-1] ^= 1
	tampered := base64.RawURLEncoding.EncodeToString(tamperedBytes)
	beforeTampered := fixture.requests()
	err = runTest(t.Context(), append(rootArgs, "--page-token", tampered), strings.NewReader(""), io.Discard, io.Discard)
	if err == nil || !slices.Equal(beforeTampered, fixture.requests()) || strings.Contains(err.Error(), tampered) {
		t.Fatal("tampered page token was accepted, sent a request, or appeared in diagnostics")
	}
	validBoundaryPaths := []string{
		strings.TrimSuffix(strings.Repeat("a/", 256), "/"),
		strings.Join([]string{strings.Repeat("a", 204), strings.Repeat("b", 204), strings.Repeat("c", 204), strings.Repeat("d", 204), strings.Repeat("e", 204)}, "/"),
	}
	for _, path := range validBoundaryPaths {
		before := fixture.requests()
		err := runTest(t.Context(), []string{"library", "history", "inspect", "--client-dir", fixture.clientDir, "--worktree", fixture.worktree,
			"--commit", fixture.commitID, "--path", path}, strings.NewReader(""), io.Discard, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "history path not found") || slices.Equal(before, fixture.requests()) {
			t.Fatalf("valid boundary path was not resolved: path length=%d err=%v", len(path), err)
		}
	}
	for _, path := range []string{strings.Repeat("a", 1025), strings.Repeat("a/", 256) + "a", "docs/../readme.txt", "docs\\readme.txt"} {
		before := fixture.requests()
		err := runTest(t.Context(), []string{"library", "history", "inspect", "--client-dir", fixture.clientDir, "--worktree", fixture.worktree,
			"--commit", fixture.commitID, "--path", path}, strings.NewReader(""), io.Discard, io.Discard)
		if err == nil || !slices.Equal(before, fixture.requests()) {
			t.Fatalf("invalid path made a request: path length=%d err=%v", len(path), err)
		}
	}
	for _, extra := range [][]string{{"--page-size", "501"}, {"--page-token", strings.Repeat("x", 4097)}} {
		before := fixture.requests()
		err := runTest(t.Context(), append([]string{"library", "history", "inspect", "--client-dir", fixture.clientDir, "--worktree", fixture.worktree,
			"--commit", fixture.commitID, "--path", "."}, extra...), strings.NewReader(""), io.Discard, io.Discard)
		if err == nil || !slices.Equal(before, fixture.requests()) {
			t.Fatalf("invalid pagination arguments made a request: extra=%v err=%v", extra, err)
		}
	}
	if fixture.forbidden.Load() != 0 {
		t.Fatalf("history inspect attempted %d GetBlock or write requests", fixture.forbidden.Load())
	}
	fixture.assertUnchanged(t, beforeClient, beforeWorktree)
}

func TestLibraryHistoryInspectRejectsInvalidDetailResponsesReadOnly(t *testing.T) {
	fixture := newHistoryInspectFixture(t)
	beforeClient := captureHistoryInspectClientState(t, fixture.clientDir)
	beforeWorktree := captureHistoryInspectWorktree(t, fixture.worktree)
	body, headers := historyInspectDetailResponse(t, fixture)
	invalidID := strings.Repeat("f", 64)
	invalidBody := bytes.Replace(body, []byte(fixture.commitID), []byte(invalidID), 1)
	wrongETag := headers.Clone()
	wrongETag.Set("ETag", `"wrong"`)
	wrongCache := headers.Clone()
	wrongCache.Set("Cache-Control", "private, no-store")

	for _, test := range []struct {
		name    string
		body    []byte
		headers http.Header
	}{
		{name: "malformed fields", body: invalidBody, headers: headers},
		{name: "wrong ETag", body: body, headers: wrongETag},
		{name: "wrong Cache-Control", body: body, headers: wrongCache},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture.setHistoryOverride(&historyInspectResponseOverride{body: test.body, headers: test.headers})
			beforeRequests := fixture.requests()
			err := runTest(t.Context(), []string{"library", "history", "inspect", "--client-dir", fixture.clientDir,
				"--worktree", fixture.worktree, "--commit", fixture.commitID}, strings.NewReader(""), io.Discard, io.Discard)
			if err == nil {
				t.Fatal("history inspect accepted an invalid detail response")
			}
			afterRequests := fixture.requests()
			newRequests := afterRequests[len(beforeRequests):]
			if len(newRequests) != 1 || !strings.Contains(newRequests[0], "/history/") {
				t.Fatalf("invalid detail response triggered metadata requests: %v", newRequests)
			}
		})
	}
	fixture.setHistoryOverride(nil)
	fixture.assertUnchanged(t, beforeClient, beforeWorktree)
}

func TestLibraryHistoryInspectPageTokenIsBoundToLibrary(t *testing.T) {
	fixture := newHistoryInspectFixtureWithFiles(t, map[string]string{"a.txt": "", "b.txt": ""})
	args := []string{"library", "history", "inspect", "--client-dir", fixture.clientDir, "--worktree", fixture.worktree,
		"--commit", fixture.commitID, "--path", ".", "--page-size", "1"}
	var first bytes.Buffer
	if err := runTest(t.Context(), args, strings.NewReader(""), &first, io.Discard); err != nil {
		t.Fatal(err)
	}
	token := outputValue(first.String(), "next_page_token=")
	if token == "" {
		t.Fatal("directory page omitted continuation token")
	}

	const otherLibraryID = "32345678-9abc-4def-8123-456789abcdef"
	cloneHistoryInspectLibrary(t, fixture, otherLibraryID)
	otherClientDir, otherWorktree := newClientPaths(t)
	if err := runTest(t.Context(), bindArgs(otherClientDir, fixture.serverURL, otherLibraryID, otherWorktree, testOtherDeviceID),
		strings.NewReader(fixture.environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("bind cloned library: %v", err)
	}
	beforeRequests := fixture.requests()
	err := runTest(t.Context(), []string{"library", "history", "inspect", "--client-dir", otherClientDir, "--worktree", otherWorktree,
		"--commit", fixture.commitID, "--path", ".", "--page-token", token}, strings.NewReader(""), io.Discard, io.Discard)
	if err == nil || !slices.Equal(beforeRequests, fixture.requests()) || strings.Contains(err.Error(), token) {
		t.Fatal("cross-library page token was accepted, sent a request, or appeared in diagnostics")
	}
}

func TestLibraryHistoryInspectDirectoryPaginates500Of501Entries(t *testing.T) {
	files := make(map[string]string, 500)
	for index := range 500 {
		files[fmt.Sprintf("entry-%03d.txt", index)] = ""
	}
	fixture := newHistoryInspectFixtureWithFiles(t, files)
	beforeClient := captureHistoryInspectClientState(t, fixture.clientDir)
	beforeWorktree := captureHistoryInspectWorktree(t, fixture.worktree)
	args := []string{"library", "history", "inspect", "--client-dir", fixture.clientDir, "--worktree", fixture.worktree,
		"--commit", fixture.commitID, "--path", ".", "--page-size", "500"}
	var first bytes.Buffer
	if err := runTest(t.Context(), args, strings.NewReader(""), &first, io.Discard); err != nil {
		t.Fatalf("inspect 500-entry page: %v", err)
	}
	if got := strings.Count(first.String(), "\nEntry name="); got != 500 {
		t.Fatalf("first directory page entries = %d, want 500", got)
	}
	token := outputValue(first.String(), "next_page_token=")
	if token == "" {
		t.Fatal("500-of-501 directory page omitted continuation token")
	}
	var second bytes.Buffer
	if err := runTest(t.Context(), append(args, "--page-token", token), strings.NewReader(""), &second, io.Discard); err != nil {
		t.Fatalf("inspect final directory page: %v", err)
	}
	if got := strings.Count(second.String(), "\nEntry name="); got != 1 || outputValue(second.String(), "next_page_token=") != "" {
		t.Fatalf("final directory page entries = %d, output=%q", got, second.String())
	}
	if fixture.forbidden.Load() != 0 {
		t.Fatalf("directory pagination attempted %d GetBlock or write requests", fixture.forbidden.Load())
	}
	fixture.assertUnchanged(t, beforeClient, beforeWorktree)
}

func TestLibraryHistoryInspectRejectsMissingAndCorruptMetadataReadOnly(t *testing.T) {
	for _, kind := range []string{"Commit", "Directory", "File"} {
		for _, damage := range []string{"missing", "corrupt"} {
			t.Run(kind+"/"+damage, func(t *testing.T) {
				fixture := newHistoryInspectFixture(t)
				beforeClient := captureHistoryInspectClientState(t, fixture.clientDir)
				beforeWorktree := captureHistoryInspectWorktree(t, fixture.worktree)
				objectID := fixture.commitID
				objectKind := "commits"
				args := []string{"library", "history", "inspect", "--client-dir", fixture.clientDir,
					"--worktree", fixture.worktree, "--commit", fixture.commitID}
				if kind != "Commit" {
					objectID = fixture.rootID
					objectKind = "directories"
					args = append(args, "--path", ".")
				}
				if kind == "File" {
					root, err := object.VerifyDirectory(readHistoryInspectObjectFile(t, fixture, "directories", fixture.rootID), fixture.rootID)
					if err != nil {
						t.Fatal(err)
					}
					docs := historyInspectTestEntry(t, root, "docs")
					directory, err := object.VerifyDirectory(readHistoryInspectObjectFile(t, fixture, "directories", docs.ID), docs.ID)
					if err != nil {
						t.Fatal(err)
					}
					objectID = historyInspectTestEntry(t, directory, "readme.txt").ID
					objectKind = "files"
					args[len(args)-1] = "docs/readme.txt"
				}
				objectPath := filepath.Join(fixture.objectsDir, testClientUserID, testClientLibraryID, objectKind, objectID[:2], objectID[2:])
				var err error
				if damage == "missing" {
					err = os.Remove(objectPath)
				} else {
					err = os.WriteFile(objectPath, []byte(`{}`), 0o600)
				}
				if err != nil {
					t.Fatal(err)
				}
				err = runTest(t.Context(), args, strings.NewReader(""), io.Discard, io.Discard)
				if err == nil {
					t.Fatal("history inspect accepted unavailable metadata")
				}
				fixture.assertUnchanged(t, beforeClient, beforeWorktree)
			})
		}
	}
}

func TestLibraryHistoryInspectDiagnosticsDoNotExposeCanaries(t *testing.T) {
	const (
		pathCanary    = "history-log-path-canary.txt"
		contentCanary = "history-log-content-canary"
	)
	fixture := newHistoryInspectFixtureWithFiles(t, map[string]string{pathCanary: contentCanary, "z.txt": ""})
	beforeClient := captureHistoryInspectClientState(t, fixture.clientDir)
	beforeWorktree := captureHistoryInspectWorktree(t, fixture.worktree)
	args := []string{"library", "history", "inspect", "--client-dir", fixture.clientDir, "--worktree", fixture.worktree,
		"--commit", fixture.commitID, "--path", ".", "--page-size", "1"}
	var output bytes.Buffer
	if err := runTest(t.Context(), args, strings.NewReader(""), &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	pageToken := outputValue(output.String(), "next_page_token=")
	if pageToken == "" {
		t.Fatal("history inspect did not produce a page token canary")
	}
	fixture.serverLogs.Reset()
	commitPath := filepath.Join(fixture.objectsDir, testClientUserID, testClientLibraryID, "commits", fixture.commitID[:2], fixture.commitID[2:])
	corruptData := []byte(pathCanary + "\n" + pageToken + "\n" + contentCanary + "\nAuthorization: Bearer " + fixture.environment.token)
	if err := os.WriteFile(commitPath, corruptData, 0o600); err != nil {
		t.Fatal(err)
	}
	var diagnostics bytes.Buffer
	err := runTest(t.Context(), []string{"library", "history", "inspect", "--client-dir", fixture.clientDir,
		"--worktree", fixture.worktree, "--commit", fixture.commitID}, strings.NewReader(""), io.Discard, &diagnostics)
	if err == nil {
		t.Fatal("history inspect accepted a corrupt commit canary")
	}
	serverDiagnostics := fixture.serverLogs.String()
	if !strings.Contains(serverDiagnostics, "history_commit_"+fixture.commitID[:8]+"_run_integrity_check") {
		t.Fatal("corrupt history commit did not exercise the server integrity log path")
	}
	combinedDiagnostics := err.Error() + diagnostics.String() + serverDiagnostics
	for label, canary := range map[string]string{
		"path": pathCanary, "page token": pageToken, "file content": contentCanary,
		"access token": fixture.environment.token, "authorization header": "Authorization",
	} {
		if strings.Contains(combinedDiagnostics, canary) {
			t.Fatalf("history inspect diagnostics exposed the %s canary", label)
		}
	}
	fixture.assertUnchanged(t, beforeClient, beforeWorktree)
}

func TestLibraryHistoryListTransportErrorDoesNotExposePageToken(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, library.Config{})
	clientDir, worktree := newClientPaths(t)
	if err := os.MkdirAll(worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := runTest(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("bind: %v", err)
	}
	environment.server.Close()
	const pageToken = "page-token-privacy-canary"
	err := runTest(t.Context(), []string{"library", "history", "list", "--client-dir", clientDir, "--worktree", worktree,
		"--page-token", pageToken}, strings.NewReader(""), io.Discard, io.Discard)
	if err == nil {
		t.Fatal("history list unexpectedly succeeded after server shutdown")
	}
	if strings.Contains(err.Error(), pageToken) {
		t.Fatalf("history transport error exposed page token: %v", err)
	}
}

type historyInspectResponseOverride struct {
	body    []byte
	headers http.Header
}

type historyInspectFixture struct {
	clientDir          string
	worktree           string
	commitID           string
	rootID             string
	objectsDir         string
	serverURL          string
	environment        libraryCLIEnvironment
	serverLogs         *bytes.Buffer
	forbidden          *atomic.Int32
	requests           func() []string
	setHistoryOverride func(*historyInspectResponseOverride)
}

func newHistoryInspectFixture(t *testing.T) historyInspectFixture {
	t.Helper()
	return newHistoryInspectFixtureWithFiles(t, map[string]string{"a.txt": "a", "docs/readme.txt": "history", "z.txt": "z"})
}

func newHistoryInspectFixtureWithFiles(t *testing.T, files map[string]string) historyInspectFixture {
	t.Helper()
	environment := newLibraryCLIEnvironment(t, library.Config{})
	clientDir, worktree := newClientPaths(t)
	for path, content := range files {
		fullPath := filepath.Join(worktree, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fullPath, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(worktree, "empty"), 0o700); err != nil {
		t.Fatal(err)
	}
	var serverLogs bytes.Buffer
	handler, err := library.NewHandler(environment.store, log.New(&serverLogs, "", 0), library.Config{})
	if err != nil {
		t.Fatal(err)
	}
	var armed atomic.Bool
	var forbidden atomic.Int32
	var requestsMu sync.Mutex
	var requests []string
	var overrideMu sync.Mutex
	var override *historyInspectResponseOverride
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if armed.Load() {
			requestsMu.Lock()
			requests = append(requests, r.Method+" "+r.URL.Path)
			requestsMu.Unlock()
			if r.Method != http.MethodGet || strings.Contains(r.URL.Path, "/blocks/") {
				forbidden.Add(1)
				w.WriteHeader(http.StatusTeapot)
				return
			}
			overrideMu.Lock()
			currentOverride := override
			overrideMu.Unlock()
			if currentOverride != nil && strings.Contains(r.URL.Path, "/history/") {
				for key, values := range currentOverride.headers {
					for _, value := range values {
						w.Header().Add(key, value)
					}
				}
				w.WriteHeader(http.StatusOK)
				if _, err := w.Write(currentOverride.body); err != nil {
					t.Errorf("write overridden history response: %v", err)
				}
				return
			}
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	bind := append(bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testClientDeviceID), "--import-local")
	if err := runTest(t.Context(), bind, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("bind history inspect fixture: %v", err)
	}
	binding := readTestBinding(t, clientDir, worktree)
	stateDB, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stateDB.Close(); err != nil {
			t.Errorf("close history inspect state observer: %v", err)
		}
	})
	armed.Store(true)
	return historyInspectFixture{
		clientDir:   clientDir,
		worktree:    worktree,
		commitID:    binding.SyncBase,
		rootID:      binding.SyncBaseRoot,
		objectsDir:  environment.store.ObjectsDir(),
		serverURL:   proxy.URL,
		environment: environment,
		serverLogs:  &serverLogs,
		forbidden:   &forbidden,
		requests: func() []string {
			requestsMu.Lock()
			defer requestsMu.Unlock()
			return append([]string(nil), requests...)
		},
		setHistoryOverride: func(value *historyInspectResponseOverride) {
			overrideMu.Lock()
			defer overrideMu.Unlock()
			override = value
		},
	}
}

func readHistoryInspectObjectFile(t *testing.T, fixture historyInspectFixture, kind, id string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(fixture.objectsDir, testClientUserID, testClientLibraryID, kind, id[:2], id[2:]))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func historyInspectTestEntry(t *testing.T, directory object.Directory, name string) object.DirectoryEntry {
	t.Helper()
	for _, entry := range directory.Entries {
		if entry.Name == name {
			return entry
		}
	}
	t.Fatalf("Directory entry %q not found", name)
	return object.DirectoryEntry{}
}

func historyInspectDetailResponse(t *testing.T, fixture historyInspectFixture) ([]byte, http.Header) {
	t.Helper()
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet,
		"/v1/libraries/"+testClientLibraryID+"/history/"+fixture.commitID, nil)
	request.Header.Set("Authorization", "Bearer "+fixture.environment.token)
	fixture.environment.handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("read valid history detail: status=%d body=%q", response.Code, response.Body.String())
	}
	return bytes.Clone(response.Body.Bytes()), response.Header().Clone()
}

func cloneHistoryInspectLibrary(t *testing.T, fixture historyInspectFixture, libraryID string) {
	t.Helper()
	if _, _, err := fixture.environment.store.CreateLibrary(t.Context(), storage.Library{
		ID: libraryID, OwnerUserID: testClientUserID, Name: "History Clone",
	}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool)
	var copyObject func(string, string)
	copyObject = func(kind, id string) {
		key := kind + "\x00" + id
		if seen[key] {
			return
		}
		seen[key] = true
		data := readHistoryInspectObjectFile(t, fixture, kind, id)
		if _, err := fixture.environment.store.PutObject(t.Context(), testClientUserID, libraryID, kind, id, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		switch kind {
		case "commits":
			commit, err := object.VerifyCommit(data, id)
			if err != nil {
				t.Fatal(err)
			}
			copyObject("directories", commit.Root)
			for _, parent := range commit.Parents {
				copyObject("commits", parent)
			}
		case "directories":
			directory, err := object.VerifyDirectory(data, id)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range directory.Entries {
				if entry.Type == "Directory" {
					copyObject("directories", entry.ID)
				} else {
					copyObject("files", entry.ID)
				}
			}
		case "files":
			file, err := object.VerifyFile(data, id)
			if err != nil {
				t.Fatal(err)
			}
			for _, block := range file.Blocks {
				copyObject("blocks", block)
			}
		}
	}
	copyObject("commits", fixture.commitID)
	if _, err := fixture.environment.store.UpdateLibraryHead(t.Context(), testClientUserID, libraryID, nil, 0,
		fixture.commitID, nil, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
}

func captureHistoryInspectClientState(t *testing.T, root string) []string {
	t.Helper()
	return captureHistoryInspectTree(t, root, false)
}

func captureHistoryInspectWorktree(t *testing.T, root string) []string {
	t.Helper()
	return captureHistoryInspectTree(t, root, true)
}

func captureHistoryInspectTree(t *testing.T, root string, includeMtime bool) []string {
	t.Helper()
	var snapshot []string
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !includeMtime && (entry.Name() == _clientDatabaseName+"-wal" || entry.Name() == _clientDatabaseName+"-shm") && info.Size() == 0 {
			return nil
		}
		kind := "other"
		content := ""
		switch {
		case info.IsDir():
			kind = "directory"
		case info.Mode().IsRegular():
			kind = "file"
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			content = base64.RawStdEncoding.EncodeToString(data)
		case info.Mode()&os.ModeSymlink != 0:
			kind = "symlink"
			content, err = os.Readlink(path)
			if err != nil {
				return err
			}
		}
		mtime := int64(0)
		if includeMtime {
			mtime = info.ModTime().UnixNano()
		}
		snapshot = append(snapshot, fmt.Sprintf("%s %s mode=%s mtime=%d content=%s", kind,
			filepath.ToSlash(relative), info.Mode(), mtime, content))
		return nil
	}); err != nil {
		t.Fatalf("capture history inspect tree: %v", err)
	}
	slices.Sort(snapshot)
	return snapshot
}

func (f historyInspectFixture) assertUnchanged(t *testing.T, beforeClient, beforeWorktree []string) {
	t.Helper()
	afterClient := captureHistoryInspectClientState(t, f.clientDir)
	if !slices.Equal(beforeClient, afterClient) {
		t.Fatalf("history inspect modified client state: %s", historyInspectSnapshotDifference(beforeClient, afterClient))
	}
	afterWorktree := captureHistoryInspectWorktree(t, f.worktree)
	if !slices.Equal(beforeWorktree, afterWorktree) {
		t.Fatalf("history inspect modified worktree: %s", historyInspectSnapshotDifference(beforeWorktree, afterWorktree))
	}
}

func historyInspectSnapshotDifference(before, after []string) string {
	limit := min(len(before), len(after))
	for index := range limit {
		if before[index] != after[index] {
			return fmt.Sprintf("entry %d changed from %q to %q", index,
				historyInspectTruncateSnapshot(before[index]), historyInspectTruncateSnapshot(after[index]))
		}
	}
	return fmt.Sprintf("entry count changed from %d to %d", len(before), len(after))
}

func historyInspectTruncateSnapshot(value string) string {
	const limit = 160
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "..."
}

func outputValue(output, prefix string) string {
	for line := range strings.SplitSeq(output, "\n") {
		if value, ok := strings.CutPrefix(line, prefix); ok {
			return value
		}
	}
	return ""
}
