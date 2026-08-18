package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mingming-cn/filecloud/internal/library"
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
