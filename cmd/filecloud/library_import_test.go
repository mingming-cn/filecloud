package main

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	libraryapi "github.com/mingming-cn/filecloud/internal/library"
	"github.com/mingming-cn/filecloud/internal/object"
)

func TestLibraryBindImportsLocalSnapshotAndSyncNoOps(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	clientDir, worktree := newClientPaths(t)
	if err := os.MkdirAll(filepath.Join(worktree, "nested", "empty-dir"), 0o700); err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{
		"empty":                {},
		"nested/unicode-界.txt": []byte("unicode"),
		"below":                bytes.Repeat([]byte("a"), object.MaxBlockSize-1),
		"boundary":             bytes.Repeat([]byte("b"), object.MaxBlockSize),
		"above":                bytes.Repeat([]byte("c"), object.MaxBlockSize+1),
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(worktree, filepath.FromSlash(name)), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	args := append(bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID), "--import-local")
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("import bind: %v", err)
	}

	head, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if err != nil || head.CommitID == nil {
		t.Fatalf("Head after import: %+v err=%v", head, err)
	}
	binding := readTestBinding(t, clientDir, worktree)
	if binding.SyncBase != *head.CommitID {
		t.Fatalf("SyncBase=%s Head=%s", binding.SyncBase, *head.CommitID)
	}
	commit, err := object.VerifyCommit(getTestObject(t, environment.server.URL, environment.token, "commits", *head.CommitID), *head.CommitID)
	if err != nil || len(commit.Parents) != 1 || commit.Root != binding.SyncBaseRoot {
		t.Fatalf("import commit=%+v err=%v", commit, err)
	}
	initial, err := object.VerifyCommit(getTestObject(t, environment.server.URL, environment.token, "commits", commit.Parents[0]), commit.Parents[0])
	if err != nil || len(initial.Parents) != 0 {
		t.Fatalf("initial commit=%+v err=%v", initial, err)
	}
	_, emptyRoot, _ := canonicalEmptyDirectory()
	if initial.Root != emptyRoot || commit.Root == emptyRoot {
		t.Fatalf("roots initial=%s imported=%s empty=%s", initial.Root, commit.Root, emptyRoot)
	}
	assertRemoteSnapshot(t, environment, commit.Root, files)
	var repeatOutput bytes.Buffer
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), &repeatOutput, io.Discard); err != nil {
		t.Fatalf("repeat imported binding: %v", err)
	}
	if !strings.Contains(repeatOutput.String(), "already bound") {
		t.Fatalf("repeat imported binding output=%q", repeatOutput.String())
	}

	beforeETag := head.ETag
	var output bytes.Buffer
	if err := runTest(t.Context(), []string{"library", "sync", "--client-dir", clientDir, "--worktree", worktree}, strings.NewReader(""), &output, io.Discard); err != nil {
		t.Fatalf("no-op sync: %v", err)
	}
	after, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if err != nil || after.ETag != beforeETag || after.CommitID == nil || *after.CommitID != *head.CommitID {
		t.Fatalf("no-op sync changed Head: before=%+v after=%+v err=%v", head, after, err)
	}
	if err := os.WriteFile(filepath.Join(worktree, "changed"), []byte("change"), 0o600); err != nil {
		t.Fatal(err)
	}
	err = runTest(t.Context(), []string{"library", "sync", "--client-dir", clientDir, "--worktree", worktree}, strings.NewReader(""), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("changed sync: %v", err)
	}
	changedHead, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if err != nil || changedHead.ETag == beforeETag || changedHead.CommitID == nil || *changedHead.CommitID == *head.CommitID {
		t.Fatalf("changed sync did not advance Head: %+v err=%v", changedHead, err)
	}
}

func TestLibraryBindImportRequiresFlagBeforeRemoteMutation(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	var requests atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		environment.handler.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	clientDir, worktree := newClientPaths(t)
	if err := os.WriteFile(filepath.Join(worktree, "local"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := runTest(t.Context(), bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testClientDeviceID), strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "issue #8") {
		t.Fatalf("missing import flag error=%v", err)
	}
	if requests.Load() != 0 {
		t.Fatalf("remote received %d requests", requests.Load())
	}
	if _, err := os.Stat(clientDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("client state created: %v", err)
	}
}

func TestLibraryBindImportRejectsUnsupportedAndCollidingPaths(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(string) error
	}{
		{"symlink", func(root string) error { return os.Symlink("target", filepath.Join(root, "link")) }},
		{"fifo", func(root string) error { return syscall.Mkfifo(filepath.Join(root, "pipe"), 0o600) }},
		{"casefold", func(root string) error {
			if err := os.WriteFile(filepath.Join(root, "Readme"), []byte("a"), 0o600); err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(root, "README"), []byte("b"), 0o600)
		}},
		{"invalid-name", func(root string) error { return os.WriteFile(filepath.Join(root, "bad:name"), []byte("x"), 0o600) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
			clientDir, worktree := newClientPaths(t)
			if err := test.setup(worktree); err != nil {
				t.Fatal(err)
			}
			args := append(bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID), "--import-local")
			if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err == nil {
				t.Fatal("invalid import succeeded")
			}
			head, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
			if err != nil || head.CommitID != nil {
				t.Fatalf("invalid import changed Head: %+v err=%v", head, err)
			}
			if _, err := os.Stat(clientDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid import created state: %v", err)
			}
		})
	}
}

func TestLibraryBindImportRecoversUnknownCASAndFinalize(t *testing.T) {
	var updates atomic.Int32
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{AfterHeadUpdate: func() error {
		if updates.Add(1) == 2 {
			return errors.New("response lost")
		}
		return nil
	}})
	clientDir, worktree := newClientPaths(t)
	if err := os.WriteFile(filepath.Join(worktree, "local"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	args := append(bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID), "--import-local")
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("unknown import CAS: %v", err)
	}
	assertNoIntent(t, clientDir)

	otherEnvironment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	otherClient, otherWorktree := newClientPaths(t)
	if err := os.WriteFile(filepath.Join(otherWorktree, "local"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	otherArgs := append(bindArgs(otherClient, otherEnvironment.server.URL, testClientLibraryID, otherWorktree, testClientDeviceID), "--import-local")
	err := runLibraryWithConfig(t.Context(), otherArgs[1:], strings.NewReader(otherEnvironment.token+"\n"), io.Discard, io.Discard, libraryClientConfig{
		checkFilesystem: func(*os.File) error { return nil }, beforeFinalize: func() error { return errors.New("stop") },
	})
	if err == nil || !strings.Contains(err.Error(), "finalize") {
		t.Fatalf("finalize failure=%v", err)
	}
	assertIntentCount(t, otherClient, 1)
	if err := runTest(t.Context(), otherArgs, strings.NewReader(otherEnvironment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("recover finalize: %v", err)
	}
	assertNoIntent(t, otherClient)
}

func TestLibraryBindRepeatRejectsInvalidRemoteBaseCommitWithoutUpdatingBinding(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, importedBinding) string
	}{
		{"missing", func(t *testing.T, state importedBinding) string {
			if err := os.Remove(serverObjectPath(state.environment, "commits", state.binding.SyncBase)); err != nil {
				t.Fatal(err)
			}
			return state.binding.SyncBase
		}},
		{"corrupt", func(t *testing.T, state importedBinding) string {
			if err := os.WriteFile(serverObjectPath(state.environment, "commits", state.binding.SyncBase), []byte(`{}`), 0o600); err != nil {
				t.Fatal(err)
			}
			return state.binding.SyncBase
		}},
		{"root mismatch", func(t *testing.T, state importedBinding) string {
			head, err := getRemoteHead(t.Context(), mustServerURL(t, state.environment.server.URL), testClientLibraryID, []byte(state.environment.token))
			if err != nil || head.CommitID == nil {
				t.Fatalf("get imported Head: %+v err=%v", head, err)
			}
			_, emptyRoot, err := canonicalEmptyDirectory()
			if err != nil {
				t.Fatal(err)
			}
			data, id, err := canonicalCommit(testClientUserID, testClientDeviceID, emptyRoot, []string{*head.CommitID}, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			base := mustServerURL(t, state.environment.server.URL)
			if err := putMetadata(t.Context(), base, testClientLibraryID, []byte(state.environment.token), "commits", id, data); err != nil {
				t.Fatal(err)
			}
			if _, _, err := updateRemoteHead(t.Context(), base, testClientLibraryID, []byte(state.environment.token), head.ETag, id); err != nil {
				t.Fatal(err)
			}
			return id
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := newImportedBinding(t)
			remoteBase := test.mutate(t, state)
			db, err := openClientDB(filepath.Join(state.clientDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			preservedToken := []byte("preserved-token")
			preservedETag := `"preserved-etag"`
			if _, err := db.Exec("UPDATE bindings SET sync_base_commit = ?, access_token = ?, head_etag = ? WHERE worktree = ?",
				remoteBase, preservedToken, preservedETag, state.binding.Worktree); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			err = runTest(t.Context(), state.args, strings.NewReader(state.environment.token+"\n"), io.Discard, io.Discard)
			if err == nil {
				t.Fatal("repeat binding accepted invalid remote base commit")
			}
			db, err = openClientDB(filepath.Join(state.clientDir, _clientDatabaseName), true)
			if err != nil {
				t.Fatal(err)
			}
			var token []byte
			var etag string
			if err := db.QueryRow("SELECT access_token, head_etag FROM bindings WHERE worktree = ?", state.binding.Worktree).Scan(&token, &etag); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(token, preservedToken) || etag != preservedETag {
				t.Fatalf("failed repeat updated binding credentials: token=%q etag=%q", token, etag)
			}
		})
	}
}

type importedBinding struct {
	environment libraryCLIEnvironment
	clientDir   string
	worktree    string
	args        []string
	binding     clientBinding
}

func newImportedBinding(t *testing.T) importedBinding {
	t.Helper()
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	clientDir, worktree := newClientPaths(t)
	if err := os.WriteFile(filepath.Join(worktree, "local"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	args := append(bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID), "--import-local")
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	return importedBinding{environment, clientDir, worktree, args, readTestBinding(t, clientDir, worktree)}
}

func serverObjectPath(environment libraryCLIEnvironment, kind, id string) string {
	return filepath.Join(environment.store.ObjectsDir(), testClientUserID, testClientLibraryID, kind, id[:2], id[2:])
}

func TestLibraryBindMigratesIssue7ClientSchema(t *testing.T) {
	t.Run("existing binding", func(t *testing.T) {
		environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID)
		if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
			t.Fatal(err)
		}
		before := readTestBinding(t, clientDir, worktree)
		installIssue7ClientSchema(t, clientDir)
		if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
			t.Fatalf("rebind through migration: %v", err)
		}
		after := readTestBinding(t, clientDir, worktree)
		if before != after {
			t.Fatalf("migration changed existing binding: before=%+v after=%+v", before, after)
		}
		assertImportLocalColumn(t, clientDir)
	})

	t.Run("pending intent", func(t *testing.T) {
		environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID)
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, beforeHeadCAS: func() error { return errors.New("stop") }})
		if err == nil {
			t.Fatal("expected pending issue #7 bind intent")
		}
		candidate := readTestIntent(t, clientDir).CandidateCommit
		installIssue7ClientSchema(t, clientDir)
		if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
			t.Fatalf("recover issue #7 intent through migration: %v", err)
		}
		if binding := readTestBinding(t, clientDir, worktree); binding.SyncBase != candidate {
			t.Fatalf("migration replaced pending candidate: binding=%+v candidate=%s", binding, candidate)
		}
		assertNoIntent(t, clientDir)
		assertImportLocalColumn(t, clientDir)
	})

	t.Run("local import", func(t *testing.T) {
		environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
		clientDir, worktree := newClientPaths(t)
		if err := os.Mkdir(clientDir, 0o700); err != nil {
			t.Fatal(err)
		}
		db, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		installIssue7ClientSchema(t, clientDir)
		if err := os.WriteFile(filepath.Join(worktree, "local"), []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
		args := append(bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID), "--import-local")
		if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
			t.Fatalf("import through migration: %v", err)
		}
		binding := readTestBinding(t, clientDir, worktree)
		commit, err := object.VerifyCommit(getTestObject(t, environment.server.URL, environment.token, "commits", binding.SyncBase), binding.SyncBase)
		if err != nil || len(commit.Parents) != 1 {
			t.Fatalf("migrated import commit=%+v err=%v", commit, err)
		}
		assertImportLocalColumn(t, clientDir)
	})
}

func TestLibraryBindMigratesSingleIntentClientSchema(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	clientDir, worktree := newClientPaths(t)
	args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID)
	stopBeforeCAS := libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, beforeHeadCAS: func() error { return errors.New("stop") }}
	if err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard, stopBeforeCAS); err == nil {
		t.Fatal("expected pending bind intent")
	}
	before := readTestIntent(t, clientDir)
	installSingleIntentClientSchema(t, clientDir)
	if err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard, stopBeforeCAS); err == nil {
		t.Fatal("expected migrated intent to stop before CAS")
	}
	after := readTestIntent(t, clientDir)
	if before.ServerURL != after.ServerURL || before.LibraryID != after.LibraryID || before.Worktree != after.Worktree ||
		before.UserID != after.UserID || before.DeviceID != after.DeviceID || before.ExpectedETag != after.ExpectedETag ||
		before.CandidateCommit != after.CandidateCommit || before.CandidateRoot != after.CandidateRoot ||
		!bytes.Equal(before.CandidateData, after.CandidateData) || after.ImportLocal {
		t.Fatalf("single intent migration changed fields: before=%+v after=%+v", before, after)
	}
	assertScopedIntentSchema(t, clientDir)
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("publish migrated single intent: %v", err)
	}
	if binding := readTestBinding(t, clientDir, worktree); binding.SyncBase != before.CandidateCommit {
		t.Fatalf("migrated single intent candidate changed: binding=%+v intent=%+v", binding, before)
	}
}

func installSingleIntentClientSchema(t *testing.T, clientDir string) {
	t.Helper()
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`ALTER TABLE bind_intents RENAME TO scoped_bind_intents;
		CREATE TABLE bind_intents (
			id INTEGER PRIMARY KEY NOT NULL CHECK(id = 1),
			server_url TEXT NOT NULL, library_id TEXT NOT NULL, worktree TEXT NOT NULL,
			user_id TEXT NOT NULL, device_id TEXT NOT NULL, expected_etag TEXT NOT NULL,
			candidate_commit TEXT NOT NULL, candidate_root TEXT NOT NULL, candidate_data BLOB NOT NULL);
		INSERT INTO bind_intents(id, server_url, library_id, worktree, user_id, device_id, expected_etag, candidate_commit, candidate_root, candidate_data)
			SELECT 1, server_url, library_id, worktree, user_id, device_id, expected_etag, candidate_commit, candidate_root, candidate_data FROM scoped_bind_intents;
		DROP TABLE scoped_bind_intents`)
	if err != nil {
		t.Fatalf("install single intent client schema: %v", err)
	}
}

func assertScopedIntentSchema(t *testing.T, clientDir string) {
	t.Helper()
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var idColumns, importColumns, worktreePrimaryKey, importLocal int
	if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('bind_intents') WHERE name = 'id'").Scan(&idColumns); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('bind_intents') WHERE name = 'import_local'").Scan(&importColumns); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT pk FROM pragma_table_info('bind_intents') WHERE name = 'worktree'").Scan(&worktreePrimaryKey); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT import_local FROM bind_intents").Scan(&importLocal); err != nil {
		t.Fatal(err)
	}
	if idColumns != 0 || importColumns != 1 || worktreePrimaryKey != 1 || importLocal != 0 {
		t.Fatalf("migrated intent schema: id=%d import_local_column=%d worktree_pk=%d import_local=%d", idColumns, importColumns, worktreePrimaryKey, importLocal)
	}
}

func installIssue7ClientSchema(t *testing.T, clientDir string) {
	t.Helper()
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`ALTER TABLE bind_intents RENAME TO issue8_bind_intents;
		CREATE TABLE bind_intents (
			server_url TEXT NOT NULL, library_id TEXT NOT NULL,
			worktree TEXT PRIMARY KEY NOT NULL, user_id TEXT NOT NULL, device_id TEXT NOT NULL,
			expected_etag TEXT NOT NULL, candidate_commit TEXT NOT NULL, candidate_root TEXT NOT NULL,
			candidate_data BLOB NOT NULL, UNIQUE(server_url, library_id));
		INSERT INTO bind_intents(server_url, library_id, worktree, user_id, device_id, expected_etag, candidate_commit, candidate_root, candidate_data)
			SELECT server_url, library_id, worktree, user_id, device_id, expected_etag, candidate_commit, candidate_root, candidate_data FROM issue8_bind_intents;
		DROP TABLE issue8_bind_intents`)
	if err != nil {
		t.Fatalf("install issue #7 client schema: %v", err)
	}
}

func assertImportLocalColumn(t *testing.T, clientDir string) {
	t.Helper()
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('bind_intents') WHERE name = 'import_local'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("import_local column count=%d err=%v", count, err)
	}
}

func assertRemoteSnapshot(t *testing.T, environment libraryCLIEnvironment, root string, want map[string][]byte) {
	t.Helper()
	seen := make(map[string][]byte)
	seenDirectories := make(map[string]bool)
	var walk func(string, string)
	walk = func(directoryID, prefix string) {
		directory, err := object.VerifyDirectory(getTestObject(t, environment.server.URL, environment.token, "directories", directoryID), directoryID)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range directory.Entries {
			path := entry.Name
			if prefix != "" {
				path = prefix + "/" + entry.Name
			}
			if entry.Type == "Directory" {
				seenDirectories[path] = true
				walk(entry.ID, path)
				continue
			}
			file, err := object.VerifyFile(getTestObject(t, environment.server.URL, environment.token, "files", entry.ID), entry.ID)
			if err != nil {
				t.Fatal(err)
			}
			wantBlocks := (len(want[path]) + object.MaxBlockSize - 1) / object.MaxBlockSize
			if len(file.Blocks) != wantBlocks {
				t.Fatalf("snapshot file %q has %d blocks, want %d", path, len(file.Blocks), wantBlocks)
			}
			var data []byte
			for _, block := range file.Blocks {
				data = append(data, getTestBlock(t, environment.server.URL, environment.token, block)...)
			}
			seen[path] = data
		}
	}
	walk(root, "")
	for path, data := range want {
		if !bytes.Equal(seen[path], data) {
			t.Fatalf("snapshot file %q differs: got=%d want=%d", path, len(seen[path]), len(data))
		}
		delete(seen, path)
	}
	if len(seen) != 0 {
		t.Fatalf("unexpected snapshot files: %v", seen)
	}
	if !seenDirectories["nested"] || !seenDirectories["nested/empty-dir"] {
		t.Fatalf("empty directory missing from snapshot: %v", seenDirectories)
	}
}

func getTestBlock(t *testing.T, serverURL, token, id string) []byte {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, serverURL+"/v1/libraries/"+testClientLibraryID+"/blocks/"+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("get block: status=%d err=%v", response.StatusCode, err)
	}
	return data
}
