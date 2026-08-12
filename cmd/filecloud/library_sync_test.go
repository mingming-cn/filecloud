package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	libraryapi "github.com/mingming-cn/filecloud/internal/library"
	"github.com/mingming-cn/filecloud/internal/object"
	"golang.org/x/text/cases"
)

func TestLibrarySyncNoOpSendsNoPUTAndCreatesNoCommit(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	var puts, commits atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			puts.Add(1)
			if strings.Contains(r.URL.Path, "/objects/commits/") {
				commits.Add(1)
			}
		}
		environment.handler.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	clientDir, worktree := newClientPaths(t)
	if err := runTest(t.Context(), bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testClientDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	puts.Store(0)
	commits.Store(0)
	before := readTestBinding(t, clientDir, worktree)
	if err := syncTestWorktree(t, clientDir, worktree); err != nil {
		t.Fatal(err)
	}
	after := readTestBinding(t, clientDir, worktree)
	if puts.Load() != 0 || commits.Load() != 0 || before != after {
		t.Fatalf("no-op puts=%d commits=%d before=%+v after=%+v", puts.Load(), commits.Load(), before, after)
	}
}

func TestLibrarySyncPublishesLocalTreeOperations(t *testing.T) {
	state := newImportedBinding(t)
	steps := []struct {
		name   string
		change func() error
	}{
		{"create", func() error { return os.WriteFile(filepath.Join(state.worktree, "new.txt"), []byte("one"), 0o600) }},
		{"modify", func() error { return os.WriteFile(filepath.Join(state.worktree, "new.txt"), []byte("two"), 0o600) }},
		{"move", func() error {
			return os.Rename(filepath.Join(state.worktree, "new.txt"), filepath.Join(state.worktree, "moved.txt"))
		}},
		{"empty directory", func() error { return os.Mkdir(filepath.Join(state.worktree, "empty"), 0o700) }},
		{"delete", func() error { return os.Remove(filepath.Join(state.worktree, "moved.txt")) }},
		{"create replacement file", func() error { return os.WriteFile(filepath.Join(state.worktree, "replace"), []byte("file"), 0o600) }},
		{"file to directory", func() error {
			if err := os.Remove(filepath.Join(state.worktree, "replace")); err != nil {
				return err
			}
			if err := os.Mkdir(filepath.Join(state.worktree, "replace"), 0o700); err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(state.worktree, "replace", "child"), []byte("child"), 0o600)
		}},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			before := readTestBinding(t, state.clientDir, state.worktree)
			if err := step.change(); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, state.clientDir, state.worktree); err != nil {
				var required *deleteConfirmationRequiredError
				if !errors.As(err, &required) {
					t.Fatal(err)
				}
				if err := confirmTestDeletion(t, state.clientDir, state.worktree, required.candidate[:deleteCandidatePrefixLen], libraryClientConfig{}); err != nil {
					t.Fatal(err)
				}
			}
			after := assertTestConverged(t, state.environment, state.clientDir, state.worktree)
			commit, err := object.VerifyCommit(getTestObject(t, state.environment.server.URL, state.environment.token, "commits", after.SyncBase), after.SyncBase)
			if err != nil || len(commit.Parents) != 1 || commit.Parents[0] != before.SyncBase {
				t.Fatalf("published commit=%+v err=%v before=%+v", commit, err, before)
			}
		})
	}
}

func TestLibrarySyncAppliesRemoteTreeOperationsWithoutPUT(t *testing.T) {
	environment, publisherDir, publisherTree, subscriberDir, subscriberTree, puts, commits := newSyncPair(t)
	operations := []func() error{
		func() error { return os.WriteFile(filepath.Join(publisherTree, "remote.txt"), []byte("one"), 0o600) },
		func() error { return os.WriteFile(filepath.Join(publisherTree, "remote.txt"), []byte("two"), 0o600) },
		func() error {
			return os.Rename(filepath.Join(publisherTree, "remote.txt"), filepath.Join(publisherTree, "moved.txt"))
		},
		func() error { return os.Mkdir(filepath.Join(publisherTree, "empty"), 0o700) },
		func() error { return os.Remove(filepath.Join(publisherTree, "moved.txt")) },
		func() error { return os.WriteFile(filepath.Join(publisherTree, "replace"), []byte("file"), 0o600) },
		func() error {
			if err := os.Remove(filepath.Join(publisherTree, "replace")); err != nil {
				return err
			}
			return os.Mkdir(filepath.Join(publisherTree, "replace"), 0o700)
		},
	}
	for index, operation := range operations {
		if err := operation(); err != nil {
			t.Fatal(err)
		}
		if err := syncTestWorktreeConfirmingDeletes(t, publisherDir, publisherTree); err != nil {
			t.Fatalf("publish operation %d: %v", index, err)
		}
		puts.Store(0)
		commits.Store(0)
		if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
			t.Fatalf("apply operation %d: %v", index, err)
		}
		if puts.Load() != 0 || commits.Load() != 0 {
			t.Fatalf("remote apply operation %d sent puts=%d commits=%d", index, puts.Load(), commits.Load())
		}
		publisher := assertTestConverged(t, environment, publisherDir, publisherTree)
		subscriber := assertTestConverged(t, environment, subscriberDir, subscriberTree)
		if publisher.SyncBase != subscriber.SyncBase || publisher.SyncBaseRoot != subscriber.SyncBaseRoot {
			t.Fatalf("operation %d did not converge publisher=%+v subscriber=%+v", index, publisher, subscriber)
		}
	}
}

func TestLibrarySyncMergesBothSidesChanged(t *testing.T) {
	environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "local"), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"local": "local", "remote": "remote"} {
		if data, err := os.ReadFile(filepath.Join(subscriberTree, name)); err != nil || string(data) != want {
			t.Fatalf("merged %s=%q err=%v", name, data, err)
		}
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	publisher := assertTestConverged(t, environment, publisherDir, publisherTree)
	subscriber := assertTestConverged(t, environment, subscriberDir, subscriberTree)
	if publisher.SyncBase != subscriber.SyncBase || publisher.SyncBaseRoot != subscriber.SyncBaseRoot {
		t.Fatalf("merged clients differ: publisher=%+v subscriber=%+v", publisher, subscriber)
	}
}

func TestLibrarySyncExistingPendingCreatesConflictCopy(t *testing.T) {
	environment, publisherDir, publisherTree, subscriberDir, subscriberTree, puts, _ := newSyncPair(t)
	if err := os.WriteFile(filepath.Join(subscriberTree, "base"), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			beforeHeadCAS: func() error { return errors.New("stop before CAS") }})
	if err == nil {
		t.Fatal("initial pending publication unexpectedly completed")
	}
	beforePending := readTestPendingPublication(t, subscriberDir, subscriberTree)
	if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	beforeHead, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID,
		[]byte(environment.token))
	if err != nil {
		t.Fatal(err)
	}
	puts.Store(0)
	if err = syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	afterHead, headErr := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID,
		[]byte(environment.token))
	matches, globErr := filepath.Glob(filepath.Join(subscriberTree, "base (Filecloud conflict *)"))
	if headErr != nil || globErr != nil || beforeHead.CommitID == nil || afterHead.CommitID == nil ||
		*beforeHead.CommitID == *afterHead.CommitID || len(matches) != 1 || puts.Load() == 0 ||
		countClientRows(t, subscriberDir, "pending_publications", subscriberTree) != 0 {
		t.Fatalf("replacement did not publish conflict: head=%+v err=%v matches=%v puts=%d", afterHead, headErr, matches, puts.Load())
	}
	commit, verifyErr := getRemoteCommit(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID,
		[]byte(environment.token), *afterHead.CommitID)
	if verifyErr != nil || len(commit.Parents) != 2 || commit.Parents[1] != beforePending.CandidateCommit {
		t.Fatalf("replacement parents=%v verify=%v", commit.Parents, verifyErr)
	}
}

func TestLibrarySyncMigratesV20TrivialMergePending(t *testing.T) {
	for _, headState := range []string{"expected", "candidate", "successor", "conflict"} {
		t.Run(headState, func(t *testing.T) {
			state := newImportedBinding(t)
			baseURL := mustServerURL(t, state.environment.server.URL)
			token := []byte(state.environment.token)
			if err := os.WriteFile(filepath.Join(state.worktree, "local"), []byte("changed"), 0o600); err != nil {
				t.Fatal(err)
			}
			root, err := openWorktreeRoot(state.worktree, func(*os.File) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := scanWorktreeWithConfig(root, worktreeScanConfig{})
			if err != nil {
				root.Close()
				t.Fatal(err)
			}
			options := bindOptions{base: baseURL, libraryID: testClientLibraryID, token: token, worktreeRoot: root}
			if err := uploadSnapshot(t.Context(), options, snapshot); err != nil {
				root.Close()
				t.Fatal(err)
			}
			if err := root.Close(); err != nil {
				t.Fatal(err)
			}
			now := func(second int) func() time.Time {
				return func() time.Time { return time.Date(2026, 8, 10, 1, 2, second, 0, time.UTC) }
			}
			localData, localID, err := canonicalCommit(testClientUserID, testClientDeviceID, snapshot.root,
				[]string{state.binding.SyncBase}, now(1))
			if err != nil {
				t.Fatal(err)
			}
			expectedData, expectedID, err := canonicalCommit(testClientUserID, testClientDeviceID, state.binding.SyncBaseRoot,
				[]string{state.binding.SyncBase}, now(2))
			if err != nil {
				t.Fatal(err)
			}
			for id, data := range map[string][]byte{localID: localData, expectedID: expectedData} {
				if err := putMetadata(t.Context(), baseURL, testClientLibraryID, token, "commits", id, data); err != nil {
					t.Fatal(err)
				}
			}
			expectedHead, _, err := updateRemoteHead(t.Context(), baseURL, testClientLibraryID, token,
				state.binding.HeadETag, expectedID)
			if err != nil {
				t.Fatal(err)
			}
			mergeData, mergeID, err := canonicalCommit(testClientUserID, testClientDeviceID, snapshot.root,
				[]string{expectedID, localID}, now(3))
			if err != nil {
				t.Fatal(err)
			}
			if err := putMetadata(t.Context(), baseURL, testClientLibraryID, token, "commits", mergeID, mergeData); err != nil {
				t.Fatal(err)
			}
			currentHead := expectedHead
			switch headState {
			case "candidate":
				currentHead, _, err = updateRemoteHead(t.Context(), baseURL, testClientLibraryID, token, currentHead.ETag, mergeID)
			case "successor":
				currentHead, _, err = updateRemoteHead(t.Context(), baseURL, testClientLibraryID, token, currentHead.ETag, mergeID)
				if err != nil {
					break
				}
				successorData, successorID, commitErr := canonicalCommit(testClientUserID, testClientDeviceID, snapshot.root,
					[]string{mergeID}, now(4))
				if commitErr == nil {
					commitErr = putMetadata(t.Context(), baseURL, testClientLibraryID, token, "commits", successorID, successorData)
				}
				if commitErr == nil {
					currentHead, _, commitErr = updateRemoteHead(t.Context(), baseURL, testClientLibraryID, token, currentHead.ETag, successorID)
				}
				err = commitErr
			case "conflict":
				conflictData, conflictID, commitErr := canonicalCommit(testClientUserID, testClientDeviceID,
					state.binding.SyncBaseRoot, []string{expectedID}, now(5))
				if commitErr == nil {
					commitErr = putMetadata(t.Context(), baseURL, testClientLibraryID, token, "commits", conflictID, conflictData)
				}
				if commitErr == nil {
					currentHead, _, commitErr = updateRemoteHead(t.Context(), baseURL, testClientLibraryID, token, currentHead.ETag, conflictID)
				}
				err = commitErr
			}
			if err != nil {
				t.Fatal(err)
			}
			db, err := openClientDB(filepath.Join(state.clientDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`DROP TABLE pending_publications;
				DELETE FROM client_schema_migrations WHERE version IN (21,22)`); err != nil {
				db.Close()
				t.Fatal(err)
			}
			if _, err := db.Exec(clientV20PendingSQL); err != nil {
				db.Close()
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO pending_publications VALUES(?,?,?,?,?,?,?,?,0,0,0,0,0)`, state.worktree,
				state.binding.SyncBase, state.binding.SyncBaseRoot, expectedID, expectedHead.ETag, mergeID, snapshot.root, mergeData); err != nil {
				db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, state.clientDir, state.worktree); err != nil {
				t.Fatalf("migrated v20 %s Head=%+v: %v", headState, currentHead, err)
			}
			assertTestConverged(t, state.environment, state.clientDir, state.worktree)
		})
	}
}

func TestLibrarySyncRejectsStalePendingBeforeHeadCAS(t *testing.T) {
	state := newImportedBinding(t)
	if err := os.WriteFile(filepath.Join(state.worktree, "local"), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	beforeBinding := readTestBinding(t, state.clientDir, state.worktree)
	beforeRoot := scanTestRoot(t, state.worktree)
	beforeHead, err := getRemoteHead(t.Context(), mustServerURL(t, state.environment.server.URL), testClientLibraryID,
		[]byte(state.environment.token))
	if err != nil {
		t.Fatal(err)
	}
	err = runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", state.clientDir, "--worktree", state.worktree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			beforeHeadCAS: func() error {
				db, err := openClientDB(filepath.Join(state.clientDir, _clientDatabaseName), false)
				if err != nil {
					return err
				}
				defer db.Close()
				_, err = db.Exec(`UPDATE pending_publications SET tracked_count=tracked_count+1 WHERE worktree=?`, state.worktree)
				return err
			}})
	if err == nil || !strings.Contains(err.Error(), "pending publication changed before remote publication") {
		t.Fatalf("stale pending publication error=%v", err)
	}
	afterHead, headErr := getRemoteHead(t.Context(), mustServerURL(t, state.environment.server.URL), testClientLibraryID,
		[]byte(state.environment.token))
	if headErr != nil || beforeHead.CommitID == nil || afterHead.CommitID == nil || *beforeHead.CommitID != *afterHead.CommitID ||
		beforeHead.ETag != afterHead.ETag || readTestBinding(t, state.clientDir, state.worktree) != beforeBinding ||
		scanTestRoot(t, state.worktree) != beforeRoot {
		t.Fatalf("stale CAS changed head=%+v binding=%+v root=%s err=%v", afterHead,
			readTestBinding(t, state.clientDir, state.worktree), scanTestRoot(t, state.worktree), headErr)
	}
}

func TestLibraryBindRejectsStaleBindingCredentialRefresh(t *testing.T) {
	state := newImportedBinding(t)
	beforeRoot := scanTestRoot(t, state.worktree)
	err := runLibraryWithConfig(t.Context(), state.args[1:], strings.NewReader(state.environment.token+"\n"),
		io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			beforeBindingRefresh: func() error {
				db, err := openClientDB(filepath.Join(state.clientDir, _clientDatabaseName), false)
				if err != nil {
					return err
				}
				defer db.Close()
				_, err = db.Exec(`UPDATE bindings SET device_id=? WHERE worktree=?`, testOtherDeviceID, state.worktree)
				return err
			}})
	after := readTestBinding(t, state.clientDir, state.worktree)
	if err == nil || !strings.Contains(err.Error(), "existing binding changed before credential refresh") ||
		after.DeviceID != testOtherDeviceID || after.SyncBase != state.binding.SyncBase ||
		after.SyncBaseRoot != state.binding.SyncBaseRoot || after.HeadETag != state.binding.HeadETag ||
		scanTestRoot(t, state.worktree) != beforeRoot {
		t.Fatalf("stale binding refresh error=%v binding=%+v", err, after)
	}
}

func TestLibrarySyncRejectsOversizedPendingBlobsBeforeMutation(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	var puts atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			puts.Add(1)
		}
		environment.handler.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	clientDir, worktree := newClientPaths(t)
	if err := os.WriteFile(filepath.Join(worktree, "local"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	args := append(bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testClientDeviceID), "--import-local")
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktree, "pending"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", clientDir, "--worktree", worktree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			beforeHeadCAS: func() error { return errors.New("stop before CAS") }})
	if err == nil {
		t.Fatal("expected pending publication")
	}
	pending := readTestPendingPublication(t, clientDir, worktree)
	beforeBinding, beforeRoot := readTestBinding(t, clientDir, worktree), scanTestRoot(t, worktree)
	beforeHead, err := getRemoteHead(t.Context(), mustServerURL(t, proxy.URL), testClientLibraryID, []byte(environment.token))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		column string
		value  any
		size   int
	}{
		{"candidate_data", pending.CandidateData, 65537},
		{"captured_data", pending.CapturedData, 65537},
		{"candidate_history", pending.CandidateHistory, _maxCandidateHistoryBytes + 1},
	} {
		t.Run(test.column, func(t *testing.T) {
			db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`PRAGMA ignore_check_constraints=ON`); err == nil {
				_, err = db.Exec(`UPDATE pending_publications SET `+test.column+`=zeroblob(?) WHERE worktree=?`, test.size, worktree)
			}
			if closeErr := db.Close(); err != nil || closeErr != nil {
				t.Fatal(errors.Join(err, closeErr))
			}
			puts.Store(0)
			err = syncTestWorktree(t, clientDir, worktree)
			if err == nil || !strings.Contains(err.Error(), "exceeds synchronization budget") {
				t.Fatalf("oversized %s error=%v", test.column, err)
			}
			afterHead, headErr := getRemoteHead(t.Context(), mustServerURL(t, proxy.URL), testClientLibraryID, []byte(environment.token))
			if headErr != nil || puts.Load() != 0 || readTestBinding(t, clientDir, worktree) != beforeBinding ||
				scanTestRoot(t, worktree) != beforeRoot || countClientRows(t, clientDir, "pending_publications", worktree) != 1 ||
				beforeHead.CommitID == nil || afterHead.CommitID == nil || *beforeHead.CommitID != *afterHead.CommitID ||
				beforeHead.ETag != afterHead.ETag {
				t.Fatalf("oversized %s mutated state: puts=%d head=%+v err=%v", test.column, puts.Load(), afterHead, headErr)
			}
			db, err = openClientDB(filepath.Join(clientDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`PRAGMA ignore_check_constraints=ON`); err == nil {
				_, err = db.Exec(`UPDATE pending_publications SET `+test.column+`=? WHERE worktree=?`, test.value, worktree)
			}
			if closeErr := db.Close(); err != nil || closeErr != nil {
				t.Fatal(errors.Join(err, closeErr))
			}
		})
	}
}

func TestFinalizePublishedRollsBackStaleRows(t *testing.T) {
	for _, stale := range []string{"pending field", "binding device"} {
		t.Run(stale, func(t *testing.T) {
			state := newImportedBinding(t)
			if err := os.WriteFile(filepath.Join(state.worktree, "pending"), []byte("data"), 0o600); err != nil {
				t.Fatal(err)
			}
			err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", state.clientDir, "--worktree", state.worktree},
				strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
					beforeHeadCAS: func() error { return errors.New("stop before CAS") }})
			if err == nil {
				t.Fatal("expected pending publication")
			}
			binding := readTestBinding(t, state.clientDir, state.worktree)
			pending := readTestPendingPublication(t, state.clientDir, state.worktree)
			db, err := openClientDB(filepath.Join(state.clientDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if stale == "pending field" {
				_, err = db.Exec(`UPDATE pending_publications SET tracked_count=tracked_count+1 WHERE worktree=?`, state.worktree)
			} else {
				_, err = db.Exec(`UPDATE bindings SET device_id=? WHERE worktree=?`, testOtherDeviceID, state.worktree)
			}
			if err != nil {
				t.Fatal(err)
			}
			root, err := openWorktreeRoot(state.worktree, func(*os.File) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			snapshot, scanErr := scanWorktree(root)
			if err := errors.Join(scanErr, root.Close()); err != nil {
				t.Fatal(err)
			}
			beforeIndex := countClientRows(t, state.clientDir, "path_index", state.worktree)
			head := remoteHead{CommitID: &pending.CandidateCommit, ETag: "published-etag"}
			if err := finalizePublished(t.Context(), db, binding, snapshot, head, pending, io.Discard); err == nil {
				t.Fatal("stale finalization succeeded")
			}
			afterBinding := readTestBinding(t, state.clientDir, state.worktree)
			afterPending := readTestPendingPublication(t, state.clientDir, state.worktree)
			if countClientRows(t, state.clientDir, "path_index", state.worktree) != beforeIndex ||
				afterBinding.SyncBase != binding.SyncBase || afterBinding.SyncBaseRoot != binding.SyncBaseRoot ||
				afterBinding.HeadETag != binding.HeadETag || afterPending.CandidateCommit != pending.CandidateCommit {
				t.Fatalf("stale %s partially finalized: binding=%+v pending=%+v", stale, afterBinding, afterPending)
			}
			if stale == "pending field" && afterPending.TrackedCount != pending.TrackedCount+1 {
				t.Fatalf("stale pending row was overwritten: before=%+v after=%+v", pending, afterPending)
			}
			if stale == "binding device" && afterBinding.DeviceID != testOtherDeviceID {
				t.Fatalf("stale binding row was overwritten: %+v", afterBinding)
			}
		})
	}
}

func TestLibrarySyncRecursivelyMergesNestedDirectory(t *testing.T) {
	environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	if err := os.MkdirAll(filepath.Join(publisherTree, "nested", "deep"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{"local.txt": "base-local", "remote.txt": "base-remote"} {
		if err := os.WriteFile(filepath.Join(publisherTree, "nested", "deep", name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(publisherTree, "nested", "deep", "remote.txt"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "nested", "deep", "local.txt"), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"local.txt": "local", "remote.txt": "remote"} {
		data, err := os.ReadFile(filepath.Join(subscriberTree, "nested", "deep", name))
		if err != nil || string(data) != want {
			t.Fatalf("nested %s=%q err=%v", name, data, err)
		}
	}
	assertTestConverged(t, environment, subscriberDir, subscriberTree)
}

func TestLibrarySyncStructuralConflictsPreserveCompleteLocalObject(t *testing.T) {
	structuralMtime := time.Date(2025, 6, 7, 8, 9, 10, 0, time.UTC)
	var held *os.File
	write := func(t *testing.T, root, path, content string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, path)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, path), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	conflicts := func(t *testing.T, root, leaf string) []string {
		t.Helper()
		ext := filepath.Ext(leaf)
		matches, err := filepath.Glob(filepath.Join(root,
			strings.TrimSuffix(leaf, ext)+" (Filecloud conflict *)"+ext))
		if err != nil {
			t.Fatal(err)
		}
		return matches
	}
	for _, test := range []struct {
		name         string
		base         func(*testing.T, string)
		remote       func(*testing.T, string)
		local        func(*testing.T, string)
		assertResult func(*testing.T, string)
	}{
		{name: "local delete remote modify file", base: func(t *testing.T, root string) { write(t, root, "item.txt", "base") },
			remote: func(t *testing.T, root string) { write(t, root, "item.txt", "remote") },
			local:  func(t *testing.T, root string) { os.Remove(filepath.Join(root, "item.txt")) },
			assertResult: func(t *testing.T, root string) {
				if data, err := os.ReadFile(filepath.Join(root, "item.txt")); err != nil || string(data) != "remote" || len(conflicts(t, root, "item.txt")) != 0 {
					t.Fatalf("remote original=%q err=%v conflicts=%v", data, err, conflicts(t, root, "item.txt"))
				}
			}},
		{name: "local modify remote delete file", base: func(t *testing.T, root string) { write(t, root, "item.txt", "base") },
			remote: func(t *testing.T, root string) { os.Remove(filepath.Join(root, "item.txt")) },
			local:  func(t *testing.T, root string) { write(t, root, "item.txt", "local") },
			assertResult: func(t *testing.T, root string) {
				matches := conflicts(t, root, "item.txt")
				data, err := os.ReadFile(matches[0])
				if _, originalErr := os.Lstat(filepath.Join(root, "item.txt")); len(matches) != 1 || err != nil || string(data) != "local" || !errors.Is(originalErr, os.ErrNotExist) {
					t.Fatalf("local conflict=%v data=%q err=%v original=%v", matches, data, err, originalErr)
				}
			}},
		{name: "local delete remote modify directory", base: func(t *testing.T, root string) { write(t, root, "item/base.txt", "base") },
			remote: func(t *testing.T, root string) {
				write(t, root, "item/base.txt", "remote")
				write(t, root, "item/nested/new.txt", "remote nested")
			}, local: func(t *testing.T, root string) { os.RemoveAll(filepath.Join(root, "item")) },
			assertResult: func(t *testing.T, root string) {
				for path, want := range map[string]string{"item/base.txt": "remote", "item/nested/new.txt": "remote nested"} {
					if data, err := os.ReadFile(filepath.Join(root, path)); err != nil || string(data) != want {
						t.Fatalf("%s=%q err=%v", path, data, err)
					}
				}
			}},
		{name: "local modify remote delete directory", base: func(t *testing.T, root string) { write(t, root, "item/base.txt", "base") },
			remote: func(t *testing.T, root string) { os.RemoveAll(filepath.Join(root, "item")) },
			local: func(t *testing.T, root string) {
				write(t, root, "item/base.txt", "local")
				write(t, root, "item/nested/new.txt", "local nested")
				if err := os.MkdirAll(filepath.Join(root, "item/empty"), 0o700); err != nil {
					t.Fatal(err)
				}
				for _, path := range []string{"item/nested", "item/empty", "item"} {
					if err := os.Chtimes(filepath.Join(root, path), structuralMtime, structuralMtime); err != nil {
						t.Fatal(err)
					}
				}
				var err error
				held, err = os.Open(filepath.Join(root, "item/nested/new.txt"))
				if err != nil {
					t.Fatal(err)
				}
			}, assertResult: func(t *testing.T, root string) {
				matches := conflicts(t, root, "item")
				if len(matches) != 1 {
					t.Fatalf("conflict directories=%v", matches)
				}
				for path, want := range map[string]string{"base.txt": "local", "nested/new.txt": "local nested"} {
					if data, err := os.ReadFile(filepath.Join(matches[0], path)); err != nil || string(data) != want {
						t.Fatalf("%s=%q err=%v", path, data, err)
					}
				}
				for _, path := range []string{"", "nested", "empty"} {
					info, err := os.Stat(filepath.Join(matches[0], path))
					if err != nil || !info.IsDir() || !info.ModTime().Equal(structuralMtime) {
						t.Fatalf("directory %q info=%v err=%v", path, info, err)
					}
				}
				heldInfo, heldErr := held.Stat()
				pathInfo, pathErr := os.Stat(filepath.Join(matches[0], "nested/new.txt"))
				if heldErr != nil || pathErr != nil || !os.SameFile(heldInfo, pathInfo) {
					t.Fatalf("held descendant identity held=%v/%v path=%v/%v", heldInfo, heldErr, pathInfo, pathErr)
				}
				if err := held.Close(); err != nil {
					t.Fatal(err)
				}
				held = nil
			}},
		{name: "local file remote directory", base: func(t *testing.T, root string) { write(t, root, "item.txt", "base") },
			remote: func(t *testing.T, root string) {
				os.Remove(filepath.Join(root, "item.txt"))
				write(t, root, "item.txt/nested/remote.txt", "remote")
				os.MkdirAll(filepath.Join(root, "item.txt/empty"), 0o700)
			}, local: func(t *testing.T, root string) { write(t, root, "item.txt", "local") },
			assertResult: func(t *testing.T, root string) {
				matches := conflicts(t, root, "item.txt")
				data, err := os.ReadFile(matches[0])
				if len(matches) != 1 || err != nil || string(data) != "local" {
					t.Fatalf("file conflict=%v data=%q err=%v", matches, data, err)
				}
				if data, err := os.ReadFile(filepath.Join(root, "item.txt/nested/remote.txt")); err != nil || string(data) != "remote" {
					t.Fatalf("remote directory=%q err=%v", data, err)
				}
			}},
		{name: "local directory remote file", base: func(t *testing.T, root string) { write(t, root, "item.tar", "base") },
			remote: func(t *testing.T, root string) { write(t, root, "item.tar", "remote") },
			local: func(t *testing.T, root string) {
				os.Remove(filepath.Join(root, "item.tar"))
				write(t, root, "item.tar/nested/local.txt", "local")
				os.MkdirAll(filepath.Join(root, "item.tar/empty"), 0o700)
			}, assertResult: func(t *testing.T, root string) {
				if data, err := os.ReadFile(filepath.Join(root, "item.tar")); err != nil || string(data) != "remote" {
					t.Fatalf("remote file=%q err=%v", data, err)
				}
				matches := conflicts(t, root, "item.tar")
				if len(matches) != 1 {
					t.Fatalf("directory conflicts=%v", matches)
				}
				if data, err := os.ReadFile(filepath.Join(matches[0], "nested/local.txt")); err != nil || string(data) != "local" {
					t.Fatalf("local subtree=%q err=%v", data, err)
				}
				if info, err := os.Stat(filepath.Join(matches[0], "empty")); err != nil || !info.IsDir() {
					t.Fatalf("empty directory info=%v err=%v", info, err)
				}
			}},
		{name: "both delete", base: func(t *testing.T, root string) { write(t, root, "item", "base") },
			remote: func(t *testing.T, root string) { os.Remove(filepath.Join(root, "item")) },
			local:  func(t *testing.T, root string) { os.Remove(filepath.Join(root, "item")) },
			assertResult: func(t *testing.T, root string) {
				if _, err := os.Lstat(filepath.Join(root, "item")); !errors.Is(err, os.ErrNotExist) || len(conflicts(t, root, "item")) != 0 {
					t.Fatalf("both-delete path err=%v conflicts=%v", err, conflicts(t, root, "item"))
				}
			}},
		{name: "identical change", base: func(t *testing.T, root string) { write(t, root, "item", "base") },
			remote: func(t *testing.T, root string) { write(t, root, "item", "same") },
			local:  func(t *testing.T, root string) { write(t, root, "item", "same") },
			assertResult: func(t *testing.T, root string) {
				if data, err := os.ReadFile(filepath.Join(root, "item")); err != nil || string(data) != "same" || len(conflicts(t, root, "item")) != 0 {
					t.Fatalf("identical path=%q err=%v conflicts=%v", data, err, conflicts(t, root, "item"))
				}
			}},
		{name: "rename is delete plus add", base: func(t *testing.T, root string) { write(t, root, "old.txt", "rename") },
			remote: func(t *testing.T, root string) { write(t, root, "remote.txt", "remote") },
			local: func(t *testing.T, root string) {
				if err := os.Rename(filepath.Join(root, "old.txt"), filepath.Join(root, "renamed.txt")); err != nil {
					t.Fatal(err)
				}
			}, assertResult: func(t *testing.T, root string) {
				if _, err := os.Lstat(filepath.Join(root, "old.txt")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("old path err=%v", err)
				}
				for path, want := range map[string]string{"renamed.txt": "rename", "remote.txt": "remote"} {
					if data, err := os.ReadFile(filepath.Join(root, path)); err != nil || string(data) != want {
						t.Fatalf("%s=%q err=%v", path, data, err)
					}
				}
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, commits := newSyncPair(t)
			test.base(t, publisherTree)
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
				t.Fatal(err)
			}
			test.remote(t, publisherTree)
			if err := syncTestWorktreeConfirmingDeletes(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			test.local(t, subscriberTree)
			if err := syncTestWorktreeConfirmingDeletes(t, subscriberDir, subscriberTree); err != nil {
				t.Fatal(err)
			}
			test.assertResult(t, subscriberTree)
			before := assertTestConverged(t, environment, subscriberDir, subscriberTree)
			commits.Store(0)
			if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
				t.Fatal(err)
			}
			after := assertTestConverged(t, environment, subscriberDir, subscriberTree)
			if commits.Load() != 0 || before.SyncBase != after.SyncBase {
				t.Fatalf("repeat sync commits=%d before=%s after=%s", commits.Load(), before.SyncBase, after.SyncBase)
			}
		})
	}
}

func TestLibrarySyncRecursiveMergeParentsAndCapturedSnapshot(t *testing.T) {
	_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "local"), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			now:           func() time.Time { return time.Date(2026, 8, 9, 15, 0, 0, 0, time.UTC) },
			beforeHeadCAS: func() error { return errors.New("inspect merge") }})
	if err == nil || !strings.Contains(err.Error(), "inspect merge") {
		t.Fatalf("merge inspection error=%v", err)
	}
	pending := readTestPendingPublication(t, subscriberDir, subscriberTree)
	commit, err := object.VerifyCommit(pending.CandidateData, pending.CandidateCommit)
	if err != nil || len(commit.Parents) != 2 || commit.Parents[0] != pending.ExpectedHead ||
		commit.Parents[1] != pending.CapturedCommit || pending.CapturedRoot != scanTestRoot(t, subscriberTree) ||
		pending.CandidateRoot == pending.CapturedRoot {
		t.Fatalf("pending=%+v commit=%+v err=%v", pending, commit, err)
	}
}

func TestLibrarySyncProtectedRecursiveMergeUploadsOnlyAfterConfirmation(t *testing.T) {
	for _, test := range []struct {
		name, count string
		tracked     int
		deleted     int
	}{
		{name: "ten percent", tracked: 10, deleted: 1},
		{name: "more than one hundred", tracked: 102, deleted: 101},
	} {
		t.Run(test.name, func(t *testing.T) {
			environment, publisherDir, publisherTree, subscriberDir, subscriberTree, puts := newTrackedSyncPair(t, test.tracked)
			if err := os.WriteFile(filepath.Join(publisherTree, "remote-addition"), []byte("remote"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			for index := 0; index < test.deleted; index++ {
				if err := os.Remove(filepath.Join(subscriberTree, fmt.Sprintf("tracked-%03d", index))); err != nil {
					t.Fatal(err)
				}
			}
			before := readTestBinding(t, subscriberDir, subscriberTree)
			beforeIndex := countClientRows(t, subscriberDir, "path_index", subscriberTree)
			head, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID,
				[]byte(environment.token))
			if err != nil {
				t.Fatal(err)
			}
			puts.Store(0)
			err = syncTestWorktree(t, subscriberDir, subscriberTree)
			var required *deleteConfirmationRequiredError
			if !errors.As(err, &required) || required.deleted != int64(test.deleted) || required.tracked != int64(test.tracked) {
				t.Fatalf("protected recursive merge error=%v required=%+v", err, required)
			}
			pending := readTestPendingPublication(t, subscriberDir, subscriberTree)
			if puts.Load() != 0 || pending.CandidateCommit != required.candidate ||
				pending.CapturedCommit == pending.CandidateCommit || !pending.RequiresDeleteConfirmation || pending.DeleteConfirmed ||
				readTestBinding(t, subscriberDir, subscriberTree) != before ||
				countClientRows(t, subscriberDir, "path_index", subscriberTree) != beforeIndex {
				t.Fatalf("protected recursive state puts=%d pending=%+v", puts.Load(), pending)
			}
			afterHead, headErr := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID,
				[]byte(environment.token))
			if headErr != nil || head.CommitID == nil || afterHead.CommitID == nil || *head.CommitID != *afterHead.CommitID ||
				head.ETag != afterHead.ETag || err.Error() != deletionConfirmationError(pending).Error() {
				t.Fatalf("protected recursive Head/error changed: before=%+v after=%+v err=%v", head, afterHead, err)
			}
			if err := confirmTestDeletion(t, subscriberDir, subscriberTree,
				pending.CandidateCommit[:deleteCandidatePrefixLen], libraryClientConfig{}); err != nil {
				t.Fatal(err)
			}
			assertTestConverged(t, environment, subscriberDir, subscriberTree)
		})
	}
}

func TestLibrarySyncProtectedRecursiveMergeSurvivesRepeatedHeadAdvances(t *testing.T) {
	environment, publisherDir, publisherTree, subscriberDir, subscriberTree, puts := newTrackedSyncPair(t, 10)
	if err := os.WriteFile(filepath.Join(publisherTree, "remote-0"), []byte("remote-0"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"tracked-000", "tracked-001"} {
		if err := os.Remove(filepath.Join(subscriberTree, name)); err != nil {
			t.Fatal(err)
		}
	}
	puts.Store(0)
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err == nil {
		t.Fatal("initial protected merge succeeded")
	}
	first := readTestPendingPublication(t, subscriberDir, subscriberTree)
	if history, err := _decodeCandidateHistory(first.CandidateHistory); err != nil || len(history) != 0 || puts.Load() != 0 {
		t.Fatalf("initial history=%d puts=%d err=%v", len(history), puts.Load(), err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err == nil || puts.Load() != 0 {
		t.Fatalf("restart error=%v puts=%d", err, puts.Load())
	}

	advance := func(name string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(publisherTree, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
			t.Fatal(err)
		}
		puts.Store(0)
	}
	advance("remote-1")
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err == nil {
		t.Fatal("first protected replacement succeeded")
	}
	second := readTestPendingPublication(t, subscriberDir, subscriberTree)
	history, err := _decodeCandidateHistory(second.CandidateHistory)
	if err != nil || len(history) != 1 || !bytes.Equal(history[0], first.CandidateData) || puts.Load() != 0 ||
		second.BaseCommit != first.ExpectedHead || second.CapturedCommit != first.CapturedCommit {
		t.Fatalf("first replacement=%+v history=%d puts=%d err=%v", second, len(history), puts.Load(), err)
	}

	advance("remote-2")
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err == nil {
		t.Fatal("second protected replacement succeeded")
	}
	third := readTestPendingPublication(t, subscriberDir, subscriberTree)
	history, err = _decodeCandidateHistory(third.CandidateHistory)
	if err != nil || len(history) != 2 || !bytes.Equal(history[0], first.CandidateData) ||
		!bytes.Equal(history[1], second.CandidateData) || puts.Load() != 0 || third.BaseCommit != second.ExpectedHead ||
		third.CapturedCommit != first.CapturedCommit || third.CapturedRoot != first.CapturedRoot ||
		!bytes.Equal(third.CapturedData, first.CapturedData) {
		t.Fatalf("second replacement=%+v history=%d puts=%d err=%v", third, len(history), puts.Load(), err)
	}
	binding := readTestBinding(t, subscriberDir, subscriberTree)
	db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	root, err := openWorktreeRoot(subscriberTree, func(*os.File) error { return nil })
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	snapshot, err := scanWorktree(root)
	if err != nil {
		root.Close()
		db.Close()
		t.Fatal(err)
	}
	head, err := getRemoteHead(t.Context(), mustServerURL(t, binding.ServerURL), testClientLibraryID, []byte(environment.token))
	if err != nil {
		root.Close()
		db.Close()
		t.Fatal(err)
	}
	budget := &_replayBudget{commitLimit: 2, treeLimit: _mergeMaxObjects, pathLimit: _mergeMaxObjects,
		commits: make(map[string]object.Commit), walked: make(map[string]bool)}
	puts.Store(0)
	err = resumePublication(t.Context(), db, bindOptions{base: mustServerURL(t, binding.ServerURL),
		libraryID: testClientLibraryID, token: []byte(environment.token), clientDir: subscriberDir, worktreeRoot: root},
		binding, snapshot, head, third, io.Discard, normalizeLibraryClientConfig(libraryClientConfig{}), budget)
	if closeErr := errors.Join(root.Close(), db.Close()); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err == nil || !strings.Contains(err.Error(), "cumulative synchronization budget") || puts.Load() != 0 ||
		!reflect.DeepEqual(readTestPendingPublication(t, subscriberDir, subscriberTree), third) ||
		readTestBinding(t, subscriberDir, subscriberTree) != binding {
		t.Fatalf("cumulative history budget changed state: err=%v puts=%d", err, puts.Load())
	}

	firstMerge, err := object.VerifyCommit(history[0], object.ID(history[0]))
	if err != nil {
		t.Fatal(err)
	}
	created, err := time.Parse("2006-01-02T15:04:05Z", firstMerge.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	alter := func(owner, device, root string, parents []string) []byte {
		t.Helper()
		data, _, err := canonicalCommit(owner, device, root, parents, func() time.Time { return created })
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	corruptions := map[string][][]byte{
		"noncanonical commit": {append(append([]byte(nil), history[0]...), ' '), history[1]},
		"wrong owner":         {alter("11111111-2222-4333-8444-555555555555", testClientDeviceID, firstMerge.Root, firstMerge.Parents), history[1]},
		"wrong device":        {alter(testClientUserID, testClientDeviceID, firstMerge.Root, firstMerge.Parents), history[1]},
		"wrong parents":       {alter(testClientUserID, testClientDeviceID, firstMerge.Root, []string{firstMerge.Parents[1], firstMerge.Parents[0]}), history[1]},
		"wrong root":          {alter(testClientUserID, testClientDeviceID, third.CapturedRoot, firstMerge.Parents), history[1]},
		"reordered":           {history[1], history[0]},
		"duplicate":           {history[0], history[0]},
	}
	for name, commits := range corruptions {
		t.Run(name, func(t *testing.T) {
			encoded, err := _encodeCandidateHistory(commits)
			if err != nil {
				t.Fatal(err)
			}
			corrupt := third
			corrupt.CandidateHistory = encoded
			if err := verifyPendingPublication(corrupt, binding); err == nil {
				t.Fatal("corrupt candidate history was accepted")
			}
		})
	}
	if err := confirmTestDeletion(t, subscriberDir, subscriberTree,
		third.CandidateCommit[:deleteCandidatePrefixLen], libraryClientConfig{}); err != nil {
		t.Fatal(err)
	}
	assertTestConverged(t, environment, subscriberDir, subscriberTree)
}

func TestRecursiveMergeReplayUsesCumulativePathBudget(t *testing.T) {
	fileData, fileID, err := canonicalFile("a", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, id, err := canonicalDirectory("", []scanEntry{{name: "a", kind: "File", id: fileID,
		modified: "2026-08-10T00:00:00Z"}})
	if err != nil {
		t.Fatal(err)
	}
	budget := &_replayBudget{commitLimit: 10, treeLimit: 10, pathLimit: 1, commits: make(map[string]object.Commit),
		walked: make(map[string]bool)}
	merger := &_treeMerger{ctx: t.Context(), directories: map[string][]byte{id: data}, files: map[string][]byte{fileID: fileData},
		synthesized: make(map[string][]byte), active: make(map[string]bool), seen: make(map[string]bool), budget: budget}
	if paths, err := merger.paths(id, "", 0); err != nil || len(paths) != 1 {
		t.Fatalf("first bounded traversal paths=%d err=%v", len(paths), err)
	}
	if _, err := merger.paths(id, "", 0); err == nil || !strings.Contains(err.Error(), "cumulative path budget") {
		t.Fatalf("second traversal error=%v", err)
	}
}

func TestCandidateHistoryCodecRejectsCorruption(t *testing.T) {
	valid, err := _encodeCandidateHistory([][]byte{[]byte("commit")})
	if err != nil {
		t.Fatal(err)
	}
	tooMany := append([]byte(nil), _emptyCandidateHistory...)
	binary.BigEndian.PutUint32(tooMany[4:], maxSyncParentWalk+1)
	cases := map[string][]byte{
		"short header":       valid[:7],
		"truncated length":   valid[:10],
		"zero length":        append(append([]byte(nil), _emptyCandidateHistory[:4]...), 0, 0, 0, 1, 0, 0, 0, 0),
		"oversized metadata": append(append([]byte(nil), _emptyCandidateHistory[:4]...), 0, 0, 0, 1, 0, 1, 0, 1),
		"trailing data":      append(append([]byte(nil), valid...), 0),
		"too many commits":   tooMany,
		"wrong magic":        append([]byte("BAD!"), _emptyCandidateHistory[4:]...),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := _decodeCandidateHistory(data); err == nil {
				t.Fatal("corrupt candidate history was accepted")
			}
		})
	}
}

func TestLibrarySyncProtectedRecursiveMutationInvalidatesConfirmation(t *testing.T) {
	_, publisherDir, publisherTree, subscriberDir, subscriberTree, _ := newTrackedSyncPair(t, 10)
	if err := os.WriteFile(filepath.Join(publisherTree, "remote-addition"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(subscriberTree, "tracked-000")); err != nil {
		t.Fatal(err)
	}
	err := syncTestWorktree(t, subscriberDir, subscriberTree)
	var required *deleteConfirmationRequiredError
	if !errors.As(err, &required) {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "mutation"), []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	err = confirmTestDeletion(t, subscriberDir, subscriberTree, required.candidate[:deleteCandidatePrefixLen], libraryClientConfig{})
	if err == nil || !strings.Contains(err.Error(), "stale candidate discarded") ||
		countClientRows(t, subscriberDir, "pending_publications", subscriberTree) != 0 {
		t.Fatalf("mutated recursive confirmation error=%v", err)
	}
}

func TestLibrarySyncProtectedReplacementStoresBeforeUpload(t *testing.T) {
	environment, publisherDir, publisherTree, subscriberDir, subscriberTree, puts := newTrackedSyncPair(t, 10)
	if err := os.Remove(filepath.Join(subscriberTree, "tracked-000")); err != nil {
		t.Fatal(err)
	}
	err := syncTestWorktree(t, subscriberDir, subscriberTree)
	var required *deleteConfirmationRequiredError
	if !errors.As(err, &required) {
		t.Fatal(err)
	}
	old := readTestPendingPublication(t, subscriberDir, subscriberTree)
	var putsAtReplacement atomic.Int32
	config := libraryClientConfig{beforeHeadCAS: func() error {
		if err := os.WriteFile(filepath.Join(publisherTree, "remote-addition"), []byte("remote"), 0o600); err != nil {
			return err
		}
		return syncTestWorktree(t, publisherDir, publisherTree)
	}, afterPendingReplacement: func() error {
		putsAtReplacement.Store(puts.Load())
		return nil
	}}
	puts.Store(0)
	err = confirmTestDeletion(t, subscriberDir, subscriberTree, old.CandidateCommit[:deleteCandidatePrefixLen], config)
	if !errors.As(err, &required) {
		t.Fatalf("protected replacement error=%v", err)
	}
	next := readTestPendingPublication(t, subscriberDir, subscriberTree)
	if next.CandidateCommit == old.CandidateCommit ||
		next.CandidateCommit[:deleteCandidatePrefixLen] == old.CandidateCommit[:deleteCandidatePrefixLen] ||
		required.candidate != next.CandidateCommit || next.DeleteConfirmed || !next.RequiresDeleteConfirmation ||
		puts.Load() != putsAtReplacement.Load() ||
		puts.Load() == 0 || next.CapturedCommit != old.CapturedCommit || !bytes.Equal(next.CapturedData, old.CapturedData) {
		t.Fatalf("replacement old=%+v next=%+v puts=%d at-replacement=%d", old, next, puts.Load(), putsAtReplacement.Load())
	}
	head, headErr := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID,
		[]byte(environment.token))
	if headErr != nil || head.CommitID == nil || *head.CommitID != next.ExpectedHead {
		t.Fatalf("replacement Head=%+v err=%v", head, headErr)
	}
}

func TestRecursiveMergeDirectoryMtimeFollowsMergedContent(t *testing.T) {
	emptyData, emptyID, err := canonicalEmptyDirectory()
	if err != nil {
		t.Fatal(err)
	}
	makeDirectory := func(entries ...scanEntry) ([]byte, string) {
		t.Helper()
		data, id, err := canonicalDirectory("child", entries)
		if err != nil {
			t.Fatal(err)
		}
		return data, id
	}
	baseData, baseID := makeDirectory(scanEntry{name: "base", kind: "File", id: emptyID, modified: "2026-01-01T00:00:00Z"})
	localData, localID := makeDirectory(
		scanEntry{name: "base", kind: "File", id: emptyID, modified: "2026-01-01T00:00:00Z"},
		scanEntry{name: "local", kind: "File", id: emptyID, modified: "2026-01-02T00:00:00Z"})
	remoteData, remoteID := makeDirectory(
		scanEntry{name: "base", kind: "File", id: emptyID, modified: "2026-01-01T00:00:00Z"},
		scanEntry{name: "remote", kind: "File", id: emptyID, modified: "2026-01-02T00:00:00Z"})

	const t0, t1, t2 = "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", "2026-01-03T00:00:00Z"
	for _, test := range []struct {
		name                          string
		localID, localMtime           string
		remoteID, remoteMtime, wantID string
		wantMtime                     string
		synthesized                   bool
	}{
		{name: "local content beats newer remote mtime", localID: localID, localMtime: t1,
			remoteID: baseID, remoteMtime: t2, wantID: localID, wantMtime: t1},
		{name: "remote content beats newer local mtime", localID: baseID, localMtime: t2,
			remoteID: remoteID, remoteMtime: t1, wantID: remoteID, wantMtime: t1},
		{name: "same content takes newer mtime", localID: localID, localMtime: t1,
			remoteID: localID, remoteMtime: t2, wantID: localID, wantMtime: t2},
		{name: "synthesized content takes newer mtime", localID: localID, localMtime: t1,
			remoteID: remoteID, remoteMtime: t2, wantMtime: t2, synthesized: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			merge := func(localChildID, localMtime, remoteChildID, remoteMtime string) (string, string, string) {
				t.Helper()
				makeParent := func(childID, mtime string) ([]byte, string) {
					return makeDirectory(scanEntry{name: "child", kind: "Directory", id: childID, modified: mtime})
				}
				baseParentData, baseParentID := makeParent(baseID, t0)
				localParentData, localParentID := makeParent(localChildID, localMtime)
				remoteParentData, remoteParentID := makeParent(remoteChildID, remoteMtime)
				merger := &_treeMerger{directories: map[string][]byte{emptyID: emptyData, baseID: baseData,
					localID: localData, remoteID: remoteData, baseParentID: baseParentData,
					localParentID: localParentData, remoteParentID: remoteParentData}, synthesized: make(map[string][]byte),
					active: make(map[string]bool), seen: make(map[string]bool)}
				root, err := merger.merge(baseParentID, localParentID, remoteParentID, "", 0)
				if err != nil {
					t.Fatal(err)
				}
				directory, err := merger.loadDirectory(root)
				if err != nil {
					t.Fatal(err)
				}
				entry := directory.Entries[0]
				return root, entry.ID, entry.ModifiedAt
			}
			forwardRoot, forwardID, forwardMtime := merge(test.localID, test.localMtime, test.remoteID, test.remoteMtime)
			reverseRoot, reverseID, reverseMtime := merge(test.remoteID, test.remoteMtime, test.localID, test.localMtime)
			if forwardRoot != reverseRoot || forwardID != reverseID || forwardMtime != test.wantMtime || reverseMtime != forwardMtime {
				t.Fatalf("forward=%s/%s/%s reverse=%s/%s/%s", forwardRoot, forwardID, forwardMtime,
					reverseRoot, reverseID, reverseMtime)
			}
			if test.synthesized {
				if forwardID == test.localID || forwardID == test.remoteID || forwardID == baseID {
					t.Fatalf("merged child ID %s was not synthesized", forwardID)
				}
			} else if forwardID != test.wantID {
				t.Fatalf("merged child ID=%s, want %s", forwardID, test.wantID)
			}
		})
	}
}

func TestLibrarySyncStructuralConflictWithConflictedDescendantRestarts(t *testing.T) {
	_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	child := "inside (Filecloud conflict deadbeef 20240102T030405Z).txt"
	for _, root := range []string{publisherTree, subscriberTree} {
		if err := os.MkdirAll(filepath.Join(root, "item"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "item", child), []byte("base"), 0o600); err != nil {
			t.Fatal(err)
		}
		for index := 0; index < 30; index++ {
			if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("stable-%02d", index)), []byte("stable"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(publisherTree, "item")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(publisherTree, "item"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "item", child), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	var interrupted atomic.Bool
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			fsActionFault: func(point string, action fsAction) error {
				if point == "after_completed" && action.ExpectedObject != "" && action.OriginActionID == "" && interrupted.CompareAndSwap(false, true) {
					return errors.New("interrupt nested conflict promotion")
				}
				return nil
			}})
	if err == nil || !strings.Contains(err.Error(), "interrupt nested conflict promotion") {
		t.Fatalf("promotion interruption=%v", err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(subscriberTree, "item")); err != nil || string(data) != "remote" {
		t.Fatalf("remote structural target=%q/%v", data, err)
	}
	directories, err := filepath.Glob(filepath.Join(subscriberTree, "item (Filecloud conflict *)"))
	if err != nil || len(directories) != 1 {
		t.Fatalf("structural conflict directories=%v err=%v", directories, err)
	}
	if data, err := os.ReadFile(filepath.Join(directories[0], child)); err != nil || string(data) != "local" {
		t.Fatalf("conflicted descendant=%q/%v", data, err)
	}
}

func TestRecursiveMergeStructuralConflictTruthTable(t *testing.T) {
	const (
		baseFile   = "1111111111111111111111111111111111111111111111111111111111111111"
		localFile  = "2222222222222222222222222222222222222222222222222222222222222222"
		remoteFile = "3333333333333333333333333333333333333333333333333333333333333333"
		t0         = "2026-01-01T00:00:00Z"
		t1         = "2026-01-02T00:00:00Z"
		t2         = "2026-01-03T00:00:00Z"
	)
	seed := object.Commit{DeviceID: testClientDeviceID, CreatedAt: "2025-02-03T04:05:06Z"}
	conflict := "item (Filecloud conflict aaaaaaaa 20250203T040506Z)"
	extConflict := "item (Filecloud conflict aaaaaaaa 20250203T040506Z).txt"
	directories := make(map[string][]byte)
	makeDirectory := func(path string, entries ...scanEntry) string {
		t.Helper()
		data, id, err := canonicalDirectory(path, entries)
		if err != nil {
			t.Fatal(err)
		}
		directories[id] = data
		return id
	}
	emptyID := makeDirectory("")
	localChild := makeDirectory("item", scanEntry{name: "nested.txt", kind: "File", id: localFile, modified: t1})
	remoteChild := makeDirectory("item", scanEntry{name: "remote.txt", kind: "File", id: remoteFile, modified: t2})

	entry := func(name, kind, id, mtime string) scanEntry {
		return scanEntry{name: name, kind: kind, id: id, modified: mtime}
	}
	for _, test := range []struct {
		name                string
		base, local, remote []scanEntry
		want                []scanEntry
		lineage             map[string]_conflictPromotion
		wantTarget          string
	}{
		{name: "local delete remote modify file", base: []scanEntry{entry("item.txt", "File", baseFile, t0)},
			remote: []scanEntry{entry("item.txt", "File", remoteFile, t2)},
			want:   []scanEntry{entry("item.txt", "File", remoteFile, t2)}},
		{name: "local modify remote delete file", base: []scanEntry{entry("item.txt", "File", baseFile, t0)},
			local: []scanEntry{entry("item.txt", "File", localFile, t1)},
			want:  []scanEntry{entry(extConflict, "File", localFile, t1)},
			lineage: map[string]_conflictPromotion{"item.txt": {source: "item.txt", target: "item.txt", id: localFile,
				mtime: t1}}, wantTarget: extConflict},
		{name: "both delete", base: []scanEntry{entry("item", "File", baseFile, t0)}},
		{name: "same changed file canonical mtime", base: []scanEntry{entry("item", "Directory", localChild, t0)},
			local:  []scanEntry{entry("item", "File", localFile, t1)},
			remote: []scanEntry{entry("item", "File", localFile, t2)},
			want:   []scanEntry{entry("item", "File", localFile, t2)}},
		{name: "base absent divergent files", local: []scanEntry{entry("item", "File", localFile, t1)},
			remote: []scanEntry{entry("item", "File", remoteFile, t2)},
			want:   []scanEntry{entry("item", "File", remoteFile, t2), entry(conflict, "File", localFile, t1)}},
		{name: "local directory remote delete", base: []scanEntry{entry("item", "Directory", emptyID, t0)},
			local: []scanEntry{entry("item", "Directory", localChild, t1)},
			want:  []scanEntry{entry(conflict, "Directory", localChild, t1)},
			lineage: map[string]_conflictPromotion{"item/nested.txt": {source: "item/nested.txt", target: "item/nested.txt",
				id: localFile, mtime: t1}}, wantTarget: conflict + "/nested.txt"},
		{name: "local delete remote directory modify", base: []scanEntry{entry("item", "Directory", emptyID, t0)},
			remote: []scanEntry{entry("item", "Directory", remoteChild, t2)},
			want:   []scanEntry{entry("item", "Directory", remoteChild, t2)}},
		{name: "local file remote directory", base: []scanEntry{entry("item", "File", baseFile, t0)},
			local: []scanEntry{entry("item", "File", localFile, t1)}, remote: []scanEntry{entry("item", "Directory", remoteChild, t2)},
			want: []scanEntry{entry("item", "Directory", remoteChild, t2), entry(conflict, "File", localFile, t1)}},
		{name: "local directory remote file", base: []scanEntry{entry("item", "File", baseFile, t0)},
			local: []scanEntry{entry("item", "Directory", localChild, t1)}, remote: []scanEntry{entry("item", "File", remoteFile, t2)},
			want: []scanEntry{entry("item", "File", remoteFile, t2), entry(conflict, "Directory", localChild, t1)},
			lineage: map[string]_conflictPromotion{"item/nested.txt": {source: "item/nested.txt", target: "item/nested.txt",
				id: localFile, mtime: t1}}, wantTarget: conflict + "/nested.txt"},
		{name: "directories recurse over base file", base: []scanEntry{entry("item", "File", baseFile, t0)},
			local: []scanEntry{entry("item", "Directory", localChild, t1)}, remote: []scanEntry{entry("item", "Directory", remoteChild, t2)},
			want: []scanEntry{entry("item", "Directory", "", t2)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			baseID := makeDirectory("", test.base...)
			localID := makeDirectory("", test.local...)
			remoteID := makeDirectory("", test.remote...)
			merger := &_treeMerger{directories: directories, synthesized: make(map[string][]byte), active: make(map[string]bool),
				seen: make(map[string]bool), localSeed: seed, lineage: test.lineage}
			root, err := merger.merge(baseID, localID, remoteID, "", 0)
			if err != nil {
				t.Fatal(err)
			}
			got, err := merger.loadDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Entries) != len(test.want) {
				t.Fatalf("entries=%+v want=%+v", got.Entries, test.want)
			}
			for index, want := range test.want {
				entry := got.Entries[index]
				if entry.Name != want.name || entry.Type != want.kind || entry.ModifiedAt != want.modified ||
					want.id != "" && entry.ID != want.id {
					t.Fatalf("entry[%d]=%+v want=%+v", index, entry, want)
				}
			}
			if test.wantTarget != "" && test.lineage["item/nested.txt"].target != test.wantTarget &&
				test.lineage["item.txt"].target != test.wantTarget {
				t.Fatalf("lineage=%+v want target %q", test.lineage, test.wantTarget)
			}
		})
	}
}

func TestConflictCopyName(t *testing.T) {
	seed := object.Commit{AuthorUserID: testClientUserID, DeviceID: testClientDeviceID, CreatedAt: "2025-02-03T04:05:06Z"}
	for _, test := range []struct {
		name, leaf, want string
	}{
		{"no extension", "notes", "notes (Filecloud conflict aaaaaaaa 20250203T040506Z)"},
		{"dotfile", ".env", ".env (Filecloud conflict aaaaaaaa 20250203T040506Z)"},
		{"last extension", "archive.tar.gz", "archive.tar (Filecloud conflict aaaaaaaa 20250203T040506Z).gz"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := _conflictCopyName(test.leaf, "", seed, nil)
			if err != nil || got != test.want {
				t.Fatalf("name=%q err=%v, want %q", got, err, test.want)
			}
		})
	}
	occupied := []string{"notes (Filecloud conflict aaaaaaaa 20250203T040506Z)",
		"NOTES (FILECLOUD CONFLICT AAAAAAAA 20250203T040506Z) 2"}
	if got, err := _conflictCopyName("notes", "", seed, occupied); err != nil ||
		got != "notes (Filecloud conflict aaaaaaaa 20250203T040506Z) 3" {
		t.Fatalf("collision name=%q err=%v", got, err)
	}
}

func TestRootPromotionTargetValidation(t *testing.T) {
	expected := "nested/base (Filecloud conflict aaaaaaaa 20250203T040506Z).txt"
	second, err := nextConflictChainPath(expected)
	if err != nil {
		t.Fatal(err)
	}
	third, err := nextConflictChainPath(second)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{expected, second, third} {
		if err := validateRootPromotionTarget(expected, target); err != nil {
			t.Fatalf("valid target %q: %v", target, err)
		}
	}
	for name, pair := range map[string][2]string{
		"ordinary zero-step":  {"unrelated/new.txt", "unrelated/new.txt"},
		"malformed zero-step": {"base (Filecloud conflict invalid)", "base (Filecloud conflict invalid)"},
		"ambiguous fallback":  {"Filecloud Conflicts/0123456789ab-name (Filecloud conflict 01)", "Filecloud Conflicts/0123456789ab-name (Filecloud conflict 01)"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateRootPromotionTarget(pair[0], pair[1]); err == nil {
				t.Fatalf("invalid zero-step family %q was accepted", pair[0])
			}
		})
	}
	for name, target := range map[string]string{
		"arbitrary prefix":         "nested/copy-" + strings.TrimPrefix(expected, "nested/"),
		"lookalike":                strings.Replace(expected, "Filecloud conflict", "Filecloud conflict-copy", 1),
		"wrong parent":             strings.TrimPrefix(expected, "nested/"),
		"alternate seed":           strings.Replace(expected, "aaaaaaaa", "bbbbbbbb", 1),
		"alternate timestamp":      strings.Replace(expected, "20250203T040506Z", "20250203T040507Z", 1),
		"alternate extension":      strings.TrimSuffix(expected, ".txt") + ".bin",
		"malformed skipped suffix": strings.TrimSuffix(expected, ".txt") + " 2 4.txt",
		"casefold alias":           strings.ToUpper(expected),
		"normalized alias":         strings.Replace(expected, "base", "basee\u0301", 1),
		"outside parent":           "../" + expected,
		"path overflow":            strings.Repeat("p/", 500) + second,
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateRootPromotionTarget(expected, target); err == nil {
				t.Fatalf("invalid root target %q was accepted", target)
			}
		})
	}
}

func TestConflictPromotionUsesDeepestCanonicalFamily(t *testing.T) {
	normal := "dir (Filecloud conflict aaaaaaaa 20250203T040506Z)"
	fallback := "Filecloud Conflicts/0123456789ab-dir (Filecloud conflict 1)"
	file := "file (Filecloud conflict bbbbbbbb 20250203T040507Z).txt"
	for _, ancestor := range []string{normal, fallback} {
		seed := strings.Repeat("1", 64)
		if ancestor == fallback {
			seed = "0123456789ab" + strings.Repeat("1", 52)
		}
		for _, leaf := range []string{file, "0123456789ab-file (Filecloud conflict 1)"} {
			expected := ancestor + "/" + leaf
			family, suffix, err := conflictPromotionFamilyPath(expected)
			if err != nil || family != expected || suffix != "" {
				t.Fatalf("deepest family %q/%q err=%v", family, suffix, err)
			}
			next, err := nextPromotionChainPath(expected, seed)
			if err != nil || validateRootPromotionTarget(expected, next, seed) != nil {
				t.Fatalf("successor %q err=%v", next, err)
			}
			ancestorNext, err := nextConflictChainPath(ancestor)
			if err != nil {
				t.Fatal(err)
			}
			if err := validateRootPromotionTarget(expected, ancestorNext+"/"+leaf, strings.Repeat("1", 64)); err == nil {
				t.Fatal("earlier family component advanced")
			}
		}
	}
	descendant := normal + "/nested/plain.txt"
	next, err := nextPromotionChainPath(descendant, strings.Repeat("1", 64))
	want := normal + "/nested/plain (Filecloud conflict aaaaaaaa 20250203T040506Z).txt"
	if err != nil || next != want || validateRootPromotionTarget(descendant, next, strings.Repeat("1", 64)) != nil {
		t.Fatalf("descendant successor=%q want=%q err=%v", next, want, err)
	}
	if err := validateRootPromotionTarget(descendant, normal+"/other/"+filepath.Base(next), strings.Repeat("1", 64)); err == nil {
		t.Fatal("tampered descendant parent accepted")
	}
}

func TestRecursiveMergeConflictProvenanceIgnoresLegalContentAmbiguity(t *testing.T) {
	makeDirectory := func(entries ...scanEntry) ([]byte, string) {
		t.Helper()
		data, id, err := canonicalDirectory("", entries)
		if err != nil {
			t.Fatal(err)
		}
		return data, id
	}
	seed := object.Commit{AuthorUserID: testClientUserID, DeviceID: testClientDeviceID,
		CreatedAt: "2025-02-03T04:05:06Z"}
	conflict := "base (Filecloud conflict aaaaaaaa 20250203T040506Z)"
	baseData, baseID := makeDirectory(
		scanEntry{name: "base", kind: "File", id: strings.Repeat("1", 64), modified: "2025-01-01T00:00:00Z"},
		scanEntry{name: conflict, kind: "File", id: strings.Repeat("2", 64), modified: "2025-01-02T00:00:00Z"})
	localData, localID := makeDirectory(
		scanEntry{name: "base", kind: "File", id: strings.Repeat("2", 64), modified: "2025-01-02T00:00:00Z"},
		scanEntry{name: conflict, kind: "File", id: strings.Repeat("2", 64), modified: "2025-01-02T00:00:00Z"})
	remoteData, remoteID := makeDirectory(
		scanEntry{name: "base", kind: "File", id: strings.Repeat("3", 64), modified: "2025-01-03T00:00:00Z"},
		scanEntry{name: conflict, kind: "File", id: strings.Repeat("2", 64), modified: "2025-01-02T00:00:00Z"})
	lineage := map[string]_conflictPromotion{
		"base":   {source: "base", target: "base", id: strings.Repeat("2", 64), mtime: "2025-01-02T00:00:00Z", size: 5},
		conflict: {source: conflict, target: conflict, id: strings.Repeat("2", 64), mtime: "2025-01-02T00:00:00Z", size: 5},
	}
	merger := &_treeMerger{directories: map[string][]byte{baseID: baseData, localID: localData, remoteID: remoteData},
		synthesized: make(map[string][]byte), active: make(map[string]bool), seen: make(map[string]bool),
		budget: _newReplayBudget(), localSeed: seed, lineage: lineage}
	if _, err := merger.merge(baseID, localID, remoteID, "", 0); err != nil {
		t.Fatal(err)
	}
	promotions := _movedConflictPromotions(lineage)
	want := conflict + " 2"
	if len(promotions) != 1 || promotions[0].source != "base" || promotions[0].target != want {
		t.Fatalf("conflict promotions=%+v, want base -> %q", promotions, want)
	}
	encoded, err := _encodeConflictPromotions(promotions)
	decoded, decodeErr := _decodeConflictPromotions(encoded)
	if err != nil || decodeErr != nil || !reflect.DeepEqual(decoded, promotions) {
		t.Fatalf("provenance round trip=%+v encode=%v decode=%v", decoded, err, decodeErr)
	}
}

func TestConflictPromotionLineageKeepsPerMergeSeeds(t *testing.T) {
	firstSeed := strings.Repeat("a", 64)
	secondSeed := strings.Repeat("b", 64)
	lineage := map[string]_conflictPromotion{
		"first":  {source: "first", target: "first", id: strings.Repeat("1", 64), mtime: "2025-01-02T00:00:00Z", size: 5},
		"second": {source: "second", target: "second", id: strings.Repeat("2", 64), mtime: "2025-01-03T00:00:00Z", size: 6},
	}
	merger := &_treeMerger{localSeedID: firstSeed, lineage: lineage}
	merger.moveLineage("first", "first (Filecloud conflict aaaaaaaa 20250203T040506Z)")
	merger.localSeedID = secondSeed
	merger.moveLineage("second", "second (Filecloud conflict aaaaaaaa 20250203T040507Z)")
	promotions := _movedConflictPromotions(lineage)
	encoded, err := _encodeConflictPromotions(promotions)
	decoded, decodeErr := _decodeConflictPromotions(encoded)
	if err != nil || decodeErr != nil || len(decoded) != 2 || decoded[0].namingSeed != firstSeed || decoded[1].namingSeed != secondSeed {
		t.Fatalf("per-merge FCP2 seeds=%+v encode=%v decode=%v", decoded, err, decodeErr)
	}
}

func TestConflictPromotionCodecAndTargetValidation(t *testing.T) {
	first := _conflictPromotion{source: "a", target: "a (Filecloud conflict aaaaaaaa 20250203T040506Z)",
		id: strings.Repeat("1", 64), mtime: "2025-01-02T00:00:00Z", size: 5}
	second := _conflictPromotion{source: "b", target: "b (Filecloud conflict aaaaaaaa 20250203T040506Z)",
		id: strings.Repeat("2", 64), mtime: "2025-01-03T00:00:00Z", size: 6}
	for name, values := range map[string][]_conflictPromotion{
		"duplicate source": {first, {source: "A", target: second.target, id: second.id, mtime: second.mtime, size: second.size}},
		"duplicate target": {first, {source: second.source, target: strings.ToUpper(first.target), id: second.id, mtime: second.mtime, size: second.size}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := _encodeConflictPromotions(values); err == nil {
				t.Fatal("duplicate provenance was accepted")
			}
		})
	}
	encoded, err := _encodeConflictPromotions([]_conflictPromotion{second, first})
	if err != nil || string(encoded[:4]) != "FCP1" {
		t.Fatalf("legacy provenance header=%q err=%v", encoded[:min(4, len(encoded))], err)
	}
	decoded, err := _decodeConflictPromotions(encoded)
	if err != nil || len(decoded) != 2 || decoded[0] != first || decoded[1] != second {
		t.Fatalf("canonical provenance=%+v err=%v", decoded, err)
	}
	reencoded, err := _encodeConflictPromotions(decoded)
	if err != nil || !bytes.Equal(reencoded, encoded) {
		t.Fatalf("legacy canonical re-encoding=%x err=%v", reencoded, err)
	}
	if _, err := _decodeConflictPromotions([]byte{'F', 'C', 'P', '2', 0, 0, 0, 0}); err == nil {
		t.Fatal("noncanonical empty FCP2 provenance was accepted")
	}
	if _, err := _decodeConflictPromotions(append(encoded, 0)); err == nil {
		t.Fatal("noncanonical trailing provenance was accepted")
	}
	if _, err := _decodeConflictPromotions(make([]byte, _maxConflictPromotionsBytes+1)); err == nil {
		t.Fatal("oversized provenance was accepted")
	}
	seededFirst, seededSecond := first, second
	seededFirst.namingSeed = strings.Repeat("a", 64)
	seededSecond.namingSeed = strings.Repeat("b", 64)
	seeded, err := _encodeConflictPromotions([]_conflictPromotion{seededSecond, seededFirst})
	if err != nil || string(seeded[:4]) != "FCP2" {
		t.Fatalf("seeded provenance header=%q err=%v", seeded[:min(4, len(seeded))], err)
	}
	seededDecoded, err := _decodeConflictPromotions(seeded)
	if err != nil || len(seededDecoded) != 2 || seededDecoded[0] != seededFirst || seededDecoded[1] != seededSecond {
		t.Fatalf("seeded provenance=%+v err=%v", seededDecoded, err)
	}
	tampered := append([]byte(nil), seeded...)
	seedAt := bytes.Index(tampered, []byte(strings.Repeat("a", 64)))
	if seedAt < 0 {
		t.Fatal("FCP2 seed bytes absent")
	}
	tampered[seedAt] = 'A'
	if _, err := _decodeConflictPromotions(tampered); err == nil {
		t.Fatal("tampered FCP2 seed accepted")
	}
	mixed := seededFirst
	mixed.namingSeed = ""
	if _, err := _encodeConflictPromotions([]_conflictPromotion{mixed, seededSecond}); err == nil {
		t.Fatal("mixed FCP1/FCP2 records accepted")
	}
	paths := []checkoutPath{{path: first.target, kind: "File", id: first.id, mtime: first.mtime, size: first.size}}
	if err := _validatePromotionTargets([]_conflictPromotion{first}, paths); err != nil {
		t.Fatal(err)
	}
	paths[0].size++
	if err := _validatePromotionTargets([]_conflictPromotion{first}, paths); err == nil {
		t.Fatal("provenance target size mismatch was accepted")
	}
}

func TestConflictCopyNameBoundaries(t *testing.T) {
	seed := object.Commit{AuthorUserID: testClientUserID, DeviceID: testClientDeviceID,
		CreatedAt: "2025-02-03T04:05:06Z"}
	for name, value := range map[string][2]string{
		"240 byte segment":   {strings.Repeat("n", 240), ""},
		"241 byte segment":   {strings.Repeat("n", 241), ""},
		"multibyte boundary": {strings.Repeat("界", 80), ""},
		"reserved source":    {"CON", ""},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := _conflictCopyName(value[0], value[1], seed, nil)
			if err != nil || len(got) > 240 || !utf8.ValidString(got) || !validRecoveryVisibleName(got) {
				t.Fatalf("bounded name=%q bytes=%d error=%v", got, len(got), err)
			}
		})
	}
	exactParent := strings.Repeat("a", 240) + "/" + strings.Repeat("b", 240) + "/" +
		strings.Repeat("c", 240) + "/" + strings.Repeat("d", 240)
	exact, err := _conflictCopyName(strings.Repeat("f", 60), exactParent, seed, nil)
	if err != nil || len(exactParent)+1+len(exact) != 1024 {
		t.Fatalf("exact path bytes=%d name=%q err=%v", len(exactParent)+1+len(exact), exact, err)
	}
	if _, err := _conflictCopyName("name. ", "", seed, nil); !errors.Is(err, errConflictPathNeedsFallback) {
		t.Fatalf("trailing source did not select fallback: %v", err)
	}
	if fallback, err := _fallbackConflictName("name. ", _fallbackConflictRoot, strings.Repeat("1", 64), 1); err != nil ||
		!validRecoveryVisibleName(fallback) {
		t.Fatalf("trailing source fallback=%q err=%v", fallback, err)
	}
	parent := exactParent + "/" + strings.Repeat("a", 13)
	if _, err := _conflictCopyName("f", parent, seed, nil); !errors.Is(err, errConflictPathNeedsFallback) {
		t.Fatalf("path pressure error=%v", err)
	}
	if normalized, err := _conflictCopyName("cafe\u0301.txt", "", seed, nil); err != nil || strings.Contains(normalized, "e\u0301") {
		t.Fatalf("NFC conflict name=%q err=%v", normalized, err)
	}
}

func TestConflictCopyNameLongExtensionUsesLegalOrdinalOne(t *testing.T) {
	seed := object.Commit{AuthorUserID: testClientUserID, DeviceID: testClientDeviceID,
		CreatedAt: "2025-02-03T04:05:06Z"}
	leaf := "x." + strings.Repeat("a", 190)
	name, err := _conflictCopyName(leaf, "", seed, nil)
	want := "x (Filecloud conflict aaaaaaaa 20250203T040506Z)." + strings.Repeat("a", 190)
	if err != nil || name != want || len(name) != 239 {
		t.Fatalf("long extension name=%q bytes=%d err=%v", name, len(name), err)
	}
	current := name
	failedOrdinal := 0
	for ordinal := 2; ordinal <= _conflictMaxOrdinal; ordinal++ {
		next, nextErr := nextConflictChainPath(current)
		if nextErr != nil {
			failedOrdinal = ordinal
			break
		}
		current = next
	}
	if failedOrdinal != 10 {
		t.Fatalf("first unavailable ordinal=%d want=10", failedOrdinal)
	}
	if _, err := nextPromotionChainPath(current, "", leaf); err == nil {
		t.Fatal("legacy FCP1 normal-to-fallback transition was accepted without a seed")
	}
	fallback, err := nextPromotionChainPath(current, strings.Repeat("1", 64), leaf)
	wantFallback := "Filecloud Conflicts/111111111111-x." + strings.Repeat("a", 190) + " (Filecloud conflict 1)"
	if err != nil || fallback != wantFallback {
		t.Fatalf("runtime fallback=%q want=%q err=%v", fallback, wantFallback, err)
	}
	fallbackDescendant := "111111111111-dir (Filecloud conflict 1)/plain.txt"
	if _, err := nextPromotionChainPath(fallbackDescendant, strings.Repeat("2", 64), "plain.txt"); err == nil {
		t.Fatal("fallback family with a different FCP2 seed was accepted")
	}
}

func TestConflictNameFamiliesRetruncateOnOrdinalGrowth(t *testing.T) {
	seed := object.Commit{AuthorUserID: testClientUserID, DeviceID: testClientDeviceID,
		CreatedAt: "2025-02-03T04:05:06Z"}
	first, err := _conflictCopyName(strings.Repeat("x", 240)+".txt", "", seed, nil)
	if err != nil {
		t.Fatal(err)
	}
	current := first
	for ordinal := 2; ordinal <= 100; ordinal++ {
		current, err = nextConflictChainPath(current)
		if err != nil || len(current) > 240 || !validRecoveryVisibleName(current) {
			t.Fatalf("ordinal %d path=%q bytes=%d err=%v", ordinal, current, len(current), err)
		}
	}
	fallback := "Filecloud Conflicts/0123456789ab-" + strings.Repeat("界", 60) + " (Filecloud conflict 9)"
	fallback, err = nextConflictChainPath(fallback)
	if err != nil || !strings.HasSuffix(fallback, " (Filecloud conflict 10)") || len(filepath.Base(fallback)) > 240 {
		t.Fatalf("fallback successor=%q err=%v", fallback, err)
	}
	if err := validateRootPromotionTarget("Filecloud Conflicts/0123456789ab-name (Filecloud conflict 1)",
		"Filecloud Conflicts/0123456789ab-name (Filecloud conflict 3)"); err != nil {
		t.Fatal(err)
	}
	if err := validateRootPromotionTarget("Filecloud Conflicts/0123456789ab-name (Filecloud conflict 1)",
		"Filecloud Conflicts/ffffffffffff-name (Filecloud conflict 2)"); err == nil {
		t.Fatal("cross-seed fallback family accepted")
	}
}

func TestFallbackConflictOrdinalExhaustionDoesNotWrap(t *testing.T) {
	seed := strings.Repeat("1", 64)
	last := "Filecloud Conflicts/" + seed[:12] + "-name (Filecloud conflict 9999)"
	if _, err := nextConflictChainPath(last); err == nil {
		t.Fatal("fallback leaf ordinal wrapped after 9999")
	}
	mtime := "2026-01-01T00:00:00Z"
	fileID := strings.Repeat("2", 64)
	request := _fallbackConflict{source: "nested/name", leaf: "name",
		entry: object.DirectoryEntry{Name: "name", Type: "File", ID: fileID, ModifiedAt: mtime}}

	rootEntries := make([]scanEntry, 0, _conflictMaxOrdinal)
	for ordinal := 1; ordinal <= _conflictMaxOrdinal; ordinal++ {
		name := _fallbackConflictRoot
		if ordinal > 1 {
			name += " " + strconv.Itoa(ordinal)
		}
		rootEntries = append(rootEntries, scanEntry{name: name, kind: "File", id: fileID, modified: mtime})
	}
	sort.Slice(rootEntries, func(i, j int) bool { return rootEntries[i].name < rootEntries[j].name })
	rootData, rootID, err := canonicalDirectory("", rootEntries)
	if err != nil {
		t.Fatal(err)
	}
	rootMerger := &_treeMerger{directories: map[string][]byte{rootID: rootData}, synthesized: make(map[string][]byte),
		active: make(map[string]bool), seen: make(map[string]bool), budget: _newReplayBudget(), localSeedID: seed,
		lineage:   map[string]_conflictPromotion{"nested/name": {source: "nested/name", target: "nested/name"}},
		fallbacks: []_fallbackConflict{request}}
	if _, err := rootMerger.applyFallbacks(rootID); err == nil || !strings.Contains(err.Error(), "root collision sequence exhausted") {
		t.Fatalf("fallback root exhaustion error=%v", err)
	}

	fallbackEntries := make([]scanEntry, 0, _conflictMaxOrdinal)
	for ordinal := 1; ordinal <= _conflictMaxOrdinal; ordinal++ {
		name, err := _fallbackConflictName("name", _fallbackConflictRoot, seed, ordinal)
		if err != nil {
			t.Fatal(err)
		}
		fallbackEntries = append(fallbackEntries, scanEntry{name: name, kind: "File", id: fileID, modified: mtime})
	}
	sort.Slice(fallbackEntries, func(i, j int) bool { return fallbackEntries[i].name < fallbackEntries[j].name })
	fallbackData, fallbackID, err := canonicalDirectory(_fallbackConflictRoot, fallbackEntries)
	if err != nil {
		t.Fatal(err)
	}
	occupiedRootData, occupiedRootID, err := canonicalDirectory("", []scanEntry{{name: _fallbackConflictRoot,
		kind: "Directory", id: fallbackID, modified: mtime}})
	if err != nil {
		t.Fatal(err)
	}
	leafMerger := &_treeMerger{directories: map[string][]byte{occupiedRootID: occupiedRootData, fallbackID: fallbackData},
		synthesized: make(map[string][]byte), active: make(map[string]bool), seen: make(map[string]bool),
		budget: _newReplayBudget(), localSeedID: seed,
		lineage:   map[string]_conflictPromotion{"nested/name": {source: "nested/name", target: "nested/name"}},
		fallbacks: []_fallbackConflict{request}}
	if _, err := leafMerger.applyFallbacks(occupiedRootID); err == nil || !strings.Contains(err.Error(), "collision sequence exhausted") {
		t.Fatalf("fallback leaf exhaustion error=%v", err)
	}
}

func TestFallbackConflictRequestsUseCanonicalSourceOrder(t *testing.T) {
	emptyData, emptyID, err := canonicalEmptyDirectory()
	if err != nil {
		t.Fatal(err)
	}
	seedID := strings.Repeat("1", 64)
	makeMerger := func() *_treeMerger {
		return &_treeMerger{directories: map[string][]byte{emptyID: emptyData}, synthesized: make(map[string][]byte),
			active: make(map[string]bool), seen: make(map[string]bool), budget: _newReplayBudget(), localSeedID: seedID,
			lineage: map[string]_conflictPromotion{
				"z/f": {source: "z/f", target: "z/f"},
				"a/f": {source: "a/f", target: "a/f"},
			}, fallbacks: []_fallbackConflict{
				{source: "z/f", leaf: "f", entry: object.DirectoryEntry{Name: "f", Type: "File", ID: strings.Repeat("2", 64), ModifiedAt: "2026-01-01T00:00:00Z"}},
				{source: "a/f", leaf: "f", entry: object.DirectoryEntry{Name: "f", Type: "File", ID: strings.Repeat("3", 64), ModifiedAt: "2026-01-02T00:00:00Z"}},
			}}
	}
	first := makeMerger()
	rootID, err := first.applyFallbacks(emptyID)
	if err != nil {
		t.Fatal(err)
	}
	root, err := first.loadDirectory(rootID)
	if err != nil || len(root.Entries) != 1 || root.Entries[0].ModifiedAt != "2026-01-02T00:00:00Z" {
		t.Fatalf("fallback root=%+v err=%v", root, err)
	}
	fallback, err := first.loadDirectory(root.Entries[0].ID)
	if err != nil || len(fallback.Entries) != 2 ||
		fallback.Entries[0].Name != "111111111111-f (Filecloud conflict 1)" || fallback.Entries[0].ID != strings.Repeat("3", 64) ||
		fallback.Entries[1].Name != "111111111111-f (Filecloud conflict 2)" || fallback.Entries[1].ID != strings.Repeat("2", 64) {
		t.Fatalf("fallback entries=%+v err=%v", fallback.Entries, err)
	}
	second := makeMerger()
	replayed, err := second.applyFallbacks(emptyID)
	if err != nil || replayed != rootID {
		t.Fatalf("fallback replay root=%s/%s err=%v", rootID, replayed, err)
	}
}

func TestLibrarySyncLongConflictNamesConverge(t *testing.T) {
	longFallback := strings.Repeat("a", 240) + "/" + strings.Repeat("b", 240) + "/" +
		strings.Repeat("c", 240) + "/" + strings.Repeat("d", 240) + "/" + strings.Repeat("a", 13) + "/f"
	for _, test := range []struct {
		name, relative, existingRoot, existingKind, wantRoot string
		fallback                                             bool
	}{
		{name: "240 byte segment", relative: strings.Repeat("n", 240)},
		{name: "root fallback absent", relative: longFallback, fallback: true, wantRoot: "Filecloud Conflicts"},
		{name: "root fallback exact directory reuse", relative: longFallback, fallback: true,
			existingRoot: "Filecloud Conflicts", existingKind: "Directory", wantRoot: "Filecloud Conflicts"},
		{name: "root fallback exact file collision", relative: longFallback, fallback: true,
			existingRoot: "Filecloud Conflicts", existingKind: "File", wantRoot: "Filecloud Conflicts 2"},
		{name: "root fallback casefold alias", relative: longFallback, fallback: true,
			existingRoot: "FILECLOUD CONFLICTS", existingKind: "Directory", wantRoot: "Filecloud Conflicts 2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, commits := newSyncPair(t)
			if test.existingRoot != "" {
				existing := filepath.Join(publisherTree, test.existingRoot)
				if test.existingKind == "Directory" {
					if err := os.Mkdir(existing, 0o700); err != nil {
						t.Fatal(err)
					}
					existing = filepath.Join(existing, "preserved")
				}
				if err := os.WriteFile(existing, []byte("preserved"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			publisherPath := filepath.Join(publisherTree, filepath.FromSlash(test.relative))
			if err := os.MkdirAll(filepath.Dir(publisherPath), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(publisherPath, []byte("base"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(publisherPath, []byte("remote"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			subscriberPath := filepath.Join(subscriberTree, filepath.FromSlash(test.relative))
			if err := os.WriteFile(subscriberPath, []byte("local"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
				t.Fatal(err)
			}
			pattern := filepath.Join(filepath.Dir(subscriberPath), "* (Filecloud conflict *)")
			if test.fallback {
				pattern = filepath.Join(subscriberTree, test.wantRoot, "* (Filecloud conflict *)")
			}
			matches, err := filepath.Glob(pattern)
			remote, remoteErr := os.ReadFile(subscriberPath)
			if err != nil || len(matches) != 1 || remoteErr != nil || string(remote) != "remote" {
				t.Fatalf("matches=%v glob=%v remote=%q/%v", matches, err, remote, remoteErr)
			}
			local, err := os.ReadFile(matches[0])
			if err != nil || string(local) != "local" || len(filepath.Base(matches[0])) > 240 {
				t.Fatalf("local conflict=%q/%v path=%q", local, err, matches[0])
			}
			if test.existingRoot != "" {
				existing := filepath.Join(subscriberTree, test.existingRoot)
				if test.existingKind == "Directory" {
					existing = filepath.Join(existing, "preserved")
				}
				if preserved, err := os.ReadFile(existing); err != nil || string(preserved) != "preserved" {
					t.Fatalf("existing fallback collision changed=%q/%v", preserved, err)
				}
			}
			commits.Store(0)
			if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil || commits.Load() != 0 {
				t.Fatalf("repeated sync error=%v commits=%d", err, commits.Load())
			}
			assertTestConverged(t, environment, subscriberDir, subscriberTree)
		})
	}
}

func TestLibrarySyncLongDirectoryConflictUsesRootFallback(t *testing.T) {
	environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, commits := newSyncPair(t)
	relative := strings.Repeat("a", 240) + "/" + strings.Repeat("b", 240) + "/" +
		strings.Repeat("c", 240) + "/" + strings.Repeat("d", 240) + "/" + strings.Repeat("a", 13) + "/f"
	publisherPath := filepath.Join(publisherTree, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Join(publisherPath, "nested", "empty"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(publisherPath, "nested", "local.txt"), []byte("base"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(publisherPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(publisherPath, []byte("remote file"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := syncTestWorktree(t, publisherDir, publisherTree)
	var required *deleteConfirmationRequiredError
	if errors.As(err, &required) {
		err = confirmTestDeletion(t, publisherDir, publisherTree, required.candidate[:deleteCandidatePrefixLen], libraryClientConfig{})
	}
	if err != nil {
		t.Fatal(err)
	}
	subscriberPath := filepath.Join(subscriberTree, filepath.FromSlash(relative))
	if err := os.WriteFile(filepath.Join(subscriberPath, "nested", "local.txt"), []byte("local directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	err = syncTestWorktree(t, subscriberDir, subscriberTree)
	required = nil
	if errors.As(err, &required) {
		err = confirmTestDeletion(t, subscriberDir, subscriberTree, required.candidate[:deleteCandidatePrefixLen], libraryClientConfig{})
	}
	if err != nil {
		t.Fatal(err)
	}
	matches, err := filepath.Glob(filepath.Join(subscriberTree, "Filecloud Conflicts", "* (Filecloud conflict *)"))
	remote, remoteErr := os.ReadFile(subscriberPath)
	if err != nil || len(matches) != 1 || remoteErr != nil || string(remote) != "remote file" {
		t.Fatalf("fallback directories=%v glob=%v remote=%q/%v", matches, err, remote, remoteErr)
	}
	local, localErr := os.ReadFile(filepath.Join(matches[0], "nested", "local.txt"))
	empty, emptyErr := os.Stat(filepath.Join(matches[0], "nested", "empty"))
	if localErr != nil || string(local) != "local directory" || emptyErr != nil || !empty.IsDir() {
		t.Fatalf("fallback subtree local=%q/%v empty=%v/%v", local, localErr, empty, emptyErr)
	}
	commits.Store(0)
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil || commits.Load() != 0 {
		t.Fatalf("repeated directory fallback sync error=%v commits=%d", err, commits.Load())
	}
	assertTestConverged(t, environment, subscriberDir, subscriberTree)
}

func TestLibrarySyncBothCreatedDivergentFileCreatesConflictCopy(t *testing.T) {
	environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	if err := os.WriteFile(filepath.Join(publisherTree, "created"), []byte("remote-created"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "created"), []byte("local-created"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	matches, err := filepath.Glob(filepath.Join(subscriberTree, "created (Filecloud conflict *)"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("conflict copies=%v err=%v", matches, err)
	}
	remote, remoteErr := os.ReadFile(filepath.Join(subscriberTree, "created"))
	local, localErr := os.ReadFile(matches[0])
	if remoteErr != nil || localErr != nil || string(remote) != "remote-created" || string(local) != "local-created" {
		t.Fatalf("remote=%q/%v local=%q/%v", remote, remoteErr, local, localErr)
	}
	assertTestConverged(t, environment, subscriberDir, subscriberTree)
}

func TestLibrarySyncMultipleDivergentFilesInOneRecovery(t *testing.T) {
	environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	for _, root := range []string{publisherTree, subscriberTree} {
		if err := os.MkdirAll(filepath.Join(root, "nested", "deep"), 0o700); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"first.txt", "second.txt"} {
			if err := os.WriteFile(filepath.Join(root, "nested", "deep", name), []byte("base-"+name), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"first.txt", "second.txt"} {
		if err := os.WriteFile(filepath.Join(publisherTree, "nested", "deep", name), []byte("remote-"+name), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(subscriberTree, "nested", "deep", name), []byte("local-"+name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"first.txt", "second.txt"} {
		remote, err := os.ReadFile(filepath.Join(subscriberTree, "nested", "deep", name))
		stem := strings.TrimSuffix(name, filepath.Ext(name))
		matches, globErr := filepath.Glob(filepath.Join(subscriberTree, "nested", "deep",
			stem+" (Filecloud conflict *)"+filepath.Ext(name)))
		if err != nil || globErr != nil || len(matches) != 1 || string(remote) != "remote-"+name {
			t.Fatalf("%s remote=%q/%v conflicts=%v/%v", name, remote, err, matches, globErr)
		}
		local, err := os.ReadFile(matches[0])
		if err != nil || string(local) != "local-"+name {
			t.Fatalf("%s local=%q err=%v", name, local, err)
		}
	}
	assertNoSyncInternalPaths(t, subscriberTree)
	assertTestConverged(t, environment, subscriberDir, subscriberTree)
}

func TestLibrarySyncSecondPromotionCrashMatrix(t *testing.T) {
	for _, point := range []string{"before_intent_commit", "after_intent_commit", "after_action", "after_parent_sync", "after_completed"} {
		t.Run(point, func(t *testing.T) {
			environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
			for _, root := range []string{publisherTree, subscriberTree} {
				if err := os.MkdirAll(filepath.Join(root, "nested", "deep"), 0o700); err != nil {
					t.Fatal(err)
				}
				for _, name := range []string{"first", "second"} {
					if err := os.WriteFile(filepath.Join(root, "nested", "deep", name), []byte("base-"+name), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"first", "second"} {
				if err := os.WriteFile(filepath.Join(publisherTree, "nested", "deep", name), []byte("remote-"+name), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(subscriberTree, "nested", "deep", name), []byte("local-"+name), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
			command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1",
				"FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree,
				"FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase, "FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename,
				"FILECLOUD_PUBLIC_CRASH_KIND=File", "FILECLOUD_PUBLIC_CRASH_POINT="+point,
				"FILECLOUD_PUBLIC_CRASH_ROLE=promotion", "FILECLOUD_PUBLIC_CRASH_MATCH_INDEX=2")
			assertProcessSIGKILL(t, command.Run())
			db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), true)
			if err != nil {
				t.Fatal(err)
			}
			var completedRoots int
			if err := db.QueryRow(`SELECT COUNT(*) FROM fs_actions WHERE worktree=? AND origin_action_id IS NULL
				AND expected_object<>'' AND state='completed'`, subscriberTree).Scan(&completedRoots); err != nil {
				db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if completedRoots < 1 {
				t.Fatal("second promotion crashed before the first root completed")
			}
			for attempt := 0; attempt < 4; attempt++ {
				err = syncTestWorktree(t, subscriberDir, subscriberTree)
				if err == nil {
					break
				}
				if !strings.Contains(err.Error(), "rerun sync") {
					t.Fatalf("restart %d: %v", attempt, err)
				}
			}
			if err != nil {
				t.Fatalf("second promotion did not converge: %v", err)
			}
			for _, name := range []string{"first", "second"} {
				remote, readErr := os.ReadFile(filepath.Join(subscriberTree, "nested", "deep", name))
				matches, globErr := filepath.Glob(filepath.Join(subscriberTree, "nested", "deep", name+" (Filecloud conflict *)"))
				if readErr != nil || globErr != nil || len(matches) != 1 || string(remote) != "remote-"+name {
					t.Fatalf("%s remote=%q/%v conflicts=%v/%v", name, remote, readErr, matches, globErr)
				}
				local, readErr := os.ReadFile(matches[0])
				if readErr != nil || string(local) != "local-"+name {
					t.Fatalf("%s local=%q err=%v", name, local, readErr)
				}
			}
			assertNoSyncInternalPaths(t, subscriberTree)
			assertTestConverged(t, environment, subscriberDir, subscriberTree)
		})
	}
}

func TestLibrarySyncStructuralDirectoryConflictCrashMatrix(t *testing.T) {
	for _, point := range []string{"before_intent_commit", "after_intent_commit", "after_action", "after_parent_sync", "after_completed"} {
		t.Run(point, func(t *testing.T) {
			environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
			for index := 0; index < 30; index++ {
				if err := os.WriteFile(filepath.Join(publisherTree, fmt.Sprintf("ballast-%02d", index)), []byte("base"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.MkdirAll(filepath.Join(publisherTree, "item", "nested"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(publisherTree, "item", "nested", "local.txt"), []byte("base"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(filepath.Join(publisherTree, "item")); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(subscriberTree, "item", "nested", "local.txt"), []byte("local"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(subscriberTree, "item", "empty"), 0o700); err != nil {
				t.Fatal(err)
			}
			command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
			command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1",
				"FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree,
				"FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase, "FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename,
				"FILECLOUD_PUBLIC_CRASH_KIND=File", "FILECLOUD_PUBLIC_CRASH_POINT="+point,
				"FILECLOUD_PUBLIC_CRASH_ROLE=promotion")
			assertProcessSIGKILL(t, command.Run())
			var err error
			for attempt := 0; attempt < 4; attempt++ {
				err = syncTestWorktree(t, subscriberDir, subscriberTree)
				if err == nil {
					break
				}
				if !strings.Contains(err.Error(), "rerun sync") {
					t.Fatalf("restart %d: %v", attempt, err)
				}
			}
			if err != nil {
				t.Fatalf("structural conflict did not converge: %v", err)
			}
			matches, err := filepath.Glob(filepath.Join(subscriberTree, "item (Filecloud conflict *)"))
			if err != nil || len(matches) != 1 {
				t.Fatalf("conflict directories=%v err=%v", matches, err)
			}
			if data, err := os.ReadFile(filepath.Join(matches[0], "nested", "local.txt")); err != nil || string(data) != "local" {
				t.Fatalf("local descendant=%q err=%v", data, err)
			}
			if info, err := os.Stat(filepath.Join(matches[0], "empty")); err != nil || !info.IsDir() {
				t.Fatalf("empty descendant=%v err=%v", info, err)
			}
			if _, err := os.Lstat(filepath.Join(subscriberTree, "item")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("remote deletion was not preserved: %v", err)
			}
			assertNoSyncInternalPaths(t, subscriberTree)
			assertTestConverged(t, environment, subscriberDir, subscriberTree)
		})
	}
}

func TestLibrarySyncDivergentFileCreatesConflictCopy(t *testing.T) {
	environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "base"), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	matches, err := filepath.Glob(filepath.Join(subscriberTree, "base (Filecloud conflict *)"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("conflict copies=%v err=%v", matches, err)
	}
	remote, remoteErr := os.ReadFile(filepath.Join(subscriberTree, "base"))
	local, localErr := os.ReadFile(matches[0])
	if remoteErr != nil || localErr != nil || string(remote) != "remote" || string(local) != "local" {
		t.Fatalf("remote=%q/%v local=%q/%v", remote, remoteErr, local, localErr)
	}
	head, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	binding := readTestBinding(t, subscriberDir, subscriberTree)
	if err != nil || head.CommitID == nil || binding.SyncBase != *head.CommitID || scanTestRoot(t, subscriberTree) != binding.SyncBaseRoot {
		t.Fatalf("did not converge: head=%+v binding=%+v err=%v", head, binding, err)
	}
}

func TestLibrarySyncConflictPromotionPreservesOpenFileIdentity(t *testing.T) {
	for _, relative := range []string{"base", "nested/base"} {
		t.Run(relative, func(t *testing.T) {
			_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
			if strings.Contains(relative, "/") {
				for _, root := range []string{publisherTree, subscriberTree} {
					if err := os.Mkdir(filepath.Join(root, "nested"), 0o700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(root, relative), []byte("base"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
					t.Fatal(err)
				}
				if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(publisherTree, relative), []byte("remote"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(subscriberTree, relative), []byte("local"), 0o600); err != nil {
				t.Fatal(err)
			}
			held, err := os.OpenFile(filepath.Join(subscriberTree, relative), os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
				t.Fatal(err)
			}
			parent, leaf := filepath.Split(filepath.Join(subscriberTree, relative))
			stem := strings.TrimSuffix(leaf, filepath.Ext(leaf))
			matches, err := filepath.Glob(filepath.Join(parent, stem+" (Filecloud conflict *)"+filepath.Ext(leaf)))
			if err != nil || len(matches) != 1 {
				t.Fatalf("conflict copies=%v err=%v", matches, err)
			}
			if _, err := held.WriteAt([]byte("after"), 0); err != nil {
				t.Fatal(err)
			}
			if err := held.Sync(); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(matches[0])
			if err != nil || !strings.HasPrefix(string(data), "after") {
				t.Fatalf("promoted inode=%q err=%v", data, err)
			}
			assertNoSyncInternalPaths(t, subscriberTree)
		})
	}
}

func TestLibrarySyncPromotionCrashMatrix(t *testing.T) {
	for _, relative := range []string{"base", "nested/base"} {
		for _, point := range []string{"before_intent_commit", "after_intent_commit", "after_action", "after_parent_sync", "after_completed"} {
			t.Run(relative+"/"+point, func(t *testing.T) {
				environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
				if strings.Contains(relative, "/") {
					for _, root := range []string{publisherTree, subscriberTree} {
						if err := os.Mkdir(filepath.Join(root, "nested"), 0o700); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(filepath.Join(root, relative), []byte("base"), 0o600); err != nil {
							t.Fatal(err)
						}
					}
					if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
						t.Fatal(err)
					}
					if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.WriteFile(filepath.Join(publisherTree, relative), []byte("remote"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(subscriberTree, relative), []byte("local"), 0o600); err != nil {
					t.Fatal(err)
				}
				var held *os.File
				if point == "after_completed" {
					var openErr error
					held, openErr = os.OpenFile(filepath.Join(subscriberTree, relative), os.O_RDWR, 0)
					if openErr != nil {
						t.Fatal(openErr)
					}
					defer held.Close()
				}
				command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
				command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1",
					"FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree,
					"FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase, "FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename,
					"FILECLOUD_PUBLIC_CRASH_KIND=File", "FILECLOUD_PUBLIC_CRASH_POINT="+point,
					"FILECLOUD_PUBLIC_CRASH_ROLE=promotion")
				assertProcessSIGKILL(t, command.Run())
				if held != nil {
					if _, err := held.WriteAt([]byte("late!"), 0); err != nil {
						t.Fatal(err)
					}
					if err := held.Sync(); err != nil {
						t.Fatal(err)
					}
				}
				restartErr := syncTestWorktree(t, subscriberDir, subscriberTree)
				if held == nil && restartErr != nil {
					t.Fatalf("promotion restart: %v", restartErr)
				}
				if held != nil && (restartErr == nil || !strings.Contains(restartErr.Error(), "rerun sync")) {
					t.Fatalf("late Completed promotion restart error=%v", restartErr)
				}
				if held != nil {
					if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
						t.Fatal(err)
					}
				}
				parent, leaf := filepath.Split(filepath.Join(subscriberTree, relative))
				matches, err := filepath.Glob(filepath.Join(parent,
					strings.TrimSuffix(leaf, filepath.Ext(leaf))+" (Filecloud conflict *)*"+filepath.Ext(leaf)))
				wantMatches := 1
				if held != nil {
					wantMatches = 2
				}
				if err != nil || len(matches) != wantMatches {
					t.Fatalf("conflict copies=%v err=%v", matches, err)
				}
				contents := make(map[string]bool, len(matches))
				for _, match := range matches {
					data, err := os.ReadFile(match)
					if err != nil {
						t.Fatal(err)
					}
					contents[string(data)] = true
				}
				if !contents["local"] || (held != nil && !contents["late!"]) {
					t.Fatalf("promoted contents=%v", contents)
				}
				assertTestConverged(t, environment, subscriberDir, subscriberTree)
				assertNoSyncInternalPaths(t, subscriberTree)
			})
		}
	}
}

func TestLibrarySyncMutationBeforeFirstPromotionCrashMatrix(t *testing.T) {
	for _, relative := range []string{"base", "nested/base"} {
		for _, point := range []string{"before_intent_commit", "after_intent_commit", "after_action", "after_parent_sync", "after_completed"} {
			t.Run(relative+"/"+point, func(t *testing.T) {
				environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
				if strings.Contains(relative, "/") {
					for _, root := range []string{publisherTree, subscriberTree} {
						if err := os.Mkdir(filepath.Join(root, "nested"), 0o700); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(relative)), []byte("base"), 0o600); err != nil {
							t.Fatal(err)
						}
					}
					if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
						t.Fatal(err)
					}
					if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.WriteFile(filepath.Join(publisherTree, filepath.FromSlash(relative)), []byte("remote"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(subscriberTree, filepath.FromSlash(relative))
				if err := os.WriteFile(path, []byte("local"), 0o600); err != nil {
					t.Fatal(err)
				}
				held, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer held.Close()
				beforeInfo, err := held.Stat()
				if err != nil {
					t.Fatal(err)
				}
				before, ok := beforeInfo.Sys().(*syscall.Stat_t)
				if !ok {
					t.Fatal("read held conflict identity")
				}
				command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
				command.ExtraFiles = []*os.File{held}
				command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1",
					"FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree,
					"FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase, "FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename,
					"FILECLOUD_PUBLIC_CRASH_KIND=File", "FILECLOUD_PUBLIC_CRASH_POINT="+point,
					"FILECLOUD_PUBLIC_CRASH_ROLE=pre-promotion-mutation", "FILECLOUD_PUBLIC_MUTATION_PATH="+relative)
				if !strings.Contains(relative, "/") {
					command.Env = append(command.Env, "FILECLOUD_PUBLIC_CRASH_COLLISION=1")
				}
				assertProcessSIGKILL(t, command.Run())

				db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), true)
				if err != nil {
					t.Fatal(err)
				}
				promotions, err := loadConflictPromotions(t.Context(), db, subscriberTree)
				if err != nil || len(promotions) != 1 {
					db.Close()
					t.Fatalf("promotions=%+v err=%v", promotions, err)
				}
				expected := promotions[0].target
				wantTarget, err := nextConflictChainPath(expected)
				selectedTarget := wantTarget
				if !strings.Contains(relative, "/") && err == nil {
					selectedTarget, err = nextConflictChainPath(wantTarget)
					if point != "before_intent_commit" {
						wantTarget = selectedTarget
					}
				}
				if err != nil {
					db.Close()
					t.Fatal(err)
				}
				var recorded string
				recordErr := db.QueryRow(`SELECT target_name FROM fs_actions WHERE worktree=? AND op=? AND expected_object<>'' AND origin_action_id IS NULL`,
					subscriberTree, fsOpRename).Scan(&recorded)
				if point == "before_intent_commit" {
					if !errors.Is(recordErr, sql.ErrNoRows) {
						db.Close()
						t.Fatalf("uncommitted root target=%q err=%v", recorded, recordErr)
					}
				} else if recordErr != nil || recorded != selectedTarget {
					db.Close()
					t.Fatalf("recorded root target=%q err=%v want=%q", recorded, recordErr, selectedTarget)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(relative, "/") {
					second, _ := nextConflictChainPath(expected)
					parent, leaf := splitFSActionPath(second)
					collision := filepath.Join(subscriberTree, filepath.FromSlash(parent), strings.ToUpper(leaf))
					if occupied, err := os.ReadFile(collision); err != nil || string(occupied) != "occupied" {
						t.Fatalf("casefold collision=%q err=%v", occupied, err)
					}
					if err := os.Remove(collision); err != nil {
						t.Fatal(err)
					}
				}

				restartErr := syncTestWorktree(t, subscriberDir, subscriberTree)
				if restartErr == nil || !strings.Contains(restartErr.Error(), "rerun sync") {
					t.Fatalf("mutation restart error=%v", restartErr)
				}
				mutated, err := os.Open(filepath.Join(subscriberTree, filepath.FromSlash(wantTarget)))
				if err != nil {
					t.Fatal(err)
				}
				afterInfo, statErr := mutated.Stat()
				if statErr != nil {
					mutated.Close()
					t.Fatal(statErr)
				}
				after, ok := afterInfo.Sys().(*syscall.Stat_t)
				data, readErr := io.ReadAll(mutated)
				closeErr := mutated.Close()
				if !ok || readErr != nil || closeErr != nil || after.Dev != before.Dev || after.Ino != before.Ino || string(data) != "changed" {
					t.Fatalf("mutated target identity=%v content=%q err=%v", ok && after.Dev == before.Dev && after.Ino == before.Ino, data, errors.Join(readErr, closeErr))
				}
				if fixed, err := os.ReadFile(filepath.Join(subscriberTree, filepath.FromSlash(expected))); err != nil || string(fixed) != "local" {
					t.Fatalf("fixed Candidate=%q err=%v", fixed, err)
				}
				assertNoSyncInternalPaths(t, subscriberTree)
				if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
					t.Fatal(err)
				}
				assertTestConverged(t, environment, subscriberDir, subscriberTree)
			})
		}
	}
}

func TestLibrarySyncLatePromotionCrashMatrix(t *testing.T) {
	for _, relative := range []string{"base", "nested/base"} {
		for _, point := range []string{"before_intent_commit", "after_intent_commit", "after_action", "after_parent_sync", "after_completed"} {
			t.Run(relative+"/"+point, func(t *testing.T) {
				environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
				if strings.Contains(relative, "/") {
					for _, root := range []string{publisherTree, subscriberTree} {
						if err := os.Mkdir(filepath.Join(root, "nested"), 0o700); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(filepath.Join(root, relative), []byte("base"), 0o600); err != nil {
							t.Fatal(err)
						}
					}
					if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
						t.Fatal(err)
					}
					if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.WriteFile(filepath.Join(publisherTree, relative), []byte("remote"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(subscriberTree, relative), []byte("local"), 0o600); err != nil {
					t.Fatal(err)
				}
				held, err := os.OpenFile(filepath.Join(subscriberTree, relative), os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer held.Close()
				setup := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
				setup.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1",
					"FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree,
					"FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase, "FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename,
					"FILECLOUD_PUBLIC_CRASH_KIND=File", "FILECLOUD_PUBLIC_CRASH_POINT=after_completed",
					"FILECLOUD_PUBLIC_CRASH_ROLE=promotion")
				assertProcessSIGKILL(t, setup.Run())
				parent, leaf := filepath.Split(filepath.Join(subscriberTree, relative))
				matches, err := filepath.Glob(filepath.Join(parent,
					strings.TrimSuffix(leaf, filepath.Ext(leaf))+" (Filecloud conflict *)"+filepath.Ext(leaf)))
				if err != nil || len(matches) != 1 {
					t.Fatalf("initial conflict copies=%v err=%v", matches, err)
				}
				promoted, err := filepath.Rel(subscriberTree, matches[0])
				if err != nil {
					t.Fatal(err)
				}
				promoted = filepath.ToSlash(promoted)
				successor, err := nextConflictChainPath(promoted)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := held.WriteAt([]byte("late!"), 0); err != nil {
					t.Fatal(err)
				}
				if err := held.Sync(); err != nil {
					t.Fatal(err)
				}
				command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
				command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1",
					"FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree,
					"FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase, "FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename,
					"FILECLOUD_PUBLIC_CRASH_KIND=File", "FILECLOUD_PUBLIC_CRASH_POINT="+point,
					"FILECLOUD_PUBLIC_CRASH_ROLE=late-promotion")
				assertProcessSIGKILL(t, command.Run())
				wantSuccessor, wantLate := successor, "late!"
				if point == "after_completed" {
					if _, err := held.WriteAt([]byte("again"), 0); err != nil {
						t.Fatal(err)
					}
					if err := held.Sync(); err != nil {
						t.Fatal(err)
					}
					wantSuccessor, err = nextConflictChainPath(successor)
					if err != nil {
						t.Fatal(err)
					}
					wantLate = "again"
				}
				for attempt := 0; attempt < 4; attempt++ {
					err = syncTestWorktree(t, subscriberDir, subscriberTree)
					if err == nil {
						break
					}
					if !strings.Contains(err.Error(), "rerun sync") {
						t.Fatalf("late promotion restart %d: %v", attempt, err)
					}
				}
				if err != nil {
					t.Fatalf("late promotion did not converge: %v", err)
				}
				fixed, fixedErr := os.ReadFile(filepath.Join(subscriberTree, filepath.FromSlash(promoted)))
				late, lateErr := os.ReadFile(filepath.Join(subscriberTree, filepath.FromSlash(wantSuccessor)))
				if fixedErr != nil || lateErr != nil || string(fixed) != "local" || string(late) != wantLate {
					t.Fatalf("fixed=%q/%v late=%q/%v", fixed, fixedErr, late, lateErr)
				}
				assertNoSyncInternalPaths(t, subscriberTree)
				assertTestConverged(t, environment, subscriberDir, subscriberTree)
			})
		}
	}
}

func TestLibrarySyncLatePromotionPushesContiguousOccupiedChain(t *testing.T) {
	for _, test := range []struct {
		name, relative         string
		occupied               int
		crashFallback          bool
		fallbackRoot, rootKind string
		wantFallbackRoot       string
		crashPoint             string
		rootRaceAlias          bool
	}{
		{name: "ordinal-2-and-3", relative: "base", occupied: 2},
		{name: "normal-to-fallback-before-intent", relative: "x." + strings.Repeat("a", 190), occupied: 8, crashFallback: true,
			wantFallbackRoot: "Filecloud Conflicts", crashPoint: "before_intent_commit"},
		{name: "normal-to-fallback-after-intent", relative: "x." + strings.Repeat("a", 190), occupied: 8, crashFallback: true,
			wantFallbackRoot: "Filecloud Conflicts", crashPoint: "after_intent_commit"},
		{name: "normal-to-fallback-after-rename", relative: "x." + strings.Repeat("a", 190), occupied: 8, crashFallback: true,
			wantFallbackRoot: "Filecloud Conflicts", crashPoint: "after_action"},
		{name: "normal-to-fallback-after-parent-sync", relative: "x." + strings.Repeat("a", 190), occupied: 8, crashFallback: true,
			wantFallbackRoot: "Filecloud Conflicts", crashPoint: "after_parent_sync"},
		{name: "normal-to-fallback-after-completion", relative: "x." + strings.Repeat("a", 190), occupied: 8, crashFallback: true,
			wantFallbackRoot: "Filecloud Conflicts", crashPoint: "after_completed"},
		{name: "fallback exact directory reuse", relative: "x." + strings.Repeat("a", 190), occupied: 8, crashFallback: true,
			fallbackRoot: "Filecloud Conflicts", rootKind: "Directory", wantFallbackRoot: "Filecloud Conflicts"},
		{name: "fallback exact file advances", relative: "x." + strings.Repeat("a", 190), occupied: 8, crashFallback: true,
			fallbackRoot: "Filecloud Conflicts", rootKind: "File", wantFallbackRoot: "Filecloud Conflicts 2"},
		{name: "fallback casefold alias advances", relative: "x." + strings.Repeat("a", 190), occupied: 8, crashFallback: true,
			fallbackRoot: "FILECLOUD CONFLICTS", rootKind: "Directory", wantFallbackRoot: "Filecloud Conflicts 2"},
		{name: "fallback casefold race advances", relative: "x." + strings.Repeat("a", 190), occupied: 8, crashFallback: true,
			wantFallbackRoot: "Filecloud Conflicts 2", rootRaceAlias: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
			publisherPath := filepath.Join(publisherTree, filepath.FromSlash(test.relative))
			subscriberPath := filepath.Join(subscriberTree, filepath.FromSlash(test.relative))
			if err := os.WriteFile(publisherPath, []byte("base"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(publisherPath, []byte("remote"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(subscriberPath, []byte("local"), 0o600); err != nil {
				t.Fatal(err)
			}
			held, err := os.OpenFile(subscriberPath, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			var stop atomic.Bool
			err = runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
				strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
					fsActionFault: func(point string, action fsAction) error {
						if point == "after_completed" && action.ExpectedObject != "" && action.OriginActionID == "" && stop.CompareAndSwap(false, true) {
							return errors.New("hold completed promotion")
						}
						return nil
					}})
			if err == nil || !strings.Contains(err.Error(), "hold completed promotion") {
				t.Fatalf("initial promotion interruption=%v", err)
			}
			if test.fallbackRoot != "" {
				path := filepath.Join(subscriberTree, test.fallbackRoot)
				if test.rootKind == "Directory" {
					if err := os.Mkdir(path, 0o700); err != nil {
						t.Fatal(err)
					}
				} else if err := os.WriteFile(path, []byte("root collision"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			promotions, loadErr := loadConflictPromotions(t.Context(), db, subscriberTree)
			closeErr := db.Close()
			if loadErr != nil || closeErr != nil || len(promotions) != 1 || promotions[0].source != test.relative {
				t.Fatalf("promotion provenance=%+v load=%v close=%v", promotions, loadErr, closeErr)
			}
			promotion := promotions[0]
			chain := []string{promotion.target}
			for index := 0; index <= test.occupied; index++ {
				next, err := nextPromotionChainPath(chain[len(chain)-1], promotion.namingSeed, promotion.source)
				if err != nil {
					t.Fatal(err)
				}
				chain = append(chain, next)
			}
			if test.crashFallback && !strings.HasPrefix(filepath.Base(chain[len(chain)-1]), promotion.namingSeed[:12]+"-") {
				t.Fatalf("normal chain did not transition to fallback: %q", chain[len(chain)-1])
			}
			for index := 1; index <= test.occupied; index++ {
				if err := os.WriteFile(filepath.Join(subscriberTree, filepath.FromSlash(chain[index])),
					[]byte(fmt.Sprintf("occupied-%d", index+1)), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := held.WriteAt([]byte("late!"), 0); err != nil {
				t.Fatal(err)
			}
			if err := held.Sync(); err != nil {
				t.Fatal(err)
			}
			if test.crashFallback {
				crashPoint := test.crashPoint
				if crashPoint == "" {
					crashPoint = "after_intent_commit"
				}
				rootCreateCrash := test.fallbackRoot == "" && !test.rootRaceAlias
				var interrupted, raced atomic.Bool
				if rootCreateCrash {
					command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
					command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1",
						"FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree,
						"FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase, "FILECLOUD_PUBLIC_CRASH_OP="+fsOpCreateDirectory,
						"FILECLOUD_PUBLIC_CRASH_KIND=Directory", "FILECLOUD_PUBLIC_CRASH_POINT="+crashPoint,
						"FILECLOUD_PUBLIC_CRASH_ROLE=fallback-root-create")
					assertProcessSIGKILL(t, command.Run())
				} else {
					err = runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
						strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
							fsActionFault: func(point string, action fsAction) error {
								if test.rootRaceAlias && point == "before_action" && action.Op == fsOpCreateDirectory &&
									strings.HasPrefix(action.InternalTarget, fsPromotionFallbackOwnerPrefix) && raced.CompareAndSwap(false, true) {
									if err := os.Mkdir(filepath.Join(subscriberTree, strings.ToUpper(action.Source)), 0o700); err != nil {
										return err
									}
								}
								parent, _ := splitFSActionPath(action.Target)
								if point == crashPoint && validFallbackRootName(parent) && interrupted.CompareAndSwap(false, true) {
									return errors.New("interrupt normal-to-fallback")
								}
								return nil
							}})
					if err == nil || !strings.Contains(err.Error(), "interrupt normal-to-fallback") || !interrupted.Load() {
						t.Fatalf("normal-to-fallback interruption=%v interrupted=%v", err, interrupted.Load())
					}
				}
				db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), true)
				if err != nil {
					t.Fatal(err)
				}
				var actualFallback string
				err = db.QueryRow(`SELECT target_name FROM fs_actions WHERE worktree=? AND origin_action_id<>''
					AND target_name LIKE 'Filecloud Conflicts%' ORDER BY attempt DESC LIMIT 1`, subscriberTree).Scan(&actualFallback)
				if errors.Is(err, sql.ErrNoRows) && (rootCreateCrash || crashPoint == "before_intent_commit") {
					_, originalLeaf := splitFSActionPath(promotion.source)
					leaf, nameErr := _fallbackConflictName(originalLeaf, test.wantFallbackRoot, promotion.namingSeed, 1)
					if nameErr != nil {
						db.Close()
						t.Fatal(nameErr)
					}
					actualFallback = test.wantFallbackRoot + "/" + leaf
					err = nil
				}
				if err != nil {
					db.Close()
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				chain[len(chain)-1] = actualFallback
				parent, _ := splitFSActionPath(actualFallback)
				if parent != test.wantFallbackRoot {
					t.Fatalf("runtime fallback root=%q want=%q", parent, test.wantFallbackRoot)
				}
			}
			for attempt := 0; attempt < 12; attempt++ {
				err = syncTestWorktree(t, subscriberDir, subscriberTree)
				if err == nil {
					break
				}
				if !strings.Contains(err.Error(), "rerun sync") {
					t.Fatalf("strict chain restart %d: %v", attempt, err)
				}
			}
			if err != nil {
				t.Fatalf("strict chain did not converge: %v", err)
			}
			want := append([]string{"local", "late!"}, make([]string, test.occupied)...)
			for index := 2; index < len(want); index++ {
				want[index] = fmt.Sprintf("occupied-%d", index)
			}
			for index, path := range chain {
				if data, err := os.ReadFile(filepath.Join(subscriberTree, filepath.FromSlash(path))); err != nil || string(data) != want[index] {
					t.Fatalf("%q=%q/%v want=%q", path, data, err, want[index])
				}
			}
		})
	}
}

func TestLibraryRejectsOrphanPromotionBeforeReplay(t *testing.T) {
	for _, operation := range []string{"recover", "sync", "unbind"} {
		t.Run(operation, func(t *testing.T) {
			_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
			if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("remote"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(subscriberTree, "base"), []byte("local"), 0o600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
			command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1",
				"FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree,
				"FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase, "FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename,
				"FILECLOUD_PUBLIC_CRASH_KIND=File", "FILECLOUD_PUBLIC_CRASH_POINT=after_intent_commit",
				"FILECLOUD_PUBLIC_CRASH_ROLE=promotion")
			assertProcessSIGKILL(t, command.Run())

			beforeBinding := readTestBinding(t, subscriberDir, subscriberTree)
			db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			actions, err := loadFSActions(t.Context(), db, subscriberTree)
			if err != nil {
				db.Close()
				t.Fatal(err)
			}
			var legitimate fsAction
			for _, action := range actions {
				if action.OriginActionID == "" && action.Op == fsOpRename && action.ExpectedObject != "" {
					legitimate = action
					break
				}
			}
			if legitimate.ActionID == "" || legitimate.State != fsStateIntent {
				db.Close()
				t.Fatalf("legitimate promotion=%+v", legitimate)
			}
			forgedPath := filepath.Join(subscriberTree, "forged")
			if err := os.WriteFile(forgedPath, []byte("forged"), 0o600); err != nil {
				db.Close()
				t.Fatal(err)
			}
			held, err := os.Open(forgedPath)
			if err != nil {
				db.Close()
				t.Fatal(err)
			}
			defer held.Close()
			info, err := held.Stat()
			if err != nil {
				db.Close()
				t.Fatal(err)
			}
			stat := info.Sys().(*syscall.Stat_t)
			snapshot := worktreeSnapshot{blocks: make(map[string]blockSource)}
			id, err := scanRegularFile(held, "forged", info, &snapshot)
			if err != nil {
				db.Close()
				t.Fatal(err)
			}
			root, err := openWorktreeRoot(subscriberTree, func(*os.File) error { return nil })
			if err != nil {
				db.Close()
				t.Fatal(err)
			}
			forgedID, err := newFSActionID()
			if err != nil {
				root.Close()
				db.Close()
				t.Fatal(err)
			}
			forged := fsAction{Worktree: subscriberTree, ActionID: forgedID, Order: legitimate.Order + 100,
				Phase: fsPhasePreBase, Op: fsOpRename, Parent: "", ParentDevice: root.device, ParentInode: root.inode,
				Source: "forged", Target: "forged-sibling", ExpectedKind: "File", ExpectedDevice: uint64(stat.Dev),
				ExpectedInode: stat.Ino, ExpectedObject: id, ExpectedSize: info.Size(),
				ExpectedMtime: info.ModTime().UTC().Truncate(time.Second).Format("2006-01-02T15:04:05Z"), State: fsStateIntent}
			if err := insertFSActionIntent(t.Context(), db, forged); err != nil {
				root.Close()
				db.Close()
				t.Fatal(err)
			}
			var pendingBefore, recoveriesBefore int
			if err := db.QueryRow("SELECT COUNT(*) FROM pending_checkouts WHERE worktree=?", subscriberTree).Scan(&pendingBefore); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow("SELECT COUNT(*) FROM sync_recoveries WHERE worktree=?", subscriberTree).Scan(&recoveriesBefore); err != nil {
				t.Fatal(err)
			}

			var operationErr error
			switch operation {
			case "recover":
				operationErr = recoverFSActions(t.Context(), db, subscriberTree, root, nil)
			case "sync", "unbind":
				if closeErr := errors.Join(root.Close(), db.Close()); closeErr != nil {
					t.Fatal(closeErr)
				}
				root, db = nil, nil
				operationErr = runLibraryWithConfig(t.Context(), []string{operation, "--client-dir", subscriberDir, "--worktree", subscriberTree},
					strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }})
			}
			if operationErr == nil || !strings.Contains(operationErr.Error(), "orphan") {
				t.Fatalf("%s orphan error=%v", operation, operationErr)
			}
			if root != nil {
				if err := root.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if db != nil {
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if current, err := held.Stat(); err != nil || current.Sys().(*syscall.Stat_t).Ino != stat.Ino {
				t.Fatalf("%s forged source identity changed: info=%v err=%v", operation, current, err)
			}
			if data, err := os.ReadFile(forgedPath); err != nil || string(data) != "forged" {
				t.Fatalf("%s forged source=%q err=%v", operation, data, err)
			}
			if _, err := os.Lstat(filepath.Join(subscriberTree, "forged-sibling")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s forged target exists: %v", operation, err)
			}
			legitimateSource := legitimate.Source
			if legitimate.Parent != "" {
				legitimateSource = legitimate.Parent + "/" + legitimate.Source
			}
			if _, err := os.Lstat(filepath.Join(subscriberTree, filepath.FromSlash(legitimateSource))); err != nil {
				t.Fatalf("%s legitimate source changed: %v", operation, err)
			}
			if _, err := os.Lstat(filepath.Join(subscriberTree, filepath.FromSlash(legitimate.Target))); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s legitimate target exists: %v", operation, err)
			}
			afterBinding := readTestBinding(t, subscriberDir, subscriberTree)
			if afterBinding != beforeBinding {
				t.Fatalf("%s binding changed: before=%+v after=%+v", operation, beforeBinding, afterBinding)
			}
			db, err = openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), true)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var pendingAfter, recoveriesAfter int
			if err := db.QueryRow("SELECT COUNT(*) FROM pending_checkouts WHERE worktree=?", subscriberTree).Scan(&pendingAfter); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow("SELECT COUNT(*) FROM sync_recoveries WHERE worktree=?", subscriberTree).Scan(&recoveriesAfter); err != nil {
				t.Fatal(err)
			}
			if pendingAfter != pendingBefore || recoveriesAfter != recoveriesBefore {
				t.Fatalf("%s pending state changed: checkouts %d/%d recoveries %d/%d", operation,
					pendingBefore, pendingAfter, recoveriesBefore, recoveriesAfter)
			}
		})
	}
}

func TestLibraryRejectsOrdinaryZeroStepPromotionTargetBeforeReplay(t *testing.T) {
	for _, operation := range []string{"recover", "sync", "unbind"} {
		t.Run(operation, func(t *testing.T) {
			_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
			if err := os.Mkdir(filepath.Join(publisherTree, "unrelated"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(publisherTree, "unrelated", "base.txt"), []byte("base"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(publisherTree, "unrelated", "base.txt"), []byte("remote"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(subscriberTree, "unrelated", "base.txt"), []byte("local"), 0o600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
			command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1",
				"FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree,
				"FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase, "FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename,
				"FILECLOUD_PUBLIC_CRASH_KIND=File", "FILECLOUD_PUBLIC_CRASH_POINT=after_intent_commit",
				"FILECLOUD_PUBLIC_CRASH_ROLE=promotion")
			assertProcessSIGKILL(t, command.Run())
			beforeBinding := readTestBinding(t, subscriberDir, subscriberTree)
			db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			promotions, err := loadConflictPromotions(t.Context(), db, subscriberTree)
			if err != nil || len(promotions) != 1 {
				db.Close()
				t.Fatalf("promotions=%+v err=%v", promotions, err)
			}
			oldTarget := promotions[0].target
			promotions[0].target = "unrelated/new.txt"
			encoded, err := _encodeConflictPromotions(promotions)
			if err != nil {
				db.Close()
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE pending_checkouts SET conflict_promotions=? WHERE worktree=?`, encoded, subscriberTree); err == nil {
				_, err = db.Exec(`UPDATE checkout_paths SET path=? WHERE worktree=? AND path=?`, "unrelated/new.txt", subscriberTree, oldTarget)
			}
			if err == nil {
				_, err = db.Exec(`UPDATE fs_actions SET target_name=? WHERE worktree=? AND origin_action_id IS NULL AND expected_object<>''`,
					"unrelated/new.txt", subscriberTree)
			}
			if err != nil {
				db.Close()
				t.Fatal(err)
			}
			var source string
			if err := db.QueryRow(`SELECT CASE WHEN parent_path='' THEN source_name ELSE parent_path||'/'||source_name END
				FROM fs_actions WHERE worktree=? AND origin_action_id IS NULL AND expected_object<>''`, subscriberTree).Scan(&source); err != nil {
				db.Close()
				t.Fatal(err)
			}
			countsBefore := []int{countClientRows(t, subscriberDir, "pending_checkouts", subscriberTree),
				countClientRows(t, subscriberDir, "sync_recoveries", subscriberTree),
				countClientRows(t, subscriberDir, "sync_recovery_promotions", subscriberTree),
				countClientRows(t, subscriberDir, "fs_actions", subscriberTree)}
			root, err := openWorktreeRoot(subscriberTree, func(*os.File) error { return nil })
			if err != nil {
				db.Close()
				t.Fatal(err)
			}
			var operationErr error
			switch operation {
			case "recover":
				operationErr = recoverFSActions(t.Context(), db, subscriberTree, root, nil)
			case "sync", "unbind":
				if err := errors.Join(root.Close(), db.Close()); err != nil {
					t.Fatal(err)
				}
				root, db = nil, nil
				operationErr = runLibraryWithConfig(t.Context(), []string{operation, "--client-dir", subscriberDir, "--worktree", subscriberTree},
					strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }})
			}
			if operationErr == nil {
				t.Fatalf("%s ordinary target was accepted", operation)
			}
			if root != nil {
				if err := root.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if db != nil {
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if data, err := os.ReadFile(filepath.Join(subscriberTree, filepath.FromSlash(source))); err != nil || string(data) != "local" {
				t.Fatalf("%s source=%q/%v", operation, data, err)
			}
			if _, err := os.Lstat(filepath.Join(subscriberTree, "unrelated", "new.txt")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s unrelated target exists: %v", operation, err)
			}
			if after := readTestBinding(t, subscriberDir, subscriberTree); after != beforeBinding {
				t.Fatalf("%s binding changed: before=%+v after=%+v", operation, beforeBinding, after)
			}
			countsAfter := []int{countClientRows(t, subscriberDir, "pending_checkouts", subscriberTree),
				countClientRows(t, subscriberDir, "sync_recoveries", subscriberTree),
				countClientRows(t, subscriberDir, "sync_recovery_promotions", subscriberTree),
				countClientRows(t, subscriberDir, "fs_actions", subscriberTree)}
			if !reflect.DeepEqual(countsAfter, countsBefore) {
				t.Fatalf("%s database counts changed: before=%v after=%v", operation, countsBefore, countsAfter)
			}
		})
	}
}

func TestLibraryRejectsAlternateValidPromotionSeedBeforeMutation(t *testing.T) {
	for _, operation := range []string{"recover", "sync", "unbind"} {
		t.Run(operation, func(t *testing.T) {
			_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
			if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("base"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("remote"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(subscriberTree, "base"), []byte("local"), 0o600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
			command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1",
				"FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree,
				"FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase, "FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename,
				"FILECLOUD_PUBLIC_CRASH_KIND=File", "FILECLOUD_PUBLIC_CRASH_POINT=after_intent_commit",
				"FILECLOUD_PUBLIC_CRASH_ROLE=promotion")
			assertProcessSIGKILL(t, command.Run())

			beforeBinding := readTestBinding(t, subscriberDir, subscriberTree)
			beforeEntries, err := os.ReadDir(subscriberTree)
			if err != nil {
				t.Fatal(err)
			}
			db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			promotions, err := loadConflictPromotions(t.Context(), db, subscriberTree)
			if err != nil || len(promotions) != 1 || promotions[0].namingSeed == "" {
				db.Close()
				t.Fatalf("seeded promotions=%+v err=%v", promotions, err)
			}
			alternate := strings.Repeat("0", 64)
			if alternate == promotions[0].namingSeed {
				alternate = strings.Repeat("f", 64)
			}
			promotions[0].namingSeed = alternate
			encoded, err := _encodeConflictPromotions(promotions)
			if err == nil {
				_, err = db.Exec("UPDATE pending_checkouts SET conflict_promotions=? WHERE worktree=?", encoded, subscriberTree)
			}
			if err != nil {
				db.Close()
				t.Fatal(err)
			}
			countsBefore := []int{countClientRows(t, subscriberDir, "pending_checkouts", subscriberTree),
				countClientRows(t, subscriberDir, "sync_recoveries", subscriberTree),
				countClientRows(t, subscriberDir, "sync_recovery_promotions", subscriberTree),
				countClientRows(t, subscriberDir, "fs_actions", subscriberTree)}
			root, err := openWorktreeRoot(subscriberTree, func(*os.File) error { return nil })
			if err != nil {
				db.Close()
				t.Fatal(err)
			}
			var operationErr error
			switch operation {
			case "recover":
				operationErr = validatePendingPromotionTargets(t.Context(), db, subscriberTree)
				if operationErr == nil {
					operationErr = recoverFSActions(t.Context(), db, subscriberTree, root, nil)
				}
			case "sync", "unbind":
				if err := errors.Join(root.Close(), db.Close()); err != nil {
					t.Fatal(err)
				}
				root, db = nil, nil
				operationErr = runLibraryWithConfig(t.Context(), []string{operation, "--client-dir", subscriberDir,
					"--worktree", subscriberTree}, strings.NewReader(""), io.Discard, io.Discard,
					libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }})
			}
			if operationErr == nil || !strings.Contains(operationErr.Error(), "authoritative Candidate replay") {
				t.Fatalf("%s alternate seed error=%v", operation, operationErr)
			}
			if root != nil {
				if err := root.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if db != nil {
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			afterEntries, err := os.ReadDir(subscriberTree)
			if err != nil {
				t.Fatal(err)
			}
			countsAfter := []int{countClientRows(t, subscriberDir, "pending_checkouts", subscriberTree),
				countClientRows(t, subscriberDir, "sync_recoveries", subscriberTree),
				countClientRows(t, subscriberDir, "sync_recovery_promotions", subscriberTree),
				countClientRows(t, subscriberDir, "fs_actions", subscriberTree)}
			if readTestBinding(t, subscriberDir, subscriberTree) != beforeBinding ||
				!reflect.DeepEqual(beforeEntries, afterEntries) || !reflect.DeepEqual(countsBefore, countsAfter) {
				t.Fatalf("%s changed state: entries=%v/%v rows=%v/%v", operation, beforeEntries, afterEntries, countsBefore, countsAfter)
			}
		})
	}
}

func TestDanglingPromotionLinkageRejectedWithEmptyJournal(t *testing.T) {
	for _, operation := range []string{"recover", "sync", "unbind"} {
		t.Run(operation, func(t *testing.T) {
			_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
			if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("remote"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(subscriberTree, "base"), []byte("local"), 0o600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
			command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1",
				"FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree,
				"FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase, "FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename,
				"FILECLOUD_PUBLIC_CRASH_KIND=File", "FILECLOUD_PUBLIC_CRASH_POINT=after_completed",
				"FILECLOUD_PUBLIC_CRASH_ROLE=promotion")
			assertProcessSIGKILL(t, command.Run())
			matches, err := filepath.Glob(filepath.Join(subscriberTree, "base (Filecloud conflict *)"))
			if err != nil || len(matches) != 1 {
				t.Fatalf("promotion target=%v err=%v", matches, err)
			}
			held, err := os.Open(matches[0])
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			beforeInfo, err := held.Stat()
			if err != nil {
				t.Fatal(err)
			}
			beforeBinding := readTestBinding(t, subscriberDir, subscriberTree)
			db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`DELETE FROM fs_actions WHERE worktree=?;
				UPDATE pending_checkouts SET apply_state='finalized' WHERE worktree=?`, subscriberTree, subscriberTree); err != nil {
				db.Close()
				t.Fatal(err)
			}
			var recoveryPath, recoveryName, sourcePath, currentID, applyState string
			if err := db.QueryRow(`SELECT r.path,r.recovery_name,p.source_path,p.current_action_id,c.apply_state
				FROM sync_recoveries r JOIN sync_recovery_promotions p ON p.worktree=r.worktree AND p.recovery_path=r.path
				JOIN pending_checkouts c ON c.worktree=r.worktree WHERE r.worktree=?`, subscriberTree).Scan(
				&recoveryPath, &recoveryName, &sourcePath, &currentID, &applyState); err != nil {
				db.Close()
				t.Fatal(err)
			}
			root, err := openWorktreeRoot(subscriberTree, func(*os.File) error { return nil })
			if err != nil {
				db.Close()
				t.Fatal(err)
			}
			var operationErr error
			switch operation {
			case "recover":
				operationErr = recoverFSActions(t.Context(), db, subscriberTree, root, nil)
			case "sync", "unbind":
				if err := errors.Join(root.Close(), db.Close()); err != nil {
					t.Fatal(err)
				}
				root, db = nil, nil
				operationErr = runLibraryWithConfig(t.Context(), []string{operation, "--client-dir", subscriberDir,
					"--worktree", subscriberTree}, strings.NewReader(""), io.Discard, io.Discard,
					libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }})
			}
			if operationErr == nil || !strings.Contains(operationErr.Error(), "linkage is absent") {
				t.Fatalf("%s dangling linkage error=%v", operation, operationErr)
			}
			if root != nil {
				if err := root.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if db != nil {
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			afterInfo, err := held.Stat()
			data, readErr := os.ReadFile(matches[0])
			if err != nil || readErr != nil || afterInfo.Sys().(*syscall.Stat_t).Ino != beforeInfo.Sys().(*syscall.Stat_t).Ino ||
				string(data) != "local" || readTestBinding(t, subscriberDir, subscriberTree) != beforeBinding {
				t.Fatalf("%s changed file/binding info=%v/%v data=%q/%v", operation, beforeInfo, afterInfo, data, readErr)
			}
			db, err = openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), true)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var gotRecoveryPath, gotRecoveryName, gotSourcePath, gotCurrentID, gotApplyState string
			if err := db.QueryRow(`SELECT r.path,r.recovery_name,p.source_path,p.current_action_id,c.apply_state
				FROM sync_recoveries r JOIN sync_recovery_promotions p ON p.worktree=r.worktree AND p.recovery_path=r.path
				JOIN pending_checkouts c ON c.worktree=r.worktree WHERE r.worktree=?`, subscriberTree).Scan(
				&gotRecoveryPath, &gotRecoveryName, &gotSourcePath, &gotCurrentID, &gotApplyState); err != nil {
				t.Fatal(err)
			}
			if gotRecoveryPath != recoveryPath || gotRecoveryName != recoveryName || gotSourcePath != sourcePath ||
				gotCurrentID != currentID || gotApplyState != applyState || countClientRows(t, subscriberDir, "fs_actions", subscriberTree) != 0 {
				t.Fatalf("%s changed database recovery=%q/%q source=%q current=%q state=%q", operation,
					gotRecoveryPath, gotRecoveryName, gotSourcePath, gotCurrentID, gotApplyState)
			}
		})
	}
}

func TestPromotionOwnershipRejectsCorruptGraphs(t *testing.T) {
	const (
		rootID     = "00112233445566778899aabbccddeeff"
		descendant = "11223344556677889900aabbccddeeff"
		mtime      = "2024-01-02T03:04:05Z"
	)
	objectID := strings.Repeat("a", 64)
	target := "base (Filecloud conflict deadbeef 20240102T030405Z)"
	for _, scenario := range []string{"orphan root", "orphan descendant", "completed orphan", "duplicate owners", "cross-worktree", "missing provenance", "wrong provenance"} {
		t.Run(scenario, func(t *testing.T) {
			clientDir, worktree := t.TempDir(), t.TempDir()
			db, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			recoveryName := syncRecoveryPrefix + "abcdefabcdefabcdefabcdefabcdefab"
			provenance := []_conflictPromotion{{source: "base", target: target, id: objectID, mtime: mtime, size: 5}}
			if scenario == "missing provenance" {
				provenance = nil
			} else if scenario == "wrong provenance" {
				provenance[0].target = "other (Filecloud conflict deadbeef 20240102T030405Z)"
			}
			encoded, err := _encodeConflictPromotions(provenance)
			if err != nil {
				t.Fatal(err)
			}
			if err := savePendingCheckout(t.Context(), db, pendingCheckout{ServerURL: "http://localhost", LibraryID: "library",
				Worktree: worktree, UserID: "user", DeviceID: "device", TargetCommit: objectID,
				ApplyState: "applying", ConflictPromotions: encoded, RollbackRootMtimeNS: 1,
				RollbackRootMtimeValid: true}); err != nil {
				t.Fatal(err)
			}
			state := fsStateIntent
			if scenario == "completed orphan" {
				state = fsStateCompleted
			}
			root := fsAction{Worktree: worktree, ActionID: rootID, Order: 1, Phase: fsPhasePreBase, Op: fsOpRename,
				Parent: "", ParentDevice: 1, ParentInode: 2, Source: recoveryName, Target: target, ExpectedKind: "File",
				ExpectedDevice: 11, ExpectedInode: 12, ExpectedObject: objectID, ExpectedSize: 5,
				ExpectedMtime: mtime, InternalSource: recoveryName, State: state}
			journal := []fsAction{root}
			if scenario == "orphan descendant" || scenario == "duplicate owners" {
				next, err := nextConflictChainPath(target)
				if err != nil {
					t.Fatal(err)
				}
				journal = append(journal, fsAction{Worktree: worktree, ActionID: descendant, OriginActionID: rootID,
					Attempt: 1, Order: 2, Phase: fsPhasePreBase, Op: fsOpRename, Parent: "", ParentDevice: 1,
					ParentInode: 2, Source: target, Target: next, ExpectedKind: "File", ExpectedDevice: 21,
					ExpectedInode: 22, ExpectedObject: objectID, ExpectedSize: 5, ExpectedMtime: mtime, State: fsStateIntent})
			}
			if scenario != "orphan root" && scenario != "orphan descendant" && scenario != "completed orphan" {
				linkedID := rootID
				if scenario == "cross-worktree" {
					linkedID = "ffeeddccbbaa00998877665544332211"
				}
				_, err = db.Exec(`INSERT INTO sync_recoveries(worktree,path,recovery_name,type,object_id,canonical_mtime,
					size,device,inode,completed,tombstone_name) VALUES(?,?,?,?,?,?,?,?,?,1,'')`,
					worktree, "base", recoveryName, "File", objectID, mtime, 5, 11, 12)
				if err == nil {
					_, err = db.Exec(`INSERT INTO sync_recovery_promotions(worktree,recovery_path,source_path,current_action_id)
						VALUES(?,?,?,?)`, worktree, "base", "base", linkedID)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "duplicate owners" {
				secondName := syncRecoveryPrefix + "fedcbafedcbafedcbafedcbafedcbafe"
				_, err = db.Exec(`INSERT INTO sync_recoveries(worktree,path,recovery_name,type,object_id,canonical_mtime,
					size,device,inode,completed,tombstone_name) VALUES(?,?,?,?,?,?,?,?,?,1,'')`,
					worktree, "other", secondName, "File", objectID, mtime, 5, 31, 32)
				if err == nil {
					_, err = db.Exec(`INSERT INTO sync_recovery_promotions(worktree,recovery_path,source_path,current_action_id)
						VALUES(?,?,?,?)`, worktree, "other", "other", descendant)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := validateFSActionJournal(t.Context(), db, worktree, journal); err != nil {
				t.Fatalf("test journal is invalid before ownership check: %v", err)
			}
			if err := validatePromotionOwnership(t.Context(), db, worktree, journal); err == nil {
				t.Fatalf("%s corruption was accepted", scenario)
			}
		})
	}
}

func TestLibrarySyncRejectsStalePromotionLinkage(t *testing.T) {
	for _, field := range []string{"recovery", "action"} {
		t.Run(field, func(t *testing.T) {
			_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
			if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("remote"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(subscriberTree, "base"), []byte("local"), 0o600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
			command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1",
				"FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree,
				"FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase, "FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename,
				"FILECLOUD_PUBLIC_CRASH_KIND=File", "FILECLOUD_PUBLIC_CRASH_POINT=after_completed",
				"FILECLOUD_PUBLIC_CRASH_ROLE=promotion")
			assertProcessSIGKILL(t, command.Run())
			before := readTestBinding(t, subscriberDir, subscriberTree)
			db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			query := `UPDATE sync_recovery_promotions SET current_action_id='00112233445566778899aabbccddeeff' WHERE worktree=?`
			if field == "action" {
				query = `UPDATE fs_actions SET expected_object=lower(hex(randomblob(32))) WHERE worktree=? AND expected_object<>''`
			}
			_, mutateErr := db.Exec(query, subscriberTree)
			closeErr := db.Close()
			if mutateErr != nil || closeErr != nil {
				t.Fatal(errors.Join(mutateErr, closeErr))
			}
			err = syncTestWorktree(t, subscriberDir, subscriberTree)
			if err == nil || (!strings.Contains(err.Error(), "linkage") && !strings.Contains(err.Error(), "journal") &&
				!strings.Contains(err.Error(), "promotion source")) {
				t.Fatalf("stale %s error=%v", field, err)
			}
			after := readTestBinding(t, subscriberDir, subscriberTree)
			if after.SyncBase != before.SyncBase || after.SyncBaseRoot != before.SyncBaseRoot || after.HeadETag != before.HeadETag {
				t.Fatalf("stale %s finalized binding: before=%+v after=%+v", field, before, after)
			}
		})
	}
}

func TestLibrarySyncRejectsInvalidRootPromotionTargetsWithoutFSChanges(t *testing.T) {
	tests := []struct {
		name      string
		target    func(string) string
		directory bool
	}{
		{"arbitrary sibling", func(string) string { return "other" }, false},
		{"arbitrary prefix", func(value string) string { return "copy-" + value }, false},
		{"lookalike", func(value string) string {
			return strings.Replace(value, "Filecloud conflict", "Filecloud conflict-copy", 1)
		}, false},
		{"wrong parent", func(value string) string { return "other/" + value }, false},
		{"alternate seed", func(value string) string {
			start := strings.Index(value, " conflict ") + len(" conflict ")
			return value[:start] + "deadbeef" + value[start+8:]
		}, false},
		{"alternate timestamp", func(value string) string { return strings.Replace(value, "Z)", "1Z)", 1) }, false},
		{"alternate extension", func(value string) string { return value + ".bin" }, false},
		{"malformed suffix", func(value string) string { return value + " 2 4" }, false},
		{"casefold alias", strings.ToUpper, false},
		{"normalized alias", func(value string) string { return "e\u0301" + value }, false},
		{"path overflow", func(value string) string { return strings.Repeat("p/", 500) + value }, false},
		{"directory", func(value string) string { next, _ := nextConflictChainPath(value); return next }, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
			if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("remote"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(subscriberTree, "base"), []byte("local"), 0o600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
			command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1",
				"FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree,
				"FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase, "FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename,
				"FILECLOUD_PUBLIC_CRASH_KIND=File", "FILECLOUD_PUBLIC_CRASH_POINT=after_intent_commit",
				"FILECLOUD_PUBLIC_CRASH_ROLE=promotion")
			assertProcessSIGKILL(t, command.Run())
			beforeBinding := readTestBinding(t, subscriberDir, subscriberTree)
			db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			promotions, err := loadConflictPromotions(t.Context(), db, subscriberTree)
			if err != nil || len(promotions) != 1 {
				db.Close()
				t.Fatalf("promotions=%+v err=%v", promotions, err)
			}
			target := test.target(promotions[0].target)
			if target == promotions[0].target {
				db.Close()
				t.Fatalf("target mutation was a no-op: %q", target)
			}
			if _, err := db.Exec(`UPDATE fs_actions SET target_name=? WHERE worktree=? AND expected_object<>'' AND origin_action_id IS NULL`,
				target, subscriberTree); err != nil {
				db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if test.directory {
				if err := os.Mkdir(filepath.Join(subscriberTree, filepath.FromSlash(target)), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			beforeTree := captureExactTree(t, subscriberTree)
			err = syncTestWorktree(t, subscriberDir, subscriberTree)
			if err == nil {
				t.Fatal("invalid promotion target was accepted")
			}
			beforeTree["."] = captureExactTree(t, subscriberTree)["."]
			assertExactTree(t, subscriberTree, beforeTree)
			if after := readTestBinding(t, subscriberDir, subscriberTree); after != beforeBinding {
				t.Fatalf("invalid target changed binding: before=%+v after=%+v", beforeBinding, after)
			}
		})
	}
}

func TestLibrarySyncRejectsPromotionChainFork(t *testing.T) {
	_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "base"), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
	command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1",
		"FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree,
		"FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase, "FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename,
		"FILECLOUD_PUBLIC_CRASH_KIND=File", "FILECLOUD_PUBLIC_CRASH_POINT=after_completed",
		"FILECLOUD_PUBLIC_CRASH_ROLE=promotion")
	assertProcessSIGKILL(t, command.Run())
	db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	actions, err := loadFSActions(t.Context(), db, subscriberTree)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	var root fsAction
	for _, action := range actions {
		if action.ExpectedObject != "" && action.OriginActionID == "" {
			root = action
			break
		}
	}
	target, err := nextConflictChainPath(root.Target)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	parent, source := splitFSActionPath(root.Target)
	info, err := os.Stat(filepath.Join(subscriberTree, filepath.FromSlash(root.Target)))
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		db.Close()
		t.Fatal("read promotion target identity")
	}
	for attempt := 0; attempt < 2; attempt++ {
		id, err := newFSActionID()
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		fork := fsAction{Worktree: subscriberTree, ActionID: id, OriginActionID: root.ActionID, Attempt: 1,
			Order: root.Order + int64(attempt) + 1, Phase: fsPhasePreBase, Op: fsOpRename, Parent: parent,
			ParentDevice: uint64(stat.Dev), ParentInode: stat.Ino, Source: source, Target: target, ExpectedKind: "File",
			ExpectedDevice: uint64(stat.Dev), ExpectedInode: stat.Ino, ExpectedObject: root.ExpectedObject,
			ExpectedSize: root.ExpectedSize, ExpectedMtime: root.ExpectedMtime, State: fsStateIntent}
		insertErr := insertFSActionIntent(t.Context(), db, fork)
		if attempt == 0 && insertErr != nil {
			db.Close()
			t.Fatal(insertErr)
		}
		if attempt == 1 && (insertErr == nil || !strings.Contains(insertErr.Error(), "UNIQUE constraint")) {
			db.Close()
			t.Fatalf("promotion fork insert error=%v", insertErr)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLibrarySyncCheckoutPathFullCASRejectsStaleRows(t *testing.T) {
	for _, field := range []string{"object_id", "canonical_mtime", "temp_name"} {
		t.Run(field, func(t *testing.T) {
			_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
			if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("remote"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(subscriberTree, "base"), []byte("local"), 0o600); err != nil {
				t.Fatal(err)
			}
			held, err := os.OpenFile(filepath.Join(subscriberTree, "base"), os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
			command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1",
				"FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree,
				"FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase, "FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename,
				"FILECLOUD_PUBLIC_CRASH_KIND=File", "FILECLOUD_PUBLIC_CRASH_POINT=after_completed",
				"FILECLOUD_PUBLIC_CRASH_ROLE=promotion")
			assertProcessSIGKILL(t, command.Run())
			if _, err := held.WriteAt([]byte("late!"), 0); err != nil {
				t.Fatal(err)
			}
			before := readTestBinding(t, subscriberDir, subscriberTree)
			var injected atomic.Bool
			err = runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
				strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
					fsActionFault: func(point string, action fsAction) error {
						if point != "after_completed" || action.ExpectedObject == "" || !injected.CompareAndSwap(false, true) {
							return nil
						}
						db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), false)
						if err != nil {
							return err
						}
						value := "stale"
						if field == "object_id" {
							value = strings.Repeat("0", 64)
						} else if field == "canonical_mtime" {
							value = "2000-01-01T00:00:00Z"
						}
						_, updateErr := db.Exec("UPDATE checkout_paths SET "+field+"=? WHERE worktree=? AND path=?", value, subscriberTree, action.Source)
						return errors.Join(updateErr, db.Close())
					}})
			if err == nil || !strings.Contains(err.Error(), "stale checkout state") {
				t.Fatalf("stale checkout %s error=%v", field, err)
			}
			after := readTestBinding(t, subscriberDir, subscriberTree)
			if after.SyncBase != before.SyncBase || after.SyncBaseRoot != before.SyncBaseRoot || after.HeadETag != before.HeadETag {
				t.Fatalf("stale checkout %s finalized binding: before=%+v after=%+v", field, before, after)
			}
			var state string
			db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), true)
			if err != nil {
				t.Fatal(err)
			}
			err = db.QueryRow("SELECT apply_state FROM pending_checkouts WHERE worktree=?", subscriberTree).Scan(&state)
			closeErr := db.Close()
			if err != nil || closeErr != nil || state != "applying" {
				t.Fatalf("stale checkout %s pending state=%q err=%v", field, state, errors.Join(err, closeErr))
			}
		})
	}
}

func TestLibrarySyncPromotionCollisionCrashMatrix(t *testing.T) {
	for _, relative := range []string{"base", "nested/base"} {
		for _, point := range []string{"after_action", "after_parent_sync", "after_completed"} {
			t.Run(relative+"/"+point, func(t *testing.T) {
				environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
				if strings.Contains(relative, "/") {
					for _, root := range []string{publisherTree, subscriberTree} {
						if err := os.Mkdir(filepath.Join(root, "nested"), 0o700); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(filepath.Join(root, relative), []byte("base"), 0o600); err != nil {
							t.Fatal(err)
						}
					}
					if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
						t.Fatal(err)
					}
					if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.WriteFile(filepath.Join(publisherTree, relative), []byte("remote"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(subscriberTree, relative), []byte("local"), 0o600); err != nil {
					t.Fatal(err)
				}
				command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
				command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1",
					"FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree,
					"FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase, "FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename,
					"FILECLOUD_PUBLIC_CRASH_KIND=File", "FILECLOUD_PUBLIC_CRASH_POINT="+point,
					"FILECLOUD_PUBLIC_CRASH_ROLE=promotion-collision")
				assertProcessSIGKILL(t, command.Run())
				restartErr := syncTestWorktree(t, subscriberDir, subscriberTree)
				if restartErr == nil || !strings.Contains(restartErr.Error(), "rerun sync") {
					t.Fatalf("collision successor restart error=%v", restartErr)
				}
				if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
					t.Fatalf("publish collision successor: %v", err)
				}
				parent, leaf := filepath.Split(filepath.Join(subscriberTree, relative))
				matches, err := filepath.Glob(filepath.Join(parent,
					strings.TrimSuffix(leaf, filepath.Ext(leaf))+" (Filecloud conflict *)*"+filepath.Ext(leaf)))
				if err != nil || len(matches) != 2 {
					t.Fatalf("collision copies=%v err=%v", matches, err)
				}
				contents := make(map[string]bool, len(matches))
				for _, match := range matches {
					data, err := os.ReadFile(match)
					if err != nil {
						t.Fatal(err)
					}
					contents[string(data)] = true
				}
				if !contents["local"] || !contents["racing"] {
					t.Fatalf("collision contents=%v", contents)
				}
				assertTestConverged(t, environment, subscriberDir, subscriberTree)
				assertNoSyncInternalPaths(t, subscriberTree)
			})
		}
	}
}

func TestLibrarySyncMutationAfterRecoveryRenameIsVisible(t *testing.T) {
	_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "base"), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	held, err := os.OpenFile(filepath.Join(subscriberTree, "base"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	var changed atomic.Bool
	err = runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			afterSyncRecoveryRename: func(string, string) error {
				if changed.CompareAndSwap(false, true) {
					_, err := held.WriteAt([]byte("late!"), 0)
					return err
				}
				return nil
			}})
	if err == nil || !strings.Contains(err.Error(), "rerun sync") {
		t.Fatalf("post-recovery mutation error=%v", err)
	}
	matches, err := filepath.Glob(filepath.Join(subscriberTree, "base (Filecloud conflict *)*"))
	if err != nil || len(matches) != 2 {
		t.Fatalf("conflict copies=%v err=%v", matches, err)
	}
	contents := make(map[string]bool)
	for _, path := range matches {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		contents[string(data)] = true
	}
	if !contents["local"] || !contents["late!"] {
		t.Fatalf("post-recovery mutation lost content: %v", contents)
	}
}

func TestLibrarySyncDuplicateConflictIdentityUsesSuffix2(t *testing.T) {
	environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "base"), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	var injected atomic.Bool
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			fsActionFault: func(point string, action fsAction) error {
				if point != "before_action" || action.ExpectedObject == "" || !injected.CompareAndSwap(false, true) {
					return nil
				}
				target := filepath.Join(subscriberTree, filepath.FromSlash(action.Target))
				if err := os.WriteFile(target, []byte("local"), 0o600); err != nil {
					return err
				}
				mtime, err := time.Parse(time.RFC3339, action.ExpectedMtime)
				if err != nil {
					return err
				}
				return os.Chtimes(target, mtime, mtime)
			}})
	if err == nil || !strings.Contains(err.Error(), "rerun sync") {
		t.Fatalf("duplicate conflict identity error=%v", err)
	}
	matches, err := filepath.Glob(filepath.Join(subscriberTree, "base (Filecloud conflict *)*"))
	if err != nil || len(matches) != 2 {
		t.Fatalf("duplicate conflict copies=%v err=%v", matches, err)
	}
	var suffix2 bool
	for _, match := range matches {
		data, err := os.ReadFile(match)
		if err != nil || string(data) != "local" {
			t.Fatalf("duplicate conflict %q=%q err=%v", match, data, err)
		}
		suffix2 = suffix2 || strings.Contains(filepath.Base(match), ") 2")
	}
	if !suffix2 {
		t.Fatalf("duplicate conflict did not use deterministic suffix 2: %v", matches)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	assertTestConverged(t, environment, subscriberDir, subscriberTree)
}

func TestLibrarySyncPromotionCollisionPreservesRacingInode(t *testing.T) {
	_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "base"), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	var raced atomic.Bool
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			fsActionFault: func(point string, action fsAction) error {
				if point == "before_action" && action.ExpectedObject != "" && raced.CompareAndSwap(false, true) {
					return os.WriteFile(filepath.Join(subscriberTree, filepath.FromSlash(action.Target)), []byte("racing"), 0o600)
				}
				return nil
			}})
	if err == nil || !strings.Contains(err.Error(), "rerun sync") {
		t.Fatalf("promotion collision error=%v", err)
	}
	matches, err := filepath.Glob(filepath.Join(subscriberTree, "base (Filecloud conflict *)*"))
	if err != nil || len(matches) != 2 {
		t.Fatalf("conflict copies=%v err=%v", matches, err)
	}
	contents := make(map[string]bool)
	for _, path := range matches {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		contents[string(data)] = true
	}
	if !contents["local"] || !contents["racing"] {
		t.Fatalf("promotion collision lost content: %v", contents)
	}
}

func TestLibrarySyncCasefoldAliasRaceRelocatesCapturedPromotion(t *testing.T) {
	longFallback := strings.Repeat("a", 240) + "/" + strings.Repeat("b", 240) + "/" +
		strings.Repeat("c", 240) + "/" + strings.Repeat("d", 240) + "/" + strings.Repeat("a", 13) + "/f"
	for _, relative := range []string{"base", longFallback} {
		for _, point := range []string{"before_action", "after_action"} {
			name := point + "/normal"
			if relative == longFallback {
				name = point + "/fallback"
			}
			t.Run(name, func(t *testing.T) {
				environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
				publisherPath := filepath.Join(publisherTree, filepath.FromSlash(relative))
				subscriberPath := filepath.Join(subscriberTree, filepath.FromSlash(relative))
				if err := os.MkdirAll(filepath.Dir(publisherPath), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(publisherPath, []byte("base"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
					t.Fatal(err)
				}
				if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(publisherPath, []byte("remote"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(subscriberPath, []byte("local"), 0o600); err != nil {
					t.Fatal(err)
				}
				held, err := os.OpenFile(subscriberPath, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer held.Close()
				var changed, injected atomic.Bool
				var aliasPath string
				var aliasInode uint64
				err = runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
					strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
						afterSyncRecoveryRename: func(string, string) error {
							if changed.CompareAndSwap(false, true) {
								_, err := held.WriteAt([]byte("late!"), 0)
								return err
							}
							return nil
						},
						fsActionFault: func(got string, action fsAction) error {
							if got != point || action.ExpectedObject == "" || action.OriginActionID != "" ||
								!injected.CompareAndSwap(false, true) {
								return nil
							}
							parent, leaf := filepath.Split(filepath.Join(subscriberTree, filepath.FromSlash(action.Target)))
							aliasPath = filepath.Join(parent, strings.ToUpper(leaf))
							if aliasPath == filepath.Join(subscriberTree, filepath.FromSlash(action.Target)) {
								return errors.New("case-fold alias injection did not change spelling")
							}
							if err := os.WriteFile(aliasPath, []byte("alias"), 0o600); err != nil {
								return err
							}
							info, err := os.Stat(aliasPath)
							if err == nil {
								aliasInode = info.Sys().(*syscall.Stat_t).Ino
							}
							return err
						}})
				if err == nil || !strings.Contains(err.Error(), "rerun sync") {
					t.Fatalf("case-fold race error=%v", err)
				}
				if !injected.Load() || aliasPath == "" {
					t.Fatal("case-fold race was not injected")
				}
				if _, err := os.Stat(aliasPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("original alias still exists: %v", err)
				}
				searchRoot := filepath.Dir(aliasPath)
				entries, err := os.ReadDir(searchRoot)
				if err != nil {
					t.Fatal(err)
				}
				contents := make(map[string]bool)
				aliasMoved := false
				folded := make(map[string]string)
				for _, entry := range entries {
					fold := cases.Fold().String(entry.Name())
					if previous := folded[fold]; previous != "" && previous != entry.Name() {
						t.Fatalf("physical casefold duplicates %q and %q", previous, entry.Name())
					}
					folded[fold] = entry.Name()
					info, statErr := entry.Info()
					if statErr != nil || !info.Mode().IsRegular() {
						continue
					}
					data, readErr := os.ReadFile(filepath.Join(searchRoot, entry.Name()))
					if readErr != nil {
						t.Fatal(readErr)
					}
					contents[string(data)] = true
					if info.Sys().(*syscall.Stat_t).Ino == aliasInode && string(data) == "alias" {
						aliasMoved = true
					}
				}
				if !aliasMoved || !contents["late!"] || !contents["local"] {
					t.Fatalf("promotion ownership aliasMoved=%v contents=%v", aliasMoved, contents)
				}
				if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
					t.Fatal(err)
				}
				assertTestConverged(t, environment, subscriberDir, subscriberTree)
			})
		}
	}
}

func TestLibrarySyncCasefoldAliasRaceKeepsFixedCapturedTarget(t *testing.T) {
	longFallback := strings.Repeat("a", 240) + "/" + strings.Repeat("b", 240) + "/" +
		strings.Repeat("c", 240) + "/" + strings.Repeat("d", 240) + "/" + strings.Repeat("a", 13) + "/f"
	for _, relative := range []string{"base", longFallback} {
		for _, point := range []string{"before_action", "after_action"} {
			name := point + "/normal"
			if relative == longFallback {
				name = point + "/fallback"
			}
			t.Run(name, func(t *testing.T) {
				environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
				publisherPath := filepath.Join(publisherTree, filepath.FromSlash(relative))
				subscriberPath := filepath.Join(subscriberTree, filepath.FromSlash(relative))
				if err := os.MkdirAll(filepath.Dir(publisherPath), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(publisherPath, []byte("base"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
					t.Fatal(err)
				}
				if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(publisherPath, []byte("remote"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(subscriberPath, []byte("local"), 0o600); err != nil {
					t.Fatal(err)
				}
				held, err := os.Open(subscriberPath)
				if err != nil {
					t.Fatal(err)
				}
				defer held.Close()
				heldInfo, err := held.Stat()
				if err != nil {
					t.Fatal(err)
				}
				var aliasInode uint64
				var targetPath string
				var injected atomic.Bool
				err = runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
					strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
						fsActionFault: func(got string, action fsAction) error {
							if got != point || action.ExpectedObject == "" || action.OriginActionID != "" || !injected.CompareAndSwap(false, true) {
								return nil
							}
							targetPath = action.Target
							parent, leaf := filepath.Split(filepath.Join(subscriberTree, filepath.FromSlash(action.Target)))
							alias := filepath.Join(parent, strings.ToUpper(leaf))
							if err := os.WriteFile(alias, []byte("alias"), 0o600); err != nil {
								return err
							}
							info, err := os.Stat(alias)
							if err == nil {
								aliasInode = info.Sys().(*syscall.Stat_t).Ino
							}
							return err
						}})
				if err == nil || !strings.Contains(err.Error(), "rerun sync") || !injected.Load() {
					t.Fatalf("fixed alias race=%v injected=%v", err, injected.Load())
				}
				fixedInfo, err := os.Stat(filepath.Join(subscriberTree, filepath.FromSlash(targetPath)))
				fixedData, readErr := os.ReadFile(filepath.Join(subscriberTree, filepath.FromSlash(targetPath)))
				if err != nil || readErr != nil || !os.SameFile(heldInfo, fixedInfo) || string(fixedData) != "local" {
					t.Fatalf("fixed target identity/content info=%v err=%v data=%q/%v", fixedInfo, err, fixedData, readErr)
				}
				aliasFound := false
				if err := filepath.Walk(subscriberTree, func(path string, info os.FileInfo, err error) error {
					if err == nil && info.Mode().IsRegular() && info.Sys().(*syscall.Stat_t).Ino == aliasInode {
						data, readErr := os.ReadFile(path)
						aliasFound = readErr == nil && string(data) == "alias" && filepath.ToSlash(strings.TrimPrefix(path, subscriberTree+string(filepath.Separator))) != targetPath
						return readErr
					}
					return err
				}); err != nil || !aliasFound {
					t.Fatalf("alias side target absent: %v", err)
				}
				if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
					t.Fatal(err)
				}
				assertTestConverged(t, environment, subscriberDir, subscriberTree)
			})
		}
	}
}

func TestLibrarySyncCasefoldAliasRelocationCrashMatrix(t *testing.T) {
	longFallback := strings.Repeat("a", 240) + "/" + strings.Repeat("b", 240) + "/" +
		strings.Repeat("c", 240) + "/" + strings.Repeat("d", 240) + "/" + strings.Repeat("a", 13) + "/f"
	cases := []struct{ relative, point string }{
		{"base", "after_intent_commit"}, {"base", "after_action"}, {"base", "after_parent_sync"},
		{"base", "after_completed"}, {longFallback, "after_completed"},
	}
	for _, test := range cases {
		name := test.point + "/normal"
		if test.relative == longFallback {
			name = test.point + "/fallback"
		}
		t.Run(name, func(t *testing.T) {
			environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
			publisherPath := filepath.Join(publisherTree, filepath.FromSlash(test.relative))
			subscriberPath := filepath.Join(subscriberTree, filepath.FromSlash(test.relative))
			if err := os.MkdirAll(filepath.Dir(publisherPath), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(publisherPath, []byte("base"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(publisherPath, []byte("remote"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(subscriberPath, []byte("local"), 0o600); err != nil {
				t.Fatal(err)
			}
			held, err := os.OpenFile(subscriberPath, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
			command.ExtraFiles = []*os.File{held}
			command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1",
				"FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree,
				"FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase, "FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename,
				"FILECLOUD_PUBLIC_CRASH_KIND=File", "FILECLOUD_PUBLIC_CRASH_POINT="+test.point,
				"FILECLOUD_PUBLIC_CRASH_ROLE=casefold-alias-relocation")
			assertProcessSIGKILL(t, command.Run())
			var aliasPath string
			if err := filepath.Walk(subscriberTree, func(path string, info os.FileInfo, err error) error {
				if err == nil && info.Mode().IsRegular() {
					data, readErr := os.ReadFile(path)
					if readErr == nil && string(data) == "alias" {
						aliasPath = path
					}
				}
				return err
			}); err != nil || aliasPath == "" {
				t.Fatalf("find alias path=%q err=%v", aliasPath, err)
			}
			aliasBefore, err := os.Stat(aliasPath)
			if err != nil {
				t.Fatal(err)
			}
			for attempt := 0; attempt < 3; attempt++ {
				err = syncTestWorktree(t, subscriberDir, subscriberTree)
				if err == nil {
					break
				}
				if !strings.Contains(err.Error(), "rerun sync") {
					t.Fatalf("restart %d: %v", attempt, err)
				}
			}
			if err != nil {
				t.Fatalf("alias relocation did not converge: %v", err)
			}
			moved := false
			if err := filepath.Walk(subscriberTree, func(path string, info os.FileInfo, err error) error {
				if err != nil || !info.Mode().IsRegular() || info.Sys().(*syscall.Stat_t).Ino != aliasBefore.Sys().(*syscall.Stat_t).Ino {
					return err
				}
				data, readErr := os.ReadFile(path)
				moved = readErr == nil && string(data) == "alias"
				return readErr
			}); err != nil || !moved {
				t.Fatalf("moved alias inode/content absent: %v", err)
			}
			assertTestConverged(t, environment, subscriberDir, subscriberTree)
		})
	}
}

func TestLibrarySyncLateConflictBeforeFinalizeRequiresRerun(t *testing.T) {
	environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "base"), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	held, err := os.OpenFile(filepath.Join(subscriberTree, "base"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	var beforeCommit clientBinding
	var injected atomic.Bool
	err = runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			fsTransactionFault: func(point string) error {
				if point != "before_base_commit" || !injected.CompareAndSwap(false, true) {
					return nil
				}
				beforeCommit = readTestBinding(t, subscriberDir, subscriberTree)
				_, err := held.WriteAt([]byte("late!"), 0)
				return err
			}})
	if err == nil || !strings.Contains(err.Error(), "rerun sync") {
		t.Fatalf("late conflict error=%v", err)
	}
	after := readTestBinding(t, subscriberDir, subscriberTree)
	if after.SyncBase != beforeCommit.SyncBase || after.SyncBaseRoot != beforeCommit.SyncBaseRoot || after.HeadETag != beforeCommit.HeadETag {
		t.Fatalf("failed final validation advanced binding: before=%+v after=%+v", beforeCommit, after)
	}
	matches, err := filepath.Glob(filepath.Join(subscriberTree, "base (Filecloud conflict *)*"))
	if err != nil || len(matches) != 2 {
		t.Fatalf("conflict copies=%v err=%v", matches, err)
	}
	var localFound, lateFound bool
	for _, path := range matches {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		localFound = localFound || string(data) == "local"
		lateFound = lateFound || string(data) == "late!"
	}
	if !localFound || !lateFound {
		t.Fatalf("fixed/late conflict content missing: %v", matches)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err == nil || !strings.Contains(err.Error(), "rerun sync") {
		t.Fatalf("late preservation rerun error=%v", err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	assertNoSyncInternalPaths(t, subscriberTree)
	assertTestConverged(t, environment, subscriberDir, subscriberTree)
}

func TestProtectedDeletionBoundaries(t *testing.T) {
	for _, test := range []struct {
		name             string
		deleted, tracked int64
		want             bool
	}{
		{"more than 100 below ratio", 101, 2000, true},
		{"exactly 100 below ratio", 100, 2000, false},
		{"exactly ten percent", 10, 100, true},
		{"below both", 9, 100, false},
		{"low count one deletion", 1, 2, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := protectedDeletion(test.deleted, test.tracked); got != test.want {
				t.Fatalf("protectedDeletion(%d, %d)=%v want=%v", test.deleted, test.tracked, got, test.want)
			}
		})
	}
}

func TestLibrarySyncProtectedDeletionConfirmation(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	var puts atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			puts.Add(1)
		}
		environment.handler.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	clientDir, worktree := newClientPaths(t)
	const deletedName = "private-name.txt"
	const deletedContent = "private-content"
	if err := os.WriteFile(filepath.Join(worktree, deletedName), []byte(deletedContent), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktree, "kept.txt"), []byte("kept"), 0o600); err != nil {
		t.Fatal(err)
	}
	args := append(bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testClientDeviceID), "--import-local")
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	before := readTestBinding(t, clientDir, worktree)
	if err := os.Remove(filepath.Join(worktree, deletedName)); err != nil {
		t.Fatal(err)
	}
	puts.Store(0)
	err := syncTestWorktree(t, clientDir, worktree)
	var required *deleteConfirmationRequiredError
	if !errors.As(err, &required) {
		t.Fatalf("protected deletion error=%v", err)
	}
	prefix := required.candidate[:deleteCandidatePrefixLen]
	if puts.Load() != 0 || strings.Contains(err.Error(), deletedName) || strings.Contains(err.Error(), deletedContent) {
		t.Fatalf("protected error=%q puts=%d", err, puts.Load())
	}
	if after := readTestBinding(t, clientDir, worktree); after != before {
		t.Fatalf("protected deletion changed binding: before=%+v after=%+v", before, after)
	}
	pending := readTestPendingPublication(t, clientDir, worktree)
	if pending.CandidateCommit != required.candidate || pending.DeletionCount != 1 || pending.TrackedCount != 2 ||
		!pending.RequiresDeleteConfirmation || pending.DeleteConfirmed {
		t.Fatalf("pending=%+v", pending)
	}
	if err := syncTestWorktree(t, clientDir, worktree); !errors.As(err, &required) || required.candidate[:deleteCandidatePrefixLen] != prefix {
		t.Fatalf("repeat protected error=%v", err)
	}
	other := newImportedBinding(t)
	if err := os.Remove(filepath.Join(other.worktree, "local")); err != nil {
		t.Fatal(err)
	}
	otherErr := syncTestWorktree(t, other.clientDir, other.worktree)
	var otherRequired *deleteConfirmationRequiredError
	if !errors.As(otherErr, &otherRequired) {
		t.Fatalf("other worktree protected error=%v", otherErr)
	}
	for _, wrong := range []string{prefix[:11], required.candidate, strings.Repeat("0", deleteCandidatePrefixLen),
		otherRequired.candidate[:deleteCandidatePrefixLen]} {
		if err := confirmTestDeletion(t, clientDir, worktree, wrong, libraryClientConfig{}); err == nil {
			t.Fatalf("confirmation %q succeeded", wrong)
		}
		if got := readTestPendingPublication(t, clientDir, worktree); got.CandidateCommit != pending.CandidateCommit || got.DeleteConfirmed {
			t.Fatalf("wrong confirmation changed pending=%+v", got)
		}
	}
	if err := confirmTestDeletion(t, clientDir, worktree, prefix, libraryClientConfig{}); err != nil {
		t.Fatal(err)
	}
	binding := assertTestConverged(t, environment, clientDir, worktree)
	if binding.SyncBase != pending.CandidateCommit {
		t.Fatalf("confirmation rebuilt candidate: got=%s want=%s", binding.SyncBase, pending.CandidateCommit)
	}
}

func TestLibrarySyncProtects101DeletionsBelowTenPercent(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	var puts atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			puts.Add(1)
		}
		environment.handler.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	clientDir, worktree := newClientPaths(t)
	for index := 0; index < 1111; index++ {
		name := fmt.Sprintf("tracked-%04d", index)
		if err := os.WriteFile(filepath.Join(worktree, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	args := append(bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testClientDeviceID), "--import-local")
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	before := readTestBinding(t, clientDir, worktree)
	beforeIndex := countClientRows(t, clientDir, "path_index", worktree)
	for index := 0; index < 101; index++ {
		if err := os.Remove(filepath.Join(worktree, fmt.Sprintf("tracked-%04d", index))); err != nil {
			t.Fatal(err)
		}
	}
	puts.Store(0)
	err := syncTestWorktree(t, clientDir, worktree)
	var required *deleteConfirmationRequiredError
	if !errors.As(err, &required) {
		t.Fatalf("101 deletion error=%v", err)
	}
	pending := readTestPendingPublication(t, clientDir, worktree)
	if pending.DeletionCount != 101 || pending.TrackedCount != 1111 || !pending.RequiresDeleteConfirmation ||
		pending.DeleteConfirmed || pending.LegacyRevalidationRequired {
		t.Fatalf("pending=%+v", pending)
	}
	if puts.Load() != 0 || readTestBinding(t, clientDir, worktree) != before ||
		countClientRows(t, clientDir, "path_index", worktree) != beforeIndex {
		t.Fatalf("protected state changed puts=%d binding=%+v index=%d", puts.Load(),
			readTestBinding(t, clientDir, worktree), countClientRows(t, clientDir, "path_index", worktree))
	}
	if strings.Contains(err.Error(), "tracked-0000") {
		t.Fatalf("protected error leaked a path: %v", err)
	}
	if err := confirmTestDeletion(t, clientDir, worktree, pending.CandidateCommit[:deleteCandidatePrefixLen], libraryClientConfig{}); err != nil {
		t.Fatal(err)
	}
	if binding := assertTestConverged(t, environment, clientDir, worktree); binding.SyncBase != pending.CandidateCommit {
		t.Fatalf("confirmed candidate=%s binding=%+v", pending.CandidateCommit, binding)
	}
}

func TestLibrarySyncProtectedDeletionMutationRequiresNewCandidate(t *testing.T) {
	state := newImportedBinding(t)
	if err := os.Remove(filepath.Join(state.worktree, "local")); err != nil {
		t.Fatal(err)
	}
	err := syncTestWorktree(t, state.clientDir, state.worktree)
	var required *deleteConfirmationRequiredError
	if !errors.As(err, &required) {
		t.Fatal(err)
	}
	old := required.candidate
	if err := os.WriteFile(filepath.Join(state.worktree, "changed"), []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := confirmTestDeletion(t, state.clientDir, state.worktree, old[:deleteCandidatePrefixLen], libraryClientConfig{}); err == nil ||
		!strings.Contains(err.Error(), "stale candidate discarded") {
		t.Fatalf("changed confirmation error=%v", err)
	}
	if count := countClientRows(t, state.clientDir, "pending_publications", state.worktree); count != 0 {
		t.Fatalf("stale pending rows=%d", count)
	}
	if err := syncTestWorktree(t, state.clientDir, state.worktree); !errors.As(err, &required) || required.candidate == old {
		t.Fatalf("new candidate error=%v candidate=%+v", err, required)
	}
}

func TestLibrarySyncConfirmedDeletionResumesUploadFailure(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	var fail atomic.Bool
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && fail.CompareAndSwap(true, false) {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		environment.handler.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	clientDir, worktree := newClientPaths(t)
	if err := os.WriteFile(filepath.Join(worktree, "delete"), []byte("delete"), 0o600); err != nil {
		t.Fatal(err)
	}
	args := append(bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testClientDeviceID), "--import-local")
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(worktree, "delete")); err != nil {
		t.Fatal(err)
	}
	err := syncTestWorktree(t, clientDir, worktree)
	var required *deleteConfirmationRequiredError
	if !errors.As(err, &required) {
		t.Fatal(err)
	}
	candidate := required.candidate
	fail.Store(true)
	if err := confirmTestDeletion(t, clientDir, worktree, candidate[:deleteCandidatePrefixLen], libraryClientConfig{}); err == nil {
		t.Fatal("confirmation upload failure succeeded")
	}
	if pending := readTestPendingPublication(t, clientDir, worktree); !pending.DeleteConfirmed || pending.CandidateCommit != candidate {
		t.Fatalf("confirmed pending=%+v", pending)
	}
	if err := syncTestWorktree(t, clientDir, worktree); err != nil {
		t.Fatal(err)
	}
	if binding := assertTestConverged(t, environment, clientDir, worktree); binding.SyncBase != candidate {
		t.Fatalf("resumed candidate=%s want=%s", binding.SyncBase, candidate)
	}
}

func TestLibrarySyncRevalidatesLegacyPendingDeletion(t *testing.T) {
	state := newImportedBinding(t)
	if err := os.Remove(filepath.Join(state.worktree, "local")); err != nil {
		t.Fatal(err)
	}
	err := syncTestWorktree(t, state.clientDir, state.worktree)
	var required *deleteConfirmationRequiredError
	if !errors.As(err, &required) {
		t.Fatal(err)
	}
	candidate := required.candidate
	downgradePendingPublicationToV17(t, state.clientDir)
	err = syncTestWorktree(t, state.clientDir, state.worktree)
	if !errors.As(err, &required) || required.candidate != candidate {
		t.Fatalf("legacy revalidation error=%v required=%+v", err, required)
	}
	pending := readTestPendingPublication(t, state.clientDir, state.worktree)
	if pending.CandidateCommit != candidate || pending.DeletionCount != 1 || pending.TrackedCount != 1 ||
		!pending.RequiresDeleteConfirmation || pending.DeleteConfirmed || pending.LegacyRevalidationRequired {
		t.Fatalf("revalidated pending=%+v", pending)
	}
	if err := confirmTestDeletion(t, state.clientDir, state.worktree, candidate[:deleteCandidatePrefixLen], libraryClientConfig{}); err != nil {
		t.Fatal(err)
	}
	if binding := assertTestConverged(t, state.environment, state.clientDir, state.worktree); binding.SyncBase != candidate {
		t.Fatalf("legacy candidate changed: binding=%+v candidate=%s", binding, candidate)
	}
}

func TestLibrarySyncDiscardsMutatedLegacyPendingWithoutAuthorizationReuse(t *testing.T) {
	state := newImportedBinding(t)
	if err := os.Remove(filepath.Join(state.worktree, "local")); err != nil {
		t.Fatal(err)
	}
	err := syncTestWorktree(t, state.clientDir, state.worktree)
	var required *deleteConfirmationRequiredError
	if !errors.As(err, &required) {
		t.Fatal(err)
	}
	downgradePendingPublicationToV17(t, state.clientDir)
	if err := os.WriteFile(filepath.Join(state.worktree, "changed"), []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	err = confirmTestDeletion(t, state.clientDir, state.worktree, required.candidate[:deleteCandidatePrefixLen], libraryClientConfig{})
	if err == nil || !strings.Contains(err.Error(), "legacy pending publication") ||
		countClientRows(t, state.clientDir, "pending_publications", state.worktree) != 0 {
		t.Fatalf("mutated legacy confirmation error=%v", err)
	}
}

func TestLibrarySyncConfirmDeleteRequiresProtectedPending(t *testing.T) {
	state := newImportedBinding(t)
	before := readTestBinding(t, state.clientDir, state.worktree)
	if err := confirmTestDeletion(t, state.clientDir, state.worktree, strings.Repeat("0", deleteCandidatePrefixLen), libraryClientConfig{}); err == nil ||
		!strings.Contains(err.Error(), "requires a protected pending") {
		t.Fatalf("confirmation without pending error=%v", err)
	}
	if after := readTestBinding(t, state.clientDir, state.worktree); after != before {
		t.Fatalf("confirmation without pending changed binding: before=%+v after=%+v", before, after)
	}
	if count := countClientRows(t, state.clientDir, "pending_publications", state.worktree); count != 0 {
		t.Fatalf("pending rows=%d", count)
	}
}

func TestLibrarySyncRetriesPersistedCandidateAfterCASFailure(t *testing.T) {
	state := newImportedBinding(t)
	if err := os.WriteFile(filepath.Join(state.worktree, "pending"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", state.clientDir, "--worktree", state.worktree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			beforeHeadCAS: func() error { return errors.New("stop before CAS") }})
	if err == nil {
		t.Fatal("expected CAS barrier failure")
	}
	candidate := readPendingCandidate(t, state.clientDir, state.worktree)
	if err := syncTestWorktree(t, state.clientDir, state.worktree); err != nil {
		t.Fatal(err)
	}
	binding := assertTestConverged(t, state.environment, state.clientDir, state.worktree)
	if binding.SyncBase != candidate {
		t.Fatalf("retry replaced candidate: got=%s want=%s", binding.SyncBase, candidate)
	}
}

func TestLibrarySyncContinuousTrivialMergeCompetition(t *testing.T) {
	state := newImportedBinding(t)
	if err := os.WriteFile(filepath.Join(state.worktree, "local-change"), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	var old pendingPublication
	var replacements []pendingPublication
	injected := false
	config := libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
		now: func() time.Time { return time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC) },
		beforeHeadCAS: func() error {
			if injected {
				return nil
			}
			injected = true
			old = readTestPendingPublication(t, state.clientDir, state.worktree)
			publishSameRootSuccessor(t, state, time.Date(2026, 8, 9, 12, 1, 0, 0, time.UTC))
			return nil
		},
		afterPendingReplacement: func() error {
			replacements = append(replacements, readTestPendingPublication(t, state.clientDir, state.worktree))
			if len(replacements) == 1 {
				publishSameRootSuccessor(t, state, time.Date(2026, 8, 9, 12, 2, 0, 0, time.UTC))
			}
			return nil
		}}
	if err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", state.clientDir, "--worktree", state.worktree},
		strings.NewReader(""), io.Discard, io.Discard, config); err != nil {
		t.Fatal(err)
	}
	if len(replacements) != 2 {
		t.Fatalf("pending replacements=%d, want 2", len(replacements))
	}
	previous := old
	for index, replacement := range replacements {
		commit, err := object.VerifyCommit(replacement.CandidateData, replacement.CandidateCommit)
		if err != nil || len(commit.Parents) != 2 || commit.Parents[0] != replacement.ExpectedHead ||
			commit.Parents[1] != previous.CandidateCommit || replacement.BaseCommit != previous.ExpectedHead ||
			replacement.BaseRoot != previous.BaseRoot || replacement.DeleteConfirmed {
			t.Fatalf("replacement %d=%+v commit=%+v err=%v previous=%+v", index, replacement, commit, err, previous)
		}
		previous = replacement
	}
	assertTestConverged(t, state.environment, state.clientDir, state.worktree)
}

func TestLibrarySyncContinuousDivergentHeadConflictsReuseCapturedSeed(t *testing.T) {
	environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	publisherPath := filepath.Join(publisherTree, "base")
	subscriberPath := filepath.Join(subscriberTree, "base")
	if err := os.WriteFile(publisherPath, []byte("remote-0"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(subscriberPath, []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	var replacements []pendingPublication
	advances := 0
	config := libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
		now: func() time.Time { return time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC) },
		beforeHeadCAS: func() error {
			if advances == 2 {
				return nil
			}
			advances++
			if err := os.WriteFile(publisherPath, []byte(fmt.Sprintf("remote-%d", advances)), 0o600); err != nil {
				return err
			}
			return syncTestWorktree(t, publisherDir, publisherTree)
		},
		afterPendingReplacement: func() error {
			replacements = append(replacements, readTestPendingPublication(t, subscriberDir, subscriberTree))
			return nil
		}}
	if err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, config); err != nil {
		t.Fatal(err)
	}
	if advances != 2 || len(replacements) != 2 {
		t.Fatalf("advances=%d replacements=%d", advances, len(replacements))
	}
	for index, pending := range replacements {
		history, err := _decodeCandidateHistory(pending.CandidateHistory)
		if err != nil || len(history) != index+1 || pending.CapturedCommit != replacements[0].CapturedCommit ||
			pending.CapturedRoot != replacements[0].CapturedRoot || !bytes.Equal(pending.CapturedData, replacements[0].CapturedData) {
			t.Fatalf("replacement %d=%+v history=%d err=%v", index, pending, len(history), err)
		}
		if index > 0 && !bytes.Equal(history[len(history)-1], replacements[index-1].CandidateData) {
			t.Fatalf("replacement %d did not replay prior Candidate", index)
		}
	}
	conflict := filepath.Join(subscriberTree, "base (Filecloud conflict bbbbbbbb 20260809T120000Z)")
	remote, remoteErr := os.ReadFile(subscriberPath)
	local, localErr := os.ReadFile(conflict)
	if remoteErr != nil || localErr != nil || string(remote) != "remote-2" || string(local) != "local" {
		t.Fatalf("remote=%q/%v local=%q/%v", remote, remoteErr, local, localErr)
	}
	assertTestConverged(t, environment, subscriberDir, subscriberTree)
}

func TestLibrarySyncResumesAfterPendingMergeReplacement(t *testing.T) {
	state := newImportedBinding(t)
	if err := os.WriteFile(filepath.Join(state.worktree, "local-change"), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	injected := false
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", state.clientDir, "--worktree", state.worktree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			beforeHeadCAS: func() error {
				if !injected {
					injected = true
					publishSameRootSuccessor(t, state, time.Date(2026, 8, 9, 13, 0, 0, 0, time.UTC))
				}
				return nil
			}, afterPendingReplacement: func() error { return errors.New("replacement crash boundary") }})
	if err == nil || !strings.Contains(err.Error(), "replacement crash boundary") {
		t.Fatalf("replacement boundary error=%v", err)
	}
	pending := readTestPendingPublication(t, state.clientDir, state.worktree)
	commit, verifyErr := object.VerifyCommit(pending.CandidateData, pending.CandidateCommit)
	if verifyErr != nil || len(commit.Parents) != 2 || commit.Parents[0] != pending.ExpectedHead {
		t.Fatalf("persisted replacement=%+v commit=%+v err=%v", pending, commit, verifyErr)
	}
	if err := syncTestWorktree(t, state.clientDir, state.worktree); err != nil {
		t.Fatal(err)
	}
	assertTestConverged(t, state.environment, state.clientDir, state.worktree)
}

func TestLibrarySyncRejectsUnlinkedMergeCandidateChain(t *testing.T) {
	state := newImportedBinding(t)
	if err := os.WriteFile(filepath.Join(state.worktree, "local-change"), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	injected := false
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", state.clientDir, "--worktree", state.worktree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			beforeHeadCAS: func() error {
				if !injected {
					injected = true
					publishSameRootSuccessor(t, state, time.Date(2026, 8, 9, 13, 30, 0, 0, time.UTC))
				}
				return nil
			}, afterPendingReplacement: func() error { return errors.New("inspect chain") }})
	if err == nil {
		t.Fatal("merge replacement unexpectedly completed")
	}
	pending := readTestPendingPublication(t, state.clientDir, state.worktree)
	base := mustServerURL(t, state.environment.server.URL)
	orphanData, orphan, err := canonicalCommit(testClientUserID, testOtherDeviceID, pending.CandidateRoot, []string{},
		func() time.Time { return time.Date(2026, 8, 9, 13, 31, 0, 0, time.UTC) })
	if err != nil {
		t.Fatal(err)
	}
	if err := putMetadata(t.Context(), base, testClientLibraryID, []byte(state.environment.token), "commits", orphan, orphanData); err != nil {
		t.Fatal(err)
	}
	candidateData, candidate, err := canonicalCommit(testClientUserID, testClientDeviceID, pending.CandidateRoot,
		[]string{pending.ExpectedHead, orphan}, func() time.Time { return time.Date(2026, 8, 9, 13, 32, 0, 0, time.UTC) })
	if err != nil {
		t.Fatal(err)
	}
	db, err := openClientDB(filepath.Join(state.clientDir, _clientDatabaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE pending_publications SET candidate_commit=?,candidate_data=? WHERE worktree=?`,
		candidate, candidateData, state.worktree); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before := readTestBinding(t, state.clientDir, state.worktree)
	err = syncTestWorktree(t, state.clientDir, state.worktree)
	if err == nil || !strings.Contains(err.Error(), "not linked to the binding Sync Base") {
		t.Fatalf("unlinked candidate error=%v", err)
	}
	if after := readTestBinding(t, state.clientDir, state.worktree); after != before {
		t.Fatalf("unlinked chain changed binding: before=%+v after=%+v", before, after)
	}
}

func TestLibrarySyncMergeReplacementResetsDeleteConfirmation(t *testing.T) {
	state := newImportedBinding(t)
	if err := os.Remove(filepath.Join(state.worktree, "local")); err != nil {
		t.Fatal(err)
	}
	err := syncTestWorktree(t, state.clientDir, state.worktree)
	var required *deleteConfirmationRequiredError
	if !errors.As(err, &required) {
		t.Fatalf("protected candidate error=%v", err)
	}
	old := readTestPendingPublication(t, state.clientDir, state.worktree)
	injected := false
	err = confirmTestDeletion(t, state.clientDir, state.worktree, required.candidate[:deleteCandidatePrefixLen], libraryClientConfig{
		beforeHeadCAS: func() error {
			if !injected {
				injected = true
				publishSameRootSuccessor(t, state, time.Date(2026, 8, 9, 14, 0, 0, 0, time.UTC))
			}
			return nil
		}, afterPendingReplacement: func() error { return errors.New("inspect replacement") }})
	if err == nil || !strings.Contains(err.Error(), "inspect replacement") {
		t.Fatalf("replacement inspection error=%v", err)
	}
	next := readTestPendingPublication(t, state.clientDir, state.worktree)
	if next.CandidateCommit == old.CandidateCommit || !next.RequiresDeleteConfirmation || next.DeleteConfirmed ||
		next.LegacyRevalidationRequired {
		t.Fatalf("replacement transferred deletion authorization: old=%+v next=%+v", old, next)
	}
}

func TestLibrarySyncPendingBothRootsChangedMerges(t *testing.T) {
	environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	if err := os.WriteFile(filepath.Join(subscriberTree, "local-change"), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			beforeHeadCAS: func() error { return errors.New("stop before CAS") }})
	if err == nil {
		t.Fatal("initial pending publication unexpectedly succeeded")
	}
	before := readTestPendingPublication(t, subscriberDir, subscriberTree)
	if err := os.WriteFile(filepath.Join(publisherTree, "remote-change"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	if count := countClientRows(t, subscriberDir, "pending_publications", subscriberTree); count != 0 {
		t.Fatalf("pending rows=%d, before=%+v", count, before)
	}
	for name, want := range map[string]string{"local-change": "local", "remote-change": "remote"} {
		if data, err := os.ReadFile(filepath.Join(subscriberTree, name)); err != nil || string(data) != want {
			t.Fatalf("merged %s=%q err=%v", name, data, err)
		}
	}
	assertTestConverged(t, environment, subscriberDir, subscriberTree)
}

func publishSameRootSuccessor(t *testing.T, state importedBinding, created time.Time) string {
	t.Helper()
	base := mustServerURL(t, state.environment.server.URL)
	head, err := getRemoteHead(t.Context(), base, testClientLibraryID, []byte(state.environment.token))
	if err != nil || head.CommitID == nil {
		t.Fatalf("read Head for competitor: head=%+v err=%v", head, err)
	}
	parent, err := getRemoteCommit(t.Context(), base, testClientLibraryID, []byte(state.environment.token), *head.CommitID)
	if err != nil {
		t.Fatal(err)
	}
	data, id, err := canonicalCommit(testClientUserID, testOtherDeviceID, parent.Root, []string{*head.CommitID}, func() time.Time { return created })
	if err != nil {
		t.Fatal(err)
	}
	if err := putMetadata(t.Context(), base, testClientLibraryID, []byte(state.environment.token), "commits", id, data); err != nil {
		t.Fatal(err)
	}
	if _, _, err := updateRemoteHead(t.Context(), base, testClientLibraryID, []byte(state.environment.token), head.ETag, id); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestLibrarySyncInterrupted100MiBUploadSendsOnlyMissingBlocks(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	var mu sync.Mutex
	attempts := make(map[string]int)
	persisted := make(map[string]bool)
	var interrupt atomic.Bool
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/blocks/") {
			id := filepath.Base(r.URL.Path)
			mu.Lock()
			attempts[id]++
			completed := len(persisted)
			mu.Unlock()
			if interrupt.Load() && completed == 10 {
				http.Error(w, "interrupted", http.StatusServiceUnavailable)
				interrupt.Store(false)
				return
			}
			environment.handler.ServeHTTP(w, r)
			mu.Lock()
			persisted[id] = true
			mu.Unlock()
			return
		}
		environment.handler.ServeHTTP(w, r)
	}))
	defer proxy.Close()

	clientDir, worktree := newClientPaths(t)
	args := append(bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testClientDeviceID), "--import-local")
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	before := readTestBinding(t, clientDir, worktree)
	mu.Lock()
	clear(attempts)
	clear(persisted)
	mu.Unlock()
	file, err := os.Create(filepath.Join(worktree, "large.bin"))
	if err != nil {
		t.Fatal(err)
	}
	expected := make(map[string]bool, 25)
	for index := range 25 {
		block := bytes.Repeat([]byte{byte(index)}, object.MaxBlockSize)
		expected[object.ID(block)] = true
		if _, err := file.Write(block); err != nil {
			file.Close()
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	interrupt.Store(true)
	if err := syncTestWorktree(t, clientDir, worktree); err == nil {
		t.Fatal("interrupted upload succeeded")
	}
	if after := readTestBinding(t, clientDir, worktree); after != before {
		t.Fatalf("interrupted upload changed Base: before=%+v after=%+v", before, after)
	}
	head, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if err != nil || head.CommitID == nil || *head.CommitID != before.SyncBase {
		t.Fatalf("interrupted upload changed Head: head=%+v err=%v", head, err)
	}
	mu.Lock()
	firstPersisted := make(map[string]bool, len(persisted))
	for id := range persisted {
		if expected[id] {
			firstPersisted[id] = true
		}
	}
	mu.Unlock()
	if len(firstPersisted) != 10 {
		t.Fatalf("persisted blocks=%d, want 10", len(firstPersisted))
	}
	if err := syncTestWorktree(t, clientDir, worktree); err != nil {
		t.Fatalf("resume interrupted upload: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	for id := range firstPersisted {
		if attempts[id] != 1 {
			t.Fatalf("persisted block %s PUT count=%d, want 1", id, attempts[id])
		}
	}
	for id := range expected {
		if !persisted[id] {
			t.Fatalf("block %s was not persisted", id)
		}
	}
	if len(expected) != 25 {
		t.Fatalf("distinct block count=%d, want 25", len(expected))
	}
	assertTestConverged(t, environment, clientDir, worktree)
}

func TestLibrarySyncRecursiveMergeResolvesLostCASResponse(t *testing.T) {
	var updates atomic.Int32
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{AfterHeadUpdate: func() error {
		if updates.Add(1) == 4 {
			return errors.New("response lost")
		}
		return nil
	}})
	publisherDir, publisherTree := newClientPaths(t)
	if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("base"), 0o600); err != nil {
		t.Fatal(err)
	}
	args := append(bindArgs(publisherDir, environment.server.URL, testClientLibraryID, publisherTree, testClientDeviceID), "--import-local")
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	subscriberDir, subscriberTree := newClientPaths(t)
	if err := runTest(t.Context(), bindArgs(subscriberDir, environment.server.URL, testClientLibraryID, subscriberTree, testOtherDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "local"), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatalf("recursive lost response sync: %v", err)
	}
	if updates.Load() != 4 {
		t.Fatalf("Head updates=%d", updates.Load())
	}
	assertTestConverged(t, environment, subscriberDir, subscriberTree)
	for name, want := range map[string]string{"local": "local", "remote": "remote"} {
		if data, err := os.ReadFile(filepath.Join(subscriberTree, name)); err != nil || string(data) != want {
			t.Fatalf("merged %s=%q err=%v", name, data, err)
		}
	}
}

func TestLibrarySyncResolvesLostCASResponse(t *testing.T) {
	var updates atomic.Int32
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{AfterHeadUpdate: func() error {
		if updates.Add(1) == 3 {
			return errors.New("response lost")
		}
		return nil
	}})
	clientDir, worktree := newClientPaths(t)
	if err := os.WriteFile(filepath.Join(worktree, "initial"), []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}
	args := append(bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID), "--import-local")
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktree, "local"), []byte("two"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, clientDir, worktree); err != nil {
		t.Fatalf("lost response sync: %v", err)
	}
	assertTestConverged(t, environment, clientDir, worktree)
}

func TestLibrarySyncRecoversPublishedCandidateBeforeDiscardingMutatedWorktree(t *testing.T) {
	var updates atomic.Int32
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{AfterHeadUpdate: func() error {
		if updates.Add(1) == 3 {
			return errors.New("response lost")
		}
		return nil
	}})
	var published, failedGet atomic.Bool
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/head") && published.Load() && failedGet.CompareAndSwap(false, true) {
			http.Error(w, "unavailable", http.StatusInternalServerError)
			return
		}
		environment.handler.ServeHTTP(w, r)
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/head") && updates.Load() == 3 {
			published.Store(true)
		}
	}))
	defer proxy.Close()
	clientDir, worktree := newClientPaths(t)
	if err := os.WriteFile(filepath.Join(worktree, "base"), []byte("base"), 0o600); err != nil {
		t.Fatal(err)
	}
	args := append(bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testClientDeviceID), "--import-local")
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktree, "published"), []byte("candidate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, clientDir, worktree); err == nil {
		t.Fatal("expected unknown publication result")
	}
	pending := readTestPendingPublication(t, clientDir, worktree)
	if err := os.WriteFile(filepath.Join(worktree, "published"), []byte("local mutation"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktree, "after-cas"), []byte("preserved"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := syncTestWorktree(t, clientDir, worktree)
	if err == nil || !strings.Contains(err.Error(), "preserving local changes; rerun sync") {
		t.Fatalf("published candidate recovery error=%v", err)
	}
	binding := readTestBinding(t, clientDir, worktree)
	if binding.SyncBase != pending.CandidateCommit || binding.SyncBaseRoot != pending.CandidateRoot {
		t.Fatalf("recovered binding=%+v pending=%+v", binding, pending)
	}
	if count := countClientRows(t, clientDir, "pending_publications", worktree); count != 0 {
		t.Fatalf("pending rows=%d", count)
	}
	if count := countClientRows(t, clientDir, "path_index", worktree); count != 2 {
		t.Fatalf("candidate path index rows=%d", count)
	}
	for name, want := range map[string]string{"published": "local mutation", "after-cas": "preserved"} {
		if data, err := os.ReadFile(filepath.Join(worktree, name)); err != nil || string(data) != want {
			t.Fatalf("local %s=%q err=%v", name, data, err)
		}
	}
	if err := syncTestWorktree(t, clientDir, worktree); err != nil {
		t.Fatal(err)
	}
	if binding := assertTestConverged(t, environment, clientDir, worktree); binding.SyncBase == pending.CandidateCommit {
		t.Fatal("next sync did not publish the preserved local changes as a successor")
	}
}

func TestLibrarySyncRemoteApplyRejectsLocalSaveBeforeRecovery(t *testing.T) {
	_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	before := readTestBinding(t, subscriberDir, subscriberTree)
	if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			beforeSyncRecoveryPrepare: func() error {
				return os.WriteFile(filepath.Join(subscriberTree, "local"), []byte("saved"), 0o600)
			}})
	if err == nil || !strings.Contains(err.Error(), "merge is not supported") {
		t.Fatalf("local save barrier error=%v", err)
	}
	if after := readTestBinding(t, subscriberDir, subscriberTree); after != before {
		t.Fatalf("local save advanced binding: before=%+v after=%+v", before, after)
	}
	for _, name := range []string{"base", "local"} {
		if _, err := os.Stat(filepath.Join(subscriberTree, name)); err != nil {
			t.Fatalf("local save path %q moved: %v", name, err)
		}
	}
	assertNoSyncInternalPaths(t, subscriberTree)
}

func TestLibrarySyncMixedPromotionRollbackRestoresCapturedInode(t *testing.T) {
	for _, nested := range []bool{false, true} {
		name := "root"
		promotedPath := "base"
		if nested {
			name = "nested"
			promotedPath = "folder/base"
		}
		t.Run(name, func(t *testing.T) {
			environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
			if nested {
				if err := os.MkdirAll(filepath.Join(publisherTree, "folder"), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(publisherTree, promotedPath), []byte("initial"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(publisherTree, "z-second"), []byte("second-original"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(publisherTree, promotedPath), []byte("remote"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(publisherTree, "z-second"), []byte("second-remote"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(subscriberTree, promotedPath), []byte("local"), 0o600); err != nil {
				t.Fatal(err)
			}
			promotedBefore, err := os.Stat(filepath.Join(subscriberTree, promotedPath))
			if err != nil {
				t.Fatal(err)
			}
			secondBefore, err := os.Stat(filepath.Join(subscriberTree, "z-second"))
			if err != nil {
				t.Fatal(err)
			}
			hidden := make(map[string]string)
			var applyBinding clientBinding
			var disturbed, repaired atomic.Bool
			config := libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
				afterSyncRecoveryRename: func(path, recoveryName string) error {
					hidden[path] = recoveryName
					if applyBinding.Worktree == "" {
						applyBinding = readTestBinding(t, subscriberDir, subscriberTree)
					}
					return nil
				},
				fsActionFault: func(point string, action fsAction) error {
					if point == "after_completed" && action.Op == fsOpRename && action.ExpectedObject != "" &&
						action.OriginActionID == "" && disturbed.CompareAndSwap(false, true) {
						changed := secondBefore.ModTime().Add(time.Hour)
						return os.Chtimes(filepath.Join(subscriberTree, hidden["z-second"]), changed, changed)
					}
					if point == "before_intent_commit" && action.Op == fsOpRestorePromotion && repaired.CompareAndSwap(false, true) {
						return os.Chtimes(filepath.Join(subscriberTree, hidden["z-second"]), secondBefore.ModTime(), secondBefore.ModTime())
					}
					return nil
				}}
			err = runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
				strings.NewReader(""), io.Discard, io.Discard, config)
			if err == nil || !strings.Contains(err.Error(), "captured local content changed") || !disturbed.Load() || !repaired.Load() {
				t.Fatalf("mixed promotion rollback error=%v disturbed=%v repaired=%v", err, disturbed.Load(), repaired.Load())
			}
			for path, wantContent := range map[string]string{promotedPath: "local", "z-second": "second-original"} {
				data, err := os.ReadFile(filepath.Join(subscriberTree, path))
				if err != nil || string(data) != wantContent {
					t.Fatalf("restored %q=%q err=%v", path, data, err)
				}
			}
			promotedAfter, err := os.Stat(filepath.Join(subscriberTree, promotedPath))
			if err != nil || !os.SameFile(promotedBefore, promotedAfter) || !promotedBefore.ModTime().Equal(promotedAfter.ModTime()) {
				t.Fatalf("promoted inode/mtime changed before=%v after=%v err=%v", promotedBefore, promotedAfter, err)
			}
			secondAfter, err := os.Stat(filepath.Join(subscriberTree, "z-second"))
			if err != nil || !os.SameFile(secondBefore, secondAfter) || !secondBefore.ModTime().Equal(secondAfter.ModTime()) {
				t.Fatalf("second inode/mtime changed before=%v after=%v err=%v", secondBefore, secondAfter, err)
			}
			if after := readTestBinding(t, subscriberDir, subscriberTree); after != applyBinding {
				t.Fatalf("mixed rollback changed binding before=%+v after=%+v", applyBinding, after)
			}
			for _, table := range []string{"pending_checkouts", "checkout_paths", "sync_recoveries", "sync_recovery_promotions", "fs_actions"} {
				if count := countClientRows(t, subscriberDir, table, subscriberTree); count != 0 {
					t.Fatalf("%s rows=%d", table, count)
				}
			}
			assertNoSyncInternalPaths(t, subscriberTree)
			if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
				t.Fatalf("sync after mixed rollback: %v", err)
			}
			assertTestConverged(t, environment, subscriberDir, subscriberTree)
		})
	}
}

func TestRestoreRollbackRootMtimeRejectsStaleActionEvidence(t *testing.T) {
	for _, test := range []struct {
		name, state, corruptColumn string
		changeRoot, laterAction    bool
	}{
		{name: "intent parent device", state: fsStateIntent, corruptColumn: "parent_device"},
		{name: "intent parent inode", state: fsStateIntent, corruptColumn: "parent_inode"},
		{name: "completed parent device", state: fsStateCompleted, corruptColumn: "parent_device"},
		{name: "completed parent inode", state: fsStateCompleted, corruptColumn: "parent_inode"},
		{name: "completed stale mtime", state: fsStateCompleted, changeRoot: true},
		{name: "completed stale order", state: fsStateCompleted, laterAction: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			clientDir, worktree := t.TempDir(), t.TempDir()
			db, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			root, err := openWorktreeRoot(worktree, func(*os.File) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			if err := bindFSJournalRoot(t.Context(), db, worktree, root); err != nil {
				t.Fatal(err)
			}
			var stat syscall.Stat_t
			if err := syscall.Fstat(int(root.directory.Fd()), &stat); err != nil {
				t.Fatal(err)
			}
			mtimeNS := time.Unix(stat.Mtim.Sec, stat.Mtim.Nsec).UnixNano()
			if err := savePendingCheckout(t.Context(), db, pendingCheckout{ServerURL: "http://localhost", LibraryID: "library",
				Worktree: worktree, UserID: "user", DeviceID: "device", TargetCommit: "commit", ApplyState: "rolling_back",
				ConflictPromotions: _emptyConflictPromotions, RollbackRootMtimeNS: mtimeNS, RollbackRootMtimeValid: true}); err != nil {
				t.Fatal(err)
			}
			action := fsAction{Worktree: worktree, ActionID: "00112233445566778899aabbccddeeff", Order: 0,
				Phase: fsPhaseRollback, Op: fsOpMtime, ParentDevice: root.device, ParentInode: root.inode,
				ExpectedKind: "Directory", ExpectedDevice: root.device, ExpectedInode: root.inode,
				ExpectedMtime: time.Unix(0, mtimeNS).UTC().Format(time.RFC3339Nano), State: fsStateIntent}
			if err := insertFSActionIntent(t.Context(), db, action); err != nil {
				t.Fatal(err)
			}
			if test.state == fsStateCompleted {
				if _, err := db.Exec(`UPDATE fs_actions SET state='completed' WHERE worktree=? AND action_id=?`, worktree, action.ActionID); err != nil {
					t.Fatal(err)
				}
			}
			if test.corruptColumn != "" {
				if _, err := db.Exec(`UPDATE fs_actions SET `+test.corruptColumn+`=`+test.corruptColumn+`+1
					WHERE worktree=? AND action_id=?`, worktree, action.ActionID); err != nil {
					t.Fatal(err)
				}
			}
			if test.laterAction {
				later := action
				later.ActionID, later.Order, later.Source, later.State = "11223344556677889900aabbccddeeff", 1, "later", fsStateCompleted
				later.ExpectedKind = "File"
				if err := insertFSActionIntent(t.Context(), db, later); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`UPDATE fs_actions SET state='completed' WHERE worktree=? AND action_id=?`, worktree, later.ActionID); err != nil {
					t.Fatal(err)
				}
			}
			if test.changeRoot {
				changed := time.Unix(0, mtimeNS).Add(time.Second)
				if err := os.Chtimes(worktree, changed, changed); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.Stat(worktree)
			if err != nil {
				t.Fatal(err)
			}
			if err := restoreRollbackRootMtime(t.Context(), db, root, worktree, nil); err == nil {
				t.Fatal("stale root mtime action evidence was accepted")
			}
			after, err := os.Stat(worktree)
			if err != nil || !after.ModTime().Equal(before.ModTime()) {
				t.Fatalf("rejection changed root mtime before=%v after=%v err=%v", before.ModTime(), after.ModTime(), err)
			}
			var pending, actions int
			if err := db.QueryRow(`SELECT COUNT(*) FROM pending_checkouts WHERE worktree=? AND apply_state='rolling_back'`, worktree).Scan(&pending); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT COUNT(*) FROM fs_actions WHERE worktree=?`, worktree).Scan(&actions); err != nil {
				t.Fatal(err)
			}
			wantActions := 1
			if test.laterAction {
				wantActions = 2
			}
			if pending != 1 || actions != wantActions {
				t.Fatalf("rejection cleaned durable state pending=%d actions=%d", pending, actions)
			}
		})
	}
}

func TestRestoreRollbackRootMtimeRejectsWrongRootBeforeStateOrFSChanges(t *testing.T) {
	for _, state := range []string{fsStateIntent, fsStateCompleted} {
		for _, scenario := range []string{"path replacement", "wrong worktree argument"} {
			t.Run(state+"/"+scenario, func(t *testing.T) {
				clientDir, worktree := t.TempDir(), t.TempDir()
				db, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				root, err := openWorktreeRoot(worktree, func(*os.File) error { return nil })
				if err != nil {
					t.Fatal(err)
				}
				defer root.Close()
				if err := bindFSJournalRoot(t.Context(), db, worktree, root); err != nil {
					t.Fatal(err)
				}
				var stat syscall.Stat_t
				if err := syscall.Fstat(int(root.directory.Fd()), &stat); err != nil {
					t.Fatal(err)
				}
				mtimeNS := time.Unix(stat.Mtim.Sec, stat.Mtim.Nsec).UnixNano()
				if err := savePendingCheckout(t.Context(), db, pendingCheckout{ServerURL: "http://localhost", LibraryID: "library",
					Worktree: worktree, UserID: "user", DeviceID: "device", TargetCommit: "commit", ApplyState: "rolling_back",
					ConflictPromotions: _emptyConflictPromotions, RollbackRootMtimeNS: mtimeNS, RollbackRootMtimeValid: true}); err != nil {
					t.Fatal(err)
				}
				action := fsAction{Worktree: worktree, ActionID: "00112233445566778899aabbccddeeff", Order: 0,
					Phase: fsPhaseRollback, Op: fsOpMtime, ParentDevice: root.device, ParentInode: root.inode,
					ExpectedKind: "Directory", ExpectedDevice: root.device, ExpectedInode: root.inode,
					ExpectedMtime: time.Unix(0, mtimeNS).UTC().Format(time.RFC3339Nano), State: fsStateIntent}
				if err := insertFSActionIntent(t.Context(), db, action); err != nil {
					t.Fatal(err)
				}
				if state == fsStateCompleted {
					if _, err := db.Exec(`UPDATE fs_actions SET state='completed' WHERE worktree=?`, worktree); err != nil {
						t.Fatal(err)
					}
				}
				callWorktree := worktree + "-wrong"
				oldPath, replacementPath := worktree, ""
				if scenario == "path replacement" {
					oldPath = worktree + "-opened"
					if err := os.Rename(worktree, oldPath); err != nil {
						t.Fatal(err)
					}
					if err := os.Mkdir(worktree, 0o700); err != nil {
						t.Fatal(err)
					}
					replacementPath = worktree
					callWorktree = worktree
				}
				oldBefore, err := os.Stat(oldPath)
				if err != nil {
					t.Fatal(err)
				}
				var replacementBefore os.FileInfo
				if replacementPath != "" {
					replacementBefore, err = os.Stat(replacementPath)
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := restoreRollbackRootMtime(t.Context(), db, root, callWorktree, nil); err == nil {
					t.Fatal("wrong rollback root was accepted")
				}
				oldAfter, err := os.Stat(oldPath)
				if err != nil || !oldAfter.ModTime().Equal(oldBefore.ModTime()) {
					t.Fatalf("opened tree changed before=%v after=%v err=%v", oldBefore, oldAfter, err)
				}
				if replacementPath != "" {
					replacementAfter, err := os.Stat(replacementPath)
					if err != nil || !replacementAfter.ModTime().Equal(replacementBefore.ModTime()) {
						t.Fatalf("replacement tree changed before=%v after=%v err=%v", replacementBefore, replacementAfter, err)
					}
				}
				var pending, actions int
				if err := db.QueryRow(`SELECT COUNT(*) FROM pending_checkouts WHERE worktree=?`, worktree).Scan(&pending); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRow(`SELECT COUNT(*) FROM fs_actions WHERE worktree=?`, worktree).Scan(&actions); err != nil {
					t.Fatal(err)
				}
				if pending != 1 || actions != 1 {
					t.Fatalf("rejection changed durable rows pending=%d actions=%d", pending, actions)
				}
			})
		}
	}
}

func TestPublicCheckoutRecoveryRejectsCorruptRootMtimeStateBeforeFSChanges(t *testing.T) {
	for _, test := range []struct {
		name, command, state string
		valid                int64
	}{
		{name: "sync applying without mtime", command: "sync", state: "applying"},
		{name: "sync invalid valid flag", command: "sync", state: "rolling_back", valid: 2},
		{name: "unbind finalized without mtime", command: "unbind", state: "finalized"},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := newImportedBinding(t)
			db, err := openClientDB(filepath.Join(state.clientDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			root, err := openWorktreeRoot(state.worktree, func(*os.File) error { return nil })
			if err != nil {
				db.Close()
				t.Fatal(err)
			}
			if err := bindFSJournalRoot(t.Context(), db, state.worktree, root); err != nil {
				root.Close()
				db.Close()
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(state.worktree, "action-source"), []byte("source"), 0o600); err != nil {
				root.Close()
				db.Close()
				t.Fatal(err)
			}
			var source syscall.Stat_t
			if err := syscall.Stat(filepath.Join(state.worktree, "action-source"), &source); err != nil {
				root.Close()
				db.Close()
				t.Fatal(err)
			}
			action := fsAction{Worktree: state.worktree, ActionID: "00112233445566778899aabbccddeeff", Order: 0,
				Phase: fsPhasePreBase, Op: fsOpRename, ParentDevice: root.device, ParentInode: root.inode,
				Source: "action-source", Target: "action-target", ExpectedKind: "File", ExpectedDevice: uint64(source.Dev),
				ExpectedInode: source.Ino, State: fsStateIntent}
			if err := insertFSActionIntent(t.Context(), db, action); err != nil {
				root.Close()
				db.Close()
				t.Fatal(err)
			}
			if err := savePendingCheckout(t.Context(), db, pendingCheckout{ServerURL: state.binding.ServerURL,
				LibraryID: state.binding.LibraryID, Worktree: state.worktree, UserID: state.binding.UserID,
				DeviceID: state.binding.DeviceID, TargetCommit: state.binding.SyncBase, TargetRoot: state.binding.SyncBaseRoot,
				HeadETag: state.binding.HeadETag, ConflictPromotions: _emptyConflictPromotions}); err != nil {
				root.Close()
				db.Close()
				t.Fatal(err)
			}
			if _, err := db.Exec(`PRAGMA ignore_check_constraints=ON`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE pending_checkouts SET apply_state=?,rollback_root_mtime_valid=? WHERE worktree=?`,
				test.state, test.valid, state.worktree); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`PRAGMA ignore_check_constraints=OFF`); err != nil {
				t.Fatal(err)
			}
			if err := root.Close(); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			args := []string{"--client-dir", state.clientDir, "--worktree", state.worktree}
			var runErr error
			if test.command == "sync" {
				runErr = runLibrarySync(t.Context(), args, io.Discard, io.Discard,
					libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }})
			} else {
				runErr = runLibraryUnbind(t.Context(), args, io.Discard, io.Discard,
					libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }})
			}
			if runErr == nil || !strings.Contains(runErr.Error(), "state is corrupt") {
				t.Fatalf("public %s accepted corrupt state: %v", test.command, runErr)
			}
			if data, err := os.ReadFile(filepath.Join(state.worktree, "action-source")); err != nil || string(data) != "source" {
				t.Fatalf("source changed data=%q err=%v", data, err)
			}
			if _, err := os.Lstat(filepath.Join(state.worktree, "action-target")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("target created err=%v", err)
			}
			db, err = openClientDB(filepath.Join(state.clientDir, _clientDatabaseName), true)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var pending, actions, bindings int
			for table, destination := range map[string]*int{"pending_checkouts": &pending, "fs_actions": &actions, "bindings": &bindings} {
				if err := db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE worktree=?", state.worktree).Scan(destination); err != nil {
					t.Fatal(err)
				}
			}
			if pending != 1 || actions != 1 || bindings != 1 {
				t.Fatalf("public rejection changed rows pending=%d actions=%d bindings=%d", pending, actions, bindings)
			}
		})
	}
}

func TestRegisterSyncRecoveryPlanPreservesEpochRootMtime(t *testing.T) {
	clientDir, worktree := t.TempDir(), t.TempDir()
	epoch := time.Unix(0, 0)
	if err := os.Chtimes(worktree, epoch, epoch); err != nil {
		t.Fatal(err)
	}
	db, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root, err := openWorktreeRoot(worktree, func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := savePendingCheckout(t.Context(), db, pendingCheckout{ServerURL: "http://localhost", LibraryID: "library",
		Worktree: worktree, UserID: "user", DeviceID: "device", TargetCommit: "commit",
		ConflictPromotions: _emptyConflictPromotions}); err != nil {
		t.Fatal(err)
	}
	if err := registerSyncRecoveryPlan(t.Context(), db, root, worktree, nil, libraryClientConfig{}); err != nil {
		t.Fatal(err)
	}
	var state string
	var mtime int64
	var valid bool
	if err := db.QueryRow(`SELECT apply_state,rollback_root_mtime_ns,rollback_root_mtime_valid
		FROM pending_checkouts WHERE worktree=?`, worktree).Scan(&state, &mtime, &valid); err != nil {
		t.Fatal(err)
	}
	if state != "applying" || mtime != 0 || !valid {
		t.Fatalf("epoch transition state=%q mtime=%d valid=%v", state, mtime, valid)
	}
}

func TestLibrarySyncRestorePromotionCrashMatrix(t *testing.T) {
	points := []string{"before_intent_commit", "after_intent_commit", "after_action", "after_parent_sync", "after_completed"}
	type crashStage struct{ op, point string }
	for _, nested := range []bool{false, true} {
		stages := make([]crashStage, 0, len(points)*2)
		for _, point := range points {
			stages = append(stages, crashStage{fsOpRestorePromotion, point})
			if !nested {
				stages = append(stages, crashStage{fsOpMtime, point})
			}
		}
		for _, stage := range stages {
			op, point := stage.op, stage.point
			name := "root/" + op + "/" + point
			promotedPath := "base"
			if nested {
				name = "nested/" + op + "/" + point
				promotedPath = "folder/base"
			}
			t.Run(name, func(t *testing.T) {
				environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
				publisherDir, publisherTree := newClientPaths(t)
				if err := os.MkdirAll(filepath.Dir(filepath.Join(publisherTree, promotedPath)), 0o700); err != nil {
					t.Fatal(err)
				}
				for path, content := range map[string]string{promotedPath: "initial", "z-second": "second-original"} {
					if err := os.WriteFile(filepath.Join(publisherTree, path), []byte(content), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				publisherArgs := append(bindArgs(publisherDir, environment.server.URL, testClientLibraryID,
					publisherTree, testClientDeviceID), "--import-local")
				if err := runTest(t.Context(), publisherArgs, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
					t.Fatal(err)
				}
				var failRequests atomic.Bool
				proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if failRequests.Load() {
						http.Error(w, "restart barrier", http.StatusServiceUnavailable)
						return
					}
					environment.handler.ServeHTTP(w, r)
				}))
				t.Cleanup(proxy.Close)
				subscriberDir, subscriberTree := newClientPaths(t)
				if err := runTest(t.Context(), bindArgs(subscriberDir, proxy.URL, testClientLibraryID, subscriberTree,
					testOtherDeviceID), strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
					t.Fatal(err)
				}
				for path, content := range map[string]string{promotedPath: "remote", "z-second": "second-remote"} {
					if err := os.WriteFile(filepath.Join(publisherTree, path), []byte(content), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(subscriberTree, promotedPath), []byte("local"), 0o600); err != nil {
					t.Fatal(err)
				}
				initialTree := captureExactTree(t, subscriberTree)
				setupErr := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir,
					"--worktree", subscriberTree}, strings.NewReader(""), io.Discard, io.Discard,
					libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
						afterSyncCheckoutTransition: func() error { return errors.New("capture apply baseline") }})
				if setupErr == nil || !strings.Contains(setupErr.Error(), "capture apply baseline") {
					t.Fatalf("establish preapply transition error=%v", setupErr)
				}
				assertExactTree(t, subscriberTree, initialTree)
				if state := readPendingCheckoutState(t, subscriberDir, subscriberTree); state != "pending" {
					t.Fatalf("preapply checkout state=%q want pending", state)
				}

				preapplyTree := captureExactTree(t, subscriberTree)
				preapplyBinding := captureTestBinding(t, subscriberDir, subscriberTree)
				preapplyIndex := captureTestPathIndex(t, subscriberDir, subscriberTree)
				preapplyHead, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID,
					[]byte(environment.token))
				if err != nil || preapplyHead.CommitID == nil {
					t.Fatalf("read preapply Head: head=%+v err=%v", preapplyHead, err)
				}
				baselineTables := []string{"pending_publications", "pending_checkouts", "checkout_paths", "sync_recoveries",
					"sync_recovery_promotions", "fs_actions"}
				preapplyCounts := make(map[string]int, len(baselineTables))
				for _, table := range baselineTables {
					preapplyCounts[table] = countClientRows(t, subscriberDir, table, subscriberTree)
					want := 0
					if table == "pending_checkouts" {
						want = 1
					}
					if preapplyCounts[table] != want {
						t.Fatalf("unexpected preapply %s rows=%d want=%d", table, preapplyCounts[table], want)
					}
				}
				beforeJournalBindings := captureTestJournalBindings(t, subscriberDir, subscriberTree)

				crashKind := "File"
				if op == fsOpMtime {
					crashKind = "Directory"
				}
				crash := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
				crash.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1",
					"FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree,
					"FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhaseRollback, "FILECLOUD_PUBLIC_CRASH_OP="+op,
					"FILECLOUD_PUBLIC_CRASH_KIND="+crashKind, "FILECLOUD_PUBLIC_CRASH_POINT="+point,
					"FILECLOUD_PUBLIC_CRASH_ROLE=mixed-restore-promotion", "FILECLOUD_PUBLIC_SECOND_PATH=z-second")
				assertProcessSIGKILL(t, crash.Run())
				if state := readPendingCheckoutState(t, subscriberDir, subscriberTree); state != "rolling_back" {
					t.Fatalf("crashed mixed checkout state=%q", state)
				}
				db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), true)
				if err != nil {
					t.Fatal(err)
				}
				var promoted, secondRecovery int
				promotedErr := db.QueryRow(`SELECT COUNT(*) FROM sync_recovery_promotions
					WHERE worktree=? AND source_path=?`, subscriberTree, promotedPath).Scan(&promoted)
				secondErr := db.QueryRow(`SELECT COUNT(*) FROM sync_recoveries
					WHERE worktree=? AND path='z-second'`, subscriberTree).Scan(&secondRecovery)
				if err := errors.Join(promotedErr, secondErr, db.Close()); err != nil {
					t.Fatal(err)
				}
				wantRecoveryRows := 1
				if op == fsOpMtime {
					wantRecoveryRows = 0
				}
				if promoted != wantRecoveryRows || secondRecovery != wantRecoveryRows {
					t.Fatalf("completed first promotion links=%d different failing recoveries=%d want=%d",
						promoted, secondRecovery, wantRecoveryRows)
				}

				failRequests.Store(true)
				if err := syncTestWorktree(t, subscriberDir, subscriberTree); err == nil || !strings.Contains(err.Error(), "Service Unavailable") {
					t.Fatalf("rollback restart barrier error=%v", err)
				}
				assertExactTree(t, subscriberTree, preapplyTree)
				postrollbackBinding := captureTestBinding(t, subscriberDir, subscriberTree)
				if !reflect.DeepEqual(postrollbackBinding, preapplyBinding) {
					t.Fatalf("binding differs preapply vs postrollback: preapply=%+v postrollback=%+v access_token_equal=%v",
						preapplyBinding.binding, postrollbackBinding.binding,
						bytes.Equal(preapplyBinding.accessToken, postrollbackBinding.accessToken))
				}
				if postrollbackIndex := captureTestPathIndex(t, subscriberDir, subscriberTree); !reflect.DeepEqual(postrollbackIndex, preapplyIndex) {
					t.Fatalf("path_index differs preapply vs postrollback: preapply=%+v postrollback=%+v", preapplyIndex, postrollbackIndex)
				}
				for _, table := range baselineTables {
					if count := countClientRows(t, subscriberDir, table, subscriberTree); count != 0 {
						t.Fatalf("postrollback %s rows=%d; preapply=%d", table, count, preapplyCounts[table])
					}
				}
				afterJournalBindings := captureTestJournalBindings(t, subscriberDir, subscriberTree)
				if !reflect.DeepEqual(afterJournalBindings, beforeJournalBindings) {
					t.Fatalf("fs_journal_bindings differ preapply vs postrollback: preapply=%+v postrollback=%+v",
						beforeJournalBindings, afterJournalBindings)
				}
				assertNoSyncInternalPaths(t, subscriberTree)
				postrollbackHead, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID,
					[]byte(environment.token))
				if err != nil || postrollbackHead.CommitID == nil || *postrollbackHead.CommitID != *preapplyHead.CommitID ||
					postrollbackHead.ETag != preapplyHead.ETag {
					t.Fatalf("Head differs preapply vs postrollback: preapply=%+v postrollback=%+v err=%v",
						preapplyHead, postrollbackHead, err)
				}
				failRequests.Store(false)

				if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
					t.Fatalf("sync after mixed rollback: %v", err)
				}
				assertTestConverged(t, environment, subscriberDir, subscriberTree)
				remote, remoteErr := os.ReadFile(filepath.Join(subscriberTree, promotedPath))
				parent, leaf := filepath.Split(filepath.Join(subscriberTree, promotedPath))
				matches, globErr := filepath.Glob(filepath.Join(parent,
					strings.TrimSuffix(leaf, filepath.Ext(leaf))+" (Filecloud conflict *)"+filepath.Ext(leaf)))
				if remoteErr != nil || globErr != nil || string(remote) != "remote" || len(matches) != 1 {
					t.Fatalf("converged promoted file=%q/%v conflicts=%v/%v", remote, remoteErr, matches, globErr)
				}
				local, err := os.ReadFile(matches[0])
				second, secondErr := os.ReadFile(filepath.Join(subscriberTree, "z-second"))
				if err != nil || secondErr != nil || string(local) != "local" || string(second) != "second-remote" {
					t.Fatalf("converged contents local=%q/%v second=%q/%v", local, err, second, secondErr)
				}
			})
		}
	}
}

func TestRestorePromotionOwnershipRejectsInvalidStatesWithoutFSChanges(t *testing.T) {
	for _, scenario := range []string{"orphan", "wrong target", "wrong identity"} {
		t.Run(scenario, func(t *testing.T) {
			_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
			if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("remote"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(subscriberTree, "base"), []byte("local"), 0o600); err != nil {
				t.Fatal(err)
			}
			setup := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
			setup.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1",
				"FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree,
				"FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase, "FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename,
				"FILECLOUD_PUBLIC_CRASH_KIND=File", "FILECLOUD_PUBLIC_CRASH_POINT=after_completed",
				"FILECLOUD_PUBLIC_CRASH_ROLE=promotion")
			assertProcessSIGKILL(t, setup.Run())
			db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := beginSyncRollback(t.Context(), db, subscriberTree); err != nil {
				t.Fatal(err)
			}
			recoveries, err := loadSyncRecoveries(t.Context(), db, subscriberTree)
			if err != nil || len(recoveries) != 1 {
				t.Fatalf("recoveries=%+v err=%v", recoveries, err)
			}
			links, err := loadSyncRecoveryPromotions(t.Context(), db, subscriberTree)
			if err != nil || len(links) != 1 {
				t.Fatalf("links=%+v err=%v", links, err)
			}
			promotions, err := loadConflictPromotions(t.Context(), db, subscriberTree)
			if err != nil {
				t.Fatal(err)
			}
			target, _, err := linkedPromotionProvenance(recoveries[0], links[0], promotions)
			if err != nil {
				t.Fatal(err)
			}
			actions, err := loadFSActions(t.Context(), db, subscriberTree)
			if err != nil {
				t.Fatal(err)
			}
			var current fsAction
			var order int64
			for _, action := range actions {
				if action.ActionID == links[0].currentActionID {
					current = action
				}
				order = max(order, action.Order+1)
			}
			root, err := openWorktreeRoot(subscriberTree, func(*os.File) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			parentPath, leaf := splitFSActionPath(current.Target)
			parent, openedLeaf, err := promotionTargetParent(t.Context(), db, root, subscriberTree, current.Target)
			if err != nil || openedLeaf != leaf {
				t.Fatalf("source parent leaf=%q err=%v", openedLeaf, err)
			}
			var parentStat syscall.Stat_t
			if err := syscall.Fstat(int(parent.Fd()), &parentStat); err != nil {
				parent.Close()
				t.Fatal(err)
			}
			if err := parent.Close(); err != nil {
				t.Fatal(err)
			}
			id, err := newFSActionID()
			if err != nil {
				t.Fatal(err)
			}
			restore := fsAction{Worktree: subscriberTree, ActionID: id, Order: order, Phase: fsPhaseRollback,
				Op: fsOpRestorePromotion, Parent: parentPath, ParentDevice: uint64(parentStat.Dev), ParentInode: parentStat.Ino,
				Source: leaf, Target: target, ExpectedKind: "File", ExpectedDevice: current.ExpectedDevice,
				ExpectedInode: current.ExpectedInode, State: fsStateIntent}
			if scenario == "wrong target" {
				restore.Target = syncRecoveryPrefix + "00112233445566778899aabbccddeeff"
			}
			if scenario == "wrong identity" {
				restore.ExpectedInode++
			}
			if err := insertFSActionIntent(t.Context(), db, restore); err != nil {
				t.Fatal(err)
			}
			if scenario != "orphan" {
				if _, err := db.Exec(`UPDATE sync_recovery_promotions SET rollback_action_id=? WHERE worktree=?`, id, subscriberTree); err != nil {
					t.Fatal(err)
				}
			}
			before := captureExactTree(t, subscriberTree)
			if err := validatePendingPromotionTargets(t.Context(), db, subscriberTree); err == nil {
				t.Fatal("invalid restore promotion ownership was accepted")
			}
			if err := recoverFSActions(t.Context(), db, subscriberTree, root, nil); err == nil {
				t.Fatal("invalid restore promotion was replayed")
			}
			before["."] = captureExactTree(t, subscriberTree)["."]
			assertExactTree(t, subscriberTree, before)
		})
	}
}

func TestLibrarySyncPromotionCollisionRollbackPreservesSuccessor(t *testing.T) {
	_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	if err := os.WriteFile(filepath.Join(publisherTree, "z-second"), []byte("second-original"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(publisherTree, "z-second"), []byte("second-remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "base"), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	baseBefore, err := os.Stat(filepath.Join(subscriberTree, "base"))
	if err != nil {
		t.Fatal(err)
	}
	secondBefore, err := os.Stat(filepath.Join(subscriberTree, "z-second"))
	if err != nil {
		t.Fatal(err)
	}
	hidden := make(map[string]string)
	var collision, disturbed, repaired atomic.Bool
	config := libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
		afterSyncRecoveryRename: func(path, name string) error { hidden[path] = name; return nil },
		fsActionFault: func(point string, action fsAction) error {
			if point == "before_action" && action.Op == fsOpRename && action.ExpectedObject != "" &&
				action.OriginActionID == "" && collision.CompareAndSwap(false, true) {
				return os.WriteFile(filepath.Join(subscriberTree, filepath.FromSlash(action.Target)), []byte("collision"), 0o600)
			}
			if point == "after_completed" && action.Op == fsOpRename && action.ExpectedObject != "" &&
				action.OriginActionID == "" && disturbed.CompareAndSwap(false, true) {
				changed := secondBefore.ModTime().Add(time.Hour)
				return os.Chtimes(filepath.Join(subscriberTree, hidden["z-second"]), changed, changed)
			}
			if point == "before_intent_commit" && action.Op == fsOpRestorePromotion && repaired.CompareAndSwap(false, true) {
				return os.Chtimes(filepath.Join(subscriberTree, hidden["z-second"]), secondBefore.ModTime(), secondBefore.ModTime())
			}
			return nil
		}}
	err = runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, config)
	if err == nil || !strings.Contains(err.Error(), "captured local content changed") {
		t.Fatalf("collision rollback error=%v", err)
	}
	data, err := os.ReadFile(filepath.Join(subscriberTree, "base"))
	baseAfter, statErr := os.Stat(filepath.Join(subscriberTree, "base"))
	if err != nil || statErr != nil || string(data) != "local" || !os.SameFile(baseBefore, baseAfter) {
		t.Fatalf("captured base data=%q read=%v stat=%v", data, err, statErr)
	}
	entries, err := os.ReadDir(subscriberTree)
	if err != nil {
		t.Fatal(err)
	}
	var successors []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "base (Filecloud conflict ") {
			successors = append(successors, filepath.Join(subscriberTree, entry.Name()))
		}
	}
	if len(successors) != 1 {
		t.Fatalf("collision successors=%v entries=%v", successors, entries)
	}
	if data, err := os.ReadFile(successors[0]); err != nil || string(data) != "collision" {
		t.Fatalf("collision successor data=%q err=%v", data, err)
	}
	assertNoSyncInternalPaths(t, subscriberTree)
}

func TestLibrarySyncRemoteApplyPreservesOpenFileModification(t *testing.T) {
	_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	before := readTestBinding(t, subscriberDir, subscriberTree)
	if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	held, err := os.OpenFile(filepath.Join(subscriberTree, "base"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	err = runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			beforeCheckoutMaterialize: func() error {
				if _, err := held.WriteAt([]byte("late-write"), 0); err != nil {
					return err
				}
				return held.Sync()
			}})
	if err == nil || !strings.Contains(err.Error(), "captured local content changed") {
		t.Fatalf("open file modification error=%v", err)
	}
	if after := readTestBinding(t, subscriberDir, subscriberTree); after != before {
		t.Fatalf("open file modification advanced binding: before=%+v after=%+v", before, after)
	}
	found := false
	entries, err := os.ReadDir(subscriberTree)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), syncRecoveryPrefix) {
			data, err := os.ReadFile(filepath.Join(subscriberTree, entry.Name()))
			if err == nil && strings.HasPrefix(string(data), "late-write") {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("modified captured inode was not preserved in registered recovery")
	}
}

func TestLibrarySyncRecoveryPreparationRetriesEveryBoundary(t *testing.T) {
	tests := []struct {
		name   string
		config func(*atomic.Bool) libraryClientConfig
	}{
		{"registered", func(failed *atomic.Bool) libraryClientConfig {
			return libraryClientConfig{afterSyncRecoveryRegistered: func(string) error {
				if failed.CompareAndSwap(false, true) {
					return errors.New("registered boundary")
				}
				return nil
			}}
		}},
		{"before rename", func(failed *atomic.Bool) libraryClientConfig {
			return libraryClientConfig{beforeSyncRecoveryRename: func(string, string) error {
				if failed.CompareAndSwap(false, true) {
					return errors.New("rename boundary")
				}
				return nil
			}}
		}},
		{"after rename", func(failed *atomic.Bool) libraryClientConfig {
			return libraryClientConfig{afterSyncRecoveryRename: func(string, string) error {
				if failed.CompareAndSwap(false, true) {
					return errors.New("renamed boundary")
				}
				return nil
			}}
		}},
		{"before completed", func(failed *atomic.Bool) libraryClientConfig {
			return libraryClientConfig{beforeSyncRecoveryCompleted: func(string) error {
				if failed.CompareAndSwap(false, true) {
					return errors.New("completed boundary")
				}
				return nil
			}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
			if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("remote"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			var failed atomic.Bool
			config := test.config(&failed)
			config.checkFilesystem = func(*os.File) error { return nil }
			err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
				strings.NewReader(""), io.Discard, io.Discard, config)
			if err == nil || !failed.Load() {
				t.Fatalf("boundary failure=%v injected=%v", err, failed.Load())
			}
			if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
				t.Fatalf("boundary retry: %v", err)
			}
			assertTestConverged(t, environment, subscriberDir, subscriberTree)
		})
	}
}

func TestLibrarySyncFinalizeRetainsRecoveryUntilCleanup(t *testing.T) {
	t.Run("before finalize", func(t *testing.T) {
		environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
		before := readTestBinding(t, subscriberDir, subscriberTree)
		if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("remote"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
			t.Fatal(err)
		}
		err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
			strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
				beforeFinalize: func() error { return errors.New("finalize unavailable") }})
		if err == nil {
			t.Fatal("expected finalize failure")
		}
		if after := readTestBinding(t, subscriberDir, subscriberTree); after != before {
			t.Fatalf("binding advanced before finalize: %+v", after)
		}
		if countSyncInternalPaths(t, subscriberTree) == 0 {
			t.Fatal("recovery removed before finalize")
		}
		if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
			t.Fatal(err)
		}
		assertTestConverged(t, environment, subscriberDir, subscriberTree)
	})

	t.Run("cleanup pending and unbind", func(t *testing.T) {
		_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
		if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("remote"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
			t.Fatal(err)
		}
		var failed atomic.Bool
		err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
			strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
				beforeSyncRecoveryCleanup: func(string, string) error {
					if failed.CompareAndSwap(false, true) {
						return errors.New("cleanup unavailable")
					}
					return nil
				}})
		if err == nil || !failed.Load() {
			t.Fatalf("cleanup failure=%v", err)
		}
		if state := readPendingCheckoutState(t, subscriberDir, subscriberTree); state != "finalized" {
			t.Fatalf("apply state=%q", state)
		}
		if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", subscriberDir, "--worktree", subscriberTree},
			strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
			t.Fatalf("unbind cleanup-pending apply: %v", err)
		}
		assertNoSyncInternalPaths(t, subscriberTree)
	})
}

func TestLibrarySyncCleanupDoesNotDeleteReplacement(t *testing.T) {
	_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	var saved string
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			beforeSyncRecoveryCleanup: func(path, name string) error {
				if saved != "" {
					return nil
				}
				saved = name + ".saved"
				if err := os.Rename(filepath.Join(subscriberTree, name), filepath.Join(subscriberTree, saved)); err != nil {
					return err
				}
				return os.WriteFile(filepath.Join(subscriberTree, name), []byte("replacement"), 0o600)
			}})
	if err == nil || saved == "" {
		t.Fatalf("cleanup replacement error=%v saved=%q", err, saved)
	}
	recovery := strings.TrimSuffix(saved, ".saved")
	if data, err := os.ReadFile(filepath.Join(subscriberTree, recovery)); err != nil || string(data) != "replacement" {
		t.Fatalf("cleanup changed replacement data=%q err=%v", data, err)
	}
	if err := os.Remove(filepath.Join(subscriberTree, recovery)); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(subscriberTree, saved), filepath.Join(subscriberTree, recovery)); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatalf("cleanup retry: %v", err)
	}
	assertNoSyncInternalPaths(t, subscriberTree)
}

func TestLibrarySyncPendingPublicationTransitionsToSuccessor(t *testing.T) {
	var updates atomic.Int32
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{AfterHeadUpdate: func() error {
		if updates.Add(1) == 3 {
			return errors.New("response lost")
		}
		return nil
	}})
	var published, failedGet atomic.Bool
	var commitPuts atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/head") && published.Load() && failedGet.CompareAndSwap(false, true) {
			http.Error(w, "unavailable", http.StatusInternalServerError)
			return
		}
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/objects/commits/") {
			commitPuts.Add(1)
		}
		environment.handler.ServeHTTP(w, r)
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/head") && updates.Load() == 3 {
			published.Store(true)
		}
	}))
	defer proxy.Close()
	clientDir, worktree := newClientPaths(t)
	if err := os.WriteFile(filepath.Join(worktree, "base"), []byte("base"), 0o600); err != nil {
		t.Fatal(err)
	}
	args := append(bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testClientDeviceID), "--import-local")
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktree, "local"), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, clientDir, worktree); err == nil {
		t.Fatal("expected unknown publication result")
	}
	candidate := readPendingCandidate(t, clientDir, worktree)
	candidateCommit, err := getRemoteCommit(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token), candidate)
	if err != nil {
		t.Fatal(err)
	}
	data, successor, err := canonicalCommit(testClientUserID, testOtherDeviceID, candidateCommit.Root, []string{candidate}, func() time.Time {
		return time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	})
	if err != nil {
		t.Fatal(err)
	}
	base := mustServerURL(t, environment.server.URL)
	if err := putMetadata(t.Context(), base, testClientLibraryID, []byte(environment.token), "commits", successor, data); err != nil {
		t.Fatal(err)
	}
	head, err := getRemoteHead(t.Context(), base, testClientLibraryID, []byte(environment.token))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := updateRemoteHead(t.Context(), base, testClientLibraryID, []byte(environment.token), head.ETag, successor); err != nil {
		t.Fatal(err)
	}
	commitPuts.Store(0)
	if err := syncTestWorktree(t, clientDir, worktree); err != nil {
		t.Fatalf("successor recovery: %v", err)
	}
	if commitPuts.Load() != 0 {
		t.Fatalf("successor recovery uploaded %d commits", commitPuts.Load())
	}
	binding := readTestBinding(t, clientDir, worktree)
	if binding.SyncBase != successor || binding.SyncBaseRoot != candidateCommit.Root {
		t.Fatalf("successor binding=%+v", binding)
	}
}

func TestLibrarySyncUnbindRollsBackPartialApplyFromEmptyBase(t *testing.T) {
	environment, publisherDir, publisherTree, subscriberDir, subscriberTree := newEmptySyncPair(t)
	if err := os.MkdirAll(filepath.Join(publisherTree, "remote", "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(publisherTree, "remote", "nested", "file"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	installs := 0
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			afterCheckoutInstall: func(string, string) error {
				installs++
				if installs == 3 {
					return errors.New("partial install")
				}
				return nil
			}})
	if err == nil || installs != 3 {
		t.Fatalf("partial empty-base apply error=%v installs=%d", err, installs)
	}
	if count := countClientRows(t, subscriberDir, "sync_recoveries", subscriberTree); count != 0 {
		t.Fatalf("empty base recovery count=%d", count)
	}
	if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
		t.Fatalf("unbind partial empty-base apply: %v", err)
	}
	entries, err := os.ReadDir(subscriberTree)
	if err != nil || len(entries) != 0 {
		t.Fatalf("empty base rollback entries=%v err=%v", entries, err)
	}
	assertSyncStateCleared(t, subscriberDir, subscriberTree)
	_ = environment
}

func TestLibrarySyncUnbindRollsBackPartialNestedDirectory(t *testing.T) {
	_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	if err := os.MkdirAll(filepath.Join(publisherTree, "nested", "old"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(publisherTree, "nested", "old", "base"), []byte("base"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	baseRoot := scanTestRoot(t, subscriberTree)
	if err := os.MkdirAll(filepath.Join(publisherTree, "nested", "new", "deep"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(publisherTree, "nested", "new", "deep", "remote"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	installs := 0
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			afterCheckoutInstall: func(string, string) error {
				installs++
				if installs == 4 {
					return errors.New("partial nested install")
				}
				return nil
			}})
	if err == nil {
		t.Fatal("expected partial nested apply failure")
	}
	if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
		t.Fatalf("unbind partial nested apply: %v", err)
	}
	if got := scanTestRoot(t, subscriberTree); got != baseRoot {
		t.Fatalf("nested rollback root=%s want=%s", got, baseRoot)
	}
	assertSyncStateCleared(t, subscriberDir, subscriberTree)
}

func TestLibrarySyncRecoveryRollbackRetriesPerRow(t *testing.T) {
	t.Run("after first restore", func(t *testing.T) {
		_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
		if err := os.WriteFile(filepath.Join(publisherTree, "second"), []byte("second"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
			t.Fatal(err)
		}
		if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
			t.Fatal(err)
		}
		baseRoot := scanTestRoot(t, subscriberTree)
		if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("remote"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
			t.Fatal(err)
		}
		err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
			strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
				beforeCheckoutMaterialize: func() error { return errors.New("stop before target install") }})
		if err == nil {
			t.Fatal("expected apply setup failure")
		}
		var failed atomic.Bool
		err = runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", subscriberDir, "--worktree", subscriberTree},
			strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
				afterSyncRecoveryRestore: func(string) error {
					if failed.CompareAndSwap(false, true) {
						return errors.New("stop after first restored row")
					}
					return nil
				}})
		if err == nil || !failed.Load() {
			t.Fatalf("restore boundary error=%v", err)
		}
		if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", subscriberDir, "--worktree", subscriberTree},
			strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
			t.Fatalf("retry restored rows: %v", err)
		}
		if got := scanTestRoot(t, subscriberTree); got != baseRoot {
			t.Fatalf("restored root=%s want=%s", got, baseRoot)
		}
		assertSyncStateCleared(t, subscriberDir, subscriberTree)
	})

	t.Run("parent sync failure", func(t *testing.T) {
		_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
		baseRoot := scanTestRoot(t, subscriberTree)
		if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("remote"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
			t.Fatal(err)
		}
		if err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
			strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
				beforeCheckoutMaterialize: func() error { return errors.New("stop before target install") }}); err == nil {
			t.Fatal("expected apply setup failure")
		}
		var failed atomic.Bool
		err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", subscriberDir, "--worktree", subscriberTree},
			strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
				syncDirectory: func(path string) error {
					if path == subscriberTree && failed.CompareAndSwap(false, true) {
						return errors.New("parent sync unavailable")
					}
					return syncDirectory(path)
				}})
		if err == nil || !failed.Load() {
			t.Fatalf("parent sync failure=%v", err)
		}
		if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", subscriberDir, "--worktree", subscriberTree},
			strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
			t.Fatalf("retry parent sync failure: %v", err)
		}
		if got := scanTestRoot(t, subscriberTree); got != baseRoot {
			t.Fatalf("sync retry root=%s want=%s", got, baseRoot)
		}
	})
}

func TestLibrarySyncRollbackVerifiesInstalledFileContent(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(string) error
		wantError bool
		want      string
	}{
		{"content edit", func(path string) error { return os.WriteFile(path, []byte("user-edit"), 0o600) }, true, "user-edit"},
		{"mtime edit", func(path string) error {
			info, err := os.Stat(path)
			if err != nil {
				return err
			}
			changed := info.ModTime().Add(time.Hour)
			return os.Chtimes(path, changed, changed)
		}, true, "remote"},
		{"unchanged", func(string) error { return nil }, false, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, publisherDir, publisherTree, subscriberDir, subscriberTree := newEmptySyncPair(t)
			if err := os.WriteFile(filepath.Join(publisherTree, "file"), []byte("remote"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
				strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
					afterCheckoutInstall: func(path, kind string) error {
						if path == "file" && kind == "File" {
							return errors.New("stop after file install")
						}
						return nil
					}})
			if err == nil {
				t.Fatal("expected installed file setup failure")
			}
			path := filepath.Join(subscriberTree, "file")
			before, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := test.mutate(path); err != nil {
				t.Fatal(err)
			}
			after, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			beforeStat, afterStat := before.Sys().(*syscall.Stat_t), after.Sys().(*syscall.Stat_t)
			if beforeStat.Ino != afterStat.Ino {
				t.Fatal("test mutation replaced installed inode")
			}
			err = runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", subscriberDir, "--worktree", subscriberTree},
				strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }})
			if test.wantError {
				if err == nil || !strings.Contains(err.Error(), "changed after installation") {
					t.Fatalf("edited file rollback error=%v", err)
				}
				data, readErr := os.ReadFile(path)
				if readErr != nil || string(data) != test.want {
					t.Fatalf("edited file data=%q err=%v", data, readErr)
				}
				if state := readPendingCheckoutState(t, subscriberDir, subscriberTree); state != "rolling_back" {
					t.Fatalf("edited file apply state=%q", state)
				}
				if count := countClientRows(t, subscriberDir, "checkout_paths", subscriberTree); count == 0 {
					t.Fatal("edited file rollback journal was cleared")
				}
				return
			}
			if err != nil {
				t.Fatalf("unchanged file rollback: %v", err)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unchanged file remained: %v", err)
			}
			assertSyncStateCleared(t, subscriberDir, subscriberTree)
		})
	}
}

func TestLibrarySyncFinalizedCleanupDetectsHeldFileWrite(t *testing.T) {
	_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	held, err := os.OpenFile(filepath.Join(subscriberTree, "base"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	var tombstone string
	err = runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			afterSyncRecoveryCleanupRename: func(path, name string) error {
				if path != "base" || tombstone != "" {
					return nil
				}
				tombstone = name
				if _, err := held.WriteAt([]byte("late-write"), 0); err != nil {
					return err
				}
				return held.Sync()
			}})
	if err == nil || tombstone == "" || !strings.Contains(err.Error(), "before cleanup") {
		t.Fatalf("held cleanup write error=%v tombstone=%q", err, tombstone)
	}
	if state := readPendingCheckoutState(t, subscriberDir, subscriberTree); state != "finalized" {
		t.Fatalf("held cleanup write apply state=%q", state)
	}
	data, err := os.ReadFile(filepath.Join(subscriberTree, tombstone))
	if err != nil || !strings.HasPrefix(string(data), "late-write") {
		t.Fatalf("held cleanup write not preserved data=%q err=%v", data, err)
	}
	if count := countClientRows(t, subscriberDir, "sync_recoveries", subscriberTree); count == 0 {
		t.Fatal("held cleanup write recovery journal was cleared")
	}
}

func TestLibrarySyncRollbackPreservesUnexpectedUserChild(t *testing.T) {
	_, publisherDir, publisherTree, subscriberDir, subscriberTree := newEmptySyncPair(t)
	if err := os.MkdirAll(filepath.Join(publisherTree, "remote", "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	installs := 0
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			afterCheckoutInstall: func(path, kind string) error {
				installs++
				if path == "remote" && kind == "Directory" {
					return errors.New("stop after top directory install")
				}
				return nil
			}})
	if err == nil {
		t.Fatal("expected partial directory apply")
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "remote", "user"), []byte("user"), 0o600); err != nil {
		t.Fatal(err)
	}
	err = runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }})
	if err == nil || !strings.Contains(err.Error(), "unexpected user content") {
		t.Fatalf("unexpected child rollback error=%v installs=%d", err, installs)
	}
	db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatal(err)
	}
	var rollbackName string
	if err := db.QueryRow("SELECT rollback_name FROM checkout_paths WHERE worktree = ? AND path = 'remote'", subscriberTree).Scan(&rollbackName); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if rollbackName == "" {
		t.Fatal("unexpected child tombstone was not journaled")
	}
	if data, err := os.ReadFile(filepath.Join(subscriberTree, rollbackName, "user")); err != nil || string(data) != "user" {
		t.Fatalf("unexpected child not preserved data=%q err=%v", data, err)
	}
}

func TestLibrarySyncUnbindRollsBackRegisteredRemoteApply(t *testing.T) {
	_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, beforeCheckoutMaterialize: func() error {
			return errors.New("stop after recovery rename")
		}})
	if err == nil {
		t.Fatal("expected remote apply failure")
	}
	if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
		t.Fatalf("unbind pending apply: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(subscriberTree, "base")); err != nil || string(data) != "base" {
		t.Fatalf("unbind did not restore base data=%q err=%v", data, err)
	}
	if _, err := os.Stat(filepath.Join(subscriberTree, "remote")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unbind retained partial remote target: %v", err)
	}
}

func TestLibrarySyncRemoteApplyRetainsFixedTargetAndDetectsHeadAdvance(t *testing.T) {
	environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	fixed := readTestBinding(t, publisherDir, publisherTree).SyncBase
	failed := false
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, beforeCheckoutMaterialize: func() error {
			failed = true
			return errors.New("disk unavailable")
		}})
	if err == nil || !failed {
		t.Fatalf("ordinary apply failure=%v injected=%v", err, failed)
	}
	if target := readPendingCheckoutTarget(t, subscriberDir, subscriberTree); target != fixed {
		t.Fatalf("pending target=%s want=%s", target, fixed)
	}
	if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("second"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	err = syncTestWorktree(t, subscriberDir, subscriberTree)
	if err == nil || !strings.Contains(err.Error(), "Head advanced") {
		t.Fatalf("advanced Head error=%v", err)
	}
	if binding := readTestBinding(t, subscriberDir, subscriberTree); binding.SyncBase != fixed {
		t.Fatalf("fixed target drifted: binding=%+v fixed=%s", binding, fixed)
	}
	if data, err := os.ReadFile(filepath.Join(subscriberTree, "remote")); err != nil || !bytes.Equal(data, []byte("first")) {
		t.Fatalf("fixed target content=%q err=%v", data, err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatalf("rerun after advance: %v", err)
	}
	assertTestConverged(t, environment, subscriberDir, subscriberTree)
}

func TestLegacyEmptyIndexDerivesTrackedUnsupportedPathsFromSyncBase(t *testing.T) {
	for _, test := range []struct {
		name    string
		replace func(string) error
	}{
		{"symlink", func(path string) error { return os.Symlink("target", path) }},
		{"fifo", func(path string) error { return syscall.Mkfifo(path, 0o600) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
			var puts atomic.Int32
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut {
					puts.Add(1)
				}
				environment.handler.ServeHTTP(w, r)
			}))
			defer proxy.Close()
			clientDir, worktree := newClientPaths(t)
			tracked := filepath.Join(worktree, "tracked")
			if err := os.WriteFile(tracked, []byte("tracked"), 0o600); err != nil {
				t.Fatal(err)
			}
			args := append(bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testClientDeviceID), "--import-local")
			if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
				t.Fatal(err)
			}
			before := readTestBinding(t, clientDir, worktree)
			db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("DELETE FROM path_index WHERE worktree = ?", worktree); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(tracked); err != nil {
				t.Fatal(err)
			}
			if err := test.replace(tracked); err != nil {
				t.Fatal(err)
			}
			puts.Store(0)
			err = syncTestWorktree(t, clientDir, worktree)
			if err == nil || !strings.Contains(err.Error(), "unsupported") {
				t.Fatalf("legacy tracked %s error=%v", test.name, err)
			}
			assertFailedScanState(t, environment, clientDir, worktree, before, 0, puts.Load())
		})
	}
}

func TestSyncTrackedFIFODoesNotPublishDeletion(t *testing.T) {
	environment, clientDir, worktree, _, _, puts, _ := newSyncPair(t)
	before := readTestBinding(t, clientDir, worktree)
	indexCount := countClientRows(t, clientDir, "path_index", worktree)
	path := filepath.Join(worktree, "base")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	puts.Store(0)
	err := syncTestWorktree(t, clientDir, worktree)
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("tracked FIFO error=%v", err)
	}
	assertFailedScanState(t, environment, clientDir, worktree, before, indexCount, puts.Load())
}

func TestSyncTrackedOpenFailureDoesNotPublishDeletion(t *testing.T) {
	environment, clientDir, worktree, _, _, puts, _ := newSyncPair(t)
	before := readTestBinding(t, clientDir, worktree)
	indexCount := countClientRows(t, clientDir, "path_index", worktree)
	puts.Store(0)
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", clientDir, "--worktree", worktree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{
			checkFilesystem: func(*os.File) error { return nil }, now: time.Now,
			scanFault: func(event scanFault) error {
				if event.phase == "before-open" && event.path == "base" {
					return syscall.EACCES
				}
				return nil
			},
		})
	if err == nil || !errors.Is(err, syscall.EACCES) {
		t.Fatalf("tracked open failure=%v", err)
	}
	assertFailedScanState(t, environment, clientDir, worktree, before, indexCount, puts.Load())
}

func TestSyncContinuousMutationExhaustionPreservesState(t *testing.T) {
	environment, clientDir, worktree, _, _, puts, _ := newSyncPair(t)
	before := readTestBinding(t, clientDir, worktree)
	indexCount := countClientRows(t, clientDir, "path_index", worktree)
	mutations := 0
	puts.Store(0)
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", clientDir, "--worktree", worktree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{
			checkFilesystem: func(*os.File) error { return nil }, now: time.Now,
			scanFault: func(event scanFault) error {
				if event.phase == "after-file-read-1" && event.path == "base" {
					mutations++
					return os.WriteFile(filepath.Join(worktree, "base"), []byte(strings.Repeat("x", mutations+4)), 0o600)
				}
				return nil
			},
		})
	if err == nil || !strings.Contains(err.Error(), "unstable worktree") || mutations != scanRetryBudget {
		t.Fatalf("continuous mutation error=%v mutations=%d", err, mutations)
	}
	assertFailedScanState(t, environment, clientDir, worktree, before, indexCount, puts.Load())
}

func assertFailedScanState(t *testing.T, environment libraryCLIEnvironment, clientDir, worktree string, before clientBinding, indexCount int, puts int32) {
	t.Helper()
	if after := readTestBinding(t, clientDir, worktree); after != before {
		t.Fatalf("binding changed: before=%+v after=%+v", before, after)
	}
	if count := countClientRows(t, clientDir, "pending_publications", worktree); count != 0 {
		t.Fatalf("pending publication rows=%d", count)
	}
	if count := countClientRows(t, clientDir, "path_index", worktree); count != indexCount {
		t.Fatalf("path index rows=%d want=%d", count, indexCount)
	}
	if puts != 0 {
		t.Fatalf("PUT requests=%d", puts)
	}
	head, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if err != nil || head.CommitID == nil || *head.CommitID != before.SyncBase {
		t.Fatalf("Head changed: head=%+v err=%v", head, err)
	}
}

func TestSyncWarnsAndIgnoresUntrackedUnsupportedPath(t *testing.T) {
	_, clientDir, worktree, _, _, puts, _ := newSyncPair(t)
	if err := syscall.Mkfifo(filepath.Join(worktree, "pipe"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", clientDir, "--worktree", worktree},
		strings.NewReader(""), &stdout, &stderr, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), "warning:") || !strings.Contains(stderr.String(), "pipe") {
		t.Fatalf("stderr=%q", stderr.String())
	}
	if !strings.Contains(stdout.String(), "already synchronized") {
		t.Fatalf("stdout=%q", stdout.String())
	}
	if count := puts.Load(); count != 0 {
		t.Fatalf("PUT requests=%d", count)
	}
}

func TestSyncUnstableScanDoesNotPublishOrChangeClientState(t *testing.T) {
	environment, clientDir, worktree, _, _, puts, _ := newSyncPair(t)
	before := readTestBinding(t, clientDir, worktree)
	changed := false
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", clientDir, "--worktree", worktree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{
			checkFilesystem: func(*os.File) error { return nil }, now: time.Now,
			scanFault: func(event scanFault) error {
				if event.phase == "after-file-scan" && event.path == "base" && !changed {
					changed = true
					return os.WriteFile(filepath.Join(worktree, "base"), []byte("changed"), 0o600)
				}
				return nil
			},
		})
	if err == nil || !strings.Contains(err.Error(), "unstable worktree") {
		t.Fatalf("unstable sync error=%v", err)
	}
	after := readTestBinding(t, clientDir, worktree)
	if after.SyncBase != before.SyncBase || after.SyncBaseRoot != before.SyncBaseRoot || after.HeadETag != before.HeadETag {
		t.Fatalf("binding changed: before=%+v after=%+v", before, after)
	}
	if count := countClientRows(t, clientDir, "pending_publications", worktree); count != 0 {
		t.Fatalf("pending publication rows=%d", count)
	}
	if count := puts.Load(); count != 0 {
		t.Fatalf("PUT requests=%d", count)
	}
	head, headErr := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if headErr != nil || head.CommitID == nil || *head.CommitID != before.SyncBase {
		t.Fatalf("Head changed: head=%+v err=%v", head, headErr)
	}
}

func TestPublicSyncFallbackLeafOrdinalExhaustionBeforePublication(t *testing.T) {
	environment, publisherDir, publisherTree, subscriberDir, subscriberTree, puts, _ := newSyncPair(t)
	source := strings.Repeat("a", 240) + "/" + strings.Repeat("b", 240) + "/" +
		strings.Repeat("c", 240) + "/" + strings.Repeat("d", 240) + "/" + strings.Repeat("a", 13) + "/f"
	for _, root := range []string{publisherTree, subscriberTree} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, source)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, source), []byte("base"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	binding := readTestBinding(t, subscriberDir, subscriberTree)
	if err := os.WriteFile(filepath.Join(publisherTree, source), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, source), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	beforeTree := captureExactTree(t, subscriberTree)
	beforeHead, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if err != nil {
		t.Fatal(err)
	}
	puts.Store(0)
	occupied := make(map[string]bool, _conflictMaxOrdinal)
	err = runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			fallbackOccupied: func(name string) bool {
				occupied[name] = true
				return len(occupied) <= _conflictMaxOrdinal
			},
		})
	if err == nil || !strings.Contains(err.Error(), "fallback conflict collision sequence exhausted") || len(occupied) != _conflictMaxOrdinal {
		t.Fatalf("leaf ordinal exhaustion error=%v", err)
	}
	assertExactTree(t, subscriberTree, beforeTree)
	afterHead, headErr := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if headErr != nil || beforeHead.ETag != afterHead.ETag || beforeHead.CommitID == nil || afterHead.CommitID == nil ||
		*beforeHead.CommitID != *afterHead.CommitID || puts.Load() != 0 || readTestBinding(t, subscriberDir, subscriberTree) != binding ||
		countClientRows(t, subscriberDir, "pending_publications", subscriberTree) != 0 {
		t.Fatalf("leaf exhaustion changed state: puts=%d head=%+v/%+v", puts.Load(), beforeHead, afterHead)
	}
}

func TestConflictPromotionMissingAuthorityCacheMatrix(t *testing.T) {
	environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, "base"), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
	command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1", "FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir,
		"FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree, "FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase,
		"FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename, "FILECLOUD_PUBLIC_CRASH_KIND=File",
		"FILECLOUD_PUBLIC_CRASH_POINT=after_intent_commit", "FILECLOUD_PUBLIC_CRASH_ROLE=promotion")
	assertProcessSIGKILL(t, command.Run())
	beforeBinding := readTestBinding(t, subscriberDir, subscriberTree)
	db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	checkout, err := loadPendingCheckout(t.Context(), db, beforeBinding.ServerURL, testClientLibraryID, subscriberTree)
	if err != nil || checkout == nil {
		db.Close()
		t.Fatalf("pending checkout=%+v err=%v", checkout, err)
	}
	cachePath := filepath.Join(subscriberDir, "objects", "commits", checkout.TargetCommit[:2], checkout.TargetCommit[2:])
	if err := os.Remove(cachePath); err != nil {
		db.Close()
		t.Fatal(err)
	}
	beforeTree := captureExactTree(t, subscriberTree)
	beforeRows := []int{countClientRows(t, subscriberDir, "pending_checkouts", subscriberTree), countClientRows(t, subscriberDir, "fs_actions", subscriberTree),
		countClientRows(t, subscriberDir, "sync_recoveries", subscriberTree), countClientRows(t, subscriberDir, "sync_recovery_promotions", subscriberTree)}
	root, err := openWorktreeRoot(subscriberTree, func(*os.File) error { return nil })
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	err = validatePendingPromotionTargets(t.Context(), db, subscriberTree)
	if err == nil || !strings.Contains(err.Error(), "authoritative cached object is absent") {
		root.Close()
		db.Close()
		t.Fatalf("offline recover validation error=%v", err)
	}
	if err := errors.Join(root.Close(), db.Close()); err != nil {
		t.Fatal(err)
	}
	assertExactTree(t, subscriberTree, beforeTree)
	if got := []int{countClientRows(t, subscriberDir, "pending_checkouts", subscriberTree), countClientRows(t, subscriberDir, "fs_actions", subscriberTree),
		countClientRows(t, subscriberDir, "sync_recoveries", subscriberTree), countClientRows(t, subscriberDir, "sync_recovery_promotions", subscriberTree)}; !reflect.DeepEqual(got, beforeRows) {
		t.Fatalf("offline recover changed rows: got=%v want=%v", got, beforeRows)
	}
	if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", subscriberDir, "--worktree", subscriberTree}, strings.NewReader(""), io.Discard, io.Discard,
		libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err == nil || !strings.Contains(err.Error(), "authoritative cached object is absent") {
		t.Fatalf("offline unbind error=%v", err)
	}
	assertExactTree(t, subscriberTree, beforeTree)
	db, err = openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("UPDATE bindings SET server_url=? WHERE worktree=?", "http://127.0.0.1:1", subscriberTree)
	if closeErr := db.Close(); err != nil || closeErr != nil {
		t.Fatal(errors.Join(err, closeErr))
	}
	err = syncTestWorktree(t, subscriberDir, subscriberTree)
	if err == nil {
		t.Fatal("offline sync accepted missing cache closure")
	}
	db, err = openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("UPDATE bindings SET server_url=? WHERE worktree=?", beforeBinding.ServerURL, subscriberTree)
	if closeErr := db.Close(); err != nil || closeErr != nil {
		t.Fatal(errors.Join(err, closeErr))
	}
	assertExactTree(t, subscriberTree, beforeTree)
	if got := []int{countClientRows(t, subscriberDir, "pending_checkouts", subscriberTree), countClientRows(t, subscriberDir, "fs_actions", subscriberTree),
		countClientRows(t, subscriberDir, "sync_recoveries", subscriberTree), countClientRows(t, subscriberDir, "sync_recovery_promotions", subscriberTree)}; !reflect.DeepEqual(got, beforeRows) {
		t.Fatalf("offline sync changed rows: got=%v want=%v", got, beforeRows)
	}
	for attempt := 0; attempt < 4; attempt++ {
		err = syncTestWorktree(t, subscriberDir, subscriberTree)
		if err == nil {
			break
		}
		if !strings.Contains(err.Error(), "rerun sync") {
			t.Fatalf("online rehydrate sync %d: %v", attempt, err)
		}
	}
	if err != nil {
		t.Fatalf("online rehydrate did not converge: %v", err)
	}
	if _, err := os.Stat(cachePath); err != nil {
		t.Fatalf("online sync did not rehydrate required cache object: %v", err)
	}
	assertTestConverged(t, environment, subscriberDir, subscriberTree)
}

func TestAuthoritativePromotionReplayRejectsEqualContentCrossSourceSwap(t *testing.T) {
	_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
	for _, root := range []string{publisherTree, subscriberTree} {
		for _, name := range []string{"first", "second"} {
			if err := os.WriteFile(filepath.Join(root, name), []byte("base-"+name), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"first", "second"} {
		if err := os.WriteFile(filepath.Join(publisherTree, name), []byte("remote-"+name), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(subscriberTree, name), []byte("same-local"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
	command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1", "FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir,
		"FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree, "FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase,
		"FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename, "FILECLOUD_PUBLIC_CRASH_KIND=File",
		"FILECLOUD_PUBLIC_CRASH_POINT=after_intent_commit", "FILECLOUD_PUBLIC_CRASH_ROLE=promotion")
	assertProcessSIGKILL(t, command.Run())
	beforeBinding := readTestBinding(t, subscriberDir, subscriberTree)
	beforeTree := captureExactTree(t, subscriberTree)
	db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := validatePendingPromotionTargets(t.Context(), db, subscriberTree); err != nil {
		db.Close()
		t.Fatalf("exact provenance replay=%v", err)
	}
	checkout, err := loadPendingCheckout(t.Context(), db, beforeBinding.ServerURL, testClientLibraryID, subscriberTree)
	if err != nil || checkout == nil {
		db.Close()
		t.Fatalf("pending checkout=%+v err=%v", checkout, err)
	}
	promotions, err := _decodeConflictPromotions(checkout.ConflictPromotions)
	if err != nil || len(promotions) != 2 || promotions[0].id != promotions[1].id || promotions[0].mtime != promotions[1].mtime || promotions[0].size != promotions[1].size {
		db.Close()
		t.Fatalf("equal-content promotions=%+v err=%v", promotions, err)
	}
	promotions[0].source, promotions[1].source = promotions[1].source, promotions[0].source
	encoded, err := _encodeConflictPromotions(promotions)
	if err == nil {
		_, err = db.Exec("UPDATE pending_checkouts SET conflict_promotions=? WHERE worktree=?", encoded, subscriberTree)
	}
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	beforeRows := []int{countClientRows(t, subscriberDir, "pending_checkouts", subscriberTree), countClientRows(t, subscriberDir, "fs_actions", subscriberTree),
		countClientRows(t, subscriberDir, "sync_recoveries", subscriberTree), countClientRows(t, subscriberDir, "sync_recovery_promotions", subscriberTree)}
	err = validatePendingPromotionTargets(t.Context(), db, subscriberTree)
	if closeErr := db.Close(); err == nil || closeErr != nil || !strings.Contains(err.Error(), "does not match authoritative Candidate replay") {
		t.Fatalf("cross-source swap replay=%v close=%v", err, closeErr)
	}
	assertExactTree(t, subscriberTree, beforeTree)
	if readTestBinding(t, subscriberDir, subscriberTree) != beforeBinding {
		t.Fatal("cross-source swap changed binding")
	}
	if got := []int{countClientRows(t, subscriberDir, "pending_checkouts", subscriberTree), countClientRows(t, subscriberDir, "fs_actions", subscriberTree),
		countClientRows(t, subscriberDir, "sync_recoveries", subscriberTree), countClientRows(t, subscriberDir, "sync_recovery_promotions", subscriberTree)}; !reflect.DeepEqual(got, beforeRows) {
		t.Fatalf("cross-source swap changed rows: got=%v want=%v", got, beforeRows)
	}
}

func TestPublicSyncFallbackRootOrdinalExhaustionBeforePublication(t *testing.T) {
	environment, publisherDir, publisherTree, subscriberDir, subscriberTree, puts, _ := newSyncPair(t)
	source := "x." + strings.Repeat("a", 190)
	if err := os.WriteFile(filepath.Join(publisherTree, source), []byte("base"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
		t.Fatal(err)
	}
	seed := object.Commit{AuthorUserID: testClientUserID, DeviceID: testOtherDeviceID, CreatedAt: "2026-08-09T12:00:00Z"}
	occupied := make([]string, 0, 9)
	for range 9 {
		name, err := _conflictCopyName(source, "", seed, occupied)
		if err != nil {
			t.Fatal(err)
		}
		occupied = append(occupied, name)
		if err := os.WriteFile(filepath.Join(subscriberTree, name), []byte("occupied"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for ordinal := 1; ordinal <= _conflictMaxOrdinal; ordinal++ {
		name := _fallbackConflictRoot
		if ordinal > 1 {
			name += " " + strconv.Itoa(ordinal)
		}
		if err := os.WriteFile(filepath.Join(subscriberTree, name), []byte("occupied"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(publisherTree, source), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subscriberTree, source), []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	beforeBinding, beforeRoot := readTestBinding(t, subscriberDir, subscriberTree), scanTestRoot(t, subscriberTree)
	beforeHead, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if err != nil {
		t.Fatal(err)
	}
	puts.Store(0)
	err = runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
			now: func() time.Time { return time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC) }})
	if err == nil || !strings.Contains(err.Error(), "fallback conflict root collision sequence exhausted") {
		t.Fatalf("root ordinal exhaustion error=%v", err)
	}
	afterHead, headErr := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if headErr != nil || beforeHead.ETag != afterHead.ETag || beforeHead.CommitID == nil || afterHead.CommitID == nil ||
		*beforeHead.CommitID != *afterHead.CommitID || puts.Load() != 0 || readTestBinding(t, subscriberDir, subscriberTree) != beforeBinding ||
		scanTestRoot(t, subscriberTree) != beforeRoot || countClientRows(t, subscriberDir, "pending_publications", subscriberTree) != 0 {
		t.Fatalf("root exhaustion changed state: puts=%d head=%+v/%+v", puts.Load(), beforeHead, afterHead)
	}
}

func newSyncPair(t *testing.T) (libraryCLIEnvironment, string, string, string, string, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	publisherDir, publisherTree := newClientPaths(t)
	if err := os.WriteFile(filepath.Join(publisherTree, "base"), []byte("base"), 0o600); err != nil {
		t.Fatal(err)
	}
	publisherArgs := append(bindArgs(publisherDir, environment.server.URL, testClientLibraryID, publisherTree, testClientDeviceID), "--import-local")
	if err := runTest(t.Context(), publisherArgs, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	puts, commits := &atomic.Int32{}, &atomic.Int32{}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			puts.Add(1)
			if strings.Contains(r.URL.Path, "/objects/commits/") {
				commits.Add(1)
			}
		}
		environment.handler.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	subscriberDir, subscriberTree := newClientPaths(t)
	if err := runTest(t.Context(), bindArgs(subscriberDir, proxy.URL, testClientLibraryID, subscriberTree, testOtherDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	puts.Store(0)
	commits.Store(0)
	return environment, publisherDir, publisherTree, subscriberDir, subscriberTree, puts, commits
}

func newTrackedSyncPair(t *testing.T, tracked int) (libraryCLIEnvironment, string, string, string, string, *atomic.Int32) {
	t.Helper()
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	publisherDir, publisherTree := newClientPaths(t)
	for index := 0; index < tracked; index++ {
		if err := os.WriteFile(filepath.Join(publisherTree, fmt.Sprintf("tracked-%03d", index)), []byte("base"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	args := append(bindArgs(publisherDir, environment.server.URL, testClientLibraryID, publisherTree, testClientDeviceID), "--import-local")
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	puts := &atomic.Int32{}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			puts.Add(1)
		}
		environment.handler.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	subscriberDir, subscriberTree := newClientPaths(t)
	if err := runTest(t.Context(), bindArgs(subscriberDir, proxy.URL, testClientLibraryID, subscriberTree, testOtherDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	puts.Store(0)
	return environment, publisherDir, publisherTree, subscriberDir, subscriberTree, puts
}

func newEmptySyncPair(t *testing.T) (libraryCLIEnvironment, string, string, string, string) {
	t.Helper()
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	publisherDir, publisherTree := newClientPaths(t)
	if err := runTest(t.Context(), bindArgs(publisherDir, environment.server.URL, testClientLibraryID, publisherTree, testClientDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	subscriberDir, subscriberTree := newClientPaths(t)
	if err := runTest(t.Context(), bindArgs(subscriberDir, environment.server.URL, testClientLibraryID, subscriberTree, testOtherDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	return environment, publisherDir, publisherTree, subscriberDir, subscriberTree
}

func syncTestWorktree(t *testing.T, clientDir, worktree string) error {
	t.Helper()
	return runTest(t.Context(), []string{"library", "sync", "--client-dir", clientDir, "--worktree", worktree}, strings.NewReader(""), io.Discard, io.Discard)
}

func syncTestWorktreeConfirmingDeletes(t *testing.T, clientDir, worktree string) error {
	t.Helper()
	err := syncTestWorktree(t, clientDir, worktree)
	var required *deleteConfirmationRequiredError
	if !errors.As(err, &required) {
		return err
	}
	return confirmTestDeletion(t, clientDir, worktree, required.candidate[:deleteCandidatePrefixLen], libraryClientConfig{})
}

type testBindingSnapshot struct {
	binding     clientBinding
	accessToken []byte
}

func captureTestBinding(t *testing.T, clientDir, worktree string) testBindingSnapshot {
	t.Helper()
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var result testBindingSnapshot
	err = db.QueryRow(`SELECT server_url,library_id,worktree,user_id,device_id,sync_base_commit,sync_base_root,head_etag,access_token
		FROM bindings WHERE worktree=?`, worktree).Scan(&result.binding.ServerURL, &result.binding.LibraryID,
		&result.binding.Worktree, &result.binding.UserID, &result.binding.DeviceID, &result.binding.SyncBase,
		&result.binding.SyncBaseRoot, &result.binding.HeadETag, &result.accessToken)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

type testPathIndexRow struct {
	worktree, path, kind, id, canonicalMtime, actualMtime string
	size                                                  int64
}

type testJournalBindingRow struct {
	worktree                             string
	rootDevice, rootInode, journalFormat uint64
}

func captureTestJournalBindings(t *testing.T, clientDir, worktree string) []testJournalBindingRow {
	t.Helper()
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT worktree,root_device,root_inode,journal_format
		FROM fs_journal_bindings WHERE worktree=? ORDER BY worktree,root_device,root_inode,journal_format`, worktree)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []testJournalBindingRow
	for rows.Next() {
		var value testJournalBindingRow
		if err := rows.Scan(&value.worktree, &value.rootDevice, &value.rootInode, &value.journalFormat); err != nil {
			t.Fatal(err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func captureTestPathIndex(t *testing.T, clientDir, worktree string) []testPathIndexRow {
	t.Helper()
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT worktree,path,type,object_id,canonical_mtime,actual_mtime,size
		FROM path_index WHERE worktree=? ORDER BY path`, worktree)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []testPathIndexRow
	for rows.Next() {
		var value testPathIndexRow
		if err := rows.Scan(&value.worktree, &value.path, &value.kind, &value.id, &value.canonicalMtime,
			&value.actualMtime, &value.size); err != nil {
			t.Fatal(err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func assertTestConverged(t *testing.T, environment libraryCLIEnvironment, clientDir, worktree string) clientBinding {
	t.Helper()
	binding := readTestBinding(t, clientDir, worktree)
	head, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if err != nil || head.CommitID == nil || *head.CommitID != binding.SyncBase {
		t.Fatalf("binding and Head differ: binding=%+v head=%+v err=%v", binding, head, err)
	}
	commit, err := getRemoteCommit(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID,
		[]byte(environment.token), *head.CommitID)
	if err != nil || commit.Root != binding.SyncBaseRoot {
		t.Fatalf("Base and Head root differ: binding=%+v commit=%+v err=%v", binding, commit, err)
	}
	root, err := openWorktreeRoot(worktree, func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	snapshot, scanErr := scanWorktree(root)
	closeErr := root.Close()
	if scanErr != nil || closeErr != nil || snapshot.root != binding.SyncBaseRoot {
		t.Fatalf("worktree and Base differ: snapshot=%s binding=%+v scan=%v close=%v", snapshot.root, binding, scanErr, closeErr)
	}
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT path,type,object_id,canonical_mtime,size FROM path_index WHERE worktree=? ORDER BY path`, worktree)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	indexed := make(map[string]checkoutPath, len(snapshot.paths))
	for rows.Next() {
		var value checkoutPath
		if err := rows.Scan(&value.path, &value.kind, &value.id, &value.mtime, &value.size); err != nil {
			rows.Close()
			db.Close()
			t.Fatal(err)
		}
		indexed[value.path] = value
	}
	if err := errors.Join(rows.Err(), rows.Close(), db.Close()); err != nil {
		t.Fatal(err)
	}
	if len(indexed) != len(snapshot.paths) {
		t.Fatalf("path index count=%d snapshot paths=%d", len(indexed), len(snapshot.paths))
	}
	for _, path := range snapshot.paths {
		value, ok := indexed[path.path]
		if !ok || value.kind != path.kind || value.id != path.id || value.mtime != path.mtime || value.size != path.size {
			t.Fatalf("path index differs at %q: indexed=%+v snapshot=%+v", path.path, value, path)
		}
	}
	return binding
}

func readPendingCandidate(t *testing.T, clientDir, worktree string) string {
	t.Helper()
	return readTestPendingPublication(t, clientDir, worktree).CandidateCommit
}

func readTestPendingPublication(t *testing.T, clientDir, worktree string) pendingPublication {
	t.Helper()
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	pending, err := loadPendingPublication(t.Context(), db, worktree)
	if err != nil {
		t.Fatal(err)
	}
	if pending == nil {
		t.Fatal("pending publication is missing")
	}
	return *pending
}

func confirmTestDeletion(t *testing.T, clientDir, worktree, prefix string, config libraryClientConfig) error {
	t.Helper()
	if config.checkFilesystem == nil {
		config.checkFilesystem = func(*os.File) error { return nil }
	}
	if config.now == nil {
		config.now = time.Now
	}
	return runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", clientDir, "--worktree", worktree,
		"--confirm-delete", prefix}, strings.NewReader(""), io.Discard, io.Discard, config)
}

func scanTestRoot(t *testing.T, worktree string) string {
	t.Helper()
	root, err := openWorktreeRoot(worktree, func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	snapshot, scanErr := scanWorktree(root)
	if err := errors.Join(scanErr, root.Close()); err != nil {
		t.Fatal(err)
	}
	return snapshot.root
}

func downgradePendingPublicationToV17(t *testing.T, clientDir string) {
	t.Helper()
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`ALTER TABLE pending_publications RENAME TO new_pending_publications;
		CREATE TABLE pending_publications (worktree TEXT PRIMARY KEY NOT NULL, base_commit TEXT NOT NULL,
		base_root TEXT NOT NULL, expected_etag TEXT NOT NULL, candidate_commit TEXT NOT NULL,
		candidate_root TEXT NOT NULL, candidate_data BLOB NOT NULL);
		INSERT INTO pending_publications SELECT worktree,base_commit,base_root,expected_etag,candidate_commit,
			candidate_root,candidate_data FROM new_pending_publications;
		DROP TABLE new_pending_publications;
		DELETE FROM client_schema_migrations WHERE version>=18`); err != nil {
		t.Fatal(err)
	}
}

func countClientRows(t *testing.T, clientDir, table, worktree string) int {
	t.Helper()
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE worktree = ?", worktree).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func assertSyncStateCleared(t *testing.T, clientDir, worktree string) {
	t.Helper()
	for _, table := range []string{"bindings", "pending_checkouts", "checkout_paths", "sync_recoveries"} {
		if count := countClientRows(t, clientDir, table, worktree); count != 0 {
			t.Fatalf("%s rows=%d", table, count)
		}
	}
	assertNoSyncInternalPaths(t, worktree)
}

func assertNoSyncInternalPaths(t *testing.T, worktree string) {
	t.Helper()
	if count := countSyncInternalPaths(t, worktree); count != 0 {
		t.Fatalf("sync internal path count=%d", count)
	}
}

func countSyncInternalPaths(t *testing.T, worktree string) int {
	t.Helper()
	count := 0
	if err := filepath.WalkDir(worktree, func(_ string, entry os.DirEntry, err error) error {
		if err == nil && strings.HasPrefix(entry.Name(), ".filecloud-internal-") {
			count++
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return count
}

func readPendingCheckoutState(t *testing.T, clientDir, worktree string) string {
	t.Helper()
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var state string
	if err := db.QueryRow("SELECT apply_state FROM pending_checkouts WHERE worktree = ?", worktree).Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func readPendingCheckoutTarget(t *testing.T, clientDir, worktree string) string {
	t.Helper()
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var target string
	if err := db.QueryRow("SELECT target_commit FROM pending_checkouts WHERE worktree = ?", worktree).Scan(&target); err != nil {
		t.Fatal(err)
	}
	return target
}
