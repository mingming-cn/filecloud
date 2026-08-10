package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	libraryapi "github.com/mingming-cn/filecloud/internal/library"
	"github.com/mingming-cn/filecloud/internal/object"
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

func TestLibrarySyncConflictBoundaryPreservesExistingPending(t *testing.T) {
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
	beforeBinding := readTestBinding(t, subscriberDir, subscriberTree)
	beforeRoot := scanTestRoot(t, subscriberTree)
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
	err = syncTestWorktree(t, subscriberDir, subscriberTree)
	if err == nil || !strings.Contains(err.Error(), "Issue #17") {
		t.Fatalf("replacement boundary error=%v", err)
	}
	afterHead, headErr := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID,
		[]byte(environment.token))
	if afterPending := readTestPendingPublication(t, subscriberDir, subscriberTree); !reflect.DeepEqual(afterPending, beforePending) ||
		readTestBinding(t, subscriberDir, subscriberTree) != beforeBinding || scanTestRoot(t, subscriberTree) != beforeRoot ||
		puts.Load() != 0 || headErr != nil || beforeHead.CommitID == nil || afterHead.CommitID == nil ||
		*beforeHead.CommitID != *afterHead.CommitID || beforeHead.ETag != afterHead.ETag {
		t.Fatalf("replacement boundary changed state: pending=%+v head=%+v err=%v puts=%d", afterPending, afterHead, headErr, puts.Load())
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
				DELETE FROM client_schema_migrations WHERE version=21`); err != nil {
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
	data, id, err := canonicalDirectory("", []scanEntry{{name: "a", kind: "File", id: strings.Repeat("0", 64),
		modified: "2026-08-10T00:00:00Z"}})
	if err != nil {
		t.Fatal(err)
	}
	budget := &_replayBudget{commitLimit: 10, treeLimit: 10, pathLimit: 1, commits: make(map[string]object.Commit),
		walked: make(map[string]bool)}
	merger := &_treeMerger{ctx: t.Context(), directories: map[string][]byte{id: data}, synthesized: make(map[string][]byte),
		active: make(map[string]bool), seen: make(map[string]bool), budget: budget}
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

func TestLibrarySyncSamePathBoundariesLeaveStateUntouched(t *testing.T) {
	for _, test := range []struct {
		name, issue string
		remote      func(string) error
		local       func(string) error
	}{
		{"divergent file", "Issue #17", func(root string) error {
			return os.WriteFile(filepath.Join(root, "base"), []byte("remote"), 0o600)
		}, func(root string) error { return os.WriteFile(filepath.Join(root, "base"), []byte("local"), 0o600) }},
		{"delete modify", "Issue #18", func(root string) error {
			return os.WriteFile(filepath.Join(root, "base"), []byte("remote"), 0o600)
		}, func(root string) error { return os.Remove(filepath.Join(root, "base")) }},
		{"type conflict", "Issue #18", func(root string) error {
			if err := os.Remove(filepath.Join(root, "base")); err != nil {
				return err
			}
			return os.Mkdir(filepath.Join(root, "base"), 0o700)
		}, func(root string) error { return os.WriteFile(filepath.Join(root, "base"), []byte("local"), 0o600) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			environment, publisherDir, publisherTree, subscriberDir, subscriberTree, puts, _ := newSyncPair(t)
			if err := test.remote(publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktreeConfirmingDeletes(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			if err := test.local(subscriberTree); err != nil {
				t.Fatal(err)
			}
			beforeBinding := readTestBinding(t, subscriberDir, subscriberTree)
			beforeRoot := scanTestRoot(t, subscriberTree)
			beforeHead, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID,
				[]byte(environment.token))
			if err != nil {
				t.Fatal(err)
			}
			puts.Store(0)
			err = syncTestWorktree(t, subscriberDir, subscriberTree)
			if err == nil || !strings.Contains(err.Error(), test.issue) {
				t.Fatalf("boundary error=%v", err)
			}
			afterHead, headErr := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID,
				[]byte(environment.token))
			if after := readTestBinding(t, subscriberDir, subscriberTree); after != beforeBinding ||
				scanTestRoot(t, subscriberTree) != beforeRoot || countClientRows(t, subscriberDir, "pending_publications", subscriberTree) != 0 ||
				puts.Load() != 0 || headErr != nil || afterHead.CommitID == nil || beforeHead.CommitID == nil ||
				*afterHead.CommitID != *beforeHead.CommitID || afterHead.ETag != beforeHead.ETag {
				t.Fatalf("boundary changed state: binding=%+v head=%+v err=%v puts=%d", after, afterHead, headErr, puts.Load())
			}
		})
	}
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

func assertTestConverged(t *testing.T, environment libraryCLIEnvironment, clientDir, worktree string) clientBinding {
	t.Helper()
	binding := readTestBinding(t, clientDir, worktree)
	head, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if err != nil || head.CommitID == nil || *head.CommitID != binding.SyncBase {
		t.Fatalf("binding and Head differ: binding=%+v head=%+v err=%v", binding, head, err)
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
