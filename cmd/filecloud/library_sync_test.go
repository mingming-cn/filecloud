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
				t.Fatal(err)
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
		if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
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

func TestLibrarySyncRejectsBothSidesChanged(t *testing.T) {
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
	err := syncTestWorktree(t, subscriberDir, subscriberTree)
	if err == nil || !strings.Contains(err.Error(), "merge is not supported") {
		t.Fatalf("both changed error=%v", err)
	}
	if data, err := os.ReadFile(filepath.Join(subscriberTree, "local")); err != nil || string(data) != "local" {
		t.Fatalf("rejected merge changed local data=%q err=%v", data, err)
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
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var candidate string
	if err := db.QueryRow("SELECT candidate_commit FROM pending_publications WHERE worktree = ?", worktree).Scan(&candidate); err != nil {
		t.Fatal(err)
	}
	return candidate
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
	entries, err := os.ReadDir(worktree)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), syncRecoveryPrefix) {
			count++
		}
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
