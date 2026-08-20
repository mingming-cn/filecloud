package main

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	libraryapi "github.com/mingming-cn/filecloud/internal/library"
	"github.com/mingming-cn/filecloud/internal/object"
)

func TestLibraryRestoreArgumentContract(t *testing.T) {
	validID := strings.Repeat("a", 64)
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing commit", args: []string{"restore", "--client-dir", t.TempDir(), "--worktree", t.TempDir(), "--path", "file.txt"}, want: "--commit"},
		{name: "short commit", args: []string{"restore", "--client-dir", t.TempDir(), "--worktree", t.TempDir(), "--commit", validID[:63], "--path", "file.txt"}, want: "complete 64-character lowercase"},
		{name: "uppercase commit", args: []string{"restore", "--client-dir", t.TempDir(), "--worktree", t.TempDir(), "--commit", strings.ToUpper(validID), "--path", "file.txt"}, want: "complete 64-character lowercase"},
		{name: "invalid path", args: []string{"restore", "--client-dir", t.TempDir(), "--worktree", t.TempDir(), "--commit", validID, "--path", "../file.txt"}, want: "canonical"},
		{name: "confirm with commit", args: []string{"restore", "--client-dir", t.TempDir(), "--worktree", t.TempDir(), "--commit", validID, "--path", "file.txt", "--confirm", validID[:12]}, want: "cannot combine"},
		{name: "confirm wrong length", args: []string{"restore", "--client-dir", t.TempDir(), "--worktree", t.TempDir(), "--confirm", validID[:11]}, want: "12-character lowercase"},
		{name: "confirm uppercase", args: []string{"restore", "--client-dir", t.TempDir(), "--worktree", t.TempDir(), "--confirm", strings.ToUpper(validID[:12])}, want: "12-character lowercase"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := runLibraryWithConfig(t.Context(), test.args, nil, io.Discard, io.Discard, libraryClientConfig{})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("restore args error=%v, want substring %q", err, test.want)
			}
		})
	}
}

func TestLibrarySyncBlocksOnUnconfirmedRestoreWithoutMutation(t *testing.T) {
	state := newImportedBinding(t)
	sourceCommit := state.binding.SyncBase
	if err := os.WriteFile(filepath.Join(state.worktree, "local"), []byte("current"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", state.clientDir, "--worktree", state.worktree}, nil, io.Discard, io.Discard,
		libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	if err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--commit", sourceCommit, "--path", "local"}, nil, io.Discard, io.Discard, libraryClientConfig{
		checkFilesystem: func(*os.File) error { return nil },
	}); err != nil {
		t.Fatal(err)
	}
	before := readTestPendingPublication(t, state.clientDir, state.worktree)
	beforeBinding := readTestBinding(t, state.clientDir, state.worktree)
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", state.clientDir, "--worktree", state.worktree}, nil, io.Discard, io.Discard,
		libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }})
	if err == nil || !strings.Contains(err.Error(), "restore --confirm "+before.CandidateCommit[:deleteCandidatePrefixLen]) {
		t.Fatalf("sync unconfirmed restore error=%v", err)
	}
	after := readTestPendingPublication(t, state.clientDir, state.worktree)
	if after.CandidateCommit != before.CandidateCommit || after.RestoreConfirmed || readTestBinding(t, state.clientDir, state.worktree) != beforeBinding {
		t.Fatalf("sync changed unconfirmed restore: before=%+v after=%+v binding=%+v", before, after, readTestBinding(t, state.clientDir, state.worktree))
	}
}

func TestLibraryRestoreConfirmResumesAlreadyPublishedCandidate(t *testing.T) {
	state := newImportedBinding(t)
	sourceCommit := state.binding.SyncBase
	if err := os.WriteFile(filepath.Join(state.worktree, "local"), []byte("current"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", state.clientDir, "--worktree", state.worktree}, nil, io.Discard, io.Discard,
		libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	if err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--commit", sourceCommit, "--path", "local"}, nil, io.Discard, io.Discard, libraryClientConfig{
		checkFilesystem: func(*os.File) error { return nil },
	}); err != nil {
		t.Fatal(err)
	}
	pending := readTestPendingPublication(t, state.clientDir, state.worktree)
	config := libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}
	restoreState, err := openRestoreClientState(t.Context(), state.clientDir, state.worktree, io.Discard, config)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := scanWorktreeWithConfig(restoreState.root, restoreState.options.scanConfig)
	if err != nil {
		restoreState.Close()
		t.Fatal(err)
	}
	source, err := fetchRestoreSource(t.Context(), restoreState, pending.SourceCommit)
	if err != nil {
		restoreState.Close()
		t.Fatal(err)
	}
	plan, err := planRestoreSnapshot(t.Context(), restoreState, snapshot, source, pending.SourcePath)
	if err != nil {
		restoreState.Close()
		t.Fatal(err)
	}
	references := make([]clientObjectReference, 0, len(plan.directories)+1)
	for _, directory := range plan.directories {
		references = append(references, clientObjectReference{ObjectID: directory.id, ObjectType: "Directory"})
	}
	references = append(references, clientObjectReference{ObjectID: pending.CandidateCommit, ObjectType: "Commit"})
	missing, err := checkRemoteObjects(t.Context(), restoreState.options, references)
	if err != nil {
		restoreState.Close()
		t.Fatal(err)
	}
	for _, directory := range plan.directories {
		if missing["directories\x00"+directory.id] {
			if err := putMetadata(t.Context(), restoreState.options.base, restoreState.binding.LibraryID, restoreState.options.token,
				"directories", directory.id, directory.data); err != nil {
				restoreState.Close()
				t.Fatal(err)
			}
		}
	}
	if missing["commits\x00"+pending.CandidateCommit] {
		if err := putMetadata(t.Context(), restoreState.options.base, restoreState.binding.LibraryID, restoreState.options.token,
			"commits", pending.CandidateCommit, pending.CandidateData); err != nil {
			restoreState.Close()
			t.Fatal(err)
		}
	}
	if _, _, err := updateRemoteHead(t.Context(), restoreState.options.base, restoreState.binding.LibraryID, restoreState.options.token,
		pending.ExpectedETag, pending.CandidateCommit); err != nil {
		restoreState.Close()
		t.Fatal(err)
	}
	if _, err := restoreState.db.ExecContext(t.Context(), "UPDATE pending_publications SET restore_confirmed = 1 WHERE worktree = ?", state.worktree); err != nil {
		restoreState.Close()
		t.Fatal(err)
	}
	if err := restoreState.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--confirm", pending.CandidateCommit[:deleteCandidatePrefixLen]}, nil, io.Discard, io.Discard, config); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(state.worktree, "local")); err != nil || string(data) != "data" {
		t.Fatalf("already-published restore file=%q err=%v", data, err)
	}
	assertTestConverged(t, state.environment, state.clientDir, state.worktree)
}

func TestLibraryRestoreConfirmationDiscardsStaleHead(t *testing.T) {
	state := newImportedBinding(t)
	sourceCommit := state.binding.SyncBase
	if err := os.WriteFile(filepath.Join(state.worktree, "local"), []byte("current"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", state.clientDir, "--worktree", state.worktree}, nil, io.Discard, io.Discard,
		libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	if err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--commit", sourceCommit, "--path", "local"}, nil, io.Discard, io.Discard, libraryClientConfig{
		checkFilesystem: func(*os.File) error { return nil },
	}); err != nil {
		t.Fatal(err)
	}
	pending := readTestPendingPublication(t, state.clientDir, state.worktree)
	data, id, err := canonicalCommit(testClientUserID, testOtherDeviceID, pending.CapturedRoot,
		[]string{pending.ExpectedHead}, func() time.Time { return time.Date(2026, 8, 9, 1, 2, 4, 0, time.UTC) })
	if err != nil {
		t.Fatal(err)
	}
	base := mustServerURL(t, state.environment.server.URL)
	if err := putMetadata(t.Context(), base, testClientLibraryID, []byte(state.environment.token), "commits", id, data); err != nil {
		t.Fatal(err)
	}
	if _, _, err := updateRemoteHead(t.Context(), base, testClientLibraryID, []byte(state.environment.token), pending.ExpectedETag, id); err != nil {
		t.Fatal(err)
	}
	beforeBinding := readTestBinding(t, state.clientDir, state.worktree)
	err = runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--confirm", pending.CandidateCommit[:deleteCandidatePrefixLen]}, nil, io.Discard, io.Discard, libraryClientConfig{
		checkFilesystem: func(*os.File) error { return nil },
	})
	if err == nil || !strings.Contains(err.Error(), "stale restore candidate discarded") {
		t.Fatalf("stale Head confirmation error=%v", err)
	}
	if countClientRows(t, state.clientDir, "pending_publications", state.worktree) != 0 || readTestBinding(t, state.clientDir, state.worktree) != beforeBinding {
		t.Fatalf("stale Head changed local state: binding=%+v", readTestBinding(t, state.clientDir, state.worktree))
	}
}

func TestLibraryRestoreConfirmDiscardsETagOnlyChangeBeforeHeadCAS(t *testing.T) {
	state := newImportedBinding(t)
	sourceCommit := state.binding.SyncBase
	if err := os.WriteFile(filepath.Join(state.worktree, "local"), []byte("current"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", state.clientDir, "--worktree", state.worktree}, nil, io.Discard, io.Discard,
		libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	if err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--commit", sourceCommit, "--path", "local"}, nil, io.Discard, io.Discard, libraryClientConfig{
		checkFilesystem: func(*os.File) error { return nil },
	}); err != nil {
		t.Fatal(err)
	}
	pending := readTestPendingPublication(t, state.clientDir, state.worktree)
	beforeBinding := readTestBinding(t, state.clientDir, state.worktree)
	beforeRoot := scanTestRoot(t, state.worktree)
	beforeIndex := captureTestPathIndex(t, state.clientDir, state.worktree)
	beforeHead, err := getRemoteHead(t.Context(), mustServerURL(t, state.environment.server.URL), testClientLibraryID,
		[]byte(state.environment.token))
	if err != nil || beforeHead.CommitID == nil || *beforeHead.CommitID != pending.ExpectedHead || beforeHead.ETag != pending.ExpectedETag {
		t.Fatalf("pending restore did not capture current Head: head=%+v pending=%+v err=%v", beforeHead, pending, err)
	}
	err = runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--confirm", pending.CandidateCommit[:deleteCandidatePrefixLen]}, nil, io.Discard, io.Discard, libraryClientConfig{
		checkFilesystem: func(*os.File) error { return nil },
		beforeHeadCAS: func() error {
			_, err := state.environment.store.DB().ExecContext(t.Context(),
				`UPDATE libraries SET head_version = head_version + 1 WHERE owner_user_id = ? AND id = ?`,
				testClientUserID, testClientLibraryID)
			return err
		},
	})
	if err == nil || !strings.Contains(err.Error(), "rerun restore preview") {
		t.Fatalf("ETag-only restore confirmation error=%v", err)
	}
	afterHead, headErr := getRemoteHead(t.Context(), mustServerURL(t, state.environment.server.URL), testClientLibraryID,
		[]byte(state.environment.token))
	afterBinding := readTestBinding(t, state.clientDir, state.worktree)
	if headErr != nil || afterHead.CommitID == nil || *afterHead.CommitID != *beforeHead.CommitID || afterHead.ETag == beforeHead.ETag ||
		countClientRows(t, state.clientDir, "pending_publications", state.worktree) != 0 || afterBinding != beforeBinding ||
		scanTestRoot(t, state.worktree) != beforeRoot || !reflect.DeepEqual(captureTestPathIndex(t, state.clientDir, state.worktree), beforeIndex) {
		t.Fatalf("ETag-only restore changed durable state: beforeHead=%+v afterHead=%+v beforeBinding=%+v afterBinding=%+v beforeRoot=%s afterRoot=%s",
			beforeHead, afterHead, beforeBinding, afterBinding, beforeRoot, scanTestRoot(t, state.worktree))
	}
}

func TestLibraryRestoreMissingSourcePathDoesNotCreateCandidate(t *testing.T) {
	state := newImportedBinding(t)
	var output bytes.Buffer
	err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--commit", state.binding.SyncBase, "--path", "missing"}, nil, &output, io.Discard, libraryClientConfig{
		checkFilesystem: func(*os.File) error { return nil },
	})
	if err == nil || !strings.Contains(err.Error(), "source path not found") {
		t.Fatalf("missing source path error=%v", err)
	}
	if countClientRows(t, state.clientDir, "pending_publications", state.worktree) != 0 || output.Len() != 0 {
		t.Fatalf("missing source path changed state: output=%q pending=%d", output.String(), countClientRows(t, state.clientDir, "pending_publications", state.worktree))
	}
}

func TestLibraryRestoreConfirmationIsolationAndStaleWorktree(t *testing.T) {
	state := newImportedBinding(t)
	sourceCommit := state.binding.SyncBase
	if err := os.WriteFile(filepath.Join(state.worktree, "local"), []byte("current"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", state.clientDir, "--worktree", state.worktree}, nil, io.Discard, io.Discard,
		libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	if err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--commit", sourceCommit, "--path", "local"}, nil, io.Discard, io.Discard, libraryClientConfig{
		checkFilesystem: func(*os.File) error { return nil },
	}); err != nil {
		t.Fatal(err)
	}
	pending := readTestPendingPublication(t, state.clientDir, state.worktree)
	beforeBinding := readTestBinding(t, state.clientDir, state.worktree)
	if err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--confirm", strings.Repeat("0", deleteCandidatePrefixLen)}, nil, io.Discard, io.Discard, libraryClientConfig{
		checkFilesystem: func(*os.File) error { return nil },
	}); err == nil || !strings.Contains(err.Error(), "exactly match") {
		t.Fatalf("wrong restore confirmation error=%v", err)
	}
	afterWrong := readTestPendingPublication(t, state.clientDir, state.worktree)
	if afterWrong.CandidateCommit != pending.CandidateCommit || afterWrong.RestoreConfirmed || readTestBinding(t, state.clientDir, state.worktree) != beforeBinding {
		t.Fatalf("wrong confirmation changed state: before=%+v after=%+v", pending, afterWrong)
	}
	if err := os.WriteFile(filepath.Join(state.worktree, "local"), []byte("outside-preview"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--confirm", pending.CandidateCommit[:deleteCandidatePrefixLen]}, nil, io.Discard, io.Discard, libraryClientConfig{
		checkFilesystem: func(*os.File) error { return nil },
	})
	if err == nil || !strings.Contains(err.Error(), "stale restore candidate discarded") {
		t.Fatalf("stale worktree confirmation error=%v", err)
	}
	if countClientRows(t, state.clientDir, "pending_publications", state.worktree) != 0 || readTestBinding(t, state.clientDir, state.worktree) != beforeBinding {
		t.Fatalf("stale worktree confirmation changed durable state: binding=%+v", readTestBinding(t, state.clientDir, state.worktree))
	}
}

func TestLibraryRestoreNoOpCreatesNoPendingOrPublication(t *testing.T) {
	state := newImportedBinding(t)
	var cas int
	var output bytes.Buffer
	err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--commit", state.binding.SyncBase, "--path", "local"}, nil, &output, io.Discard, libraryClientConfig{
		checkFilesystem: func(*os.File) error { return nil },
		beforeHeadCAS:   func() error { cas++; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "restore no-op") || cas != 0 || countClientRows(t, state.clientDir, "pending_publications", state.worktree) != 0 {
		t.Fatalf("no-op output=%q cas=%d pending=%d", output.String(), cas, countClientRows(t, state.clientDir, "pending_publications", state.worktree))
	}
}

func TestLibraryRestoreDirectoryOverlayPreservesCurrentOnlyAndConverges(t *testing.T) {
	state := newImportedBinding(t)
	sourceFileMtime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	sourceDirectoryMtime := time.Date(2026, 1, 2, 3, 5, 0, 0, time.UTC)
	if err := os.Mkdir(filepath.Join(state.worktree, "docs"), 0o700); err != nil {
		t.Fatal(err)
	}
	for path, content := range map[string]string{
		"docs/deleted.txt":     "restore deleted",
		"docs/mtime-only.txt":  "same content",
		"docs/overwritten.txt": "restore old",
	} {
		if err := os.WriteFile(filepath.Join(state.worktree, filepath.FromSlash(path)), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		setRestoreTestMtime(t, filepath.Join(state.worktree, filepath.FromSlash(path)), sourceFileMtime)
	}
	setRestoreTestMtime(t, filepath.Join(state.worktree, "docs"), sourceDirectoryMtime)
	runRestoreTestSync(t, state.clientDir, state.worktree)
	sourceBinding := readTestBinding(t, state.clientDir, state.worktree)
	sourceCommit, sourceRoot := sourceBinding.SyncBase, sourceBinding.SyncBaseRoot

	if err := os.WriteFile(filepath.Join(state.worktree, "docs", "overwritten.txt"), []byte("current overwrite"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(state.worktree, "docs", "deleted.txt")); err != nil {
		t.Fatal(err)
	}
	currentFileMtime := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	for _, path := range []string{"docs/overwritten.txt", "docs/mtime-only.txt"} {
		setRestoreTestMtime(t, filepath.Join(state.worktree, filepath.FromSlash(path)), currentFileMtime)
	}
	if err := os.WriteFile(filepath.Join(state.worktree, "docs", "current.txt"), []byte("current file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(state.worktree, "docs", "current-dir"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state.worktree, "docs", "current-dir", "nested.txt"), []byte("current nested"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(state.worktree, "docs", "current-empty"), 0o700); err != nil {
		t.Fatal(err)
	}
	currentOnlyMtime := time.Date(2026, 2, 4, 5, 6, 7, 0, time.UTC)
	for _, path := range []string{"docs/current.txt", "docs/current-dir/nested.txt", "docs/current-dir", "docs/current-empty"} {
		setRestoreTestMtime(t, filepath.Join(state.worktree, filepath.FromSlash(path)), currentOnlyMtime)
	}
	currentDirectoryMtime := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	setRestoreTestMtime(t, filepath.Join(state.worktree, "docs"), currentDirectoryMtime)
	runRestoreTestSync(t, state.clientDir, state.worktree)
	beforeBinding := readTestBinding(t, state.clientDir, state.worktree)
	beforeHead := restoreTestHead(t, state)
	beforeIndex := captureTestPathIndex(t, state.clientDir, state.worktree)
	beforeWorktree := captureHistoryInspectWorktree(t, state.worktree)

	var preview bytes.Buffer
	config := libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, now: func() time.Time {
		return time.Date(2026, 8, 9, 1, 2, 3, 0, time.UTC)
	}}
	if err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--commit", sourceCommit, "--path", "docs"}, nil, &preview, io.Discard, config); err != nil {
		t.Fatal(err)
	}
	pending := readTestPendingPublication(t, state.clientDir, state.worktree)
	wantChanged := []string{"docs", "docs/deleted.txt", "docs/mtime-only.txt", "docs/overwritten.txt"}
	changed, err := _decodeRestorePreview(pending.ChangedPathPreview)
	if err != nil {
		t.Fatal(err)
	}
	if pending.SourceRoot != sourceRoot || pending.CreatedCount != 1 || pending.UpdatedCount != 3 ||
		pending.PreservedCurrentOnlyCount != 4 || pending.TypeReplacementCount != 0 || pending.RemovedDescendantCount != 0 ||
		pending.ChangedPathCount != int64(len(wantChanged)) || !equalStrings(changed, wantChanged) {
		t.Fatalf("directory restore pending=%+v changed=%v, want changed=%v", pending, changed, wantChanged)
	}
	for _, line := range []string{"created paths: 1", "updated paths: 3", "preserved current-only paths: 4"} {
		if !strings.Contains(preview.String(), line) {
			t.Fatalf("directory preview %q does not contain %q", preview.String(), line)
		}
	}
	afterPreviewHead := restoreTestHead(t, state)
	if beforeBinding != readTestBinding(t, state.clientDir, state.worktree) || beforeHead != afterPreviewHead ||
		!reflect.DeepEqual(beforeIndex, captureTestPathIndex(t, state.clientDir, state.worktree)) ||
		!reflect.DeepEqual(beforeWorktree, captureHistoryInspectWorktree(t, state.worktree)) {
		t.Fatal("directory restore preview mutated Head, binding, index, or worktree")
	}

	if err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--confirm", pending.CandidateCommit[:deleteCandidatePrefixLen]}, nil, io.Discard, io.Discard, config); err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]string{
		"docs/deleted.txt":            "restore deleted",
		"docs/mtime-only.txt":         "same content",
		"docs/overwritten.txt":        "restore old",
		"docs/current.txt":            "current file",
		"docs/current-dir/nested.txt": "current nested",
	} {
		data, err := os.ReadFile(filepath.Join(state.worktree, filepath.FromSlash(path)))
		if err != nil || string(data) != want {
			t.Fatalf("restored %s=%q err=%v, want %q", path, data, err, want)
		}
	}
	if info, err := os.Stat(filepath.Join(state.worktree, "docs", "current-empty")); err != nil || !info.IsDir() {
		t.Fatalf("current-only empty directory: info=%v err=%v", info, err)
	}
	assertRestoreTestMtime(t, filepath.Join(state.worktree, "docs", "deleted.txt"), sourceFileMtime)
	assertRestoreTestMtime(t, filepath.Join(state.worktree, "docs", "mtime-only.txt"), sourceFileMtime)
	assertRestoreTestMtime(t, filepath.Join(state.worktree, "docs", "overwritten.txt"), sourceFileMtime)
	for _, path := range []string{"docs/current.txt", "docs/current-dir/nested.txt", "docs/current-dir", "docs/current-empty"} {
		assertRestoreTestMtime(t, filepath.Join(state.worktree, filepath.FromSlash(path)), currentOnlyMtime)
	}
	assertRestoreTestMtime(t, filepath.Join(state.worktree, "docs"), currentDirectoryMtime)

	binding := assertTestConverged(t, state.environment, state.clientDir, state.worktree)
	for _, table := range []string{"pending_publications", "pending_checkouts", "checkout_paths", "fs_actions"} {
		if rows := countClientRows(t, state.clientDir, table, state.worktree); rows != 0 {
			t.Fatalf("directory restore left %d %s rows", rows, table)
		}
	}
	commit, err := getRemoteCommit(t.Context(), mustServerURL(t, state.environment.server.URL), testClientLibraryID,
		[]byte(state.environment.token), binding.SyncBase)
	if err != nil || len(commit.Parents) != 1 || commit.Parents[0] != beforeBinding.SyncBase || commit.Root != pending.CandidateRoot {
		t.Fatalf("directory restore commit=%+v err=%v pending=%+v", commit, err, pending)
	}
	for description, commitID := range map[string]string{"source": sourceCommit, "previous Head": beforeBinding.SyncBase} {
		if historical, err := getRemoteCommit(t.Context(), mustServerURL(t, state.environment.server.URL), testClientLibraryID,
			[]byte(state.environment.token), commitID); err != nil || historical.Root == "" {
			t.Fatalf("%s commit remains reachable: commit=%+v err=%v", description, historical, err)
		}
	}
}

func TestLibraryRestoreRootOverlayAndNoOpIgnoreFilesystemRootMtime(t *testing.T) {
	state, puts := newRestoreRequestFixture(t)
	sourceCommit := state.binding.SyncBase
	if err := os.WriteFile(filepath.Join(state.worktree, "local"), []byte("current"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state.worktree, "root-current-only.txt"), []byte("preserved"), 0o600); err != nil {
		t.Fatal(err)
	}
	runRestoreTestSync(t, state.clientDir, state.worktree)
	setRestoreTestMtime(t, state.worktree, time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC))
	puts.Store(0)

	var preview bytes.Buffer
	config := libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, now: func() time.Time {
		return time.Date(2026, 8, 9, 2, 3, 4, 0, time.UTC)
	}}
	if err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--commit", sourceCommit, "--path", "."}, nil, &preview, io.Discard, config); err != nil {
		t.Fatal(err)
	}
	pending := readTestPendingPublication(t, state.clientDir, state.worktree)
	if pending.SourcePath != "." || pending.UpdatedCount != 1 || pending.PreservedCurrentOnlyCount != 1 ||
		pending.CreatedCount != 0 || pending.TypeReplacementCount != 0 || puts.Load() != 0 {
		t.Fatalf("root preview pending=%+v puts=%d", pending, puts.Load())
	}
	if err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--confirm", pending.CandidateCommit[:deleteCandidatePrefixLen]}, nil, io.Discard, io.Discard, config); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(state.worktree, "local")); err != nil || string(data) != "data" {
		t.Fatalf("root restored local=%q err=%v", data, err)
	}
	if data, err := os.ReadFile(filepath.Join(state.worktree, "root-current-only.txt")); err != nil || string(data) != "preserved" {
		t.Fatalf("root current-only=%q err=%v", data, err)
	}
	assertTestConverged(t, state.environment, state.clientDir, state.worktree)

	puts.Store(0)
	var noOp bytes.Buffer
	if err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--commit", sourceCommit, "--path", "."}, nil, &noOp, io.Discard, config); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(noOp.String(), "restore no-op") || puts.Load() != 0 ||
		countClientRows(t, state.clientDir, "pending_publications", state.worktree) != 0 {
		t.Fatalf("root no-op output=%q puts=%d", noOp.String(), puts.Load())
	}
}

func TestLibraryRestoreMissingSourceDirectoryHasNoWrites(t *testing.T) {
	state, puts := newRestoreRequestFixture(t)
	puts.Store(0)
	var output bytes.Buffer
	err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--commit", state.binding.SyncBase, "--path", "missing-directory"}, nil, &output, io.Discard,
		libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }})
	if err == nil || !strings.Contains(err.Error(), "source path not found") {
		t.Fatalf("missing source directory error=%v", err)
	}
	if puts.Load() != 0 || output.Len() != 0 || countClientRows(t, state.clientDir, "pending_publications", state.worktree) != 0 {
		t.Fatalf("missing source directory output=%q puts=%d", output.String(), puts.Load())
	}
}

func TestLibraryRestoreTypeConflictRejectsBeforeWrites(t *testing.T) {
	state, puts := newRestoreRequestFixture(t)
	if err := os.Mkdir(filepath.Join(state.worktree, "target"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state.worktree, "target", "child.txt"), []byte("source child"), 0o600); err != nil {
		t.Fatal(err)
	}
	runRestoreTestSync(t, state.clientDir, state.worktree)
	sourceCommit := readTestBinding(t, state.clientDir, state.worktree).SyncBase
	if err := os.RemoveAll(filepath.Join(state.worktree, "target")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state.worktree, "target"), []byte("current file"), 0o600); err != nil {
		t.Fatal(err)
	}
	runRestoreTestSync(t, state.clientDir, state.worktree)
	beforeBinding := readTestBinding(t, state.clientDir, state.worktree)
	beforeHead := restoreTestHead(t, state)
	beforeIndex := captureTestPathIndex(t, state.clientDir, state.worktree)
	beforeWorktree := captureHistoryInspectWorktree(t, state.worktree)
	puts.Store(0)

	var output bytes.Buffer
	err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--commit", sourceCommit, "--path", "target/child.txt"}, nil, &output, io.Discard,
		libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }})
	if !errors.Is(err, _errRestoreTypeConflict) {
		t.Fatalf("public type conflict error=%v, want _errRestoreTypeConflict", err)
	}
	if puts.Load() != 0 || output.Len() != 0 || countClientRows(t, state.clientDir, "pending_publications", state.worktree) != 0 ||
		beforeBinding != readTestBinding(t, state.clientDir, state.worktree) || beforeHead != restoreTestHead(t, state) ||
		!reflect.DeepEqual(beforeIndex, captureTestPathIndex(t, state.clientDir, state.worktree)) ||
		!reflect.DeepEqual(beforeWorktree, captureHistoryInspectWorktree(t, state.worktree)) {
		t.Fatalf("type conflict changed state or sent PUT: puts=%d output=%q", puts.Load(), output.String())
	}
}

func runRestoreTestSync(t *testing.T, clientDir, worktree string) {
	t.Helper()
	config := libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", clientDir, "--worktree", worktree},
		nil, io.Discard, io.Discard, config)
	confirmation, ok := errors.AsType[*deleteConfirmationRequiredError](err)
	if ok {
		err = confirmTestDeletion(t, clientDir, worktree, confirmation.candidate[:deleteCandidatePrefixLen], config)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func setRestoreTestMtime(t *testing.T, path string, value time.Time) {
	t.Helper()
	if err := os.Chtimes(path, value, value); err != nil {
		t.Fatal(err)
	}
}

func assertRestoreTestMtime(t *testing.T, path string, want time.Time) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s mtime: %v", path, err)
	}
	if !info.ModTime().Equal(want) {
		t.Fatalf("%s mtime=%v, want %v", path, info.ModTime(), want)
	}
}

func restoreTestHead(t *testing.T, state importedBinding) string {
	t.Helper()
	head, err := getRemoteHead(t.Context(), mustServerURL(t, state.environment.server.URL), testClientLibraryID,
		[]byte(state.environment.token))
	if err != nil || head.CommitID == nil {
		t.Fatalf("read restore test Head: head=%+v err=%v", head, err)
	}
	return *head.CommitID + "\x00" + head.ETag
}

func newRestoreRequestFixture(t *testing.T) (importedBinding, *atomic.Int32) {
	t.Helper()
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	var puts atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			puts.Add(1)
		}
		environment.handler.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	clientDir, worktree := newClientPaths(t)
	if err := os.WriteFile(filepath.Join(worktree, "local"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	args := append(bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testClientDeviceID), "--import-local")
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	return importedBinding{environment: environment, clientDir: clientDir, worktree: worktree, args: args,
		binding: readTestBinding(t, clientDir, worktree)}, &puts
}

func TestLibraryRestoreConfirmPublishesFileRestoreAndConverges(t *testing.T) {
	state := newImportedBinding(t)
	sourceCommit := state.binding.SyncBase
	if err := os.WriteFile(filepath.Join(state.worktree, "local"), []byte("current"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", state.clientDir, "--worktree", state.worktree}, nil, io.Discard, io.Discard,
		libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	var preview bytes.Buffer
	if err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--commit", sourceCommit, "--path", "local"}, nil, &preview, io.Discard, libraryClientConfig{
		checkFilesystem: func(*os.File) error { return nil },
		now:             func() time.Time { return time.Date(2026, 8, 9, 1, 2, 3, 0, time.UTC) },
	}); err != nil {
		t.Fatal(err)
	}
	candidate := readTestPendingPublication(t, state.clientDir, state.worktree)
	if err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--confirm", candidate.CandidateCommit[:deleteCandidatePrefixLen]}, nil, io.Discard, io.Discard, libraryClientConfig{
		checkFilesystem: func(*os.File) error { return nil },
	}); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(state.worktree, "local")); err != nil || string(data) != "data" {
		t.Fatalf("restored file=%q err=%v", data, err)
	}
	binding := assertTestConverged(t, state.environment, state.clientDir, state.worktree)
	if countClientRows(t, state.clientDir, "pending_publications", state.worktree) != 0 || countClientRows(t, state.clientDir, "pending_checkouts", state.worktree) != 0 {
		t.Fatal("restore left pending publication or checkout")
	}
	commit, err := getRemoteCommit(t.Context(), mustServerURL(t, state.environment.server.URL), testClientLibraryID,
		[]byte(state.environment.token), binding.SyncBase)
	if err != nil || len(commit.Parents) != 1 || commit.Parents[0] != candidate.ExpectedHead || commit.Message != "restore "+sourceCommit+" local" {
		t.Fatalf("restore commit=%+v err=%v candidate=%+v", commit, err, candidate)
	}
	if before, err := getRemoteCommit(t.Context(), mustServerURL(t, state.environment.server.URL), testClientLibraryID,
		[]byte(state.environment.token), sourceCommit); err != nil || before.Root == "" {
		t.Fatalf("source commit is not reachable: commit=%+v err=%v", before, err)
	}
}

func TestLibraryRestoreAcceptsPublishedMergeSource(t *testing.T) {
	state := newImportedBinding(t)
	baseURL := mustServerURL(t, state.environment.server.URL)
	token := []byte(state.environment.token)
	base := state.binding.SyncBase
	sourceData, sourceCommit, err := canonicalCommit(testClientUserID, testClientDeviceID, state.binding.SyncBaseRoot,
		[]string{base}, func() time.Time { return time.Date(2026, 8, 9, 1, 2, 1, 0, time.UTC) })
	if err != nil {
		t.Fatal(err)
	}
	if err := putMetadata(t.Context(), baseURL, testClientLibraryID, token, "commits", sourceCommit, sourceData); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(state.worktree, "local"), []byte("current"), 0o600); err != nil {
		t.Fatal(err)
	}
	runRestoreTestSync(t, state.clientDir, state.worktree)
	current := readTestBinding(t, state.clientDir, state.worktree)
	mergedData, mergedCommit, err := canonicalCommit(testClientUserID, testClientDeviceID, current.SyncBaseRoot,
		[]string{current.SyncBase, sourceCommit}, func() time.Time { return time.Date(2026, 8, 9, 1, 2, 2, 0, time.UTC) })
	if err != nil {
		t.Fatal(err)
	}
	if err := putMetadata(t.Context(), baseURL, testClientLibraryID, token, "commits", mergedCommit, mergedData); err != nil {
		t.Fatal(err)
	}
	if _, conflict, err := updateRemoteHead(t.Context(), baseURL, testClientLibraryID, token, current.HeadETag, mergedCommit); err != nil || conflict {
		t.Fatalf("publish merged history: conflict=%t err=%v", conflict, err)
	}
	runRestoreTestSync(t, state.clientDir, state.worktree)

	config := libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}
	var preview bytes.Buffer
	if err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--commit", sourceCommit, "--path", "local"}, nil, &preview, io.Discard, config); err != nil {
		t.Fatalf("preview merge-source restore: %v", err)
	}
	pending := readTestPendingPublication(t, state.clientDir, state.worktree)
	if pending.SourceCommit != sourceCommit || !strings.Contains(preview.String(), "source commit: "+sourceCommit) {
		t.Fatalf("merge-source preview=%q pending=%+v", preview.String(), pending)
	}
	if err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--confirm", pending.CandidateCommit[:deleteCandidatePrefixLen]}, nil, io.Discard, io.Discard, config); err != nil {
		t.Fatalf("confirm merge-source restore: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(state.worktree, "local")); err != nil || string(data) != "data" {
		t.Fatalf("merge-source restored file=%q err=%v", data, err)
	}
	finalBinding := assertTestConverged(t, state.environment, state.clientDir, state.worktree)
	options := bindOptions{base: baseURL, libraryID: testClientLibraryID, token: token}
	for name, commitID := range map[string]string{
		"source": sourceCommit, "previous Head": pending.ExpectedHead, "restore candidate": pending.CandidateCommit,
	} {
		reachable, err := _remoteCommitDescendsFrom(t.Context(), options, finalBinding.SyncBase, commitID,
			finalBinding.UserID, _newReplayBudget())
		if err != nil || !reachable {
			t.Fatalf("%s Commit %s reachable from final Head %s = %t, error=%v",
				name, commitID, finalBinding.SyncBase, reachable, err)
		}
	}
}

func TestLibraryRestorePreviewPersistsFixedCandidateWithoutMutation(t *testing.T) {
	state := newImportedBinding(t)
	sourceCommit := state.binding.SyncBase
	if err := os.WriteFile(filepath.Join(state.worktree, "local"), []byte("current"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", state.clientDir, "--worktree", state.worktree}, nil, io.Discard, io.Discard,
		libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	beforeBinding := readTestBinding(t, state.clientDir, state.worktree)
	beforeRoot := scanTestRoot(t, state.worktree)
	var cas int
	var output bytes.Buffer
	err := runLibraryWithConfig(t.Context(), []string{"restore", "--client-dir", state.clientDir, "--worktree", state.worktree,
		"--commit", sourceCommit, "--path", "local"}, nil, &output, io.Discard, libraryClientConfig{
		beforeHeadCAS:   func() error { cas++; return nil },
		now:             func() time.Time { return time.Date(2026, 8, 9, 1, 2, 3, 0, time.UTC) },
		checkFilesystem: func(*os.File) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	pending := readTestPendingPublication(t, state.clientDir, state.worktree)
	if pending.Kind != PublicationKindRestore || pending.SourceCommit != sourceCommit || pending.SourcePath != "local" ||
		pending.ExpectedHead != beforeBinding.SyncBase || pending.CapturedRoot != beforeBinding.SyncBaseRoot || !bytes.Contains(output.Bytes(), []byte("source commit: "+sourceCommit)) ||
		!bytes.Contains(output.Bytes(), []byte("confirm: filecloud library restore")) {
		t.Fatalf("preview output=%q pending=%+v binding=%+v", output.String(), pending, beforeBinding)
	}
	candidate, err := object.VerifyCommit(pending.CandidateData, pending.CandidateCommit)
	if err != nil || candidate.Root != pending.CandidateRoot || len(candidate.Parents) != 1 || candidate.Parents[0] != pending.ExpectedHead ||
		candidate.Message != "restore "+sourceCommit+" local" || candidate.CreatedAt != "2026-08-09T01:02:03Z" {
		t.Fatalf("candidate=%+v err=%v pending=%+v", candidate, err, pending)
	}
	if cas != 0 || readTestBinding(t, state.clientDir, state.worktree) != beforeBinding || scanTestRoot(t, state.worktree) != beforeRoot {
		t.Fatalf("preview mutated state: cas=%d binding=%+v root=%s want binding=%+v root=%s", cas,
			readTestBinding(t, state.clientDir, state.worktree), scanTestRoot(t, state.worktree), beforeBinding, beforeRoot)
	}
}
