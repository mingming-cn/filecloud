package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
