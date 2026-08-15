package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mingming-cn/filecloud/internal/object"
	"github.com/mingming-cn/filecloud/internal/storage"
)

const (
	_checkTestOwner   = "41234567-89ab-4def-8123-456789abcdef"
	_checkTestLibrary = "51234567-89ab-4def-8123-456789abcdef"
	_checkTestDevice  = "61234567-89ab-4def-8123-456789abcdef"
)

func TestRunIntegrityCheckReportsStableSafeResults(t *testing.T) {
	dataDir, blockID, fileID, blockPath := newIntegrityCheckFixture(t)
	var healthy bytes.Buffer
	if err := run(t.Context(), []string{"integrity", "check", "--data-dir", dataDir}, strings.NewReader(""), &healthy, io.Discard); err != nil {
		t.Fatalf("run healthy check: %v", err)
	}
	if got, want := healthy.String(), "integrity libraries=1 objects=4 issues=0\n"; got != want {
		t.Fatalf("healthy output = %q, want %q", got, want)
	}

	privateContent := []byte("damaged private content")
	if err := os.WriteFile(blockPath, privateContent, 0o600); err != nil {
		t.Fatalf("damage block: %v", err)
	}
	var first bytes.Buffer
	err := run(t.Context(), []string{"integrity", "check", "--data-dir", dataDir}, strings.NewReader(""), &first, io.Discard)
	if err == nil || err.Error() != "integrity check found 2 issues" {
		t.Fatalf("run corrupt check error = %v, want issue count", err)
	}
	want := fmt.Sprintf(
		"library=%s owner=%s object=Block id=%s... state=corrupt\n"+
			"library=%s owner=%s object=File id=%s... state=corrupt\n"+
			"integrity libraries=1 objects=4 issues=2\n",
		_checkTestLibrary, _checkTestOwner, blockID[:12], _checkTestLibrary, _checkTestOwner, fileID[:12],
	)
	if first.String() != want {
		t.Fatalf("corrupt output = %q, want %q", first.String(), want)
	}
	for _, secret := range []string{blockID, fileID, string(privateContent), blockPath, "metadata.db"} {
		if strings.Contains(first.String(), secret) {
			t.Fatalf("check output exposed %q: %q", secret, first.String())
		}
	}

	var second bytes.Buffer
	if err := run(t.Context(), []string{"integrity", "check", "--data-dir", dataDir}, strings.NewReader(""), &second, io.Discard); err == nil {
		t.Fatal("repeated corrupt check unexpectedly succeeded")
	}
	if second.String() != first.String() {
		t.Fatalf("repeated output = %q, want stable %q", second.String(), first.String())
	}
}

func TestRunIntegrityCheckFailsForEveryPublishedObjectType(t *testing.T) {
	for _, objectType := range []string{"Block", "File", "Directory", "Commit"} {
		for _, state := range []string{"missing", "corrupt"} {
			t.Run(objectType+"/"+state, func(t *testing.T) {
				dataDir, _, _, _ := newIntegrityCheckFixture(t)
				path, id := integrityCheckFixtureObject(t, dataDir, objectType)
				if state == "missing" {
					if err := os.Remove(path); err != nil {
						t.Fatalf("remove %s: %v", objectType, err)
					}
				} else if err := os.WriteFile(path, []byte("damaged private bytes"), 0o600); err != nil {
					t.Fatalf("damage %s: %v", objectType, err)
				}

				var output bytes.Buffer
				err := run(t.Context(), []string{"integrity", "check", "--data-dir", dataDir}, strings.NewReader(""), &output, io.Discard)
				if err == nil {
					t.Fatalf("run with %s %s unexpectedly succeeded", state, objectType)
				}
				want := fmt.Sprintf("library=%s owner=%s object=%s id=%s... state=%s",
					_checkTestLibrary, _checkTestOwner, objectType, id[:12], state)
				if !strings.Contains(output.String(), want) {
					t.Fatalf("output = %q, want %q", output.String(), want)
				}
				if strings.Contains(output.String(), id) || strings.Contains(output.String(), path) {
					t.Fatalf("output exposed full identity or path: %q", output.String())
				}
			})
		}
	}
}

func TestRunIntegrityCheckLockExcludesServe(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := storage.Init(t.Context(), dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	checker, err := storage.OpenIntegrityChecker(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenIntegrityChecker: %v", err)
	}
	serveErr := run(t.Context(), []string{"serve", "--data-dir", dataDir, "--listen", "127.0.0.1:0"}, strings.NewReader(""), io.Discard, io.Discard)
	if serveErr == nil || !strings.Contains(serveErr.Error(), "locked by another process") {
		t.Fatalf("serve while integrity check holds lock error = %v, want lock conflict", serveErr)
	}
	if err := checker.Close(); err != nil {
		t.Fatalf("close checker: %v", err)
	}
}

func TestRunIntegrityCheckArguments(t *testing.T) {
	for _, args := range [][]string{
		{"integrity"},
		{"integrity", "unknown"},
		{"integrity", "check", "--data-dir", t.TempDir(), "extra"},
	} {
		err := run(t.Context(), args, strings.NewReader(""), io.Discard, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "usage: filecloud integrity check --data-dir path") {
			t.Errorf("run(%q) error = %v, want check usage", args, err)
		}
	}
}

func integrityCheckFixtureObject(t *testing.T, dataDir, objectType string) (string, string) {
	t.Helper()
	kinds := map[string]string{
		"Block": "blocks", "File": "files", "Directory": "directories", "Commit": "commits",
	}
	pattern := filepath.Join(dataDir, "objects", _checkTestOwner, _checkTestLibrary, kinds[objectType], "*", "*")
	paths, err := filepath.Glob(pattern)
	if err != nil || len(paths) != 1 {
		t.Fatalf("Glob(%s) = %v, %v; want one object", objectType, paths, err)
	}
	path := paths[0]
	id := filepath.Base(filepath.Dir(path)) + filepath.Base(path)
	return path, id
}

func newIntegrityCheckFixture(t *testing.T) (dataDir, blockID, fileID, blockPath string) {
	t.Helper()
	dataDir = filepath.Join(t.TempDir(), "data")
	if err := storage.Init(t.Context(), dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	store, err := storage.OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenForServe: %v", err)
	}
	t.Cleanup(func() {
		if store != nil {
			_ = store.Close()
		}
	})
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	if err := store.CreateUser(t.Context(), storage.User{ID: _checkTestOwner, Username: "checker", PasswordHash: "hash"}, now); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if _, _, err := store.CreateLibrary(t.Context(), storage.Library{
		ID: _checkTestLibrary, OwnerUserID: _checkTestOwner, Name: "private library",
	}, now); err != nil {
		t.Fatalf("CreateLibrary: %v", err)
	}
	blockData := []byte("private file content")
	blockID = object.ID(blockData)
	fileData, fileID, err := object.Canonicalize("files", []byte(fmt.Sprintf(
		`{"Blocks":[%q],"Size":%q,"Type":"File","Version":1}`, blockID, fmt.Sprint(len(blockData)),
	)))
	if err != nil {
		t.Fatalf("canonicalize File: %v", err)
	}
	directoryData, directoryID, err := object.Canonicalize("directories", []byte(fmt.Sprintf(
		`{"Entries":[{"Id":%q,"ModifiedAt":"2026-08-15T12:00:00Z","Name":"secret.txt","Type":"File"}],"Type":"Directory","Version":1}`, fileID,
	)))
	if err != nil {
		t.Fatalf("canonicalize Directory: %v", err)
	}
	commitData, commitID, err := object.Canonicalize("commits", []byte(fmt.Sprintf(
		`{"AuthorUserId":%q,"CreatedAt":"2026-08-15T12:00:00Z","DeviceId":%q,"Message":"private message","Parents":[],"Root":%q,"Type":"Commit","Version":1}`,
		_checkTestOwner, _checkTestDevice, directoryID,
	)))
	if err != nil {
		t.Fatalf("canonicalize Commit: %v", err)
	}
	for kind, data := range map[string][]byte{
		"blocks": blockData, "files": fileData, "directories": directoryData, "commits": commitData,
	} {
		if _, err := store.PutObject(t.Context(), _checkTestOwner, _checkTestLibrary, kind, object.ID(data), bytes.NewReader(data)); err != nil {
			t.Fatalf("PutObject(%s): %v", kind, err)
		}
	}
	if _, err := store.UpdateLibraryHead(t.Context(), _checkTestOwner, _checkTestLibrary, nil, 0, commitID, nil, now); err != nil {
		t.Fatalf("UpdateLibraryHead: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close fixture store: %v", err)
	}
	store = nil
	blockPath = filepath.Join(dataDir, "objects", _checkTestOwner, _checkTestLibrary, "blocks", blockID[:2], blockID[2:])
	return dataDir, blockID, fileID, blockPath
}
