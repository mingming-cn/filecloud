package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mingming-cn/filecloud/internal/acceptance"
)

const (
	_blackBoxLibraryID = testClientLibraryID
	_blackBoxDeviceAID = "22345678-9abc-4def-8123-456789abcdef"
	_blackBoxDeviceBID = "32345678-9abc-4def-8123-456789abcdef"
)

func TestBuiltBinaryEndToEndAcceptance(t *testing.T) {
	if os.Getenv("FILECLOUD_PLATFORM_MATRIX_CHILD") != "1" {
		t.Skip("runs inside the native 1B platform matrix")
	}
	root := os.Getenv("FILECLOUD_ACCEPTANCE_ROOT")
	if root == "" {
		t.Fatal("FILECLOUD_ACCEPTANCE_ROOT is required")
	}
	testRoot, err := os.MkdirTemp(root, ".filecloud-blackbox-")
	if err != nil {
		t.Fatalf("create black-box root: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(testRoot); err != nil {
			t.Errorf("remove black-box root: %v", err)
		}
	})

	binaryName := "filecloud"
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	binary := filepath.Join(testRoot, binaryName)
	build := exec.CommandContext(t.Context(), "go", "build", "-trimpath", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build real CLI: %v\n%s", err, output)
	}

	dataDir := filepath.Join(testRoot, "data")
	password := []byte("black-box-password-1234\n")
	defer clear(password)
	runBlackBoxCLI(t, binary, nil, "init", "--data-dir", dataDir)
	runBlackBoxCLI(t, binary, password, "user", "add", "--data-dir", dataDir, "--username", "black-box", "--password-stdin")

	server := startBlackBoxServer(t, binary, dataDir, "127.0.0.1:0")
	address := server.address
	serverURL := "http://" + address

	loginOutput := runBlackBoxCLI(t, binary, password, "login", "--server", serverURL, "--username", "black-box",
		"--device-name", "acceptance", "--password-stdin")
	var login struct {
		Session struct {
			AccessToken string
		}
	}
	if err := json.Unmarshal(loginOutput, &login); err != nil || login.Session.AccessToken == "" {
		t.Fatalf("decode login response: token_present=%t error=%v", login.Session.AccessToken != "", err)
	}
	tokenInput := []byte(login.Session.AccessToken + "\n")
	t.Cleanup(func() { clear(tokenInput) })

	createOutput := runBlackBoxCLI(t, binary, tokenInput, "library", "create", "--server", serverURL,
		"--library-id", _blackBoxLibraryID, "--name", "Black-box Documents", "--token-stdin")
	if !bytes.Contains(createOutput, []byte(_blackBoxLibraryID)) {
		t.Fatalf("create output has no library ID: %s", createOutput)
	}
	inspectOutput := runBlackBoxCLI(t, binary, tokenInput, "library", "inspect", "--server", serverURL,
		"--library-id", _blackBoxLibraryID, "--token-stdin")
	if !bytes.Contains(inspectOutput, []byte(`"Name":"Black-box Documents"`)) {
		t.Fatalf("inspect output has no library name: %s", inspectOutput)
	}
	listOutput := runBlackBoxCLI(t, binary, tokenInput, "library", "list", "--server", serverURL, "--token-stdin")
	if !bytes.Contains(listOutput, []byte(_blackBoxLibraryID)) {
		t.Fatalf("list output has no library ID: %s", listOutput)
	}

	clientA := filepath.Join(testRoot, "client-a")
	clientB := filepath.Join(testRoot, "client-b")
	worktreeA := filepath.Join(testRoot, "worktree-a")
	worktreeB := filepath.Join(testRoot, "worktree-b")
	for _, worktree := range []string{worktreeA, worktreeB} {
		if err := os.Mkdir(worktree, 0o755); err != nil {
			t.Fatalf("create worktree %s: %v", filepath.Base(worktree), err)
		}
	}
	runBlackBoxCLI(t, binary, tokenInput, "library", "bind", "--client-dir", clientA, "--server", serverURL,
		"--library-id", _blackBoxLibraryID, "--worktree", worktreeA, "--device-id", _blackBoxDeviceAID, "--token-stdin")
	runBlackBoxCLI(t, binary, tokenInput, "library", "bind", "--client-dir", clientB, "--server", serverURL,
		"--library-id", _blackBoxLibraryID, "--worktree", worktreeB, "--device-id", _blackBoxDeviceBID, "--token-stdin")

	writeBlackBoxFile(t, worktreeA, "shared.txt", "base\n")
	runBlackBoxCLI(t, binary, nil, "library", "sync", "--client-dir", clientA, "--worktree", worktreeA)
	runBlackBoxCLI(t, binary, nil, "library", "sync", "--client-dir", clientB, "--worktree", worktreeB)
	writeBlackBoxFile(t, worktreeA, "shared.txt", "from-a\n")
	writeBlackBoxFile(t, worktreeB, "shared.txt", "from-b\n")
	runBlackBoxCLI(t, binary, nil, "library", "sync", "--client-dir", clientA, "--worktree", worktreeA)
	runBlackBoxCLI(t, binary, nil, "library", "sync", "--client-dir", clientB, "--worktree", worktreeB)
	runBlackBoxCLI(t, binary, nil, "library", "sync", "--client-dir", clientA, "--worktree", worktreeA)

	snapshotA := readBlackBoxSnapshot(t, worktreeA)
	snapshotB := readBlackBoxSnapshot(t, worktreeB)
	if !slices.Equal(snapshotA, snapshotB) {
		t.Fatalf("converged snapshots differ: A=%v B=%v", snapshotA, snapshotB)
	}
	assertBlackBoxConflictLayout(t, snapshotA)

	server.stop(t)
	server = startBlackBoxServer(t, binary, dataDir, address)
	runBlackBoxCLI(t, binary, nil, "library", "sync", "--client-dir", clientA, "--worktree", worktreeA)

	oracle := libraryCLIEnvironment{server: &httptest.Server{URL: serverURL}, token: login.Session.AccessToken}
	confirmed := platformConfirmedInputs("base\n", "from-a\n", "from-b\n")
	bindingA := assertPlatformConverged(t, "real binary CLI lifecycle", oracle, clientA, worktreeA, confirmed)
	bindingB := assertPlatformConvergedWithoutAttestation(t, "real binary CLI lifecycle second client", oracle, clientB, worktreeB, confirmed)
	if bindingA.SyncBase != bindingB.SyncBase || bindingA.SyncBaseRoot != bindingB.SyncBaseRoot {
		t.Fatalf("real binary clients differ: A=%+v B=%+v", bindingA, bindingB)
	}

	runBlackBoxCLI(t, binary, tokenInput, "logout", "--server", serverURL, "--token-stdin")
	server.stop(t)
	server = startBlackBoxServer(t, binary, dataDir, address)
	command := exec.CommandContext(t.Context(), binary, "library", "inspect", "--server", serverURL,
		"--library-id", _blackBoxLibraryID, "--token-stdin")
	command.Stdin = bytes.NewReader(tokenInput)
	if output, err := command.CombinedOutput(); err == nil || !bytes.Contains(output, []byte("Unauthorized")) {
		t.Fatalf("inspect with revoked token error=%v output=%s", err, output)
	}
	server.stop(t)
}

func TestBuiltBinaryDirectoryRestoreAcceptance(t *testing.T) {
	if os.Getenv("FILECLOUD_PLATFORM_MATRIX_CHILD") != "1" {
		t.Skip("runs inside the native 1B platform matrix")
	}
	root := os.Getenv("FILECLOUD_ACCEPTANCE_ROOT")
	if root == "" {
		t.Fatal("FILECLOUD_ACCEPTANCE_ROOT is required")
	}
	testRoot, err := os.MkdirTemp(root, ".filecloud-restore-blackbox-")
	if err != nil {
		t.Fatalf("create restore black-box root: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(testRoot); err != nil {
			t.Errorf("remove restore black-box root: %v", err)
		}
	})

	binaryName := "filecloud"
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	binary := filepath.Join(testRoot, binaryName)
	build := exec.CommandContext(t.Context(), "go", "build", "-trimpath", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build restore CLI: %v\n%s", err, output)
	}
	dataDir := filepath.Join(testRoot, "data")
	password := []byte("restore-black-box-password-1234\n")
	defer clear(password)
	runBlackBoxCLI(t, binary, nil, "init", "--data-dir", dataDir)
	runBlackBoxCLI(t, binary, password, "user", "add", "--data-dir", dataDir, "--username", "restore-black-box", "--password-stdin")
	server := startBlackBoxServer(t, binary, dataDir, "127.0.0.1:0")
	serverURL := "http://" + server.address
	loginOutput := runBlackBoxCLI(t, binary, password, "login", "--server", serverURL, "--username", "restore-black-box",
		"--device-name", "restore-acceptance", "--password-stdin")
	var login struct {
		Session struct{ AccessToken string }
	}
	if err := json.Unmarshal(loginOutput, &login); err != nil || login.Session.AccessToken == "" {
		t.Fatalf("decode restore login: token_present=%t error=%v", login.Session.AccessToken != "", err)
	}
	tokenInput := []byte(login.Session.AccessToken + "\n")
	defer clear(tokenInput)
	runBlackBoxCLI(t, binary, tokenInput, "library", "create", "--server", serverURL,
		"--library-id", _blackBoxLibraryID, "--name", "Restore Acceptance", "--token-stdin")

	clientA := filepath.Join(testRoot, "client-a")
	clientB := filepath.Join(testRoot, "client-b")
	worktreeA := filepath.Join(testRoot, "worktree-a")
	worktreeB := filepath.Join(testRoot, "worktree-b")
	for _, worktree := range []string{worktreeA, worktreeB} {
		if err := os.Mkdir(worktree, 0o755); err != nil {
			t.Fatalf("create restore worktree %s: %v", filepath.Base(worktree), err)
		}
	}
	runBlackBoxCLI(t, binary, tokenInput, "library", "bind", "--client-dir", clientA, "--server", serverURL,
		"--library-id", _blackBoxLibraryID, "--worktree", worktreeA, "--device-id", _blackBoxDeviceAID, "--token-stdin")
	runBlackBoxCLI(t, binary, tokenInput, "library", "bind", "--client-dir", clientB, "--server", serverURL,
		"--library-id", _blackBoxLibraryID, "--worktree", worktreeB, "--device-id", _blackBoxDeviceBID, "--token-stdin")

	sourceMtime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := os.Mkdir(filepath.Join(worktreeA, "docs"), 0o755); err != nil {
		t.Fatal(err)
	}
	for path, content := range map[string]string{
		"docs/deleted.txt":     "source-deleted",
		"docs/mtime-only.txt":  "stable-mtime-content",
		"docs/overwritten.txt": "source-overwritten",
		"root.txt":             "source-root",
	} {
		writeBlackBoxFile(t, worktreeA, path, content)
		setBlackBoxMtime(t, filepath.Join(worktreeA, filepath.FromSlash(path)), sourceMtime)
	}
	for index := range 10 {
		writeBlackBoxFile(t, worktreeA, filepath.ToSlash(filepath.Join("docs", "stable-"+fmt.Sprintf("%02d", index)+".txt")), "stable")
	}
	writeBlackBoxFile(t, worktreeA, "replace-file", "source replacement file")
	writeBlackBoxFile(t, worktreeA, "replace-dir/child.txt", "source replacement child")
	writeBlackBoxFile(t, worktreeA, "replace-dir/nested/deep.txt", "source replacement deep")
	for index := range 101 {
		writeBlackBoxFile(t, worktreeA, fmt.Sprintf("preview-%03d.txt", index), "source preview")
	}
	setBlackBoxMtime(t, filepath.Join(worktreeA, "docs"), sourceMtime.Add(time.Minute))
	runBlackBoxCLI(t, binary, nil, "library", "sync", "--client-dir", clientA, "--worktree", worktreeA)
	runBlackBoxCLI(t, binary, nil, "library", "sync", "--client-dir", clientB, "--worktree", worktreeB)
	sourceBinding := readTestBinding(t, clientA, worktreeA)

	writeBlackBoxFile(t, worktreeA, "docs/overwritten.txt", "current-overwritten")
	setBlackBoxMtime(t, filepath.Join(worktreeA, "docs", "overwritten.txt"), sourceMtime.Add(2*time.Hour))
	setBlackBoxMtime(t, filepath.Join(worktreeA, "docs", "mtime-only.txt"), sourceMtime.Add(2*time.Hour))
	if err := os.Remove(filepath.Join(worktreeA, "docs", "deleted.txt")); err != nil {
		t.Fatal(err)
	}
	writeBlackBoxFile(t, worktreeA, "docs/current.txt", "current-file")
	if err := os.Mkdir(filepath.Join(worktreeA, "docs", "current-dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeBlackBoxFile(t, worktreeA, "docs/current-dir/nested.txt", "current-nested")
	if err := os.Mkdir(filepath.Join(worktreeA, "docs", "current-empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeBlackBoxFile(t, worktreeA, "root.txt", "current-root")
	writeBlackBoxFile(t, worktreeA, "root-current-only.txt", "current-root-only")
	for index := range 101 {
		writeBlackBoxFile(t, worktreeA, fmt.Sprintf("preview-%03d.txt", index), "current preview")
	}
	if err := os.Remove(filepath.Join(worktreeA, "replace-file")); err != nil {
		t.Fatal(err)
	}
	writeBlackBoxFile(t, worktreeA, "replace-file/child.txt", "current replacement child")
	writeBlackBoxFile(t, worktreeA, "replace-file/nested/deep.txt", "current replacement deep")
	if err := os.RemoveAll(filepath.Join(worktreeA, "replace-dir")); err != nil {
		t.Fatal(err)
	}
	writeBlackBoxFile(t, worktreeA, "replace-dir", "current replacement file")
	runBlackBoxCLI(t, binary, nil, "library", "sync", "--client-dir", clientA, "--worktree", worktreeA)
	runBlackBoxCLI(t, binary, nil, "library", "sync", "--client-dir", clientB, "--worktree", worktreeB)

	directoryPreview := runBlackBoxCLI(t, binary, nil, "library", "restore", "--client-dir", clientB,
		"--worktree", worktreeB, "--commit", sourceBinding.SyncBase, "--path", "docs")
	directoryPending := readTestPendingPublication(t, clientB, worktreeB)
	if directoryPending.CreatedCount != 1 || directoryPending.UpdatedCount != 3 ||
		directoryPending.PreservedCurrentOnlyCount != 4 || directoryPending.TypeReplacementCount != 0 ||
		outputValue(string(directoryPreview), "candidate: ") != directoryPending.CandidateCommit[:deleteCandidatePrefixLen] {
		t.Fatalf("built directory preview=%s pending=%+v", directoryPreview, directoryPending)
	}
	for _, content := range []string{"source-deleted", "source-overwritten", "current-file", "current-nested"} {
		if bytes.Contains(directoryPreview, []byte(content)) {
			t.Fatalf("built directory preview=%q contains content=%q, want metadata only", directoryPreview, content)
		}
	}
	runBlackBoxCLI(t, binary, nil, "library", "restore", "--client-dir", clientB, "--worktree", worktreeB,
		"--confirm", directoryPending.CandidateCommit[:deleteCandidatePrefixLen])
	for path, want := range map[string]string{
		"docs/deleted.txt":            "source-deleted",
		"docs/mtime-only.txt":         "stable-mtime-content",
		"docs/overwritten.txt":        "source-overwritten",
		"docs/current.txt":            "current-file",
		"docs/current-dir/nested.txt": "current-nested",
	} {
		assertBlackBoxFile(t, worktreeB, path, want)
	}
	if info, err := os.Stat(filepath.Join(worktreeB, "docs", "current-empty")); err != nil || !info.IsDir() {
		t.Fatalf("built directory restore current empty: info=%v err=%v", info, err)
	}
	assertBlackBoxMtime(t, filepath.Join(worktreeB, "docs", "deleted.txt"), sourceMtime)
	assertBlackBoxMtime(t, filepath.Join(worktreeB, "docs", "mtime-only.txt"), sourceMtime)
	assertBlackBoxMtime(t, filepath.Join(worktreeB, "docs", "overwritten.txt"), sourceMtime)
	runBlackBoxCLI(t, binary, nil, "library", "sync", "--client-dir", clientA, "--worktree", worktreeA)

	setBlackBoxMtime(t, worktreeB, time.Date(2026, 7, 8, 9, 10, 11, 0, time.UTC))
	rootPreview := runBlackBoxCLI(t, binary, nil, "library", "restore", "--client-dir", clientB,
		"--worktree", worktreeB, "--commit", sourceBinding.SyncBase, "--path", ".")
	rootPending := readTestPendingPublication(t, clientB, worktreeB)
	if rootPending.UpdatedCount != 102 || rootPending.PreservedCurrentOnlyCount != 5 || rootPending.CreatedCount != 3 ||
		rootPending.TypeReplacementCount != 2 || rootPending.RemovedDescendantCount != 3 || rootPending.ChangedPathCount != 107 ||
		!rootPending.PreviewTruncated || outputValue(string(rootPreview), "candidate: ") != rootPending.CandidateCommit[:deleteCandidatePrefixLen] ||
		outputValue(string(rootPreview), "created paths: ") != "3" ||
		outputValue(string(rootPreview), "updated paths: ") != "102" ||
		outputValue(string(rootPreview), "type replacements: ") != "2" ||
		outputValue(string(rootPreview), "removed descendants by type replacement: ") != "3" ||
		outputValue(string(rootPreview), "preserved current-only paths: ") != "5" ||
		outputValue(string(rootPreview), "truncated: ") != "true" {
		t.Fatalf("built root preview=%s pending=%+v, want 107 changes with a 100-path preview", rootPreview, rootPending)
	}
	previewPaths, err := _decodeRestorePreview(rootPending.ChangedPathPreview)
	if err != nil || len(previewPaths) != _restorePreviewPathLimit {
		t.Fatalf("built root preview paths=%d err=%v, want %d paths", len(previewPaths), err, _restorePreviewPathLimit)
	}
	for _, content := range []string{"source-root", "current-root-only"} {
		if bytes.Contains(rootPreview, []byte(content)) {
			t.Fatalf("built root preview=%q contains content=%q, want metadata only", rootPreview, content)
		}
	}

	staleResultRoot := rootPending.CandidateRoot
	db, err := openClientDB(filepath.Join(clientB, _clientDatabaseName), false)
	if err != nil {
		t.Fatalf("open built restore candidate for tampering: %v", err)
	}
	if _, err := db.ExecContext(t.Context(), "UPDATE pending_publications SET candidate_data = ? WHERE worktree = ?",
		[]byte("{}"), worktreeB); err != nil {
		t.Fatalf("tamper built restore candidate: %v", errors.Join(err, db.Close()))
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close built restore candidate after tampering: %v", err)
	}
	beforeStaleConfirmation := captureBlackBoxRestoreState(t, serverURL, login.Session.AccessToken,
		clientA, worktreeA, clientB, worktreeB)
	staleStdout, staleStderr, staleErr := runBlackBoxCLIResult(t, binary, nil, "library", "restore",
		"--client-dir", clientB, "--worktree", worktreeB,
		"--confirm", rootPending.CandidateCommit[:deleteCandidatePrefixLen])
	if staleErr == nil || len(staleStdout) != 0 || !bytes.Contains(staleStderr, []byte("stale restore candidate discarded")) {
		t.Fatalf("built stale restore confirmation error=%v stdout=%q stderr=%q, want discarded pending", staleErr, staleStdout, staleStderr)
	}
	wantAfterStale := beforeStaleConfirmation
	wantAfterStale.pendingPublicationB = 0
	assertBlackBoxRestoreState(t, "stale restore confirmation", wantAfterStale,
		captureBlackBoxRestoreState(t, serverURL, login.Session.AccessToken, clientA, worktreeA, clientB, worktreeB))

	rereview := runBlackBoxCLI(t, binary, nil, "library", "restore", "--client-dir", clientB,
		"--worktree", worktreeB, "--commit", sourceBinding.SyncBase, "--path", ".")
	rootPending = readTestPendingPublication(t, clientB, worktreeB)
	if rootPending.CandidateRoot != staleResultRoot ||
		outputValue(string(rereview), "candidate: ") != rootPending.CandidateCommit[:deleteCandidatePrefixLen] {
		t.Fatalf("built restore repreviews root=%s output=%q, want root=%s and current candidate prefix",
			rootPending.CandidateRoot, rereview, staleResultRoot)
	}

	beforeWrongConfirmation := captureBlackBoxRestoreState(t, serverURL, login.Session.AccessToken,
		clientA, worktreeA, clientB, worktreeB)
	wrongPrefix := strings.Repeat("0", deleteCandidatePrefixLen)
	if wrongPrefix == rootPending.CandidateCommit[:deleteCandidatePrefixLen] {
		wrongPrefix = strings.Repeat("1", deleteCandidatePrefixLen)
	}
	wrongStdout, wrongStderr, wrongErr := runBlackBoxCLIResult(t, binary, nil, "library", "restore",
		"--client-dir", clientB, "--worktree", worktreeB, "--confirm", wrongPrefix)
	if wrongErr == nil || len(wrongStdout) != 0 || !bytes.Contains(wrongStderr, []byte("must exactly match")) {
		t.Fatalf("built wrong restore confirmation error=%v stdout=%q stderr=%q", wrongErr, wrongStdout, wrongStderr)
	}
	assertBlackBoxRestoreState(t, "wrong type replacement confirmation", beforeWrongConfirmation,
		captureBlackBoxRestoreState(t, serverURL, login.Session.AccessToken, clientA, worktreeA, clientB, worktreeB))

	runBlackBoxCLI(t, binary, nil, "library", "restore", "--client-dir", clientB, "--worktree", worktreeB,
		"--confirm", rootPending.CandidateCommit[:deleteCandidatePrefixLen])
	assertBlackBoxFile(t, worktreeB, "root.txt", "source-root")
	assertBlackBoxFile(t, worktreeB, "root-current-only.txt", "current-root-only")
	assertBlackBoxFile(t, worktreeB, "replace-file", "source replacement file")
	assertBlackBoxFile(t, worktreeB, "replace-dir/child.txt", "source replacement child")
	assertBlackBoxFile(t, worktreeB, "replace-dir/nested/deep.txt", "source replacement deep")
	assertBlackBoxFile(t, worktreeB, "preview-000.txt", "source preview")
	assertBlackBoxFile(t, worktreeB, "preview-100.txt", "source preview")
	runBlackBoxCLI(t, binary, nil, "library", "sync", "--client-dir", clientA, "--worktree", worktreeA)

	oracle := libraryCLIEnvironment{server: &httptest.Server{URL: serverURL}, token: login.Session.AccessToken}
	confirmed := platformConfirmedInputs("source-deleted", "source-overwritten", "current-overwritten", "current-file",
		"current-nested", "source-root", "current-root", "current-root-only", "source replacement file",
		"source replacement child", "source replacement deep", "current replacement file", "current replacement child",
		"current replacement deep", "source preview", "current preview")
	bindingA := assertPlatformConvergedWithoutAttestation(t, "real binary restore first client", oracle, clientA, worktreeA, confirmed)
	bindingB := assertPlatformConvergedWithoutAttestation(t, "real binary restore second client", oracle, clientB, worktreeB, confirmed)
	if bindingA.SyncBase != bindingB.SyncBase || bindingA.SyncBaseRoot != bindingB.SyncBaseRoot ||
		bindingB.SyncBase != rootPending.CandidateCommit || bindingB.SyncBaseRoot != rootPending.CandidateRoot {
		t.Fatalf("built restore clients differ: A=%+v B=%+v pending=%+v", bindingA, bindingB, rootPending)
	}

	beforeMissing := captureBlackBoxRestoreState(t, serverURL, login.Session.AccessToken,
		clientA, worktreeA, clientB, worktreeB)
	missingStdout, missingStderr, missingErr := runBlackBoxCLIResult(t, binary, nil, "library", "restore",
		"--client-dir", clientB, "--worktree", worktreeB, "--commit", sourceBinding.SyncBase,
		"--path", "missing-directory")
	if missingErr == nil || !bytes.Contains(missingStderr, []byte("restore source path not found")) || len(missingStdout) != 0 {
		t.Fatalf("built missing directory restore error=%v stdout=%q stderr=%q", missingErr, missingStdout, missingStderr)
	}
	for _, content := range []string{"source-deleted", "source-overwritten", "current-file", "current-nested"} {
		if bytes.Contains(missingStderr, []byte(content)) {
			t.Fatal("built missing directory restore leaked file content")
		}
	}
	assertBlackBoxRestoreState(t, "missing source directory", beforeMissing,
		captureBlackBoxRestoreState(t, serverURL, login.Session.AccessToken, clientA, worktreeA, clientB, worktreeB))

	beforeNoOp := captureBlackBoxRestoreState(t, serverURL, login.Session.AccessToken,
		clientA, worktreeA, clientB, worktreeB)
	noOpOutput := runBlackBoxCLI(t, binary, nil, "library", "restore", "--client-dir", clientB,
		"--worktree", worktreeB, "--commit", sourceBinding.SyncBase, "--path", ".")
	if !bytes.Contains(noOpOutput, []byte("restore no-op")) || bytes.Contains(noOpOutput, []byte("confirm:")) ||
		bytes.Contains(noOpOutput, []byte("candidate:")) {
		t.Fatalf("built root no-op output=%q", noOpOutput)
	}
	for _, content := range []string{"source-root", "current-root-only"} {
		if bytes.Contains(noOpOutput, []byte(content)) {
			t.Fatal("built root no-op leaked file content")
		}
	}
	assertBlackBoxRestoreState(t, "root no-op", beforeNoOp,
		captureBlackBoxRestoreState(t, serverURL, login.Session.AccessToken, clientA, worktreeA, clientB, worktreeB))

	sourceHistory := runBlackBoxCLI(t, binary, nil, "library", "history", "inspect", "--client-dir", clientB,
		"--worktree", worktreeB, "--commit", sourceBinding.SyncBase)
	previousHistory := runBlackBoxCLI(t, binary, nil, "library", "history", "inspect", "--client-dir", clientB,
		"--worktree", worktreeB, "--commit", rootPending.ExpectedHead)
	sourceReachable := bytes.Contains(sourceHistory, []byte("CommitId="+sourceBinding.SyncBase))
	previousReachable := bytes.Contains(previousHistory, []byte("CommitId="+rootPending.ExpectedHead))
	owner, err := getLibraryOwner(t.Context(), mustServerURL(t, serverURL), _blackBoxLibraryID, []byte(login.Session.AccessToken))
	if err != nil {
		t.Fatalf("read restore acceptance owner: %v", err)
	}
	reachability := inspectPlatformReachability(t, oracle, bindingB.SyncBase, owner)
	platform, filesystem, enabled := acceptance.ActivePlatform()
	if !enabled {
		t.Fatal("built restore acceptance requires an active platform")
	}
	emitPlatformAttestation(t, platformAttestation{
		Kind: "restore", Scenario: "real binary directory and root restore", Platform: platform, Filesystem: filesystem,
		Head: bindingB.SyncBase, SyncBase: bindingB.SyncBase, HeadRoot: bindingB.SyncBaseRoot, BaseRoot: bindingB.SyncBaseRoot,
		Snapshot: scanTestRoot(t, worktreeB), ReachableObjects: reachability.objects, SourceCommit: sourceBinding.SyncBase,
		SourceRoot: sourceBinding.SyncBaseRoot, SourcePath: ".", PreviousHead: rootPending.ExpectedHead,
		ExpectedHead: rootPending.ExpectedHead, CandidateCommit: rootPending.CandidateCommit, ResultRoot: rootPending.CandidateRoot,
		CreatedCount: new(rootPending.CreatedCount), UpdatedCount: new(rootPending.UpdatedCount),
		TypeReplacementCount: new(rootPending.TypeReplacementCount), RemovedDescendantCount: new(rootPending.RemovedDescendantCount),
		PreservedCurrentOnlyCount: new(rootPending.PreservedCurrentOnlyCount),
		PendingPublicationRows:    new(countClientRows(t, clientB, "pending_publications", worktreeB)),
		PendingCheckoutRows:       new(countClientRows(t, clientB, "pending_checkouts", worktreeB)),
		ResidualJournalRows:       countClientRows(t, clientB, "fs_actions", worktreeB), SourceReachable: sourceReachable,
		PreviousHeadReachable: previousReachable,
	})
	server.stop(t)
}

func TestBuiltBinaryHistoryInspectReadOnlyAcceptance(t *testing.T) {
	if os.Getenv("FILECLOUD_PLATFORM_MATRIX_CHILD") != "1" {
		t.Skip("runs inside the native 1B platform matrix")
	}
	root := os.Getenv("FILECLOUD_ACCEPTANCE_ROOT")
	if root == "" {
		t.Fatal("FILECLOUD_ACCEPTANCE_ROOT is required")
	}
	testRoot, err := os.MkdirTemp(root, ".filecloud-history-inspect-")
	if err != nil {
		t.Fatalf("create history inspect acceptance root: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(testRoot); err != nil {
			t.Errorf("remove history inspect acceptance root: %v", err)
		}
	})

	binaryName := "filecloud"
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	binary := filepath.Join(testRoot, binaryName)
	build := exec.CommandContext(t.Context(), "go", "build", "-trimpath", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build history inspect CLI: %v\n%s", err, output)
	}
	dataDir := filepath.Join(testRoot, "data")
	password := []byte("history-inspect-password-1234\n")
	defer clear(password)
	runBlackBoxCLI(t, binary, nil, "init", "--data-dir", dataDir)
	runBlackBoxCLI(t, binary, password, "user", "add", "--data-dir", dataDir, "--username", "history-inspect", "--password-stdin")
	server := startBlackBoxServer(t, binary, dataDir, "127.0.0.1:0")
	serverURL := "http://" + server.address
	loginOutput := runBlackBoxCLI(t, binary, password, "login", "--server", serverURL, "--username", "history-inspect",
		"--device-name", "history-inspect", "--password-stdin")
	var login struct {
		Session struct{ AccessToken string }
	}
	if err := json.Unmarshal(loginOutput, &login); err != nil || login.Session.AccessToken == "" {
		t.Fatalf("decode history inspect login: token_present=%t error=%v", login.Session.AccessToken != "", err)
	}
	tokenInput := []byte(login.Session.AccessToken + "\n")
	defer clear(tokenInput)

	backend, err := url.Parse(serverURL)
	if err != nil {
		t.Fatalf("parse history inspect server URL: %v", err)
	}
	reverseProxy := httputil.NewSingleHostReverseProxy(backend)
	var armed atomic.Bool
	var forbidden atomic.Int32
	spy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if armed.Load() && (r.Method != http.MethodGet || strings.Contains(r.URL.Path, "/blocks/") || strings.HasSuffix(r.URL.Path, "/head")) {
			forbidden.Add(1)
			w.WriteHeader(http.StatusTeapot)
			return
		}
		reverseProxy.ServeHTTP(w, r)
	}))
	defer spy.Close()

	runBlackBoxCLI(t, binary, tokenInput, "library", "create", "--server", spy.URL,
		"--library-id", _blackBoxLibraryID, "--name", "History Inspect", "--token-stdin")
	clientDir := filepath.Join(testRoot, "client")
	worktree := filepath.Join(testRoot, "worktree")
	if err := os.Mkdir(worktree, 0o755); err != nil {
		t.Fatalf("create history inspect worktree: %v", err)
	}
	runBlackBoxCLI(t, binary, tokenInput, "library", "bind", "--client-dir", clientDir, "--server", spy.URL,
		"--library-id", _blackBoxLibraryID, "--worktree", worktree, "--device-id", _blackBoxDeviceAID, "--token-stdin")
	writeBlackBoxFile(t, worktree, "a.txt", "history-a\n")
	writeBlackBoxFile(t, worktree, "b.txt", "history-b\n")
	runBlackBoxCLI(t, binary, nil, "library", "sync", "--client-dir", clientDir, "--worktree", worktree)
	binding := readTestBinding(t, clientDir, worktree)
	baseURL := mustServerURL(t, spy.URL)
	token := []byte(login.Session.AccessToken)
	capturedData, capturedCommit, err := canonicalCommit(binding.UserID, binding.DeviceID, binding.SyncBaseRoot,
		[]string{binding.SyncBase}, func() time.Time { return time.Date(2026, 8, 9, 1, 2, 1, 0, time.UTC) })
	if err != nil {
		t.Fatalf("construct built history merge source: %v", err)
	}
	mergedData, mergedCommit, err := canonicalCommit(binding.UserID, binding.DeviceID, binding.SyncBaseRoot,
		[]string{binding.SyncBase, capturedCommit}, func() time.Time { return time.Date(2026, 8, 9, 1, 2, 2, 0, time.UTC) })
	if err != nil {
		t.Fatalf("construct built history merge: %v", err)
	}
	for id, data := range map[string][]byte{capturedCommit: capturedData, mergedCommit: mergedData} {
		if err := putMetadata(t.Context(), baseURL, _blackBoxLibraryID, token, "commits", id, data); err != nil {
			t.Fatalf("upload built history commit: %v", err)
		}
	}
	if _, conflict, err := updateRemoteHead(t.Context(), baseURL, _blackBoxLibraryID, token, binding.HeadETag, mergedCommit); err != nil || conflict {
		t.Fatalf("publish built history merge: conflict=%t err=%v", conflict, err)
	}
	stateDB, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatalf("open history inspect state observer: %v", err)
	}
	t.Cleanup(func() {
		if err := stateDB.Close(); err != nil {
			t.Errorf("close history inspect state observer: %v", err)
		}
	})
	beforeClient := captureHistoryInspectClientState(t, clientDir)
	beforeWorktree := captureHistoryInspectWorktree(t, worktree)
	armed.Store(true)

	listOutput := runBlackBoxCLI(t, binary, nil, "library", "history", "list", "--client-dir", clientDir,
		"--worktree", worktree, "--include-merged")
	if !bytes.Contains(listOutput, []byte(mergedCommit+" role=head ")) ||
		!bytes.Contains(listOutput, []byte("  "+capturedCommit+" role=merge-source mainline="+mergedCommit+" ")) {
		t.Fatalf("built merged history output missing attributed lineage: %s", listOutput)
	}
	commitOutput := runBlackBoxCLI(t, binary, nil, "library", "history", "inspect", "--client-dir", clientDir,
		"--worktree", worktree, "--commit", binding.SyncBase)
	if !bytes.Contains(commitOutput, []byte("CommitId="+binding.SyncBase)) || !bytes.Contains(commitOutput, []byte("Root="+binding.SyncBaseRoot)) {
		t.Fatalf("history commit output missing identity: %s", commitOutput)
	}
	mergedSourceOutput := runBlackBoxCLI(t, binary, nil, "library", "history", "inspect", "--client-dir", clientDir,
		"--worktree", worktree, "--commit", capturedCommit)
	if !bytes.Contains(mergedSourceOutput, []byte("Role=merge-source")) ||
		!bytes.Contains(mergedSourceOutput, []byte("MainlineCommitId="+mergedCommit)) {
		t.Fatalf("history merge-source output missing role attribution: %s", mergedSourceOutput)
	}
	fileOutput := runBlackBoxCLI(t, binary, nil, "library", "history", "inspect", "--client-dir", clientDir,
		"--worktree", worktree, "--commit", binding.SyncBase, "--path", "a.txt")
	if !bytes.Contains(fileOutput, []byte("Type=File")) || !bytes.Contains(fileOutput, []byte("Blocks=1")) {
		t.Fatalf("history file output missing metadata: %s", fileOutput)
	}
	rootOutput := runBlackBoxCLI(t, binary, nil, "library", "history", "inspect", "--client-dir", clientDir,
		"--worktree", worktree, "--commit", binding.SyncBase, "--path", ".", "--page-size", "1")
	pageToken := outputValue(string(rootOutput), "next_page_token=")
	if entryPresent := bytes.Contains(rootOutput, []byte("Entry name=a.txt type=File")); !entryPresent || pageToken == "" {
		t.Fatalf("history directory first page: entry_present=%t token_present=%t, want both true", entryPresent, pageToken != "")
	}
	secondOutput := runBlackBoxCLI(t, binary, nil, "library", "history", "inspect", "--client-dir", clientDir,
		"--worktree", worktree, "--commit", binding.SyncBase, "--path", ".", "--page-size", "1", "--page-token", pageToken)
	if !bytes.Contains(secondOutput, []byte("Entry name=b.txt type=File")) {
		t.Fatal("history directory second page omitted b.txt")
	}
	if forbidden.Load() != 0 {
		t.Fatalf("history inspect attempted %d block, write, or Head requests, want 0", forbidden.Load())
	}
	if afterClient := captureHistoryInspectClientState(t, clientDir); !slices.Equal(beforeClient, afterClient) {
		t.Fatalf("history inspect changed client state: %s", historyInspectSnapshotDifference(beforeClient, afterClient))
	}
	if afterWorktree := captureHistoryInspectWorktree(t, worktree); !slices.Equal(beforeWorktree, afterWorktree) {
		t.Fatalf("history inspect changed worktree: %s", historyInspectSnapshotDifference(beforeWorktree, afterWorktree))
	}
	server.stop(t)
}

type blackBoxServer struct {
	command *exec.Cmd
	stderr  bytes.Buffer
	address string
	stopped bool
}

func startBlackBoxServer(t *testing.T, binary, dataDir, address string) *blackBoxServer {
	t.Helper()
	server := &blackBoxServer{}
	server.command = exec.Command(binary, "serve", "--data-dir", dataDir, "--listen", address)
	stdout, err := server.command.StdoutPipe()
	if err != nil {
		t.Fatalf("open real server stdout: %v", err)
	}
	server.command.Stderr = &server.stderr
	if err := server.command.Start(); err != nil {
		t.Fatalf("start real server: %v", err)
	}
	t.Cleanup(func() { server.stop(t) })
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		server.stop(t)
		t.Fatalf("read real server address: %v: %s", scanner.Err(), server.stderr.String())
	}
	line := strings.TrimSpace(scanner.Text())
	announced, ok := strings.CutPrefix(line, "listening on ")
	host, port, splitErr := net.SplitHostPort(announced)
	if !ok || splitErr != nil || host != "127.0.0.1" || port == "0" || (address != "127.0.0.1:0" && announced != address) {
		server.stop(t)
		t.Fatalf("real server listen output = %q for requested address %q", line, address)
	}
	server.address = announced
	if err := stdout.Close(); err != nil {
		server.stop(t)
		t.Fatalf("close real server stdout: %v", err)
	}
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get("http://" + server.address + "/readyz")
		if err == nil {
			closeErr := response.Body.Close()
			if response.StatusCode == http.StatusOK && closeErr == nil {
				return server
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	server.stop(t)
	t.Fatalf("real server did not become healthy: %s", server.stderr.String())
	return nil
}

func (s *blackBoxServer) stop(t *testing.T) {
	t.Helper()
	if s == nil || s.stopped {
		return
	}
	s.stopped = true
	killErr := s.command.Process.Kill()
	if errors.Is(killErr, os.ErrProcessDone) {
		killErr = nil
	}
	waitErr := s.command.Wait()
	if _, ok := errors.AsType[*exec.ExitError](waitErr); ok {
		waitErr = nil
	}
	if err := errors.Join(killErr, waitErr); err != nil {
		t.Errorf("stop real server: %v", err)
	}
}

func runBlackBoxCLI(t *testing.T, binary string, stdin []byte, args ...string) []byte {
	t.Helper()
	stdout, stderr, err := runBlackBoxCLIResult(t, binary, stdin, args...)
	if err != nil {
		t.Fatalf("run real CLI %s: %v: %s", strings.Join(args, " "), err, stderr)
	}
	return stdout
}

func runBlackBoxCLIResult(t *testing.T, binary string, stdin []byte, args ...string) ([]byte, []byte, error) {
	t.Helper()
	command := exec.CommandContext(t.Context(), binary, args...)
	command.Stdin = bytes.NewReader(stdin)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

type blackBoxRestoreState struct {
	head                string
	bindingA            clientBinding
	bindingB            clientBinding
	worktreeA           []string
	worktreeB           []string
	pendingPublicationA int
	pendingPublicationB int
	pendingCheckoutA    int
	pendingCheckoutB    int
}

func captureBlackBoxRestoreState(t *testing.T, serverURL, token, clientA, worktreeA, clientB, worktreeB string) blackBoxRestoreState {
	t.Helper()
	head, err := getRemoteHead(t.Context(), mustServerURL(t, serverURL), _blackBoxLibraryID, []byte(token))
	if err != nil || head.CommitID == nil {
		t.Fatalf("capture black-box restore Head: head=%+v err=%v", head, err)
	}
	return blackBoxRestoreState{
		head:                *head.CommitID + "\x00" + head.ETag,
		bindingA:            readTestBinding(t, clientA, worktreeA),
		bindingB:            readTestBinding(t, clientB, worktreeB),
		worktreeA:           captureHistoryInspectWorktree(t, worktreeA),
		worktreeB:           captureHistoryInspectWorktree(t, worktreeB),
		pendingPublicationA: countClientRows(t, clientA, "pending_publications", worktreeA),
		pendingPublicationB: countClientRows(t, clientB, "pending_publications", worktreeB),
		pendingCheckoutA:    countClientRows(t, clientA, "pending_checkouts", worktreeA),
		pendingCheckoutB:    countClientRows(t, clientB, "pending_checkouts", worktreeB),
	}
}

func assertBlackBoxRestoreState(t *testing.T, scenario string, before, after blackBoxRestoreState) {
	t.Helper()
	if before.head != after.head || before.bindingA != after.bindingA || before.bindingB != after.bindingB ||
		!slices.Equal(before.worktreeA, after.worktreeA) || !slices.Equal(before.worktreeB, after.worktreeB) ||
		before.pendingPublicationA != after.pendingPublicationA || before.pendingPublicationB != after.pendingPublicationB ||
		before.pendingCheckoutA != after.pendingCheckoutA || before.pendingCheckoutB != after.pendingCheckoutB {
		t.Fatalf("built %s changed state:\nbefore=%+v\n after=%+v", scenario, before, after)
	}
}

func writeBlackBoxFile(t *testing.T, root, name, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create parent for %s: %v", name, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func setBlackBoxMtime(t *testing.T, path string, value time.Time) {
	t.Helper()
	if err := os.Chtimes(path, value, value); err != nil {
		t.Fatalf("set %s mtime: %v", path, err)
	}
}

func assertBlackBoxMtime(t *testing.T, path string, want time.Time) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if !info.ModTime().Equal(want) {
		t.Fatalf("%s mtime=%v, want %v", path, info.ModTime(), want)
	}
}

func assertBlackBoxFile(t *testing.T, root, name, want string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
	if err != nil || string(data) != want {
		t.Fatalf("%s=%q err=%v, want %q", name, data, err, want)
	}
}

func readBlackBoxSnapshot(t *testing.T, root string) []string {
	t.Helper()
	var snapshot []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		snapshot = append(snapshot, filepath.ToSlash(relative)+"="+string(data))
		return nil
	})
	if err != nil {
		t.Fatalf("read black-box snapshot: %v", err)
	}
	return snapshot
}

func assertBlackBoxConflictLayout(t *testing.T, snapshot []string) {
	t.Helper()
	if len(snapshot) != 2 {
		t.Fatalf("conflict snapshot has %d files, want 2: %v", len(snapshot), snapshot)
	}
	original := false
	conflict := false
	for _, entry := range snapshot {
		switch {
		case entry == "shared.txt=from-a\n":
			original = true
		case strings.HasPrefix(entry, "shared (Filecloud conflict ") && strings.HasSuffix(entry, ").txt=from-b\n"):
			conflict = true
		}
	}
	if !original || !conflict {
		t.Fatalf("conflict layout does not preserve remote original and deterministic local copy: %v", snapshot)
	}
}
