package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
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

	commitOutput := runBlackBoxCLI(t, binary, nil, "library", "history", "inspect", "--client-dir", clientDir,
		"--worktree", worktree, "--commit", binding.SyncBase)
	if !bytes.Contains(commitOutput, []byte("CommitId="+binding.SyncBase)) || !bytes.Contains(commitOutput, []byte("Root="+binding.SyncBaseRoot)) {
		t.Fatalf("history commit output missing identity: %s", commitOutput)
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
	command := exec.CommandContext(t.Context(), binary, args...)
	command.Stdin = bytes.NewReader(stdin)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("run real CLI %s: %v: %s", strings.Join(args, " "), err, stderr.String())
	}
	return stdout.Bytes()
}

func writeBlackBoxFile(t *testing.T, root, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
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
