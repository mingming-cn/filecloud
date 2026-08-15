// Command release-smoke verifies a packaged Filecloud binary through its public interfaces.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	_libraryID = "01234567-89ab-4def-8123-456789abcdef"
	_deviceID  = "12345678-9abc-4def-8123-456789abcdef"
)

type versionInfo struct {
	Version   string
	Commit    string
	BuildDate string
}

type loginEnvelope struct {
	Session struct {
		AccessToken string
	}
}

type headEnvelope struct {
	Head struct {
		CommitID *string `json:"CommitId"`
	}
}

func main() {
	binary := flag.String("binary", "", "path to the Filecloud binary")
	version := flag.String("version", "", "expected release version")
	flag.Parse()
	if *binary == "" || *version == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: go run ./tools/release-smoke --binary path --version version")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := smoke(ctx, *binary, *version); err != nil {
		fmt.Fprintln(os.Stderr, "release smoke:", err)
		os.Exit(1)
	}
	fmt.Println("release smoke passed")
}

func smoke(ctx context.Context, binary, expectedVersion string) error {
	absoluteBinary, err := filepath.Abs(binary)
	if err != nil {
		return fmt.Errorf("resolve binary: %w", err)
	}
	versionOutput, err := runBinary(ctx, absoluteBinary, nil, "version")
	if err != nil {
		return err
	}
	var version versionInfo
	if err := json.Unmarshal(versionOutput, &version); err != nil {
		return fmt.Errorf("decode version output: %w", err)
	}
	if version.Version != expectedVersion || version.Commit == "" || version.Commit == "unknown" || version.BuildDate == "" || version.BuildDate == "unknown" {
		return fmt.Errorf("unexpected version metadata: %+v", version)
	}

	temporaryRoot, err := os.MkdirTemp(".", ".filecloud-release-smoke-")
	if err != nil {
		return fmt.Errorf("create smoke root: %w", err)
	}
	defer os.RemoveAll(temporaryRoot)
	root, err := filepath.Abs(temporaryRoot)
	if err != nil {
		return fmt.Errorf("resolve smoke root: %w", err)
	}
	dataDir := filepath.Join(root, "data")
	clientDir := filepath.Join(root, "client")
	worktree := filepath.Join(root, "worktree")
	if err := os.Mkdir(worktree, 0o755); err != nil {
		return fmt.Errorf("create worktree: %w", err)
	}
	if _, err := runBinary(ctx, absoluteBinary, nil, "init", "--data-dir", dataDir); err != nil {
		return err
	}
	password, err := randomPassword()
	if err != nil {
		return err
	}
	defer clear(password)
	passwordInput := append(bytes.Clone(password), '\n')
	defer clear(passwordInput)
	if _, err := runBinary(ctx, absoluteBinary, passwordInput, "user", "add", "--data-dir", dataDir, "--username", "release-smoke", "--password-stdin"); err != nil {
		return err
	}

	server, err := startServer(ctx, absoluteBinary, dataDir)
	if err != nil {
		return err
	}
	defer server.stop()

	loginOutput, err := runBinary(ctx, absoluteBinary, passwordInput, "login", "--server", server.url, "--username", "release-smoke", "--device-name", "release-smoke", "--password-stdin")
	if err != nil {
		return err
	}
	var login loginEnvelope
	if err := json.Unmarshal(loginOutput, &login); err != nil || login.Session.AccessToken == "" {
		return fmt.Errorf("decode login response: token_present=%t: %w", login.Session.AccessToken != "", err)
	}
	if err := createLibrary(ctx, server.url, login.Session.AccessToken); err != nil {
		return err
	}
	if _, err := runBinary(ctx, absoluteBinary, []byte(login.Session.AccessToken+"\n"), "library", "bind", "--client-dir", clientDir,
		"--server", server.url, "--library-id", _libraryID, "--worktree", worktree, "--device-id", _deviceID, "--token-stdin"); err != nil {
		return err
	}
	initialHead, err := getHead(ctx, server.url, login.Session.AccessToken)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(worktree, "smoke.txt"), []byte("filecloud release smoke\n"), 0o644); err != nil {
		return fmt.Errorf("write smoke file: %w", err)
	}
	if _, err := runBinary(ctx, absoluteBinary, nil, "library", "sync", "--client-dir", clientDir, "--worktree", worktree); err != nil {
		return err
	}
	updatedHead, err := getHead(ctx, server.url, login.Session.AccessToken)
	if err != nil {
		return err
	}
	if initialHead == "" || updatedHead == "" || initialHead == updatedHead {
		return fmt.Errorf("sync did not advance Head: initial=%q updated=%q", initialHead, updatedHead)
	}
	return nil
}

func randomPassword() ([]byte, error) {
	random := make([]byte, 24)
	if _, err := rand.Read(random); err != nil {
		return nil, fmt.Errorf("generate smoke password: %w", err)
	}
	password := make([]byte, base64.RawURLEncoding.EncodedLen(len(random)))
	base64.RawURLEncoding.Encode(password, random)
	clear(random)
	return password, nil
}

type runningServer struct {
	cancel context.CancelFunc
	cmd    *exec.Cmd
	url    string
	stderr *bytes.Buffer
}

func startServer(parent context.Context, binary, dataDir string) (*runningServer, error) {
	ctx, cancel := context.WithCancel(parent)
	command := exec.CommandContext(ctx, binary, "serve", "--data-dir", dataDir, "--listen", "127.0.0.1:0")
	stdout, err := command.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open serve output: %w", err)
	}
	stderr := new(bytes.Buffer)
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start serve: %w", err)
	}
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		readErr := fmt.Errorf("read serve address: %w: %s", scanner.Err(), stderr.String())
		return nil, errors.Join(readErr, cancelAndWait(cancel, command))
	}
	address, ok := strings.CutPrefix(strings.TrimSpace(scanner.Text()), "listening on ")
	if !ok || !strings.HasPrefix(address, "127.0.0.1:") {
		addressErr := fmt.Errorf("unexpected serve address %q", scanner.Text())
		return nil, errors.Join(addressErr, cancelAndWait(cancel, command))
	}
	return &runningServer{cancel: cancel, cmd: command, url: "http://" + address, stderr: stderr}, nil
}

func (s *runningServer) stop() {
	if err := cancelAndWait(s.cancel, s.cmd); err != nil {
		fmt.Fprintln(os.Stderr, "release smoke: stop serve:", err, s.stderr.String())
	}
}

func cancelAndWait(cancel context.CancelFunc, command *exec.Cmd) error {
	cancel()
	err := command.Wait()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("wait for stopped process: %w", err)
	}
	return nil
}

func runBinary(ctx context.Context, binary string, stdin []byte, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, binary, args...)
	command.Stdin = bytes.NewReader(stdin)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("run filecloud %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

func createLibrary(ctx context.Context, server, token string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, server+"/v1/libraries/"+_libraryID, strings.NewReader(`{"Name":"Release Smoke"}`))
	if err != nil {
		return fmt.Errorf("create library request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("create library: %w", err)
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if readErr != nil {
		return fmt.Errorf("read create library response: %w", readErr)
	}
	if response.StatusCode != http.StatusCreated {
		return fmt.Errorf("create library returned %s: %s", response.Status, body)
	}
	return nil
}

func getHead(ctx context.Context, server, token string) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server+"/v1/libraries/"+_libraryID+"/head", nil)
	if err != nil {
		return "", fmt.Errorf("create Head request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return "", fmt.Errorf("get Head: %w", err)
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if readErr != nil {
		return "", fmt.Errorf("read Head response: %w", readErr)
	}
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("get Head returned %s: %s", response.Status, body)
	}
	var envelope headEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return "", fmt.Errorf("decode Head response: %w", err)
	}
	if envelope.Head.CommitID == nil {
		return "", errors.New("Head response has no CommitId")
	}
	return *envelope.Head.CommitID, nil
}
