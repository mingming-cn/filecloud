package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	libraryapi "github.com/mingming-cn/filecloud/internal/library"
	"github.com/mingming-cn/filecloud/internal/storage"
)

func TestLibraryWatchRejectsConcurrentWatchAndSyncForBinding(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	clientDir, worktree := newClientPaths(t)
	if err := runTest(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	release := make(chan struct{})
	watchDone := make(chan error, 1)
	go func() {
		watchDone <- runLibraryWithConfig(ctx, []string{"watch", "--client-dir", clientDir, "--worktree", worktree, "--interval", "1h"},
			strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{
				checkFilesystem: func(*os.File) error { return nil },
				scanFault: func(fault scanFault) error {
					if fault.phase == "before-final-validation" {
						close(started)
						<-release
					}
					return nil
				},
			})
	}()
	<-started

	for _, command := range []string{"sync", "watch"} {
		args := []string{command, "--client-dir", clientDir, "--worktree", worktree}
		if command == "watch" {
			args = append(args, "--interval", "1h")
		}
		attemptCtx, stopAttempt := context.WithTimeout(t.Context(), time.Second)
		err := runLibraryWithConfig(attemptCtx, args, strings.NewReader(""), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }})
		stopAttempt()
		if err == nil || !strings.Contains(err.Error(), "synchronization is already running") {
			t.Fatalf("concurrent %s error = %v", command, err)
		}
	}

	cancel()
	close(release)
	if err := <-watchDone; err != nil {
		t.Fatalf("stop watch: %v", err)
	}
}

func TestLibraryWatchRunsFullSyncImmediatelyAndAtInterval(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	clientDir, worktree := newClientPaths(t)
	if err := runTest(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var scans atomic.Int32
	err := runLibraryWithConfig(ctx, []string{"watch", "--client-dir", clientDir, "--worktree", worktree, "--interval", "10ms"},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{
			checkFilesystem: func(*os.File) error { return nil },
			scanFault: func(fault scanFault) error {
				if fault.phase == "before-final-validation" && scans.Add(1) == 2 {
					cancel()
				}
				return nil
			},
		})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	if got := scans.Load(); got != 2 {
		t.Fatalf("watch scan count = %d, want 2", got)
	}
}

func TestLibraryWatchPostponesRoundWhenIntervalIsExceeded(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	clientDir, worktree := newClientPaths(t)
	if err := runTest(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var scans atomic.Int32
	var stderr bytes.Buffer
	err := runLibraryWithConfig(ctx, []string{"watch", "--client-dir", clientDir, "--worktree", worktree, "--interval", "5ms"},
		strings.NewReader(""), io.Discard, &stderr, libraryClientConfig{
			checkFilesystem: func(*os.File) error { return nil },
			scanFault: func(fault scanFault) error {
				if fault.phase != "before-final-validation" {
					return nil
				}
				if scans.Add(1) == 1 {
					time.Sleep(20 * time.Millisecond)
				} else {
					cancel()
				}
				return nil
			},
		})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	if got := scans.Load(); got != 2 {
		t.Fatalf("watch scan count = %d, want 2", got)
	}
	if output := stderr.String(); !strings.Contains(output, "exceeded watch interval") || !strings.Contains(output, "next round postponed") {
		t.Fatalf("watch delay warning = %q", output)
	}
}

func TestLibraryWatchPreservesSyncScanAndDeletionErrors(t *testing.T) {
	t.Run("scan race", func(t *testing.T) {
		environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
		clientDir, worktree := newClientPaths(t)
		if err := runTest(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID),
			strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
			t.Fatal(err)
		}
		injected := errors.New("injected scan race")
		err := runLibraryWithConfig(t.Context(), []string{"watch", "--client-dir", clientDir, "--worktree", worktree, "--interval", "1h"},
			strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{
				checkFilesystem: func(*os.File) error { return nil },
				scanFault:       func(scanFault) error { return injected },
			})
		if !errors.Is(err, injected) {
			t.Fatalf("watch scan error = %v, want %v", err, injected)
		}
	})

	t.Run("protected deletion", func(t *testing.T) {
		environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
		clientDir, worktree := newClientPaths(t)
		for index := range 10 {
			path := filepath.Join(worktree, fmt.Sprintf("file-%02d", index))
			if err := os.WriteFile(path, []byte(path), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		args := append(bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID), "--import-local")
		if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(worktree, "file-00")); err != nil {
			t.Fatal(err)
		}
		err := runLibraryWithConfig(t.Context(), []string{"watch", "--client-dir", clientDir, "--worktree", worktree, "--interval", "1h"},
			strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }})
		var required *deleteConfirmationRequiredError
		if !errors.As(err, &required) || required.deleted != 1 || required.tracked != 10 {
			t.Fatalf("watch deletion error = %v", err)
		}
	})
}

func TestLibraryWatchStopWaitsForInFlightHeadPersistence(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	var armed atomic.Bool
	headRequest := make(chan struct{})
	releaseHead := make(chan struct{})
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if armed.Load() && r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/head") {
			close(headRequest)
			<-releaseHead
		}
		environment.handler.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)

	clientDir, worktree := newClientPaths(t)
	if err := runTest(t.Context(), bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testClientDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktree, "pending.txt"), []byte("pending"), 0o600); err != nil {
		t.Fatal(err)
	}
	armed.Store(true)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- runLibraryWithConfig(ctx, []string{"watch", "--client-dir", clientDir, "--worktree", worktree, "--interval", "1h"},
			strings.NewReader(""), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }})
	}()
	<-headRequest
	cancel()
	select {
	case err := <-done:
		t.Fatalf("watch exited during Head persistence: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseHead)
	if err := <-done; err != nil {
		t.Fatalf("stop watch: %v", err)
	}
	assertTestConverged(t, environment, clientDir, worktree)
}

func TestLibraryWatchesForDifferentBindingsRunInParallel(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	otherLibraryID := "22222222-3333-4444-8555-666666666666"
	if _, _, err := environment.store.CreateLibrary(t.Context(), storage.Library{
		ID: otherLibraryID, OwnerUserID: testClientUserID, Name: "Other",
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	clientDir := filepath.Join(t.TempDir(), "client")
	worktrees := []string{platformTestTempDir(t), platformTestTempDir(t)}
	bindings := []struct {
		libraryID string
		worktree  string
		deviceID  string
	}{
		{libraryID: testClientLibraryID, worktree: worktrees[0], deviceID: testClientDeviceID},
		{libraryID: otherLibraryID, worktree: worktrees[1], deviceID: testOtherDeviceID},
	}
	for _, binding := range bindings {
		if err := runTest(t.Context(), bindArgs(clientDir, environment.server.URL, binding.libraryID, binding.worktree, binding.deviceID),
			strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	started := []chan struct{}{make(chan struct{}), make(chan struct{})}
	release := make(chan struct{})
	results := make(chan error, len(bindings))
	for index, binding := range bindings {
		go func() {
			results <- runLibraryWithConfig(ctx, []string{"watch", "--client-dir", clientDir, "--worktree", binding.worktree, "--interval", "1h"},
				strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{
					checkFilesystem: func(*os.File) error { return nil },
					scanFault: func(fault scanFault) error {
						if fault.phase == "before-final-validation" {
							close(started[index])
							<-release
						}
						return nil
					},
				})
		}()
	}
	for index, signal := range started {
		select {
		case <-signal:
		case <-time.After(time.Second):
			t.Fatalf("watch %d did not start in parallel", index)
		}
	}
	cancel()
	close(release)
	for range bindings {
		if err := <-results; err != nil {
			t.Fatalf("stop parallel watch: %v", err)
		}
	}
}
