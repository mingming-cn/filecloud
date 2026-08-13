package main

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	libraryapi "github.com/mingming-cn/filecloud/internal/library"
	"github.com/mingming-cn/filecloud/internal/object"
)

func TestLibraryBindChecksOutRemoteHeadWithoutMutation(t *testing.T) {
	environment, target, targetRoot, files := importedRemoteCheckout(t)
	var puts atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			puts.Add(1)
		}
		environment.handler.ServeHTTP(w, r)
	}))
	defer proxy.Close()

	clientDir, worktree := newClientPaths(t)
	args := bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testOtherDeviceID)
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("checkout bind: %v", err)
	}
	if puts.Load() != 0 {
		t.Fatalf("checkout sent %d PUT requests", puts.Load())
	}
	binding := readTestBinding(t, clientDir, worktree)
	if binding.SyncBase != target || binding.SyncBaseRoot != targetRoot {
		t.Fatalf("checkout binding=%+v target=%s root=%s", binding, target, targetRoot)
	}
	root, err := openWorktreeRoot(worktree, func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := scanWorktree(root)
	closeErr := root.Close()
	if err != nil || closeErr != nil || snapshot.root != targetRoot {
		t.Fatalf("checked out root=%s want=%s err=%v close=%v", snapshot.root, targetRoot, err, closeErr)
	}
	for path, want := range files {
		got, err := os.ReadFile(filepath.Join(worktree, filepath.FromSlash(path)))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("file %q=%q want=%q err=%v", path, got, want, err)
		}
	}
	for _, path := range []string{"nested", "nested/empty-dir"} {
		info, err := os.Stat(filepath.Join(worktree, filepath.FromSlash(path)))
		if err != nil || !info.IsDir() || !info.ModTime().UTC().Equal(time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)) {
			t.Fatalf("directory %q info=%v err=%v", path, info, err)
		}
	}
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var paths, pending int
	if err := db.QueryRow("SELECT COUNT(*) FROM path_index WHERE worktree = ?", binding.Worktree).Scan(&paths); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM pending_checkouts").Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if paths != 5 || pending != 0 {
		t.Fatalf("path index count=%d pending=%d", paths, pending)
	}
	assertPlatformConverged(t, "first remote checkout", environment, clientDir, worktree, platformConfirmedFiles(files))
}

func TestLibraryBindCheckoutFetchesOnlyTargetCommitWithLongPublishedHistory(t *testing.T) {
	environment, currentHead, targetRoot, _ := importedRemoteCheckout(t)
	current, err := environment.store.GetLibrary(t.Context(), testClientUserID, testClientLibraryID)
	if err != nil || current.HeadCommitID == nil || *current.HeadCommitID != currentHead {
		t.Fatalf("current library=%+v err=%v", current, err)
	}
	parent := currentHead
	introduced := make([]string, 0, 1025)
	createdAt := func() time.Time { return time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC) }
	for range 1025 {
		data, id, err := canonicalCommit(testClientUserID, testClientDeviceID, targetRoot, []string{parent}, createdAt)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := environment.store.PutObject(t.Context(), testClientUserID, testClientLibraryID, "commits", id, bytes.NewReader(data)); err != nil {
			t.Fatalf("store history commit: %v", err)
		}
		introduced = append(introduced, id)
		parent = id
	}
	latest := parent
	if _, err := environment.store.UpdateLibraryHead(t.Context(), testClientUserID, testClientLibraryID, current.HeadCommitID,
		current.HeadVersion, latest, introduced, createdAt()); err != nil {
		t.Fatalf("publish long history Head: %v", err)
	}

	var commitGets []string
	var commitGetsMu sync.Mutex
	var puts atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			puts.Add(1)
		}
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/objects/commits/") {
			commitGetsMu.Lock()
			commitGets = append(commitGets, filepath.Base(r.URL.Path))
			commitGetsMu.Unlock()
		}
		environment.handler.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	clientDir, worktree := newClientPaths(t)
	if err := runTest(t.Context(), bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testOtherDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatalf("checkout long published history: %v", err)
	}
	commitGetsMu.Lock()
	gotCommitGets := append([]string(nil), commitGets...)
	commitGetsMu.Unlock()
	if len(gotCommitGets) != 1 || gotCommitGets[0] != latest {
		t.Fatalf("commit GETs=%v, want only target %s", gotCommitGets, latest)
	}
	if puts.Load() != 0 {
		t.Fatalf("checkout long history sent %d PUT requests", puts.Load())
	}
	binding := readTestBinding(t, clientDir, worktree)
	if binding.SyncBase != latest || binding.SyncBaseRoot != targetRoot {
		t.Fatalf("long history checkout binding=%+v", binding)
	}
}

func TestLibraryBindCheckoutDownloadAndDiskFailuresRetryFixedTarget(t *testing.T) {
	t.Run("download cache", func(t *testing.T) {
		environment, target, _, files := importedRemoteCheckout(t)
		var blockGets atomic.Int32
		var failed atomic.Bool
		proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/blocks/") {
				count := blockGets.Add(1)
				if count == 2 && failed.CompareAndSwap(false, true) {
					response := httptest.NewRecorder()
					environment.handler.ServeHTTP(response, r)
					connection, stream, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Errorf("hijack truncated block response: %v", err)
						return
					}
					body := response.Body.Bytes()
					_, _ = fmt.Fprintf(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: %d\r\nConnection: close\r\n\r\n", len(body))
					_, _ = stream.Write(body[:len(body)/2])
					_ = stream.Flush()
					_ = connection.Close()
					return
				}
			}
			environment.handler.ServeHTTP(w, r)
		}))
		defer proxy.Close()
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testOtherDeviceID)
		if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err == nil {
			t.Fatal("checkout unexpectedly survived injected download failure")
		}
		assertPendingCheckout(t, clientDir, target)
		if got := blockGets.Load(); got != 2 {
			t.Fatalf("first block GET count=%d", got)
		}
		if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
			t.Fatalf("retry checkout: %v", err)
		}
		if got := blockGets.Load(); got != 4 {
			t.Fatalf("retry refetched completed block: GET count=%d", got)
		}
		if binding := readTestBinding(t, clientDir, worktree); binding.SyncBase != target {
			t.Fatalf("retry target drifted: %+v target=%s", binding, target)
		}
		assertPlatformConverged(t, "checkout resumes truncated download", environment, clientDir, worktree,
			platformConfirmedFiles(files))
	})

	t.Run("wrong digest", func(t *testing.T) {
		environment, target, _, files := importedRemoteCheckout(t)
		var blockGets atomic.Int32
		var corrupted atomic.Bool
		proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/blocks/") {
				blockGets.Add(1)
				if corrupted.CompareAndSwap(false, true) {
					response := httptest.NewRecorder()
					environment.handler.ServeHTTP(response, r)
					body := response.Body.Bytes()
					if len(body) != 0 {
						body[0] ^= 0xff
					}
					for key, values := range response.Header() {
						for _, value := range values {
							w.Header().Add(key, value)
						}
					}
					w.WriteHeader(response.Code)
					_, _ = w.Write(body)
					return
				}
			}
			environment.handler.ServeHTTP(w, r)
		}))
		defer proxy.Close()
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testOtherDeviceID)
		if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err == nil {
			t.Fatal("checkout accepted wrong-digest block")
		}
		assertPendingCheckout(t, clientDir, target)
		assertNoBinding(t, clientDir, worktree)
		if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
			t.Fatalf("retry wrong-digest block: %v", err)
		}
		if blockGets.Load() < 2 {
			t.Fatalf("wrong-digest block was cached: GET count=%d", blockGets.Load())
		}
		assertPlatformConverged(t, "checkout rejects wrong digest then resumes", environment, clientDir, worktree,
			platformConfirmedFiles(files))
	})

	t.Run("disk", func(t *testing.T) {
		environment, target, _, files := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		failed := false
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, syncFile: func(file *os.File) error {
				if strings.HasPrefix(filepath.Base(file.Name()), checkoutTempPrefix) && !failed {
					failed = true
					return errors.New("disk unavailable")
				}
				return file.Sync()
			}})
		if err == nil || !failed {
			t.Fatalf("disk failure=%v injected=%v", err, failed)
		}
		assertPendingCheckout(t, clientDir, target)
		assertNoBinding(t, clientDir, worktree)
		if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
			t.Fatalf("retry disk checkout: %v", err)
		}
		assertPlatformConverged(t, "checkout resumes disk failure", environment, clientDir, worktree,
			platformConfirmedFiles(files))
	})
}

func TestLibraryBindCheckoutRecoversTemporaryIdentityWindow(t *testing.T) {
	t.Run("random capabilities", func(t *testing.T) {
		environment, _, _, _ := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		writes := 0
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, syncFile: func(file *os.File) error {
				if strings.HasPrefix(filepath.Base(file.Name()), checkoutTempPrefix) {
					writes++
					if writes == 2 {
						return errors.New("stop after two temp capabilities")
					}
				}
				return file.Sync()
			}})
		if err == nil || writes != 2 {
			t.Fatalf("capability setup error=%v writes=%d", err, writes)
		}
		db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		rows, err := db.Query("SELECT path, temp_name FROM checkout_paths WHERE temp_name <> '' AND type = 'File' ORDER BY path")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		seen := make(map[string]bool)
		for rows.Next() {
			var path, name string
			if err := rows.Scan(&path, &name); err != nil {
				t.Fatal(err)
			}
			if !validCheckoutTempName(name) || name == checkoutTempPrefix+object.ID([]byte(path))[:24] || seen[name] {
				t.Fatalf("invalid or deterministic temp capability path=%q name=%q", path, name)
			}
			seen[name] = true
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if len(seen) != 2 {
			t.Fatalf("temp capability count=%d, want 2", len(seen))
		}
	})

	t.Run("identity update failure", func(t *testing.T) {
		environment, target, _, _ := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		failed := false
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, beforeCheckoutTempIdentity: func() error {
				if !failed {
					failed = true
					return errors.New("identity update unavailable")
				}
				return nil
			}})
		if err == nil || !failed {
			t.Fatalf("identity update failure=%v injected=%v", err, failed)
		}
		assertPendingCheckout(t, clientDir, target)
		_, temp := readZeroIdentityTemp(t, clientDir, worktree)
		if info, err := os.Lstat(temp); err != nil || !info.Mode().IsRegular() {
			t.Fatalf("failed identity update did not retain temporary inode: info=%v err=%v", info, err)
		}
		db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), false)
		if err != nil {
			t.Fatal(err)
		}
		var durableCreates int
		queryErr := db.QueryRow(`SELECT COUNT(*) FROM fs_actions WHERE worktree = ? AND op = 'create_file'
			AND state = 'completed' AND expected_device <> 0 AND expected_inode <> 0`, worktree).Scan(&durableCreates)
		if closeErr := db.Close(); queryErr != nil || closeErr != nil || durableCreates == 0 {
			t.Fatalf("durable creates=%d query=%v close=%v", durableCreates, queryErr, closeErr)
		}
		if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
			t.Fatalf("retry identity update failure: %v", err)
		}
	})

	t.Run("zero identity exact-name hardlink rejected", func(t *testing.T) {
		environment, target, _, _ := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		failed := false
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, beforeCheckoutTempIdentity: func() error {
				if !failed {
					failed = true
					return errors.New("identity update unavailable")
				}
				return nil
			}})
		if err == nil {
			t.Fatal("expected identity update failure")
		}
		_, temp := readZeroIdentityTemp(t, clientDir, worktree)
		external := filepath.Join(t.TempDir(), "external")
		mtime := time.Date(2024, 7, 8, 9, 10, 11, 0, time.UTC)
		if err := os.WriteFile(external, []byte("external hardlink"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(external, mtime, mtime); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(temp); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(external, temp); err != nil {
			t.Fatal(err)
		}
		if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err == nil {
			t.Fatal("checkout adopted zero-identity exact-name hardlink")
		}
		for _, path := range []string{external, temp} {
			data, readErr := os.ReadFile(path)
			info, statErr := os.Stat(path)
			if readErr != nil || statErr != nil || string(data) != "external hardlink" || !info.ModTime().UTC().Equal(mtime) {
				t.Fatalf("zero-identity hardlink changed at %q: data=%q info=%v read=%v stat=%v", path, data, info, readErr, statErr)
			}
		}
		unbindErr := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", clientDir, "--worktree", worktree},
			strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }})
		if unbindErr == nil {
			t.Fatal("unbind deleted zero-identity exact-name hardlink")
		}
		if data, err := os.ReadFile(temp); err != nil || string(data) != "external hardlink" {
			t.Fatalf("unbind changed zero-identity hardlink: %q err=%v", data, err)
		}
		assertPendingCheckout(t, clientDir, target)
	})

	t.Run("registered identity hardlink retry rejected", func(t *testing.T) {
		environment, target, _, _ := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		failed := false
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, syncFile: func(file *os.File) error {
				if strings.HasPrefix(filepath.Base(file.Name()), checkoutTempPrefix) && !failed {
					failed = true
					return errors.New("disk unavailable")
				}
				return file.Sync()
			}})
		if err == nil || !failed {
			t.Fatalf("registered hardlink setup error=%v failed=%v", err, failed)
		}
		temp := findCheckoutTemp(t, worktree)
		external := filepath.Join(t.TempDir(), "external")
		if err := os.Link(temp, external); err != nil {
			t.Fatal(err)
		}
		mtime := time.Date(2024, 9, 10, 11, 12, 13, 0, time.UTC)
		if err := os.WriteFile(external, []byte("external linked data"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(external, mtime, mtime); err != nil {
			t.Fatal(err)
		}
		if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err == nil {
			t.Fatal("retry wrote registered temporary inode with external hardlink")
		}
		for _, path := range []string{external, temp} {
			data, readErr := os.ReadFile(path)
			info, statErr := os.Stat(path)
			if readErr != nil || statErr != nil || string(data) != "external linked data" || !info.ModTime().UTC().Equal(mtime) {
				t.Fatalf("registered hardlink changed at %q: data=%q info=%v read=%v stat=%v", path, data, info, readErr, statErr)
			}
		}
		assertPendingCheckout(t, clientDir, target)
	})

	t.Run("hardlink at write barrier rejected", func(t *testing.T) {
		environment, target, _, _ := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		external := filepath.Join(t.TempDir(), "external")
		linked := false
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, beforeCheckoutFileWrite: func(path, temp string) error {
				if !linked {
					linked = true
					return os.Link(filepath.Join(worktree, filepath.Dir(path), temp), external)
				}
				return nil
			}})
		if err == nil || !linked {
			t.Fatalf("write barrier hardlink error=%v linked=%v", err, linked)
		}
		data, readErr := os.ReadFile(external)
		info, statErr := os.Stat(external)
		if readErr != nil || statErr != nil || len(data) != 0 || info.Size() != 0 || info.ModTime().UTC().Format("2006-01-02T15:04:05Z") == "2026-02-03T04:05:06Z" {
			t.Fatalf("write barrier hardlink changed: data=%q info=%v read=%v stat=%v", data, info, readErr, statErr)
		}
		assertPendingCheckout(t, clientDir, target)
	})

	t.Run("hardlink at rename barrier rejected", func(t *testing.T) {
		environment, target, _, files := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		external := filepath.Join(t.TempDir(), "external")
		linkedPath := ""
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, beforeCheckoutFileRename: func(path, temp string) error {
				if linkedPath == "" {
					linkedPath = path
					return os.Link(filepath.Join(worktree, filepath.Dir(path), temp), external)
				}
				return nil
			}})
		if err == nil || linkedPath == "" {
			t.Fatalf("rename barrier hardlink error=%v path=%q", err, linkedPath)
		}
		data, readErr := os.ReadFile(external)
		info, statErr := os.Stat(external)
		if readErr != nil || statErr != nil || !bytes.Equal(data, files[linkedPath]) || info.ModTime().UTC().Format("2006-01-02T15:04:05Z") != "2026-02-03T04:05:06Z" {
			t.Fatalf("rename barrier hardlink changed: data=%q info=%v read=%v stat=%v", data, info, readErr, statErr)
		}
		if _, statErr := os.Stat(filepath.Join(worktree, linkedPath)); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("hardlinked temporary installed as target: %v", statErr)
		}
		assertPendingCheckout(t, clientDir, target)
	})

	for _, kind := range []string{"symlink", "fifo"} {
		t.Run("zero identity rejects "+kind, func(t *testing.T) {
			environment, target, _, _ := importedRemoteCheckout(t)
			clientDir, worktree := newClientPaths(t)
			args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
			failed := false
			err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
				libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, beforeCheckoutTempIdentity: func() error {
					if !failed {
						failed = true
						return errors.New("identity update unavailable")
					}
					return nil
				}})
			if err == nil {
				t.Fatal("expected identity update failure")
			}
			_, temp := readZeroIdentityTemp(t, clientDir, worktree)
			if err := os.Remove(temp); err != nil {
				t.Fatal(err)
			}
			if kind == "symlink" {
				if err := os.Symlink("outside", temp); err != nil {
					t.Fatal(err)
				}
			} else if err := syscall.Mkfifo(temp, 0o600); err != nil {
				t.Fatal(err)
			}
			err = runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
			if err == nil {
				t.Fatalf("retry accepted zero-identity %s", kind)
			}
			assertPendingCheckout(t, clientDir, target)
			info, statErr := os.Lstat(temp)
			if statErr != nil || (kind == "symlink" && info.Mode()&os.ModeSymlink == 0) || (kind == "fifo" && info.Mode()&os.ModeNamedPipe == 0) {
				t.Fatalf("rejected %s changed: info=%v err=%v", kind, info, statErr)
			}
		})
	}
}

func TestLibraryBindCheckoutOwnsOnlyRegisteredDirectories(t *testing.T) {
	t.Run("unregistered existing directory", func(t *testing.T) {
		environment, target, _, _ := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		mtime := time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, beforeCheckoutMaterialize: func() error {
				directory := filepath.Join(worktree, "nested")
				if err := os.Mkdir(directory, 0o700); err != nil {
					return err
				}
				if err := os.WriteFile(filepath.Join(directory, "user.txt"), []byte("user"), 0o600); err != nil {
					return err
				}
				return os.Chtimes(directory, mtime, mtime)
			}})
		if err == nil {
			t.Fatal("checkout adopted unregistered existing directory")
		}
		assertPendingCheckout(t, clientDir, target)
		assertNoBinding(t, clientDir, worktree)
		data, readErr := os.ReadFile(filepath.Join(worktree, "nested", "user.txt"))
		info, statErr := os.Stat(filepath.Join(worktree, "nested"))
		if readErr != nil || statErr != nil || string(data) != "user" || !info.ModTime().UTC().Equal(mtime) {
			t.Fatalf("unregistered directory changed: data=%q info=%v read=%v stat=%v", data, info, readErr, statErr)
		}
		if _, err := os.Stat(filepath.Join(worktree, "nested", "a.txt")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("checkout wrote into unregistered directory: %v", err)
		}
	})

	t.Run("identity record failure cleanup", func(t *testing.T) {
		environment, target, _, _ := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		failed := false
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, beforeCheckoutDirectoryIdentity: func() error {
				if !failed {
					failed = true
					return errors.New("directory identity unavailable")
				}
				return nil
			}})
		if err == nil || !failed {
			t.Fatalf("directory identity failure=%v injected=%v", err, failed)
		}
		assertPendingCheckout(t, clientDir, target)
		if _, err := os.Lstat(filepath.Join(worktree, "nested")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("identity failure left created directory: %v", err)
		}
		if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
			t.Fatalf("retry directory identity failure: %v", err)
		}
	})

	t.Run("registered directory replacement", func(t *testing.T) {
		environment, target, _, _ := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		failed := false
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, syncFile: func(file *os.File) error {
				if strings.HasPrefix(filepath.Base(file.Name()), checkoutTempPrefix) && !failed {
					failed = true
					return errors.New("stop after directory registration")
				}
				return file.Sync()
			}})
		if err == nil || !failed {
			t.Fatalf("replacement setup error=%v failed=%v", err, failed)
		}
		directory := filepath.Join(worktree, "nested", "empty-dir")
		if err := os.Remove(directory); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		replacementMtime := time.Date(2024, 4, 5, 6, 7, 8, 0, time.UTC)
		if err := os.WriteFile(filepath.Join(directory, "replacement.txt"), []byte("replacement"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(directory, replacementMtime, replacementMtime); err != nil {
			t.Fatal(err)
		}
		err = runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
		if err == nil {
			t.Fatal("checkout accepted registered directory replacement")
		}
		assertPendingCheckout(t, clientDir, target)
		assertNoBinding(t, clientDir, worktree)
		data, readErr := os.ReadFile(filepath.Join(directory, "replacement.txt"))
		info, statErr := os.Stat(directory)
		if readErr != nil || statErr != nil || string(data) != "replacement" || !info.ModTime().UTC().Equal(replacementMtime) {
			t.Fatalf("replacement directory changed: data=%q info=%v read=%v stat=%v", data, info, readErr, statErr)
		}
	})
}

func TestCheckoutDirectoryIdentitySchemaMigration(t *testing.T) {
	clientDir, worktree := newClientPaths(t)
	if err := os.Mkdir(clientDir, 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("ALTER TABLE checkout_paths DROP COLUMN target_device; ALTER TABLE checkout_paths DROP COLUMN target_inode"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO checkout_paths(worktree, path, type, object_id, canonical_mtime, completed)
		VALUES (?, 'nested', 'Directory', ?, '2026-02-03T04:05:06Z', 1)`, worktree, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	if err := initializeClientSchema(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	var device, inode uint64
	var completed int
	if err := db.QueryRow("SELECT target_device, target_inode, completed FROM checkout_paths WHERE worktree = ?", worktree).Scan(&device, &inode, &completed); err != nil {
		t.Fatal(err)
	}
	if device != 0 || inode != 0 || completed != 1 {
		t.Fatalf("directory identity migration device=%d inode=%d completed=%d", device, inode, completed)
	}
}

func TestLibraryBindCheckoutDirectoryCapabilityRenameBarriers(t *testing.T) {
	t.Run("external target appears", func(t *testing.T) {
		environment, target, _, _ := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		mtime := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, beforeCheckoutDirectoryRename: func(path, temp string) error {
				if path != "nested" {
					return nil
				}
				directory := filepath.Join(worktree, path)
				if err := os.Mkdir(directory, 0o700); err != nil {
					return err
				}
				if err := os.WriteFile(filepath.Join(directory, "external.txt"), []byte("external"), 0o600); err != nil {
					return err
				}
				return os.Chtimes(directory, mtime, mtime)
			}})
		if err == nil {
			t.Fatal("directory rename replaced external target")
		}
		assertPendingCheckout(t, clientDir, target)
		data, readErr := os.ReadFile(filepath.Join(worktree, "nested", "external.txt"))
		info, statErr := os.Stat(filepath.Join(worktree, "nested"))
		if readErr != nil || statErr != nil || string(data) != "external" || !info.ModTime().UTC().Equal(mtime) {
			t.Fatalf("external target changed: data=%q info=%v read=%v stat=%v", data, info, readErr, statErr)
		}
	})

	t.Run("external temp swap", func(t *testing.T) {
		environment, target, _, _ := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		mtime := time.Date(2024, 6, 7, 8, 9, 10, 0, time.UTC)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		var swapped string
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, beforeCheckoutDirectoryRename: func(path, temp string) error {
				if path != "nested" {
					return nil
				}
				swapped = filepath.Join(worktree, temp)
				if err := os.Remove(swapped); err != nil {
					return err
				}
				if err := os.Mkdir(swapped, 0o700); err != nil {
					return err
				}
				if err := os.WriteFile(filepath.Join(swapped, "replacement.txt"), []byte("replacement"), 0o600); err != nil {
					return err
				}
				return os.Chtimes(swapped, mtime, mtime)
			}})
		if err == nil || swapped == "" {
			t.Fatalf("directory temp swap error=%v swapped=%q", err, swapped)
		}
		assertPendingCheckout(t, clientDir, target)
		data, readErr := os.ReadFile(filepath.Join(swapped, "replacement.txt"))
		info, statErr := os.Stat(swapped)
		if readErr != nil || statErr != nil || string(data) != "replacement" || !info.ModTime().UTC().Equal(mtime) {
			t.Fatalf("replacement temp changed: data=%q info=%v read=%v stat=%v", data, info, readErr, statErr)
		}
		if _, err := os.Stat(filepath.Join(worktree, "nested")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("swapped temp installed as target: %v", err)
		}
	})

	t.Run("registered temp retry", func(t *testing.T) {
		environment, target, _, _ := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		stopped := false
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, beforeCheckoutDirectoryRename: func(path, temp string) error {
				if !stopped {
					stopped = true
					return errors.New("stop before directory rename")
				}
				return nil
			}})
		if err == nil || !stopped {
			t.Fatalf("registered temp retry setup error=%v stopped=%v", err, stopped)
		}
		assertPendingCheckout(t, clientDir, target)
		if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
			t.Fatalf("retry registered temp directory: %v", err)
		}
		if binding := readTestBinding(t, clientDir, worktree); binding.SyncBase != target {
			t.Fatalf("registered temp retry binding=%+v", binding)
		}
	})

	t.Run("unbind cleans registered temp directory", func(t *testing.T) {
		environment, _, _, _ := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		var tempPath string
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, beforeCheckoutDirectoryRename: func(path, temp string) error {
				tempPath = filepath.Join(worktree, temp)
				return errors.New("stop before directory rename")
			}})
		if err == nil || tempPath == "" {
			t.Fatalf("directory temp setup error=%v temp=%q", err, tempPath)
		}
		if info, err := os.Stat(tempPath); err != nil || !info.IsDir() {
			t.Fatalf("registered temp directory info=%v err=%v", info, err)
		}
		if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", clientDir, "--worktree", worktree},
			strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
			t.Fatalf("unbind registered temp directory: %v", err)
		}
		if _, err := os.Lstat(tempPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("registered temp directory remains: %v", err)
		}
	})
}

func TestLibraryBindCheckoutRecoversInstalledTargetsBeforeCompletion(t *testing.T) {
	for _, test := range []struct {
		name, kind, path string
	}{
		{name: "mkdir", kind: "Directory", path: "nested"},
		{name: "rename", kind: "File", path: "top.txt"},
	} {
		t.Run(test.name, func(t *testing.T) {
			environment, target, _, files := importedRemoteCheckout(t)
			clientDir, worktree := newClientPaths(t)
			args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
			failed := false
			err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
				libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, afterCheckoutInstall: func(path, kind string) error {
					if path == test.path && kind == test.kind && !failed {
						failed = true
						return errors.New("completion unavailable")
					}
					return nil
				}})
			if err == nil || !failed {
				t.Fatalf("completion failure=%v injected=%v", err, failed)
			}
			assertPendingCheckout(t, clientDir, target)
			assertNoBinding(t, clientDir, worktree)
			if test.kind == "File" {
				mtime := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
				if err := os.Chtimes(filepath.Join(worktree, test.path), mtime, mtime); err != nil {
					t.Fatal(err)
				}
			}
			if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
				t.Fatalf("recover installed target: %v", err)
			}
			if test.kind == "File" {
				data, err := os.ReadFile(filepath.Join(worktree, test.path))
				info, statErr := os.Stat(filepath.Join(worktree, test.path))
				wantMtime := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
				if err != nil || statErr != nil || !bytes.Equal(data, files[test.path]) || !info.ModTime().UTC().Equal(wantMtime) {
					t.Fatalf("recovered file data=%q info=%v read=%v stat=%v", data, info, err, statErr)
				}
			}
		})
	}

	t.Run("newly installed target external hardlink", func(t *testing.T) {
		environment, target, _, files := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		external := filepath.Join(t.TempDir(), "external")
		linked := false
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, afterCheckoutInstall: func(path, kind string) error {
				if path == "top.txt" && kind == "File" && !linked {
					linked = true
					return os.Link(filepath.Join(worktree, path), external)
				}
				return nil
			}})
		if err == nil || !linked {
			t.Fatalf("newly installed hardlink error=%v linked=%v", err, linked)
		}
		targetPath := filepath.Join(worktree, "top.txt")
		for _, path := range []string{external, targetPath} {
			data, readErr := os.ReadFile(path)
			info, statErr := os.Stat(path)
			if readErr != nil || statErr != nil || !bytes.Equal(data, files["top.txt"]) || info.ModTime().UTC().Format("2006-01-02T15:04:05Z") != "2026-02-03T04:05:06Z" {
				t.Fatalf("newly installed hardlink changed at %q: data=%q info=%v read=%v stat=%v", path, data, info, readErr, statErr)
			}
		}
		assertPendingCheckout(t, clientDir, target)
		assertNoBinding(t, clientDir, worktree)
	})

	t.Run("recovered target replaced before completion", func(t *testing.T) {
		environment, target, _, files := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		failed := false
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, afterCheckoutInstall: func(path, kind string) error {
				if path == "top.txt" && kind == "File" && !failed {
					failed = true
					return errors.New("completion unavailable")
				}
				return nil
			}})
		if err == nil || !failed {
			t.Fatalf("recovery replacement setup error=%v failed=%v", err, failed)
		}
		targetPath := filepath.Join(worktree, "top.txt")
		replacement := filepath.Join(t.TempDir(), "replacement")
		if err := os.WriteFile(replacement, files["top.txt"], 0o600); err != nil {
			t.Fatal(err)
		}
		mtime := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
		if err := os.Chtimes(replacement, mtime, mtime); err != nil {
			t.Fatal(err)
		}
		replacementInfo, err := os.Stat(replacement)
		if err != nil {
			t.Fatal(err)
		}
		moved := filepath.Join(t.TempDir(), "original")
		swapped := false
		err = runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, afterCheckoutInstall: func(path, kind string) error {
				if path != "top.txt" || kind != "File" {
					return nil
				}
				if err := os.Rename(targetPath, moved); err != nil {
					return err
				}
				if err := os.Rename(replacement, targetPath); err != nil {
					return err
				}
				swapped = true
				return nil
			}})
		if err == nil || !swapped {
			t.Fatalf("recovered replacement error=%v swapped=%v", err, swapped)
		}
		data, readErr := os.ReadFile(targetPath)
		info, statErr := os.Stat(targetPath)
		if readErr != nil || statErr != nil || !bytes.Equal(data, files["top.txt"]) || !info.ModTime().UTC().Equal(mtime) || !os.SameFile(replacementInfo, info) {
			t.Fatalf("recovered replacement changed: data=%q info=%v read=%v stat=%v", data, info, readErr, statErr)
		}
		assertPendingCheckout(t, clientDir, target)
		assertNoBinding(t, clientDir, worktree)
	})

	t.Run("same-content hardlink replacement", func(t *testing.T) {
		environment, target, _, files := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		failed := false
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, afterCheckoutInstall: func(path, kind string) error {
				if path == "top.txt" && kind == "File" && !failed {
					failed = true
					return errors.New("completion unavailable")
				}
				return nil
			}})
		if err == nil || !failed {
			t.Fatalf("hardlink replacement setup error=%v failed=%v", err, failed)
		}
		targetPath := filepath.Join(worktree, "top.txt")
		if err := os.Remove(targetPath); err != nil {
			t.Fatal(err)
		}
		external := filepath.Join(t.TempDir(), "external")
		mtime := time.Date(2024, 8, 9, 10, 11, 12, 0, time.UTC)
		if err := os.WriteFile(external, files["top.txt"], 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(external, mtime, mtime); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(external, targetPath); err != nil {
			t.Fatal(err)
		}
		err = runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
		if err == nil {
			t.Fatal("checkout accepted same-content target hardlink replacement")
		}
		for _, path := range []string{external, targetPath} {
			data, readErr := os.ReadFile(path)
			info, statErr := os.Stat(path)
			if readErr != nil || statErr != nil || !bytes.Equal(data, files["top.txt"]) || !info.ModTime().UTC().Equal(mtime) {
				t.Fatalf("target hardlink changed at %q: data=%q info=%v read=%v stat=%v", path, data, info, readErr, statErr)
			}
		}
		assertPendingCheckout(t, clientDir, target)
		assertNoBinding(t, clientDir, worktree)
	})

	t.Run("installed target external hardlink", func(t *testing.T) {
		environment, target, _, files := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		failed := false
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, afterCheckoutInstall: func(path, kind string) error {
				if path == "top.txt" && kind == "File" && !failed {
					failed = true
					return errors.New("completion unavailable")
				}
				return nil
			}})
		if err == nil || !failed {
			t.Fatalf("installed hardlink setup error=%v failed=%v", err, failed)
		}
		targetPath := filepath.Join(worktree, "top.txt")
		external := filepath.Join(t.TempDir(), "external")
		mtime := time.Date(2024, 10, 11, 12, 13, 14, 0, time.UTC)
		if err := os.Chtimes(targetPath, mtime, mtime); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(targetPath, external); err != nil {
			t.Fatal(err)
		}
		if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err == nil {
			t.Fatal("checkout recovered installed target with external hardlink")
		}
		for _, path := range []string{external, targetPath} {
			data, readErr := os.ReadFile(path)
			info, statErr := os.Stat(path)
			if readErr != nil || statErr != nil || !bytes.Equal(data, files["top.txt"]) || !info.ModTime().UTC().Equal(mtime) {
				t.Fatalf("installed hardlink changed at %q: data=%q info=%v read=%v stat=%v", path, data, info, readErr, statErr)
			}
		}
		assertPendingCheckout(t, clientDir, target)
		assertNoBinding(t, clientDir, worktree)
	})

	t.Run("mismatched file", func(t *testing.T) {
		environment, target, _, files := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		failed := false
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, afterCheckoutInstall: func(path, kind string) error {
				if path == "top.txt" && !failed {
					failed = true
					return errors.New("completion unavailable")
				}
				return nil
			}})
		if err == nil {
			t.Fatal("expected completion failure")
		}
		changed := bytes.Repeat([]byte("x"), len(files["top.txt"]))
		if err := os.WriteFile(filepath.Join(worktree, "top.txt"), changed, 0o600); err != nil {
			t.Fatal(err)
		}
		err = runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
		if err == nil {
			t.Fatal("retry accepted mismatched installed file")
		}
		assertPendingCheckout(t, clientDir, target)
		assertNoBinding(t, clientDir, worktree)
		if data, readErr := os.ReadFile(filepath.Join(worktree, "top.txt")); readErr != nil || !bytes.Equal(data, changed) {
			t.Fatalf("retry overwrote mismatched file: %q err=%v", data, readErr)
		}
	})
}

func TestLibraryCheckoutPendingUnbindCleansOnlyRegisteredTemp(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		name := "cleanup"
		if mismatch {
			name = "identity mismatch"
		}
		t.Run(name, func(t *testing.T) {
			environment, target, _, _ := importedRemoteCheckout(t)
			clientDir, worktree := newClientPaths(t)
			args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
			failed := false
			err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
				libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, syncFile: func(file *os.File) error {
					if strings.HasPrefix(filepath.Base(file.Name()), checkoutTempPrefix) && !failed {
						failed = true
						return errors.New("disk unavailable")
					}
					return file.Sync()
				}})
			if err == nil || !failed {
				t.Fatalf("disk failure=%v injected=%v", err, failed)
			}
			temp := findCheckoutTemp(t, worktree)
			if mismatch {
				if err := os.Remove(temp); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(temp, []byte("user replacement"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			unbindErr := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", clientDir, "--worktree", worktree},
				strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }})
			if mismatch {
				if unbindErr == nil {
					t.Fatal("unbind deleted mismatched temporary inode")
				}
				if data, err := os.ReadFile(temp); err != nil || string(data) != "user replacement" {
					t.Fatalf("mismatched inode changed: %q err=%v", data, err)
				}
				assertPendingCheckout(t, clientDir, target)
				return
			}
			if unbindErr != nil {
				t.Fatalf("unbind pending checkout: %v", unbindErr)
			}
			if _, err := os.Lstat(temp); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("registered temporary remains: %v", err)
			}
		})
	}
}

func TestLibraryCheckoutPendingUnbindAllowsMissingWorktree(t *testing.T) {
	environment, target, _, _ := importedRemoteCheckout(t)
	clientDir, worktree := newClientPaths(t)
	args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
	failed := false
	err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
		libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, syncFile: func(file *os.File) error {
			if strings.HasPrefix(filepath.Base(file.Name()), checkoutTempPrefix) && !failed {
				failed = true
				return errors.New("disk unavailable")
			}
			return file.Sync()
		}})
	if err == nil || !failed {
		t.Fatalf("disk failure=%v injected=%v", err, failed)
	}
	if err := os.RemoveAll(worktree); err != nil {
		t.Fatal(err)
	}
	if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", clientDir, "--worktree", worktree},
		strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
		t.Fatalf("unbind missing pending worktree: %v", err)
	}
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var pending, paths int
	if err := db.QueryRow("SELECT COUNT(*) FROM pending_checkouts").Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM checkout_paths").Scan(&paths); err != nil {
		t.Fatal(err)
	}
	if pending != 0 || paths != 0 {
		t.Fatalf("missing worktree unbind retained pending=%d paths=%d", pending, paths)
	}
	head, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if err != nil || head.CommitID == nil || *head.CommitID != target {
		t.Fatalf("missing worktree unbind changed Head: %+v err=%v", head, err)
	}
}

func TestLibraryUnbindMissingWorktreeResolvesSymlinkAlias(t *testing.T) {
	for _, pending := range []bool{false, true} {
		name := "binding"
		if pending {
			name = "pending checkout"
		}
		t.Run(name, func(t *testing.T) {
			environment, target, _, _ := importedRemoteCheckout(t)
			realParent := t.TempDir()
			realWorktree := filepath.Join(realParent, "missing", "worktree")
			if err := os.MkdirAll(realWorktree, 0o700); err != nil {
				t.Fatal(err)
			}
			alias := filepath.Join(t.TempDir(), "alias")
			if err := os.Symlink(realParent, alias); err != nil {
				t.Fatal(err)
			}
			aliasWorktree := filepath.Join(alias, "missing", "worktree")
			clientDir := filepath.Join(t.TempDir(), "client")
			args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, aliasWorktree, testOtherDeviceID)
			if pending {
				failed := false
				err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
					libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, syncFile: func(file *os.File) error {
						if strings.HasPrefix(filepath.Base(file.Name()), checkoutTempPrefix) && !failed {
							failed = true
							return errors.New("disk unavailable")
						}
						return file.Sync()
					}})
				if err == nil || !failed {
					t.Fatalf("pending alias setup error=%v failed=%v", err, failed)
				}
			} else if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
				t.Fatalf("bind through alias: %v", err)
			}
			if err := os.RemoveAll(filepath.Join(realParent, "missing")); err != nil {
				t.Fatal(err)
			}
			canonical, err := canonicalUnbindPath(aliasWorktree)
			if err != nil || canonical != realWorktree {
				t.Fatalf("missing alias canonical=%q want=%q err=%v", canonical, realWorktree, err)
			}
			if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", clientDir, "--worktree", aliasWorktree},
				strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
				t.Fatalf("unbind missing alias: %v", err)
			}
			db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, table := range []string{"bindings", "pending_checkouts", "checkout_paths"} {
				var count int
				if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
					t.Fatalf("%s count=%d err=%v", table, count, err)
				}
			}
			head, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
			if err != nil || head.CommitID == nil || *head.CommitID != target {
				t.Fatalf("alias unbind changed Head: %+v err=%v", head, err)
			}
		})
	}
	if _, err := canonicalUnbindPath(t.TempDir() + "/a/../b"); err == nil {
		t.Fatal("canonical unbind accepted '..' path component")
	}
}

func TestLibraryCheckoutCacheRejectsIntermediateSymlink(t *testing.T) {
	environment, target, _, _ := importedRemoteCheckout(t)
	clientDir, worktree := newClientPaths(t)
	if err := os.Mkdir(clientDir, 0o700); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(clientDir, "objects")); err != nil {
		t.Fatal(err)
	}
	err := runTest(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
	if err == nil {
		t.Fatal("checkout followed cache symlink")
	}
	assertPendingCheckout(t, clientDir, target)
	entries, readErr := os.ReadDir(outside)
	if readErr != nil || len(entries) != 0 {
		t.Fatalf("cache symlink target modified: %v err=%v", entries, readErr)
	}
}

func TestPendingCheckoutV21InflightMigrationRejectedBeforeDDL(t *testing.T) {
	for _, state := range []string{"applying", "rolling_back"} {
		for _, consumed := range []bool{false, true} {
			name := state + "/empty"
			if consumed {
				name = state + "/consumed checkout path"
			}
			t.Run(name, func(t *testing.T) {
				db, err := initializeClientDB(t.Context(), t.TempDir(), syncDirectory)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if _, err := db.Exec(`DROP TABLE sync_recovery_promotions;
					ALTER TABLE pending_checkouts RENAME TO v22_pending_checkouts;`+_clientV21CheckoutSQL+`;
					DROP TABLE v22_pending_checkouts;
					DELETE FROM client_schema_migrations WHERE version=22;
					INSERT INTO pending_checkouts VALUES('http://localhost','library','/work','user','device','commit','root','etag',?)`, state); err != nil {
					t.Fatal(err)
				}
				if consumed {
					if _, err := db.Exec(`INSERT INTO checkout_paths(worktree,path,type,object_id,canonical_mtime,actual_mtime,
						size,completed) VALUES('/work','base','File','object','2024-01-02T03:04:05Z','2024-01-02T03:04:05Z',1,1)`); err != nil {
						t.Fatal(err)
					}
				}
				var beforeSchema string
				if err := db.QueryRow(`SELECT group_concat(type||'|'||name||'|'||COALESCE(sql,''), char(10)) FROM
					(SELECT type,name,sql FROM sqlite_schema ORDER BY type,name)`).Scan(&beforeSchema); err != nil {
					t.Fatal(err)
				}
				var beforeVersion, beforePending, beforePaths int
				if err := db.QueryRow(`SELECT MAX(version) FROM client_schema_migrations`).Scan(&beforeVersion); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRow(`SELECT COUNT(*) FROM pending_checkouts`).Scan(&beforePending); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRow(`SELECT COUNT(*) FROM checkout_paths`).Scan(&beforePaths); err != nil {
					t.Fatal(err)
				}
				err = initializeClientSchema(t.Context(), db)
				if err == nil || !strings.Contains(err.Error(), "no exact rollback root mtime") {
					t.Fatalf("v21 in-flight migration error=%v", err)
				}
				var afterSchema string
				if err := db.QueryRow(`SELECT group_concat(type||'|'||name||'|'||COALESCE(sql,''), char(10)) FROM
					(SELECT type,name,sql FROM sqlite_schema ORDER BY type,name)`).Scan(&afterSchema); err != nil {
					t.Fatal(err)
				}
				var afterVersion, afterPending, afterPaths int
				if err := db.QueryRow(`SELECT MAX(version) FROM client_schema_migrations`).Scan(&afterVersion); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRow(`SELECT COUNT(*) FROM pending_checkouts`).Scan(&afterPending); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRow(`SELECT COUNT(*) FROM checkout_paths`).Scan(&afterPaths); err != nil {
					t.Fatal(err)
				}
				if afterSchema != beforeSchema || afterVersion != beforeVersion || afterPending != beforePending || afterPaths != beforePaths {
					t.Fatalf("rejected migration changed database schema=%v version=%d/%d pending=%d/%d paths=%d/%d",
						afterSchema != beforeSchema, beforeVersion, afterVersion, beforePending, afterPending, beforePaths, afterPaths)
				}
			})
		}
	}
}

func TestPendingCheckoutV22RequiresExactMtimeForInflightState(t *testing.T) {
	db, err := initializeClientDB(t.Context(), t.TempDir(), syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`INSERT INTO pending_checkouts(server_url,library_id,worktree,user_id,device_id,target_commit,target_root,
		head_etag,apply_state,conflict_promotions,rollback_root_mtime_ns,rollback_root_mtime_valid)
		VALUES('http://localhost','library','/work','user','device','commit','root','etag','applying',X'4643503100000000',0,0)`)
	if err == nil || !strings.Contains(err.Error(), "CHECK constraint") {
		t.Fatalf("invalid in-flight root mtime error=%v", err)
	}
	if err := _validateClientV22CheckoutSchema(t.Context(), db); err != nil {
		t.Fatal(err)
	}
}

func TestPendingCheckoutV21ToV22Migration(t *testing.T) {
	db, err := initializeClientDB(t.Context(), t.TempDir(), syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`ALTER TABLE pending_checkouts RENAME TO v22_pending_checkouts;` + _clientV21CheckoutSQL + `;
		INSERT INTO pending_checkouts(server_url,library_id,worktree,user_id,device_id,target_commit,target_root,head_etag,apply_state)
		SELECT server_url,library_id,worktree,user_id,device_id,target_commit,target_root,head_etag,apply_state FROM v22_pending_checkouts;
		INSERT INTO pending_checkouts VALUES('http://localhost','11111111-2222-4333-8444-555555555555','/work',
		'11111111-2222-4333-8444-555555555555','aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee','commit','','etag','pending');
		DROP TABLE v22_pending_checkouts;
		ALTER TABLE sync_recoveries RENAME TO v22_sync_recoveries;` + _clientV21SyncRecoverySQL + `;
		INSERT INTO sync_recoveries(worktree,path,recovery_name,type,object_id,canonical_mtime,size,device,inode,completed,tombstone_name)
		SELECT worktree,path,recovery_name,type,object_id,canonical_mtime,size,device,inode,completed,tombstone_name FROM v22_sync_recoveries;
		DROP TABLE v22_sync_recoveries; DELETE FROM client_schema_migrations WHERE version=22`); err != nil {
		t.Fatal(err)
	}
	if err := initializeClientSchema(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	var version int
	var encoded []byte
	if err := db.QueryRow("SELECT MAX(version) FROM client_schema_migrations").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT conflict_promotions FROM pending_checkouts LIMIT 1").Scan(&encoded); err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	var recoveryColumns, rollbackColumns, promotionRows int
	if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('sync_recoveries') WHERE name='promotion_action_id'").Scan(&recoveryColumns); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('sync_recovery_promotions') WHERE name='rollback_action_id'").Scan(&rollbackColumns); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM sync_recovery_promotions").Scan(&promotionRows); err != nil {
		t.Fatal(err)
	}
	if version != 22 || !bytes.Equal(encoded, _emptyConflictPromotions) || recoveryColumns != 0 || rollbackColumns != 1 || promotionRows != 0 {
		t.Fatalf("migration version=%d provenance=%x scalar columns=%d rollback columns=%d promotion rows=%d",
			version, encoded, recoveryColumns, rollbackColumns, promotionRows)
	}
	if err := _validateClientSyncRecoverySchema(t.Context(), db, 22, _clientV22SyncRecoverySQL, _clientV22SyncRecoveryColumns); err != nil {
		t.Fatal(err)
	}
	if err := _validateClientSyncRecoveryPromotionSchema(t.Context(), db); err != nil {
		t.Fatal(err)
	}
}

func TestClientV22MigratesFSActionRestorePromotionConstraint(t *testing.T) {
	db, err := initializeClientDB(t.Context(), t.TempDir(), syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const worktree = "/work"
	if _, err := db.Exec(`INSERT INTO fs_journal_bindings(worktree,root_device,root_inode,journal_format) VALUES(?,1,2,1)`, worktree); err != nil {
		t.Fatal(err)
	}
	action := fsAction{Worktree: worktree, ActionID: "00112233445566778899aabbccddeeff", Order: 1,
		Phase: fsPhasePreBase, Op: fsOpMtime, ParentDevice: 1, ParentInode: 2, Source: "base",
		ExpectedKind: "File", ExpectedDevice: 3, ExpectedInode: 4, ExpectedMtime: "2026-08-09T00:00:00Z"}
	if err := insertFSActionIntent(t.Context(), db, action); err != nil {
		t.Fatal(err)
	}
	var createSQL string
	if err := db.QueryRow(`SELECT sql FROM sqlite_schema WHERE type='table' AND name='fs_actions'`).Scan(&createSQL); err != nil {
		t.Fatal(err)
	}
	oldSQL := strings.Replace(createSQL, ", 'restore_promotion'", "", 1)
	if oldSQL == createSQL {
		t.Fatal("current filesystem action schema has no restore promotion operation")
	}
	if _, err := db.Exec(`DROP INDEX fs_actions_pending; DROP INDEX fs_actions_preserve_attempt;
		ALTER TABLE fs_actions RENAME TO current_fs_actions;` + oldSQL + `;
		INSERT INTO fs_actions SELECT * FROM current_fs_actions; DROP TABLE current_fs_actions;
		CREATE INDEX fs_actions_pending ON fs_actions(worktree,phase,state,action_order);
		CREATE UNIQUE INDEX fs_actions_preserve_attempt ON fs_actions(worktree,origin_action_id,attempt)
		WHERE origin_action_id IS NOT NULL`); err != nil {
		t.Fatal(err)
	}
	if err := initializeClientSchema(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	var migratedSQL string
	var actions int
	if err := db.QueryRow(`SELECT sql FROM sqlite_schema WHERE type='table' AND name='fs_actions'`).Scan(&migratedSQL); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM fs_actions WHERE worktree=?`, worktree).Scan(&actions); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(migratedSQL, "'restore_promotion'") || actions != 1 {
		t.Fatalf("restore operation migrated=%v preserved actions=%d", strings.Contains(migratedSQL, "'restore_promotion'"), actions)
	}
}

func TestSyncRecoveryV21DriftRejectedBeforePromotionDDL(t *testing.T) {
	db, err := initializeClientDB(t.Context(), t.TempDir(), syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`DROP TABLE sync_recovery_promotions;
		ALTER TABLE pending_checkouts RENAME TO v22_pending_checkouts;` + _clientV21CheckoutSQL + `;
		INSERT INTO pending_checkouts(server_url,library_id,worktree,user_id,device_id,target_commit,target_root,head_etag,apply_state)
		SELECT server_url,library_id,worktree,user_id,device_id,target_commit,target_root,head_etag,apply_state FROM v22_pending_checkouts;
		DROP TABLE v22_pending_checkouts;
		ALTER TABLE sync_recoveries ADD COLUMN stale TEXT NOT NULL DEFAULT '';
		DELETE FROM client_schema_migrations WHERE version=22`); err != nil {
		t.Fatal(err)
	}
	if err := initializeClientSchema(t.Context(), db); err == nil || !strings.Contains(err.Error(), "v21 sync recovery canonical SQL changed") {
		t.Fatalf("v21 drift error=%v", err)
	}
	var promotionTables int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='sync_recovery_promotions'`).Scan(&promotionTables); err != nil {
		t.Fatal(err)
	}
	if promotionTables != 0 {
		t.Fatal("v21 drift validation created promotion linkage table")
	}
}

func TestSyncRecoveryCurrentSchemaRejectsDrift(t *testing.T) {
	db, err := initializeClientDB(t.Context(), t.TempDir(), syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("ALTER TABLE sync_recoveries ADD COLUMN stale TEXT NOT NULL DEFAULT ''"); err != nil {
		t.Fatal(err)
	}
	if err := initializeClientSchema(t.Context(), db); err == nil || !strings.Contains(err.Error(), "sync recovery canonical SQL changed") {
		t.Fatalf("current sync recovery drift error=%v", err)
	}
	var columns int
	if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('sync_recoveries') WHERE name='stale'").Scan(&columns); err != nil || columns != 1 {
		t.Fatalf("schema rejection changed stale columns=%d err=%v", columns, err)
	}
}

func TestSyncRecoveryPromotionSchemaAndConstraints(t *testing.T) {
	for _, test := range []struct {
		name string
		ddl  string
	}{
		{"extra column", "ALTER TABLE sync_recovery_promotions ADD COLUMN stale TEXT NOT NULL DEFAULT ''"},
		{"extra index", "CREATE INDEX stale_promotion_index ON sync_recovery_promotions(recovery_path)"},
		{"trigger", "CREATE TRIGGER stale_promotion_trigger AFTER INSERT ON sync_recovery_promotions BEGIN SELECT 1; END"},
		{"view", "CREATE VIEW stale_promotion_view AS SELECT * FROM sync_recovery_promotions"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, err := initializeClientDB(t.Context(), t.TempDir(), syncDirectory)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec(test.ddl); err != nil {
				t.Fatal(err)
			}
			if err := initializeClientSchema(t.Context(), db); err == nil {
				t.Fatal("promotion schema drift was accepted")
			}
		})
	}

	db, err := initializeClientDB(t.Context(), t.TempDir(), syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	valid := []any{"/work", "top", "top/one", "00112233445566778899aabbccddeeff"}
	if _, err := db.Exec(`INSERT INTO sync_recovery_promotions(worktree,recovery_path,source_path,current_action_id)
		VALUES(?,?,?,?)`, valid...); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE sync_recovery_promotions SET rollback_action_id='ffeeddccbbaa00998877665544332211'
		WHERE worktree='/work' AND source_path='top/one'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE sync_recovery_promotions SET rollback_action_id='bad' WHERE worktree='/work'`); err == nil {
		t.Fatal("malformed rollback action identity was accepted")
	}
	if _, err := db.Exec(`INSERT INTO sync_recovery_promotions(worktree,recovery_path,source_path,current_action_id,rollback_action_id)
		VALUES('/work','top','top/two','11223344556677889900aabbccddeeff','ffeeddccbbaa00998877665544332211')`); err == nil {
		t.Fatal("duplicate rollback action identity was accepted")
	}
	for _, test := range []struct {
		name string
		row  []any
	}{
		{"oversize source", []any{"/work", "top", strings.Repeat("x", 4097), "11223344556677889900aabbccddeeff"}},
		{"malformed action", []any{"/work", "top", "top/two", "short"}},
		{"nonhex action", []any{"/work", "top", "top/two", "zz11223344556677889900aabbccddee"}},
		{"duplicate source", []any{"/work", "top", "top/one", "11223344556677889900aabbccddeeff"}},
		{"duplicate action", []any{"/work", "top", "top/two", "00112233445566778899aabbccddeeff"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := db.Exec(`INSERT INTO sync_recovery_promotions(worktree,recovery_path,source_path,current_action_id)
				VALUES(?,?,?,?)`, test.row...); err == nil {
				t.Fatal("malformed promotion linkage was accepted")
			}
		})
	}
}

func TestPendingCheckoutCurrentSchemaRejectsAccessTokenDrift(t *testing.T) {
	clientDir, _ := newClientPaths(t)
	if err := os.Mkdir(clientDir, 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("ALTER TABLE pending_checkouts ADD COLUMN access_token BLOB NOT NULL DEFAULT X''"); err != nil {
		t.Fatal(err)
	}
	if err := initializeClientSchema(t.Context(), db); err == nil || !strings.Contains(err.Error(), "pending checkout canonical SQL changed") {
		t.Fatalf("current pending checkout drift error=%v", err)
	}
	var columns int
	if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('pending_checkouts') WHERE name = 'access_token'").Scan(&columns); err != nil || columns != 1 {
		t.Fatalf("schema rejection changed access_token columns=%d err=%v", columns, err)
	}
}

func readZeroIdentityTemp(t *testing.T, clientDir, worktree string) (string, string) {
	t.Helper()
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var path, tempName string
	var device, inode uint64
	if err := db.QueryRow(`SELECT path, temp_name, temp_device, temp_inode FROM checkout_paths
		WHERE worktree = ? AND temp_name <> '' AND completed = 0 LIMIT 1`, worktree).Scan(&path, &tempName, &device, &inode); err != nil {
		t.Fatal(err)
	}
	if device != 0 || inode != 0 {
		t.Fatalf("temporary identity=%d/%d, want zero", device, inode)
	}
	if !validCheckoutTempName(tempName) || tempName == checkoutTempPrefix+object.ID([]byte(path))[:24] {
		t.Fatalf("temporary capability is invalid or deterministic: path=%q name=%q", path, tempName)
	}
	directory := filepath.Dir(filepath.FromSlash(path))
	if directory == "." {
		directory = ""
	}
	return path, filepath.Join(worktree, directory, tempName)
}

func findCheckoutTemp(t *testing.T, worktree string) string {
	t.Helper()
	var found string
	err := filepath.WalkDir(worktree, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasPrefix(entry.Name(), checkoutTempPrefix) {
			found = path
		}
		return nil
	})
	if err != nil || found == "" {
		t.Fatalf("find checkout temporary: found=%q err=%v", found, err)
	}
	return found
}

func TestLibraryBindCheckoutRejectsBothNonEmptyWithoutMutation(t *testing.T) {
	environment, target, _, _ := importedRemoteCheckout(t)
	clientDir, worktree := newClientPaths(t)
	local := filepath.Join(worktree, "local.txt")
	if err := os.WriteFile(local, []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	var puts atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			puts.Add(1)
		}
		environment.handler.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	err := runTest(t.Context(), bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testOtherDeviceID),
		strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
	if err == nil {
		t.Fatal("both-nonempty bind succeeded")
	}
	if puts.Load() != 0 {
		t.Fatalf("both-nonempty bind sent %d PUT requests", puts.Load())
	}
	head, headErr := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if headErr != nil || head.CommitID == nil || *head.CommitID != target {
		t.Fatalf("both-nonempty bind changed Head: %+v err=%v", head, headErr)
	}
	if data, readErr := os.ReadFile(local); readErr != nil || string(data) != "local" {
		t.Fatalf("both-nonempty bind changed local file: %q err=%v", data, readErr)
	}
	if _, statErr := os.Stat(clientDir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("both-nonempty bind created client state: %v", statErr)
	}
}

func TestLibraryBindCheckoutPinsHeadAndRejectsSymlinkParent(t *testing.T) {
	t.Run("Head changes", func(t *testing.T) {
		environment, target, targetRoot, _ := importedRemoteCheckout(t)
		base := mustServerURL(t, environment.server.URL)
		head, err := getRemoteHead(t.Context(), base, testClientLibraryID, []byte(environment.token))
		if err != nil {
			t.Fatal(err)
		}
		_, emptyRoot, _ := canonicalEmptyDirectory()
		data, successor, err := canonicalCommit(testClientUserID, testClientDeviceID, emptyRoot, []string{target}, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		if err := putMetadata(t.Context(), base, testClientLibraryID, []byte(environment.token), "commits", successor, data); err != nil {
			t.Fatal(err)
		}
		var advanced atomic.Bool
		proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/objects/directories/") && advanced.CompareAndSwap(false, true) {
				if _, _, err := updateRemoteHead(t.Context(), base, testClientLibraryID, []byte(environment.token), head.ETag, successor); err != nil {
					t.Errorf("advance Head: %v", err)
				}
			}
			environment.handler.ServeHTTP(w, r)
		}))
		defer proxy.Close()
		clientDir, worktree := newClientPaths(t)
		args := bindArgs(clientDir, proxy.URL, testClientLibraryID, worktree, testOtherDeviceID)
		err = runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "Head advanced") {
			t.Fatalf("advanced Head checkout error=%v", err)
		}
		binding := readTestBinding(t, clientDir, worktree)
		if !advanced.Load() || binding.SyncBase != target || binding.SyncBaseRoot != targetRoot {
			t.Fatalf("checkout drifted after Head change: advanced=%v binding=%+v", advanced.Load(), binding)
		}
		if err := syncTestWorktree(t, clientDir, worktree); err != nil {
			t.Fatalf("sync after initial checkout Head advance: %v", err)
		}
		assertTestConverged(t, environment, clientDir, worktree)
	})

	t.Run("symlink parent", func(t *testing.T) {
		environment, target, _, _ := importedRemoteCheckout(t)
		clientDir, worktree := newClientPaths(t)
		outside := t.TempDir()
		args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
		err := runLibraryWithConfig(t.Context(), args[1:], strings.NewReader(environment.token+"\n"), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, beforeCheckoutMaterialize: func() error {
				return os.Symlink(outside, filepath.Join(worktree, "nested"))
			}})
		if err == nil {
			t.Fatal("checkout followed a symlink parent")
		}
		assertPendingCheckout(t, clientDir, target)
		assertNoBinding(t, clientDir, worktree)
		entries, err := os.ReadDir(outside)
		if err != nil || len(entries) != 0 {
			t.Fatalf("checkout escaped worktree: entries=%v err=%v", entries, err)
		}
	})
}

func importedRemoteCheckout(t *testing.T) (libraryCLIEnvironment, string, string, map[string][]byte) {
	t.Helper()
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	clientDir, worktree := newClientPaths(t)
	files := map[string][]byte{
		"empty":        {},
		"top.txt":      []byte("top-level"),
		"nested/a.txt": bytes.Repeat([]byte("a"), object.MaxBlockSize+1),
	}
	if err := os.MkdirAll(filepath.Join(worktree, "nested", "empty-dir"), 0o700); err != nil {
		t.Fatal(err)
	}
	mtime := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	for path, data := range files {
		name := filepath.Join(worktree, filepath.FromSlash(path))
		if err := os.WriteFile(name, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(name, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{"nested/empty-dir", "nested"} {
		name := filepath.Join(worktree, filepath.FromSlash(path))
		if err := os.Chtimes(name, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	args := append(bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testClientDeviceID), "--import-local")
	if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	binding := readTestBinding(t, clientDir, worktree)
	return environment, binding.SyncBase, binding.SyncBaseRoot, files
}

func TestPendingCheckoutRootMtimeStateValidation(t *testing.T) {
	for _, test := range []struct {
		name, state  string
		mtime, valid int64
		accepted     bool
	}{
		{name: "pending sentinel", state: "pending", accepted: true},
		{name: "applying epoch", state: "applying", valid: 1, accepted: true},
		{name: "applying missing mtime", state: "applying"},
		{name: "rolling back missing mtime", state: "rolling_back"},
		{name: "finalized missing mtime", state: "finalized"},
		{name: "pending valid mtime", state: "pending", valid: 1},
		{name: "noncanonical sentinel", state: "pending", mtime: 1},
		{name: "invalid valid flag", state: "applying", valid: 2},
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
			if err := os.WriteFile(filepath.Join(worktree, "source"), []byte("source"), 0o600); err != nil {
				t.Fatal(err)
			}
			var source syscall.Stat_t
			if err := syscall.Stat(filepath.Join(worktree, "source"), &source); err != nil {
				t.Fatal(err)
			}
			action := fsAction{Worktree: worktree, ActionID: "00112233445566778899aabbccddeeff", Order: 0,
				Phase: fsPhasePreBase, Op: fsOpRename, ParentDevice: root.device, ParentInode: root.inode,
				Source: "source", Target: "target", ExpectedKind: "File", ExpectedDevice: uint64(source.Dev),
				ExpectedInode: source.Ino, State: fsStateIntent}
			if err := insertFSActionIntent(t.Context(), db, action); err != nil {
				t.Fatal(err)
			}
			if err := savePendingCheckout(t.Context(), db, pendingCheckout{ServerURL: "http://localhost", LibraryID: "library",
				Worktree: worktree, UserID: "user", DeviceID: "device", TargetCommit: "commit",
				ConflictPromotions: _emptyConflictPromotions}); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`PRAGMA ignore_check_constraints=ON`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE pending_checkouts SET apply_state=?,rollback_root_mtime_ns=?,rollback_root_mtime_valid=?
				WHERE worktree=?`, test.state, test.mtime, test.valid, worktree); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`PRAGMA ignore_check_constraints=OFF`); err != nil {
				t.Fatal(err)
			}
			loaded, loadErr := loadPendingCheckout(t.Context(), db, "http://localhost", "library", worktree)
			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			loadedWith, withErr := loadPendingCheckoutWith(t.Context(), tx, "http://localhost", "library", worktree)
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if test.accepted {
				if loadErr != nil || withErr != nil || loaded == nil || loadedWith == nil ||
					loaded.RollbackRootMtimeValid != (test.valid == 1) || loaded.RollbackRootMtimeNS != test.mtime {
					t.Fatalf("valid pending load=%+v/%+v err=%v/%v", loaded, loadedWith, loadErr, withErr)
				}
				return
			}
			if loadErr == nil || withErr == nil || !strings.Contains(loadErr.Error(), "state is corrupt") ||
				!strings.Contains(withErr.Error(), "state is corrupt") {
				t.Fatalf("corrupt pending load=%+v/%+v err=%v/%v", loaded, loadedWith, loadErr, withErr)
			}
			if err := recoverFSActions(t.Context(), db, worktree, root, nil); err == nil ||
				!strings.Contains(err.Error(), "state is corrupt") {
				t.Fatalf("corrupt pending recovery error=%v", err)
			}
			if data, err := os.ReadFile(filepath.Join(worktree, "source")); err != nil || string(data) != "source" {
				t.Fatalf("source changed data=%q err=%v", data, err)
			}
			if _, err := os.Lstat(filepath.Join(worktree, "target")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("target created err=%v", err)
			}
			var state string
			var mtime, valid int64
			var actionState string
			if err := db.QueryRow(`SELECT apply_state,rollback_root_mtime_ns,rollback_root_mtime_valid
				FROM pending_checkouts WHERE worktree=?`, worktree).Scan(&state, &mtime, &valid); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT state FROM fs_actions WHERE worktree=?`, worktree).Scan(&actionState); err != nil {
				t.Fatal(err)
			}
			if state != test.state || mtime != test.mtime || valid != test.valid || actionState != fsStateIntent {
				t.Fatalf("rejection changed DB state=%q/%d/%d action=%q", state, mtime, valid, actionState)
			}
		})
	}
}

func assertPendingCheckout(t *testing.T, clientDir, target string) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(clientDir, _clientDatabaseName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var got string
	if err := db.QueryRow("SELECT target_commit FROM pending_checkouts").Scan(&got); err != nil || got != target {
		t.Fatalf("pending target=%s want=%s err=%v", got, target, err)
	}
}

func assertNoBinding(t *testing.T, clientDir, worktree string) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(clientDir, _clientDatabaseName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM bindings WHERE worktree = ?", worktree).Scan(&count); err != nil || count != 0 {
		t.Fatalf("binding count=%d err=%v", count, err)
	}
}
