package main

import (
	"bytes"
	"database/sql"
	"errors"
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
		environment, target, _, _ := importedRemoteCheckout(t)
		var blockGets atomic.Int32
		var failed atomic.Bool
		proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/blocks/") {
				count := blockGets.Add(1)
				if count == 2 && failed.CompareAndSwap(false, true) {
					http.Error(w, "retry", http.StatusServiceUnavailable)
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
	})

	t.Run("disk", func(t *testing.T) {
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
		assertPendingCheckout(t, clientDir, target)
		assertNoBinding(t, clientDir, worktree)
		if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
			t.Fatalf("retry disk checkout: %v", err)
		}
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
		if _, err := os.Lstat(temp); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("failed identity update left temporary inode: %v", err)
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

func TestPendingCheckoutAccessTokenColumnMigration(t *testing.T) {
	clientDir, worktree := newClientPaths(t)
	if err := os.Mkdir(clientDir, 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("ALTER TABLE pending_checkouts ADD COLUMN access_token BLOB NOT NULL DEFAULT X''"); err != nil {
		t.Fatal(err)
	}
	pending := pendingCheckout{ServerURL: "http://localhost", LibraryID: testClientLibraryID, Worktree: worktree,
		UserID: testClientUserID, DeviceID: testClientDeviceID, TargetCommit: strings.Repeat("a", 64), HeadETag: `"head-version-1"`}
	if _, err := db.Exec(`INSERT INTO pending_checkouts(server_url, library_id, worktree, user_id, device_id,
		target_commit, target_root, head_etag, access_token) VALUES (?, ?, ?, ?, ?, ?, '', ?, ?)`, pending.ServerURL,
		pending.LibraryID, pending.Worktree, pending.UserID, pending.DeviceID, pending.TargetCommit, pending.HeadETag, []byte("obsolete")); err != nil {
		t.Fatal(err)
	}
	if err := initializeClientSchema(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	var columns, secureDelete int
	if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('pending_checkouts') WHERE name = 'access_token'").Scan(&columns); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("PRAGMA secure_delete").Scan(&secureDelete); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadPendingCheckout(t.Context(), db, pending.ServerURL, pending.LibraryID, pending.Worktree)
	if err != nil || loaded == nil || loaded.TargetCommit != pending.TargetCommit || columns != 0 || secureDelete != 1 {
		t.Fatalf("migration loaded=%+v columns=%d secure_delete=%d err=%v", loaded, columns, secureDelete, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
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
		if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
			t.Fatal(err)
		}
		binding := readTestBinding(t, clientDir, worktree)
		if !advanced.Load() || binding.SyncBase != target || binding.SyncBaseRoot != targetRoot {
			t.Fatalf("checkout drifted after Head change: advanced=%v binding=%+v", advanced.Load(), binding)
		}
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
