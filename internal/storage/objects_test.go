package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/mingming-cn/filecloud/internal/object"
)

func TestObjectStorePublishesWithoutReplacementAndIsolatesLibraries(t *testing.T) {
	store := newObjectStore(t)
	defer closeObjectStore(t, store)
	ctx := t.Context()
	owner := "owner"
	libraryID := "01234567-89ab-4def-8123-456789abcdef"
	otherLibraryID := "01234567-89ab-4def-8123-456789abcdee"
	createObjectLibrary(t, store, owner, libraryID, "one")
	createObjectLibrary(t, store, owner, otherLibraryID, "two")

	data := []byte("content")
	id := object.ID(data)
	created, err := store.PutObject(ctx, owner, libraryID, "blocks", id, bytes.NewReader(data))
	if err != nil || !created {
		t.Fatalf("PutObject first = %v, %v", created, err)
	}
	created, err = store.PutObject(ctx, owner, libraryID, "blocks", id, bytes.NewReader(data))
	if err != nil || created {
		t.Fatalf("PutObject replay = %v, %v", created, err)
	}

	reader, size, err := store.GetObject(ctx, owner, libraryID, "blocks", id)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || size != int64(len(data)) || !bytes.Equal(got, data) {
		t.Fatalf("read = %q/%d, %v, %v", got, size, readErr, closeErr)
	}
	if _, _, err := store.GetObject(ctx, owner, otherLibraryID, "blocks", id); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("other library GetObject error = %v", err)
	}

	objectPath := filepath.Join(store.ObjectsDir(), owner, libraryID, "blocks", id[:2], id[2:])
	if err := os.WriteFile(objectPath, []byte("different"), 0o600); err != nil {
		t.Fatalf("replace fixture object: %v", err)
	}
	if _, err := store.PutObject(ctx, owner, libraryID, "blocks", id, bytes.NewReader(data)); !errors.Is(err, ErrObjectConflict) {
		t.Fatalf("PutObject conflict error = %v", err)
	}
	if preserved, err := os.ReadFile(objectPath); err != nil || string(preserved) != "different" {
		t.Fatalf("conflicting object = %q, %v", preserved, err)
	}
}

func TestObjectStoreSerializesPublicationThroughDirectorySync(t *testing.T) {
	store := newObjectStore(t)
	defer closeObjectStore(t, store)
	libraryID := "01234567-89ab-4def-8123-456789abcdef"
	createObjectLibrary(t, store, "owner", libraryID, "serialized")
	data := []byte("concurrent content")
	id := object.ID(data)
	destination := store.objectPath("owner", libraryID, "blocks", id)

	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	var blockSync sync.Once
	store.syncObjectDirectory = func(path string) error {
		if path == filepath.Dir(destination) {
			if _, err := os.Stat(destination); err == nil {
				blockSync.Do(func() {
					close(syncStarted)
					<-releaseSync
				})
			}
		}
		return syncDirectory(path)
	}

	type result struct {
		created bool
		err     error
	}
	firstResult := make(chan result, 1)
	var wait sync.WaitGroup
	wait.Go(func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				firstResult <- result{err: errors.New("first PUT panicked")}
			}
		}()
		created, err := store.PutObject(t.Context(), "owner", libraryID, "blocks", id, bytes.NewReader(data))
		firstResult <- result{created: created, err: err}
	})
	<-syncStarted

	queued := make(chan struct{})
	store.objectLockQueued = func(key string) {
		if key == destination {
			close(queued)
		}
	}
	readStarted := make(chan struct{})
	secondResult := make(chan result, 1)
	wait.Go(func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				secondResult <- result{err: errors.New("second PUT panicked")}
			}
		}()
		created, err := store.PutObject(t.Context(), "owner", libraryID, "blocks", id, &observedReader{reader: bytes.NewReader(data), started: readStarted})
		secondResult <- result{created: created, err: err}
	})
	<-queued
	select {
	case <-readStarted:
		t.Fatal("second PUT read input before first publication directory sync completed")
	default:
	}

	close(releaseSync)
	first := <-firstResult
	second := <-secondResult
	wait.Wait()
	if first.err != nil || !first.created || second.err != nil || second.created {
		t.Fatalf("concurrent PUTs = first(%v, %v), second(%v, %v)", first.created, first.err, second.created, second.err)
	}
	store.objectLocksMu.Lock()
	remainingLocks := len(store.objectLocks)
	store.objectLocksMu.Unlock()
	if remainingLocks != 0 {
		t.Fatalf("object publication locks remaining = %d, want 0", remainingLocks)
	}
}

func TestObjectStoreCanceledWaiterDoesNotReadOrCreateTemporaryFile(t *testing.T) {
	store := newObjectStore(t)
	defer closeObjectStore(t, store)
	libraryID := "01234567-89ab-4def-8123-456789abcdef"
	createObjectLibrary(t, store, "owner", libraryID, "cancel waiter")
	data := []byte("held publication")
	id := object.ID(data)
	destination := store.objectPath("owner", libraryID, "blocks", id)

	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	var once sync.Once
	store.syncObjectDirectory = func(path string) error {
		if path == filepath.Dir(destination) {
			if _, err := os.Stat(destination); err == nil {
				once.Do(func() {
					close(syncStarted)
					<-releaseSync
				})
			}
		}
		return syncDirectory(path)
	}
	firstResult := make(chan error, 1)
	var wait sync.WaitGroup
	wait.Go(func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				firstResult <- errors.New("first PUT panicked")
			}
		}()
		_, err := store.PutObject(t.Context(), "owner", libraryID, "blocks", id, bytes.NewReader(data))
		firstResult <- err
	})
	<-syncStarted

	queued := make(chan struct{})
	store.objectLockQueued = func(key string) {
		if key == destination {
			close(queued)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	readStarted := make(chan struct{})
	secondResult := make(chan error, 1)
	wait.Go(func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				secondResult <- errors.New("second PUT panicked")
			}
		}()
		_, err := store.PutObject(ctx, "owner", libraryID, "blocks", id, &observedReader{reader: bytes.NewReader(data), started: readStarted})
		secondResult <- err
	})
	<-queued
	before, err := os.ReadDir(filepath.Dir(destination))
	if err != nil {
		t.Fatalf("ReadDir before cancel: %v", err)
	}
	cancel()
	if err := <-secondResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled PutObject error = %v", err)
	}
	after, err := os.ReadDir(filepath.Dir(destination))
	if err != nil {
		t.Fatalf("ReadDir after cancel: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("directory entries after canceled waiter = %d, want %d", len(after), len(before))
	}
	select {
	case <-readStarted:
		t.Fatal("canceled waiter read its source")
	default:
	}

	close(releaseSync)
	if err := <-firstResult; err != nil {
		t.Fatalf("first PutObject: %v", err)
	}
	wait.Wait()
	store.objectLocksMu.Lock()
	remaining := len(store.objectLocks)
	store.objectLocksMu.Unlock()
	if remaining != 0 {
		t.Fatalf("object publication locks remaining = %d, want 0", remaining)
	}
}

func TestObjectStoreCanceledAfterCopyDoesNotPublish(t *testing.T) {
	store := newObjectStore(t)
	defer closeObjectStore(t, store)
	libraryID := "01234567-89ab-4def-8123-456789abcdef"
	createObjectLibrary(t, store, "owner", libraryID, "cancel copy")
	data := []byte("cancel after copy")
	id := object.ID(data)
	ctx, cancel := context.WithCancel(t.Context())

	if _, err := store.PutObject(ctx, "owner", libraryID, "blocks", id, &cancelAtEOFReader{reader: bytes.NewReader(data), cancel: cancel}); !errors.Is(err, context.Canceled) {
		t.Fatalf("PutObject error = %v, want context.Canceled", err)
	}
	if _, err := os.Stat(store.objectPath("owner", libraryID, "blocks", id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled object Stat error = %v, want not exist", err)
	}
}

func TestObjectStoreStreamsLargeReplay(t *testing.T) {
	store := newObjectStore(t)
	defer closeObjectStore(t, store)
	libraryID := "01234567-89ab-4def-8123-456789abcdef"
	createObjectLibrary(t, store, "owner", libraryID, "large replay")

	fixture, err := os.CreateTemp(t.TempDir(), "large-object-")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	hash := sha256.New()
	const size = int64(36 << 20)
	if _, err := io.CopyN(io.MultiWriter(fixture, hash), zeroReader{}, size); err != nil {
		t.Fatalf("write large fixture: %v", err)
	}
	if err := fixture.Close(); err != nil {
		t.Fatalf("close large fixture: %v", err)
	}
	id := hex.EncodeToString(hash.Sum(nil))
	for attempt, wantCreated := range []bool{true, false} {
		source, err := os.Open(fixture.Name())
		if err != nil {
			t.Fatalf("open large fixture: %v", err)
		}
		created, putErr := store.PutObject(t.Context(), "owner", libraryID, "blocks", id, source)
		closeErr := source.Close()
		if putErr != nil || closeErr != nil || created != wantCreated {
			t.Fatalf("large PutObject attempt %d = %v, %v/%v", attempt, created, putErr, closeErr)
		}
	}
}

func TestObjectStoreRetriesLeafSyncAfterPublishFailure(t *testing.T) {
	store := newObjectStore(t)
	defer closeObjectStore(t, store)
	libraryID := "01234567-89ab-4def-8123-456789abcdef"
	createObjectLibrary(t, store, "owner", libraryID, "retry sync")
	data := []byte("retry durable sync")
	id := object.ID(data)
	destination := store.objectPath("owner", libraryID, "blocks", id)

	leafSyncs := 0
	store.syncObjectDirectory = func(path string) error {
		if path == filepath.Dir(destination) {
			if _, err := os.Stat(destination); err == nil {
				leafSyncs++
				if leafSyncs == 1 {
					return errors.New("injected directory sync failure")
				}
			}
		}
		return syncDirectory(path)
	}
	if _, err := store.PutObject(t.Context(), "owner", libraryID, "blocks", id, bytes.NewReader(data)); err == nil {
		t.Fatal("first PutObject succeeded despite directory sync failure")
	}
	if created, err := store.PutObject(t.Context(), "owner", libraryID, "blocks", id, bytes.NewReader(data)); err != nil || created {
		t.Fatalf("replayed PutObject = %v, %v", created, err)
	}
	if leafSyncs != 2 {
		t.Fatalf("leaf directory syncs = %d, want 2", leafSyncs)
	}
}

func TestObjectStoreDurablyCreatesEachDirectoryLevel(t *testing.T) {
	store := newObjectStore(t)
	defer closeObjectStore(t, store)
	libraryID := "01234567-89ab-4def-8123-456789abcdef"
	createObjectLibrary(t, store, "owner", libraryID, "durable directories")
	data := []byte("directory chain")
	id := object.ID(data)

	var synced []string
	store.syncObjectDirectory = func(path string) error {
		synced = append(synced, path)
		return syncDirectory(path)
	}
	if created, err := store.PutObject(t.Context(), "owner", libraryID, "blocks", id, bytes.NewReader(data)); err != nil || !created {
		t.Fatalf("PutObject = %v, %v", created, err)
	}
	want := []string{
		store.objectsDir,
		filepath.Join(store.objectsDir, "owner"),
		filepath.Join(store.objectsDir, "owner", libraryID),
		filepath.Join(store.objectsDir, "owner", libraryID, "blocks"),
		filepath.Join(store.objectsDir, "owner", libraryID, "blocks", id[:2]),
	}
	if !slices.Equal(synced, want) {
		t.Fatalf("synced directories = %v, want %v", synced, want)
	}
}

func TestObjectStoreRejectsEscapingScopeIDs(t *testing.T) {
	store := newObjectStore(t)
	defer closeObjectStore(t, store)
	id := object.ID([]byte("escape"))
	for _, scope := range []struct{ owner, library string }{
		{owner: ".", library: "library"},
		{owner: "..", library: "library"},
		{owner: "owner", library: "."},
		{owner: "owner", library: ".."},
		{owner: "../outside", library: "library"},
	} {
		if _, err := store.PutObject(t.Context(), scope.owner, scope.library, "blocks", id, bytes.NewReader([]byte("escape"))); err == nil {
			t.Fatalf("PutObject(%q, %q) succeeded", scope.owner, scope.library)
		}
	}
}

type observedReader struct {
	reader  io.Reader
	started chan struct{}
}

type cancelAtEOFReader struct {
	reader io.Reader
	cancel context.CancelFunc
}

func (r *cancelAtEOFReader) Read(buffer []byte) (int, error) {
	n, err := r.reader.Read(buffer)
	if errors.Is(err, io.EOF) {
		r.cancel()
	}
	return n, err
}

type zeroReader struct{}

func (zeroReader) Read(buffer []byte) (int, error) {
	clear(buffer)
	return len(buffer), nil
}

func (r *observedReader) Read(buffer []byte) (int, error) {
	select {
	case <-r.started:
	default:
		close(r.started)
	}
	return r.reader.Read(buffer)
}

func newObjectStore(t *testing.T) *Store {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := Init(t.Context(), dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	store, err := OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenForServe: %v", err)
	}
	if err := store.CreateUser(t.Context(), User{ID: "owner", Username: "owner", PasswordHash: "hash"}, time.Now()); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	return store
}

func closeObjectStore(t *testing.T, store *Store) {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func createObjectLibrary(t *testing.T, store *Store, owner, id, name string) {
	t.Helper()
	if _, _, err := store.CreateLibrary(t.Context(), Library{ID: id, OwnerUserID: owner, Name: name}, time.Now()); err != nil {
		t.Fatalf("CreateLibrary: %v", err)
	}
}
