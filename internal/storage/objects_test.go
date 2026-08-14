package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mingming-cn/filecloud/internal/acceptance"
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

func TestObjectStorePublicationCrashMatrixPreservesOldHead(t *testing.T) {
	for _, point := range []string{
		_objectBeforeTemporaryWrite, _objectAfterTemporaryWrite,
		_objectBeforeTemporarySync, _objectAfterTemporarySync,
		_objectBeforeInstall, _objectAfterInstall,
		_objectBeforeParentSync, _objectAfterParentSync,
	} {
		t.Run(point, func(t *testing.T) {
			dataDir, oldCommitID, oldRootID := newObjectPublicationCrashStore(t)
			command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestObjectStorePublicationCrashHelper$")
			command.Env = append(os.Environ(),
				"FILECLOUD_OBJECT_CRASH_DATA_DIR="+dataDir,
				"FILECLOUD_OBJECT_CRASH_POINT="+point,
			)
			assertObjectPublicationSIGKILL(t, command.Run())

			store, err := OpenForServe(t.Context(), dataDir)
			if err != nil {
				t.Fatalf("reopen after %s: %v", point, err)
			}
			defer closeObjectStore(t, store)
			library, err := store.GetLibrary(t.Context(), _objectCrashOwnerID, _objectCrashLibraryID)
			if err != nil || library.HeadCommitID == nil || *library.HeadCommitID != oldCommitID || library.HeadVersion != 1 {
				t.Fatalf("Head after %s = %+v, %v", point, library, err)
			}
			reachableObjects := assertStoredHeadGraph(t, store, oldCommitID, oldRootID)
			emitObjectPublicationAttestation(t, point, oldCommitID, *library.HeadCommitID, reachableObjects)

			data := []byte(_objectCrashNewBlock)
			created, err := store.PutObject(t.Context(), _objectCrashOwnerID, _objectCrashLibraryID,
				"blocks", object.ID(data), bytes.NewReader(data))
			if err != nil {
				t.Fatalf("replay after %s: %v", point, err)
			}
			wantCreated := point != _objectAfterInstall && point != _objectBeforeParentSync && point != _objectAfterParentSync
			if created != wantCreated {
				t.Fatalf("replay after %s created=%v want=%v", point, created, wantCreated)
			}
			reader, size, err := store.GetObject(t.Context(), _objectCrashOwnerID, _objectCrashLibraryID, "blocks", object.ID(data))
			if err != nil {
				t.Fatalf("read replayed object after %s: %v", point, err)
			}
			got, readErr := io.ReadAll(reader)
			closeErr := reader.Close()
			if readErr != nil || closeErr != nil || size != int64(len(data)) || !bytes.Equal(got, data) {
				t.Fatalf("replayed object after %s = %q/%d read=%v close=%v", point, got, size, readErr, closeErr)
			}
		})
	}
}

func TestObjectStorePublicationCrashHelper(t *testing.T) {
	dataDir := os.Getenv("FILECLOUD_OBJECT_CRASH_DATA_DIR")
	point := os.Getenv("FILECLOUD_OBJECT_CRASH_POINT")
	if dataDir == "" || point == "" {
		t.Skip("object publication crash subprocess helper")
	}
	store, err := OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer closeObjectStore(t, store)
	store.objectPublicationFault = func(actual string) error {
		if actual != point {
			return nil
		}
		if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
			return err
		}
		select {}
	}
	data := []byte(_objectCrashNewBlock)
	if _, err := store.PutObject(t.Context(), _objectCrashOwnerID, _objectCrashLibraryID,
		"blocks", object.ID(data), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	t.Fatalf("object publication did not reach crash point %q", point)
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

const (
	_objectCrashOwnerID   = "12345678-9abc-4def-8123-456789abcdef"
	_objectCrashLibraryID = "01234567-89ab-4def-8123-456789abcdef"
	_objectCrashNewBlock  = "new object after old Head"
)

func newObjectPublicationCrashStore(t *testing.T) (string, string, string) {
	t.Helper()
	root := acceptance.Root()
	if root == "" {
		root = "."
	}
	dataDir, err := os.MkdirTemp(root, ".linux-ext4-object-store-")
	if err != nil {
		t.Fatal(err)
	}
	dataDir, err = filepath.Abs(dataDir)
	if err != nil {
		t.Fatal(errors.Join(err, os.RemoveAll(dataDir)))
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dataDir); err != nil {
			t.Errorf("remove crash store: %v", err)
		}
	})
	if err := Init(t.Context(), dataDir); err != nil {
		t.Fatal(err)
	}
	store, err := OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateUser(t.Context(), User{ID: _objectCrashOwnerID, Username: "owner", PasswordHash: "hash"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	createObjectLibrary(t, store, _objectCrashOwnerID, _objectCrashLibraryID, "crash")
	blockData := []byte("content reachable from old Head")
	blockID := object.ID(blockData)
	fileInput := fmt.Sprintf(`{"Blocks":["%s"],"Size":"%d","Type":"File","Version":1}`, blockID, len(blockData))
	fileData, fileID, err := object.Canonicalize("files", []byte(fileInput))
	if err != nil {
		t.Fatal(err)
	}
	rootInput := fmt.Sprintf(`{"Entries":[{"Id":"%s","ModifiedAt":"2026-08-09T00:00:00Z","Name":"confirmed.txt","Type":"File"}],"Type":"Directory","Version":1}`, fileID)
	rootData, rootID, err := object.Canonicalize("directories", []byte(rootInput))
	if err != nil {
		t.Fatal(err)
	}
	commitInput := fmt.Sprintf(`{"AuthorUserId":"%s","CreatedAt":"2026-08-09T00:00:00Z","DeviceId":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee","Message":"sync","Parents":[],"Root":"%s","Type":"Commit","Version":1}`,
		_objectCrashOwnerID, rootID)
	commitData, commitID, err := object.Canonicalize("commits", []byte(commitInput))
	if err != nil {
		t.Fatal(err)
	}
	for kind, values := range map[string]map[string][]byte{
		"blocks":      {blockID: blockData},
		"files":       {fileID: fileData},
		"directories": {rootID: rootData},
		"commits":     {commitID: commitData},
	} {
		for id, data := range values {
			if _, err := store.PutObject(t.Context(), _objectCrashOwnerID, _objectCrashLibraryID, kind, id, bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := store.UpdateLibraryHead(t.Context(), _objectCrashOwnerID, _objectCrashLibraryID, nil, 0, commitID, nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return dataDir, commitID, rootID
}

func assertStoredHeadGraph(t *testing.T, store *Store, commitID, rootID string) int {
	t.Helper()
	commitData := readStoredObject(t, store, "commits", commitID)
	commit, err := object.VerifyCommit(commitData, commitID)
	if err != nil || commit.Root != rootID {
		t.Fatalf("verify old Head commit %s: commit=%+v err=%v", commitID, commit, err)
	}
	directoryData := readStoredObject(t, store, "directories", rootID)
	directory, err := object.VerifyDirectory(directoryData, rootID)
	if err != nil || len(directory.Entries) != 1 || directory.Entries[0].Type != "File" {
		t.Fatalf("verify old Head directory %s: directory=%+v err=%v", rootID, directory, err)
	}
	fileData := readStoredObject(t, store, "files", directory.Entries[0].ID)
	file, err := object.VerifyFile(fileData, directory.Entries[0].ID)
	if err != nil || len(file.Blocks) != 1 {
		t.Fatalf("verify old Head file %s: file=%+v err=%v", directory.Entries[0].ID, file, err)
	}
	block := readStoredObject(t, store, "blocks", file.Blocks[0])
	if object.ID(block) != file.Blocks[0] || int64(len(block)) != file.Size {
		t.Fatalf("verify old Head block %s: size=%d want=%d", file.Blocks[0], len(block), file.Size)
	}
	return 4
}

func emitObjectPublicationAttestation(t *testing.T, point, oldHead, currentHead string, reachableObjects int) {
	t.Helper()
	platform, filesystem, enabled := acceptance.ActivePlatform()
	if !enabled {
		return
	}
	line, err := acceptance.Encode(acceptance.Attestation{
		Kind: "server-readability", Scenario: "object publication " + point, Platform: platform,
		Filesystem: filesystem, ReachableObjects: reachableObjects, FailurePoint: point,
		OldHead: oldHead, CurrentHead: currentHead,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(line)
}

func readStoredObject(t *testing.T, store *Store, kind, id string) []byte {
	t.Helper()
	reader, _, err := store.GetObject(t.Context(), _objectCrashOwnerID, _objectCrashLibraryID, kind, id)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read stored %s/%s: read=%v close=%v", kind, id, readErr, closeErr)
	}
	return data
}

func assertObjectPublicationSIGKILL(t *testing.T, err error) {
	t.Helper()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("object publication process was not killed: %v", err)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("object publication process status=%v err=%v", status, err)
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
