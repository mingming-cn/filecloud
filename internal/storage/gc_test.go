package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mingming-cn/filecloud/internal/object"
)

const (
	_gcTestOwnerA  = "01234567-89ab-4def-8123-456789abcdef"
	_gcTestOwnerB  = "11234567-89ab-4def-8123-456789abcdef"
	_gcTestDevice  = "21234567-89ab-4def-8123-456789abcdef"
	_gcTestLibrary = "31234567-89ab-4def-8123-456789abcdef"
)

func TestGarbageCollectorPreservesPublishedHistoryAndGraceObjects(t *testing.T) {
	dataDir, oldObjects, preserved := newGarbageCollectionFixture(t)
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	beforeDatabase, err := os.ReadFile(filepath.Join(dataDir, _databaseName))
	if err != nil {
		t.Fatalf("read database before GC: %v", err)
	}

	collector, err := OpenGarbageCollector(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenGarbageCollector dry-run: %v", err)
	}
	dryReport, err := collector.Collect(t.Context(), GarbageCollectionOptions{
		DryRun: true, GracePeriod: 24 * time.Hour, Now: now,
	})
	closeErr := collector.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("dry-run Collect/Close = %v / %v", err, closeErr)
	}
	want := GarbageCollectionReport{Objects: []GarbageCollectionObjectStats{
		{Type: "blocks", Count: 1, Bytes: int64(len(oldObjects["blocks"]))},
		{Type: "files", Count: 1, Bytes: int64(len(oldObjects["files"]))},
		{Type: "directories", Count: 1, Bytes: int64(len(oldObjects["directories"]))},
		{Type: "commits", Count: 1, Bytes: int64(len(oldObjects["commits"]))},
	}}
	if !reflect.DeepEqual(dryReport, want) {
		t.Fatalf("dry-run report = %+v, want %+v", dryReport, want)
	}
	assertGCObjects(t, dataDir, oldObjects, true)
	assertPreservedGCObjects(t, preserved)
	afterDryDatabase, err := os.ReadFile(filepath.Join(dataDir, _databaseName))
	if err != nil || !bytes.Equal(afterDryDatabase, beforeDatabase) {
		t.Fatalf("database changed during dry-run: read=%v equal=%v", err, bytes.Equal(afterDryDatabase, beforeDatabase))
	}

	collector, err = OpenGarbageCollector(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenGarbageCollector delete: %v", err)
	}
	actualReport, err := collector.Collect(t.Context(), GarbageCollectionOptions{
		GracePeriod: 24 * time.Hour, Now: now,
	})
	closeErr = collector.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("Collect/Close = %v / %v", err, closeErr)
	}
	if !reflect.DeepEqual(actualReport, dryReport) {
		t.Fatalf("actual report = %+v, want dry-run %+v", actualReport, dryReport)
	}
	assertGCObjects(t, dataDir, oldObjects, false)
	assertPreservedGCObjects(t, preserved)

	collector, err = OpenGarbageCollector(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenGarbageCollector idempotent: %v", err)
	}
	empty, err := collector.Collect(t.Context(), GarbageCollectionOptions{GracePeriod: 24 * time.Hour, Now: now})
	closeErr = collector.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("idempotent Collect/Close = %v / %v", err, closeErr)
	}
	for _, stats := range empty.Objects {
		if stats.Count != 0 || stats.Bytes != 0 {
			t.Fatalf("idempotent report = %+v, want empty", empty)
		}
	}
}

func TestGarbageCollectorRequiresExclusiveLockBeforeReading(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := Init(t.Context(), dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	serving, err := OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenForServe: %v", err)
	}
	defer closeObjectStore(t, serving)

	if _, err := OpenGarbageCollector(t.Context(), dataDir); err == nil || !strings.Contains(err.Error(), "locked by another process") {
		t.Fatalf("OpenGarbageCollector error = %v, want lock conflict", err)
	}
}

func TestGarbageCollectorResumesAfterEveryDeleteBoundary(t *testing.T) {
	for _, point := range []string{_gcBeforeDelete, _gcAfterDelete} {
		for deletion := 1; deletion <= 4; deletion++ {
			t.Run(fmt.Sprintf("%s/%d", point, deletion), func(t *testing.T) {
				dataDir, oldObjects, preserved := newGarbageCollectionFixture(t)
				command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestGarbageCollectorCrashHelper$")
				command.Env = append(os.Environ(),
					"FILECLOUD_GC_CRASH_DATA_DIR="+dataDir,
					"FILECLOUD_GC_CRASH_POINT="+point,
					"FILECLOUD_GC_CRASH_DELETION="+strconv.Itoa(deletion),
				)
				assertObjectPublicationSIGKILL(t, command.Run())
				assertAllGCHeadsReadable(t, dataDir)
				assertPreservedGCObjects(t, preserved)

				collector, err := OpenGarbageCollector(t.Context(), dataDir)
				if err != nil {
					t.Fatalf("reopen collector: %v", err)
				}
				_, collectErr := collector.Collect(t.Context(), GarbageCollectionOptions{
					GracePeriod: MinimumGarbageCollectionGracePeriod,
					Now:         time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC),
				})
				closeErr := collector.Close()
				if collectErr != nil || closeErr != nil {
					t.Fatalf("resumed Collect/Close = %v / %v", collectErr, closeErr)
				}
				assertGCObjects(t, dataDir, oldObjects, false)
				assertAllGCHeadsReadable(t, dataDir)
				assertPreservedGCObjects(t, preserved)
			})
		}
	}
}

func TestGarbageCollectorCrashHelper(t *testing.T) {
	dataDir := os.Getenv("FILECLOUD_GC_CRASH_DATA_DIR")
	point := os.Getenv("FILECLOUD_GC_CRASH_POINT")
	deletionText := os.Getenv("FILECLOUD_GC_CRASH_DELETION")
	if dataDir == "" || point == "" || deletionText == "" {
		t.Skip("garbage collection crash subprocess helper")
	}
	deletion, err := strconv.Atoi(deletionText)
	if err != nil || deletion < 1 {
		t.Fatalf("invalid crash deletion %q", deletionText)
	}
	collector, err := OpenGarbageCollector(t.Context(), dataDir)
	if err != nil {
		t.Fatal(err)
	}
	deletions := 0
	collector.fault = func(actual string) error {
		if actual != point {
			return nil
		}
		deletions++
		if deletions == deletion {
			return killObjectTestProcess()
		}
		return nil
	}
	_, err = collector.Collect(t.Context(), GarbageCollectionOptions{
		GracePeriod: MinimumGarbageCollectionGracePeriod,
		Now:         time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Fatalf("garbage collection did not reach %s for deletion %d", point, deletion)
}

func TestGarbageCollectorRejectsInvalidOptionsBeforeDeletion(t *testing.T) {
	dataDir, oldObjects, _ := newGarbageCollectionFixture(t)
	collector, err := OpenGarbageCollector(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenGarbageCollector: %v", err)
	}
	defer func() {
		if err := collector.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	if _, err := collector.Collect(t.Context(), GarbageCollectionOptions{GracePeriod: MinimumGarbageCollectionGracePeriod - time.Second, Now: time.Now()}); err == nil {
		t.Fatal("unsafe grace period unexpectedly accepted")
	}
	assertGCObjects(t, dataDir, oldObjects, true)
}

func newGarbageCollectionFixture(t *testing.T) (string, map[string][]byte, map[string][]byte) {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := Init(t.Context(), dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	store, err := OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenForServe: %v", err)
	}
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	for _, user := range []User{
		{ID: _gcTestOwnerA, Username: "owner-a", PasswordHash: "hash"},
		{ID: _gcTestOwnerB, Username: "owner-b", PasswordHash: "hash"},
	} {
		if err := store.CreateUser(t.Context(), user, now); err != nil {
			t.Fatalf("CreateUser(%s): %v", user.ID, err)
		}
	}
	for _, library := range []Library{
		{ID: _gcTestLibrary, OwnerUserID: _gcTestOwnerA, Name: "one"},
		{ID: _gcTestLibrary, OwnerUserID: _gcTestOwnerB, Name: "two"},
	} {
		if _, _, err := store.CreateLibrary(t.Context(), library, now); err != nil {
			t.Fatalf("CreateLibrary(%s): %v", library.OwnerUserID, err)
		}
	}

	base := putGCTestGraph(t, store, _gcTestOwnerA, _gcTestLibrary, []byte("base"), nil, now, true)
	branch := putGCTestGraph(t, store, _gcTestOwnerA, _gcTestLibrary, []byte("branch"), []string{base.commitID}, now.Add(time.Second), false)
	current := putGCTestGraph(t, store, _gcTestOwnerA, _gcTestLibrary, []byte("current"), []string{base.commitID, branch.commitID}, now.Add(2*time.Second), false)
	publishedBase, err := store.UpdateLibraryHead(t.Context(), _gcTestOwnerA, _gcTestLibrary, nil, 0, base.commitID, nil, now)
	if err != nil {
		t.Fatalf("publish base: %v", err)
	}
	if _, err := store.UpdateLibraryHead(t.Context(), _gcTestOwnerA, _gcTestLibrary, publishedBase.HeadCommitID, publishedBase.HeadVersion,
		current.commitID, []string{branch.commitID}, now.Add(2*time.Second)); err != nil {
		t.Fatalf("publish merge: %v", err)
	}
	other := putGCTestGraph(t, store, _gcTestOwnerB, _gcTestLibrary, []byte("other library"), nil, now, false)
	if _, err := store.UpdateLibraryHead(t.Context(), _gcTestOwnerB, _gcTestLibrary, nil, 0, other.commitID, nil, now); err != nil {
		t.Fatalf("publish other library: %v", err)
	}

	orphan := putGCTestGraph(t, store, _gcTestOwnerA, _gcTestLibrary, []byte("expired orphan"), nil, now, false)
	recentData := []byte("recent orphan")
	recentID := object.ID(recentData)
	if _, err := store.PutObject(t.Context(), _gcTestOwnerA, _gcTestLibrary, "blocks", recentID, bytes.NewReader(recentData)); err != nil {
		t.Fatalf("put recent orphan: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close fixture store: %v", err)
	}

	oldTime := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC)
	oldObjects := map[string][]byte{
		"blocks": orphan.blockData, "files": orphan.fileData,
		"directories": orphan.directoryData, "commits": orphan.commitData,
	}
	for kind, data := range oldObjects {
		id := object.ID(data)
		path := gcTestObjectPath(dataDir, _gcTestOwnerA, _gcTestLibrary, kind, id)
		if err := os.Chtimes(path, oldTime, oldTime); err != nil {
			t.Fatalf("age %s orphan: %v", kind, err)
		}
	}
	preserved := make(map[string][]byte)
	for _, graph := range []gcTestGraph{base, branch, current, other} {
		for kind, data := range map[string][]byte{
			"blocks": graph.blockData, "files": graph.fileData,
			"directories": graph.directoryData, "commits": graph.commitData,
		} {
			path := gcTestObjectPath(dataDir, graph.owner, _gcTestLibrary, kind, object.ID(data))
			preserved[path] = bytes.Clone(data)
		}
		if len(graph.nestedDirectoryData) != 0 {
			path := gcTestObjectPath(dataDir, graph.owner, _gcTestLibrary, "directories", object.ID(graph.nestedDirectoryData))
			preserved[path] = bytes.Clone(graph.nestedDirectoryData)
		}
	}
	recentPath := gcTestObjectPath(dataDir, _gcTestOwnerA, _gcTestLibrary, "blocks", recentID)
	recentTime := time.Date(2026, 8, 20, 6, 0, 0, 0, time.UTC)
	if err := os.Chtimes(recentPath, recentTime, recentTime); err != nil {
		t.Fatalf("set recent orphan time: %v", err)
	}
	preserved[recentPath] = bytes.Clone(recentData)
	return dataDir, oldObjects, preserved
}

type gcTestGraph struct {
	owner                                                               string
	blockData, fileData, directoryData, nestedDirectoryData, commitData []byte
	commitID                                                            string
}

func putGCTestGraph(t *testing.T, store *Store, owner, libraryID string, blockData []byte, parents []string, now time.Time, nested bool) gcTestGraph {
	t.Helper()
	blockID := object.ID(blockData)
	fileInput := fmt.Sprintf(`{"Blocks":[%q],"Size":%q,"Type":"File","Version":1}`, blockID, fmt.Sprint(len(blockData)))
	fileData, fileID, err := object.Canonicalize("files", []byte(fileInput))
	if err != nil {
		t.Fatalf("canonicalize file: %v", err)
	}
	directoryInput := fmt.Sprintf(`{"Entries":[{"Id":%q,"ModifiedAt":"2026-08-18T12:00:00Z","Name":"file","Type":"File"}],"Type":"Directory","Version":1}`, fileID)
	directoryData, directoryID, err := object.Canonicalize("directories", []byte(directoryInput))
	if err != nil {
		t.Fatalf("canonicalize directory: %v", err)
	}
	var nestedDirectoryData []byte
	if nested {
		nestedDirectoryData = directoryData
		directoryInput = fmt.Sprintf(`{"Entries":[{"Id":%q,"ModifiedAt":"2026-08-18T12:00:00Z","Name":"nested","Type":"Directory"}],"Type":"Directory","Version":1}`, directoryID)
		directoryData, directoryID, err = object.Canonicalize("directories", []byte(directoryInput))
		if err != nil {
			t.Fatalf("canonicalize nested root directory: %v", err)
		}
	}
	parentJSON := make([]string, len(parents))
	for i, parent := range parents {
		parentJSON[i] = fmt.Sprintf("%q", parent)
	}
	commitInput := fmt.Sprintf(`{"AuthorUserId":%q,"CreatedAt":%q,"DeviceId":%q,"Message":"gc fixture","Parents":[%s],"Root":%q,"Type":"Commit","Version":1}`,
		owner, now.UTC().Format("2006-01-02T15:04:05Z"), _gcTestDevice, strings.Join(parentJSON, ","), directoryID)
	commitData, commitID, err := object.Canonicalize("commits", []byte(commitInput))
	if err != nil {
		t.Fatalf("canonicalize commit: %v", err)
	}
	for kind, data := range map[string][]byte{
		"blocks": blockData, "files": fileData, "directories": directoryData, "commits": commitData,
	} {
		if _, err := store.PutObject(t.Context(), owner, libraryID, kind, object.ID(data), bytes.NewReader(data)); err != nil {
			t.Fatalf("PutObject(%s): %v", kind, err)
		}
	}
	if len(nestedDirectoryData) != 0 {
		if _, err := store.PutObject(t.Context(), owner, libraryID, "directories", object.ID(nestedDirectoryData), bytes.NewReader(nestedDirectoryData)); err != nil {
			t.Fatalf("PutObject(nested directory): %v", err)
		}
	}
	return gcTestGraph{owner: owner, blockData: bytes.Clone(blockData), fileData: fileData,
		directoryData: directoryData, nestedDirectoryData: nestedDirectoryData, commitData: commitData, commitID: commitID}
}

func gcTestObjectPath(dataDir, owner, libraryID, kind, id string) string {
	return filepath.Join(dataDir, _objectsName, owner, libraryID, kind, id[:2], id[2:])
}

func assertGCObjects(t *testing.T, dataDir string, objects map[string][]byte, wantPresent bool) {
	t.Helper()
	for kind, data := range objects {
		path := gcTestObjectPath(dataDir, _gcTestOwnerA, _gcTestLibrary, kind, object.ID(data))
		_, err := os.Stat(path)
		if wantPresent && err != nil {
			t.Errorf("stat retained %s object: %v", kind, err)
		}
		if !wantPresent && !errors.Is(err, os.ErrNotExist) {
			t.Errorf("stat collected %s object error = %v, want not exist", kind, err)
		}
	}
}

func assertAllGCHeadsReadable(t *testing.T, dataDir string) {
	t.Helper()
	store, err := OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenForServe after GC interruption: %v", err)
	}
	defer closeObjectStore(t, store)
	for _, owner := range []string{_gcTestOwnerA, _gcTestOwnerB} {
		library, err := store.GetLibrary(t.Context(), owner, _gcTestLibrary)
		if err != nil || library.HeadCommitID == nil {
			t.Fatalf("GetLibrary(%s) after GC interruption = %+v, %v", owner, library, err)
		}
		assertGCCommitGraphReadable(t, store, owner, *library.HeadCommitID)
	}
}

func assertGCCommitGraphReadable(t *testing.T, store *Store, owner, head string) {
	t.Helper()
	commits := []string{head}
	seenCommits := make(map[string]struct{})
	seenDirectories := make(map[string]struct{})
	seenFiles := make(map[string]struct{})
	for len(commits) > 0 {
		id := commits[len(commits)-1]
		commits = commits[:len(commits)-1]
		if _, seen := seenCommits[id]; seen {
			continue
		}
		seenCommits[id] = struct{}{}
		data := readGCObject(t, store, owner, "commits", id)
		commit, err := object.VerifyCommit(data, id)
		if err != nil {
			t.Fatalf("VerifyCommit(%s): %v", id, err)
		}
		commits = append(commits, commit.Parents...)
		directories := []string{commit.Root}
		for len(directories) > 0 {
			directoryID := directories[len(directories)-1]
			directories = directories[:len(directories)-1]
			if _, seen := seenDirectories[directoryID]; seen {
				continue
			}
			seenDirectories[directoryID] = struct{}{}
			directoryData := readGCObject(t, store, owner, "directories", directoryID)
			directory, err := object.VerifyDirectory(directoryData, directoryID)
			if err != nil {
				t.Fatalf("VerifyDirectory(%s): %v", directoryID, err)
			}
			for _, entry := range directory.Entries {
				if entry.Type == "Directory" {
					directories = append(directories, entry.ID)
					continue
				}
				if _, seen := seenFiles[entry.ID]; seen {
					continue
				}
				seenFiles[entry.ID] = struct{}{}
				fileData := readGCObject(t, store, owner, "files", entry.ID)
				file, err := object.VerifyFile(fileData, entry.ID)
				if err != nil {
					t.Fatalf("VerifyFile(%s): %v", entry.ID, err)
				}
				for _, blockID := range file.Blocks {
					block := readGCObject(t, store, owner, "blocks", blockID)
					if object.ID(block) != blockID {
						t.Fatalf("block %s digest mismatch", blockID)
					}
				}
			}
		}
	}
}

func readGCObject(t *testing.T, store *Store, owner, kind, id string) []byte {
	t.Helper()
	reader, _, err := store.GetObject(t.Context(), owner, _gcTestLibrary, kind, id)
	if err != nil {
		t.Fatalf("GetObject(%s, %s): %v", kind, id, err)
	}
	data, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		t.Fatalf("read %s object %s: %v", kind, id, err)
	}
	return data
}

func assertPreservedGCObjects(t *testing.T, preserved map[string][]byte) {
	t.Helper()
	for path, want := range preserved {
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, want) {
			t.Errorf("preserved object %s = %d bytes, %v; want %d unchanged", filepath.Base(path), len(got), err, len(want))
		}
	}
}
