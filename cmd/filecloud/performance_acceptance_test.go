package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mingming-cn/filecloud/internal/acceptance"
	"github.com/mingming-cn/filecloud/internal/auth"
	libraryapi "github.com/mingming-cn/filecloud/internal/library"
	"github.com/mingming-cn/filecloud/internal/object"
	"github.com/mingming-cn/filecloud/internal/storage"
)

const (
	_performancePrefix     = "FILECLOUD_PERFORMANCE "
	_performanceSmallFiles = 10000
	_performanceSmallBytes = 4 << 10
	_performanceLargeBytes = int64(10 << 30)
)

type performanceResult struct {
	Scenario               string `json:"scenario"`
	ElapsedNanoseconds     int64  `json:"elapsedNanoseconds,omitempty"`
	PeakHeapBytes          uint64 `json:"peakHeapBytes,omitempty"`
	Files                  int    `json:"files,omitempty"`
	FileBytes              int64  `json:"fileBytes,omitempty"`
	IncrementalObjectBytes int64  `json:"incrementalObjectBytes,omitempty"`
	HeadVerifyNanoseconds  int64  `json:"headVerifyNanoseconds,omitempty"`
}

func TestPerformanceBaselineSmallFiles(t *testing.T) {
	requirePerformanceAcceptance(t)
	worktree := t.TempDir()
	data := make([]byte, _performanceSmallBytes)
	for index := range _performanceSmallFiles {
		path := filepath.Join(worktree, fmt.Sprintf("file-%05d", index))
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("create small-file fixture %d: %v", index, err)
		}
	}

	root := openPerformanceWorktree(t, worktree)
	defer root.Close()
	var snapshot worktreeSnapshot
	peak, elapsed, err := acceptance.MeasurePeakHeap(func() error {
		var scanErr error
		snapshot, scanErr = scanWorktree(root)
		return scanErr
	})
	if err != nil {
		t.Fatalf("scan 10000 small files: %v", err)
	}
	if len(snapshot.paths) != _performanceSmallFiles {
		t.Fatalf("scanned paths = %d, want %d", len(snapshot.paths), _performanceSmallFiles)
	}
	emitPerformanceResult(t, performanceResult{
		Scenario: "scan-10000-4kib-files", ElapsedNanoseconds: elapsed.Nanoseconds(),
		PeakHeapBytes: peak, Files: _performanceSmallFiles, FileBytes: _performanceSmallBytes,
	})
}

func TestPerformanceBaselineLargeFile(t *testing.T) {
	requirePerformanceAcceptance(t)
	usePerformanceTempRoot(t)
	worktree := t.TempDir()
	path := filepath.Join(worktree, "large.bin")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(_performanceLargeBytes); err != nil {
		t.Fatalf("create sparse 10 GiB fixture: %v", err)
	}
	var marker [8]byte
	for index := range int(_performanceLargeBytes / object.MaxBlockSize) {
		binary.LittleEndian.PutUint64(marker[:], uint64(index+1))
		if _, err := file.WriteAt(marker[:], int64(index)*object.MaxBlockSize); err != nil {
			t.Fatalf("make 10 GiB fixture block %d unique: %v", index, err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	root := openPerformanceWorktree(t, worktree)
	defer root.Close()
	started := time.Now()
	initial, err := scanWorktree(root)
	if err != nil {
		t.Fatalf("scan 10 GiB file: %v", err)
	}
	scanElapsed := time.Since(started)

	file, err = os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte("filecloud incremental baseline"), object.MaxBlockSize+4096); err != nil {
		t.Fatalf("modify 10 GiB fixture: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	modified, err := scanWorktree(root)
	if err != nil {
		t.Fatalf("rescan modified 10 GiB file: %v", err)
	}
	incrementalBytes := changedSnapshotBytes(initial, modified)
	headElapsed, commitBytes := measureSnapshotHeadValidation(t, root, modified)
	incrementalBytes += commitBytes
	if incrementalBytes <= object.MaxBlockSize || incrementalBytes >= object.MaxBlockSize+(1<<20) {
		t.Fatalf("incremental object bytes = %d, want one Block plus bounded metadata", incrementalBytes)
	}

	emitPerformanceResult(t, performanceResult{
		Scenario: "scan-10gib-file", ElapsedNanoseconds: scanElapsed.Nanoseconds(),
		FileBytes: _performanceLargeBytes, IncrementalObjectBytes: incrementalBytes,
		HeadVerifyNanoseconds: headElapsed.Nanoseconds(),
	})
}

func TestPerformanceBaselineKDF(t *testing.T) {
	requirePerformanceAcceptance(t)
	peak, elapsed, err := acceptance.MeasurePeakHeap(func() error {
		_, hashErr := auth.HashPassword([]byte("deployment baseline password"), auth.DefaultParams(), bytes.NewReader(make([]byte, 16)))
		return hashErr
	})
	if err != nil {
		t.Fatalf("run default Argon2id KDF: %v", err)
	}
	emitPerformanceResult(t, performanceResult{
		Scenario: "argon2id-default", ElapsedNanoseconds: elapsed.Nanoseconds(), PeakHeapBytes: peak,
	})
}

func TestSafetyDefaultsMatch1CBaseline(t *testing.T) {
	params := auth.DefaultParams()
	if params.Memory != 64*1024 || params.Iterations != 3 || params.Parallelism != 2 ||
		_defaultGlobalKDFCapacity != 2 || _defaultSourceIPKDFCapacity != 1 || _defaultUsernameKDFCapacity != 1 {
		t.Fatalf("KDF defaults = %+v concurrency = %d/%d/%d", params,
			_defaultGlobalKDFCapacity, _defaultSourceIPKDFCapacity, _defaultUsernameKDFCapacity)
	}
	upload := storage.DefaultUploadConfig()
	if upload.GlobalConcurrency != 8 || upload.UserConcurrency != 2 || upload.RequestTimeout != time.Minute ||
		upload.BudgetBytes != 12<<30 || upload.BudgetWindow != time.Hour {
		t.Fatalf("upload defaults = %+v", upload)
	}
	head := libraryapi.DefaultHeadValidationConfig()
	if head.GlobalConcurrency != 2 || head.RequestTimeout != 2*time.Minute || head.MaxSnapshotDepth != 256 ||
		head.MaxTraversalContexts != 65536 || head.MaxCommitDepth != 1024 || head.MaxIntroducedCommits != 1024 ||
		head.MaxValidatedObjects != 2000000 {
		t.Fatalf("Head validation defaults = %+v", head)
	}
	if _requestReadPeriod != 30*time.Second || _shutdownPeriod != 5*time.Second {
		t.Fatalf("request/shutdown defaults = %s/%s", _requestReadPeriod, _shutdownPeriod)
	}
}

func requirePerformanceAcceptance(t *testing.T) {
	t.Helper()
	if os.Getenv("FILECLOUD_RUN_1C") != "1" {
		t.Skip("set FILECLOUD_RUN_1C=1 to run deployment performance baselines")
	}
}

func usePerformanceTempRoot(t *testing.T) {
	t.Helper()
	root, err := os.MkdirTemp(".", ".filecloud-performance-")
	if err != nil {
		t.Fatal(err)
	}
	root, err = filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", root)
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("remove performance temporary root: %v", err)
		}
	})
}

func openPerformanceWorktree(t *testing.T, path string) *openedWorktree {
	t.Helper()
	root, err := openWorktreeRoot(path, func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func changedSnapshotBytes(initial, modified worktreeSnapshot) int64 {
	var total int64
	for id, source := range modified.blocks {
		if _, exists := initial.blocks[id]; !exists {
			total += source.size
		}
	}
	initialObjects := make(map[string]bool, len(initial.objects))
	for _, value := range initial.objects {
		initialObjects[value.kind+"\x00"+value.id] = true
	}
	for _, value := range modified.objects {
		if !initialObjects[value.kind+"\x00"+value.id] {
			total += int64(len(value.data))
		}
	}
	return total
}

func measureSnapshotHeadValidation(t *testing.T, root *openedWorktree, snapshot worktreeSnapshot) (time.Duration, int64) {
	t.Helper()
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	for id, source := range snapshot.blocks {
		data, err := root.readBlock(source, id)
		if err != nil {
			t.Fatalf("read performance block: %v", err)
		}
		if _, err := environment.store.PutObject(t.Context(), testClientUserID, testClientLibraryID, "blocks", id, bytes.NewReader(data)); err != nil {
			t.Fatalf("put performance block: %v", err)
		}
	}
	for _, value := range snapshot.objects {
		if _, err := environment.store.PutObject(t.Context(), testClientUserID, testClientLibraryID, value.kind, value.id, bytes.NewReader(value.data)); err != nil {
			t.Fatalf("put performance %s: %v", value.kind, err)
		}
	}
	commitData, commitID, err := canonicalCommit(testClientUserID, testClientDeviceID, snapshot.root, []string{}, func() time.Time {
		return time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := environment.store.PutObject(t.Context(), testClientUserID, testClientLibraryID, "commits", commitID, bytes.NewReader(commitData)); err != nil {
		t.Fatalf("put performance commit: %v", err)
	}
	head, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	updated, conflict, err := updateRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID,
		[]byte(environment.token), head.ETag, commitID)
	if err != nil || conflict || updated.CommitID == nil || *updated.CommitID != commitID {
		t.Fatalf("validate performance Head = %+v conflict=%v err=%v", updated, conflict, err)
	}
	return time.Since(started), int64(len(commitData))
}

func emitPerformanceResult(t *testing.T, result performanceResult) {
	t.Helper()
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(_performancePrefix + string(data))
}
