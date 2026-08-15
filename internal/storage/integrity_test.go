package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/mingming-cn/filecloud/internal/object"
)

func TestIntegrityCheckerVerifiesCompletePublishedHistoryReadOnly(t *testing.T) {
	dataDir, _, preserved := newGarbageCollectionFixture(t)
	beforeDatabase, err := os.ReadFile(filepath.Join(dataDir, _databaseName))
	if err != nil {
		t.Fatalf("read database before check: %v", err)
	}

	checker, err := OpenIntegrityChecker(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenIntegrityChecker: %v", err)
	}
	first, checkErr := checker.Check(t.Context())
	closeErr := checker.Close()
	if checkErr != nil || closeErr != nil {
		t.Fatalf("Check/Close = %v / %v", checkErr, closeErr)
	}
	if first.Libraries != 2 || first.Objects == 0 || len(first.Issues) != 0 {
		t.Fatalf("healthy report = %+v, want two clean libraries and checked objects", first)
	}

	checker, err = OpenIntegrityChecker(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("reopen IntegrityChecker: %v", err)
	}
	second, checkErr := checker.Check(t.Context())
	closeErr = checker.Close()
	if checkErr != nil || closeErr != nil {
		t.Fatalf("repeated Check/Close = %v / %v", checkErr, closeErr)
	}
	if !reflect.DeepEqual(second, first) {
		t.Fatalf("repeated report = %+v, want stable %+v", second, first)
	}

	afterDatabase, err := os.ReadFile(filepath.Join(dataDir, _databaseName))
	if err != nil || !bytes.Equal(afterDatabase, beforeDatabase) {
		t.Fatalf("database changed during check: read=%v equal=%v", err, bytes.Equal(afterDatabase, beforeDatabase))
	}
	assertPreservedGCObjects(t, preserved)
}

func TestIntegrityCheckerReportsMissingAndCorruptPublishedObjects(t *testing.T) {
	for _, objectType := range []string{"Block", "File", "Directory", "Commit"} {
		for _, state := range []string{"missing", "corrupt"} {
			t.Run(objectType+"/"+state, func(t *testing.T) {
				dataDir, _, preserved := newGarbageCollectionFixture(t)
				path, fullID := integrityFixtureObject(t, preserved, objectType)
				if state == "missing" {
					if err := os.Remove(path); err != nil {
						t.Fatalf("remove %s: %v", objectType, err)
					}
				} else if err := os.WriteFile(path, []byte("damaged private bytes"), 0o600); err != nil {
					t.Fatalf("damage %s: %v", objectType, err)
				}

				checker, err := OpenIntegrityChecker(t.Context(), dataDir)
				if err != nil {
					t.Fatalf("OpenIntegrityChecker: %v", err)
				}
				report, checkErr := checker.Check(t.Context())
				closeErr := checker.Close()
				if checkErr != nil || closeErr != nil {
					t.Fatalf("Check/Close = %v / %v; data damage should be reported", checkErr, closeErr)
				}

				issue, ok := findIntegrityIssue(report, objectType, state)
				if !ok {
					t.Fatalf("report = %+v, want %s %s issue", report, objectType, state)
				}
				if issue.LibraryID == "" || issue.OwnerUserID == "" {
					t.Fatalf("issue lacks library scope: %+v", issue)
				}
				if issue.ObjectID != fullID[:12]+"..." || strings.Contains(issue.ObjectID, fullID) {
					t.Fatalf("masked ObjectID = %q, want prefix only", issue.ObjectID)
				}
				if strings.Contains(issue.ObjectID, filepath.Base(path)) {
					t.Fatalf("issue exposed object storage leaf: %+v", issue)
				}
			})
		}
	}
}

func TestIntegrityCheckerTraversesCurrentRootAndBothParentHistories(t *testing.T) {
	for _, test := range []struct {
		name       string
		timestamp  string
		objectType string
	}{
		{name: "current Root", timestamp: "2026-08-18T12:00:02Z", objectType: "Directory"},
		{name: "ordinary parent", timestamp: "2026-08-18T12:00:00Z", objectType: "Commit"},
		{name: "second parent", timestamp: "2026-08-18T12:00:01Z", objectType: "Commit"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dataDir, _, preserved := newGarbageCollectionFixture(t)
			commitID, commit := integrityFixtureCommit(t, preserved, test.timestamp)
			id := commitID
			kind := "commits"
			if test.objectType == "Directory" {
				id = commit.Root
				kind = "directories"
			}
			path := gcTestObjectPath(dataDir, _gcTestOwnerA, _gcTestLibrary, kind, id)
			if err := os.Remove(path); err != nil {
				t.Fatalf("remove %s from %s: %v", test.objectType, test.name, err)
			}

			checker, err := OpenIntegrityChecker(t.Context(), dataDir)
			if err != nil {
				t.Fatalf("OpenIntegrityChecker: %v", err)
			}
			report, checkErr := checker.Check(t.Context())
			closeErr := checker.Close()
			if checkErr != nil || closeErr != nil {
				t.Fatalf("Check/Close = %v / %v", checkErr, closeErr)
			}
			issue, ok := findIntegrityIssue(report, test.objectType, "missing")
			if !ok || issue.ObjectID != id[:12]+"..." {
				t.Fatalf("report = %+v, want missing %s %s", report, test.objectType, id[:12]+"...")
			}
		})
	}
}

func TestIntegrityCheckerReportsFileSizeReferenceMismatch(t *testing.T) {
	dataDir, _, preserved := newGarbageCollectionFixture(t)
	blockPath, _ := integrityFixtureObject(t, preserved, "Block")
	blockData, err := os.ReadFile(blockPath)
	if err != nil {
		t.Fatalf("read block: %v", err)
	}
	if len(blockData) < 2 {
		t.Fatalf("fixture block is too small: %d", len(blockData))
	}
	if err := os.WriteFile(blockPath, blockData[:len(blockData)-1], 0o600); err != nil {
		t.Fatalf("truncate block: %v", err)
	}

	checker, err := OpenIntegrityChecker(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenIntegrityChecker: %v", err)
	}
	report, checkErr := checker.Check(t.Context())
	closeErr := checker.Close()
	if checkErr != nil || closeErr != nil {
		t.Fatalf("Check/Close = %v / %v", checkErr, closeErr)
	}
	if _, ok := findIntegrityIssue(report, "Block", "corrupt"); !ok {
		t.Fatalf("report = %+v, want corrupt Block", report)
	}
	if _, ok := findIntegrityIssue(report, "File", "corrupt"); !ok {
		t.Fatalf("report = %+v, want corrupt File reference size", report)
	}
}

func TestIntegrityCheckerRejectsOversizedMetadataWithoutReadingIt(t *testing.T) {
	dataDir, _, preserved := newGarbageCollectionFixture(t)
	filePath, _ := integrityFixtureObject(t, preserved, "File")
	if err := os.Truncate(filePath, object.MaxFileObjectSize+1); err != nil {
		t.Fatalf("oversize File object: %v", err)
	}

	checker, err := OpenIntegrityChecker(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenIntegrityChecker: %v", err)
	}
	report, checkErr := checker.Check(t.Context())
	closeErr := checker.Close()
	if checkErr != nil || closeErr != nil {
		t.Fatalf("Check/Close = %v / %v", checkErr, closeErr)
	}
	if _, ok := findIntegrityIssue(report, "File", "corrupt"); !ok {
		t.Fatalf("report = %+v, want oversized File corruption", report)
	}
}

func TestIntegrityCheckerDoesNotFollowObjectSymlinks(t *testing.T) {
	dataDir, _, preserved := newGarbageCollectionFixture(t)
	filePath, _ := integrityFixtureObject(t, preserved, "File")
	if err := os.Remove(filePath); err != nil {
		t.Fatalf("remove File object: %v", err)
	}
	if err := os.Symlink(filepath.Join(dataDir, _databaseName), filePath); err != nil {
		t.Skipf("create object symlink: %v", err)
	}

	checker, err := OpenIntegrityChecker(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenIntegrityChecker: %v", err)
	}
	report, checkErr := checker.Check(t.Context())
	closeErr := checker.Close()
	if checkErr != nil || closeErr != nil {
		t.Fatalf("Check/Close = %v / %v", checkErr, closeErr)
	}
	if _, ok := findIntegrityIssue(report, "File", "unreadable"); !ok {
		t.Fatalf("report = %+v, want symlink rejected as unreadable", report)
	}
}

func TestIntegrityCheckerRequiresExclusiveLock(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := Init(t.Context(), dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	serving, err := OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenForServe: %v", err)
	}
	if _, err := OpenIntegrityChecker(t.Context(), dataDir); err == nil || !strings.Contains(err.Error(), "locked by another process") {
		t.Fatalf("OpenIntegrityChecker while serving error = %v, want lock conflict", err)
	}
	if err := serving.Close(); err != nil {
		t.Fatalf("close serving store: %v", err)
	}

	checker, err := OpenIntegrityChecker(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenIntegrityChecker: %v", err)
	}
	if _, err := OpenGarbageCollector(t.Context(), dataDir); err == nil || !strings.Contains(err.Error(), "locked by another process") {
		t.Fatalf("OpenGarbageCollector while checking error = %v, want lock conflict", err)
	}
	if _, err := OpenIntegrityChecker(t.Context(), dataDir); err == nil || !strings.Contains(err.Error(), "locked by another process") {
		t.Fatalf("second OpenIntegrityChecker error = %v, want lock conflict", err)
	}
	if err := checker.Close(); err != nil {
		t.Fatalf("close checker: %v", err)
	}
}

func integrityFixtureObject(t *testing.T, preserved map[string][]byte, objectType string) (string, string) {
	t.Helper()
	kinds := map[string]string{
		"Block": "blocks", "File": "files", "Directory": "directories", "Commit": "commits",
	}
	kind := kinds[objectType]
	for path, data := range preserved {
		if kind == "blocks" && string(data) == "recent orphan" {
			continue
		}
		if strings.Contains(path, string(filepath.Separator)+kind+string(filepath.Separator)) {
			return path, object.ID(data)
		}
	}
	t.Fatalf("fixture has no %s object", objectType)
	return "", ""
}

func integrityFixtureCommit(t *testing.T, preserved map[string][]byte, timestamp string) (string, object.Commit) {
	t.Helper()
	for path, data := range preserved {
		if !strings.Contains(path, string(filepath.Separator)+"commits"+string(filepath.Separator)) {
			continue
		}
		id := object.ID(data)
		commit, err := object.VerifyCommit(data, id)
		if err == nil && commit.CreatedAt == timestamp && commit.AuthorUserID == _gcTestOwnerA {
			return id, commit
		}
	}
	t.Fatalf("fixture has no Commit at %s", timestamp)
	return "", object.Commit{}
}

func findIntegrityIssue(report IntegrityReport, objectType, state string) (IntegrityIssue, bool) {
	for _, issue := range report.Issues {
		if issue.ObjectType == objectType && issue.State == state {
			return issue, true
		}
	}
	return IntegrityIssue{}, false
}

func TestIntegrityCheckerRejectsUseAfterClose(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := Init(t.Context(), dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	checker, err := OpenIntegrityChecker(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenIntegrityChecker: %v", err)
	}
	if err := checker.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := checker.Check(t.Context()); err == nil || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Check after Close error = %v, want os.ErrClosed", err)
	}
}
