package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestScanRegularFileRetriesConcurrentRewrite(t *testing.T) {
	root, path := newScannerTestRoot(t, map[string]string{"file": "before"})
	changed := false
	snapshot, err := scanWorktreeWithConfig(root, worktreeScanConfig{fault: func(event scanFault) error {
		if event.phase == "after-file-read-1" && event.path == "file" && !changed {
			changed = true
			return os.WriteFile(path("file"), []byte("after!"), 0o600)
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !changed || snapshot.root != scanScannerTestRoot(t, root) {
		t.Fatalf("retry did not produce the final stable snapshot: changed=%v root=%s", changed, snapshot.root)
	}
}

func TestScanRegularFileRetriesTruncateDuringRead(t *testing.T) {
	root, path := newScannerTestRoot(t, map[string]string{"file": "long-content"})
	changed := false
	snapshot, err := scanWorktreeWithConfig(root, worktreeScanConfig{fault: func(event scanFault) error {
		if event.phase == "after-file-read-1" && event.path == "file" && !changed {
			changed = true
			return os.Truncate(path("file"), 3)
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !changed || snapshot.root != scanScannerTestRoot(t, root) {
		t.Fatalf("truncate retry did not produce stable snapshot: changed=%v root=%s", changed, snapshot.root)
	}
}

func TestScanRegularFileContinuousRewriteExhaustsBudget(t *testing.T) {
	root, path := newScannerTestRoot(t, map[string]string{"file": "aaaa"})
	mutations := 0
	_, err := scanWorktreeWithConfig(root, worktreeScanConfig{fault: func(event scanFault) error {
		if event.phase == "after-file-read-1" && event.path == "file" {
			mutations++
			return os.WriteFile(path("file"), []byte(strings.Repeat(string(rune('a'+mutations%2)), 4)), 0o600)
		}
		return nil
	}})
	var unstable *unstableWorktreeError
	if !errors.As(err, &unstable) || mutations != scanRetryBudget || !strings.Contains(err.Error(), "scan attempts") {
		t.Fatalf("continuous rewrite error=%v mutations=%d", err, mutations)
	}
}

func TestScanDirectoryEnumerationChangeFailsRound(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(func(string) string) error
	}{
		{"create", func(path func(string) string) error { return os.WriteFile(path("new"), []byte("new"), 0o600) }},
		{"delete", func(path func(string) string) error { return os.Remove(path("file")) }},
		{"rename", func(path func(string) string) error { return os.Rename(path("file"), path("renamed")) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			root, path := newScannerTestRoot(t, map[string]string{"file": "data"})
			changed := false
			_, err := scanWorktreeWithConfig(root, worktreeScanConfig{fault: func(event scanFault) error {
				if event.phase == "after-directory-enumeration-1" && event.path == "" && !changed {
					changed = true
					return test.mutate(path)
				}
				return nil
			}})
			var unstable *unstableWorktreeError
			if !errors.As(err, &unstable) || !strings.Contains(err.Error(), "between enumerations") {
				t.Fatalf("directory change error=%v", err)
			}
		})
	}
}

func TestScanInodeReplacementDuringReadFailsRound(t *testing.T) {
	root, path := newScannerTestRoot(t, map[string]string{"file": "old"})
	changed := false
	_, err := scanWorktreeWithConfig(root, worktreeScanConfig{fault: func(event scanFault) error {
		if event.phase != "after-file-read-1" || event.path != "file" || changed {
			return nil
		}
		changed = true
		if err := os.Rename(path("file"), path("old")); err != nil {
			return err
		}
		return os.WriteFile(path("file"), []byte("new"), 0o600)
	}})
	var unstable *unstableWorktreeError
	if !errors.As(err, &unstable) {
		t.Fatalf("inode replacement error=%v", err)
	}
}

func TestScanFinalValidationCatchesEarlierFileChange(t *testing.T) {
	root, path := newScannerTestRoot(t, map[string]string{"a": "aaaa", "z": "zzzz"})
	changed := false
	_, err := scanWorktreeWithConfig(root, worktreeScanConfig{fault: func(event scanFault) error {
		if event.phase == "after-file-scan" && event.path == "a" && !changed {
			changed = true
			return os.WriteFile(path("a"), []byte("bbbb"), 0o600)
		}
		return nil
	}})
	var unstable *unstableWorktreeError
	if !errors.As(err, &unstable) || !strings.Contains(err.Error(), "final validation") {
		t.Fatalf("late file change error=%v", err)
	}
}

func TestScanFinalValidationUsesCTimeWhenMTimeRestored(t *testing.T) {
	root, path := newScannerTestRoot(t, map[string]string{"file": "aaaa"})
	info, err := os.Stat(path("file"))
	if err != nil {
		t.Fatal(err)
	}
	changed := false
	_, err = scanWorktreeWithConfig(root, worktreeScanConfig{fault: func(event scanFault) error {
		if event.phase != "before-final-validation" || changed {
			return nil
		}
		changed = true
		if err := os.WriteFile(path("file"), []byte("bbbb"), 0o600); err != nil {
			return err
		}
		return os.Chtimes(path("file"), info.ModTime(), info.ModTime())
	}})
	var unstable *unstableWorktreeError
	if !errors.As(err, &unstable) || !strings.Contains(err.Error(), "final validation") {
		t.Fatalf("mtime-restored rewrite error=%v", err)
	}
}

func TestScanCanonicalMtimeIgnoresSubsecondPrecisionButNotContent(t *testing.T) {
	root, path := newScannerTestRoot(t, map[string]string{"file": "aaaa"})
	second := time.Date(2026, 2, 3, 4, 5, 6, 100_000_000, time.UTC)
	if err := os.Chtimes(path("file"), second, second); err != nil {
		t.Fatal(err)
	}
	first, err := scanWorktree(root)
	if err != nil {
		t.Fatal(err)
	}
	laterNano := second.Add(800 * time.Millisecond)
	if err := os.Chtimes(path("file"), laterNano, laterNano); err != nil {
		t.Fatal(err)
	}
	secondScan, err := scanWorktree(root)
	if err != nil {
		t.Fatal(err)
	}
	if first.root != secondScan.root || len(first.objects) != len(secondScan.objects) {
		t.Fatalf("subsecond-only change root=%s/%s objects=%d/%d", first.root, secondScan.root,
			len(first.objects), len(secondScan.objects))
	}
	if err := os.WriteFile(path("file"), []byte("bbbb"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path("file"), laterNano, laterNano); err != nil {
		t.Fatal(err)
	}
	changed, err := scanWorktree(root)
	if err != nil {
		t.Fatal(err)
	}
	if changed.root == secondScan.root || changed.paths[0].id == secondScan.paths[0].id || changed.paths[0].mtime != secondScan.paths[0].mtime {
		t.Fatalf("content-preserving-mtime root=%s/%s file=%s/%s mtime=%s/%s", changed.root, secondScan.root,
			changed.paths[0].id, secondScan.paths[0].id, changed.paths[0].mtime, secondScan.paths[0].mtime)
	}
}

func TestScanTrackedSymlinkIsFatal(t *testing.T) {
	root, path := newScannerTestRoot(t, nil)
	if err := os.Symlink("target", path("tracked")); err != nil {
		t.Fatal(err)
	}
	_, err := scanWorktreeWithConfig(root, worktreeScanConfig{trackedPaths: map[string]bool{"tracked": true},
		ignoreUntrackedUnsupported: true})
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("tracked symlink error=%v", err)
	}
}

func TestScanIgnoresUntrackedFIFOWithWarning(t *testing.T) {
	root, path := newScannerTestRoot(t, map[string]string{"file": "data"})
	if err := syscall.Mkfifo(path("pipe"), 0o600); err != nil {
		t.Fatal(err)
	}
	var warning bytes.Buffer
	snapshot, err := scanWorktreeWithConfig(root, worktreeScanConfig{ignoreUntrackedUnsupported: true, warning: &warning})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(warning.String(), "pipe") {
		t.Fatalf("warning=%q", warning.String())
	}
	for _, path := range snapshot.paths {
		if path.path == "pipe" {
			t.Fatal("FIFO was included in snapshot")
		}
	}
}

func TestInitialScanStillRejectsUnsupportedPath(t *testing.T) {
	root, path := newScannerTestRoot(t, nil)
	if err := syscall.Mkfifo(path("pipe"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := scanWorktree(root); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("initial scan error=%v", err)
	}
}

func newScannerTestRoot(t *testing.T, files map[string]string) (*openedWorktree, func(string) string) {
	t.Helper()
	directory := t.TempDir()
	path := func(relative string) string { return filepath.Join(directory, relative) }
	for name, content := range files {
		if err := os.WriteFile(path(name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	root, err := openWorktreeRoot(directory, func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Close() })
	return root, path
}

func scanScannerTestRoot(t *testing.T, root *openedWorktree) string {
	t.Helper()
	snapshot, err := scanWorktree(root)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot.root
}
