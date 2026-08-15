package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mingming-cn/filecloud/internal/object"
	"github.com/mingming-cn/filecloud/internal/storage"
)

func TestRunGarbageCollectionDryRunAndDeleteMatch(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := storage.Init(t.Context(), dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	content := []byte("private orphan content")
	id := object.ID(content)
	path := filepath.Join(dataDir, "objects", "owner", "library", "blocks", id[:2], id[2:])
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("MkdirAll object path: %v", err)
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("WriteFile orphan: %v", err)
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatalf("Chtimes orphan: %v", err)
	}

	args := []string{"gc", "--data-dir", dataDir, "--grace-period", "24h", "--dry-run"}
	var dryOutput bytes.Buffer
	if err := run(t.Context(), args, strings.NewReader(""), &dryOutput, io.Discard); err != nil {
		t.Fatalf("run gc --dry-run: %v", err)
	}
	wantOutput := fmt.Sprintf("blocks objects=1 bytes=%d\nfiles objects=0 bytes=0\ndirectories objects=0 bytes=0\ncommits objects=0 bytes=0\n", len(content))
	if dryOutput.String() != wantOutput {
		t.Fatalf("dry-run output = %q, want %q", dryOutput.String(), wantOutput)
	}
	if strings.Contains(dryOutput.String(), string(content)) || strings.Contains(dryOutput.String(), id) {
		t.Fatalf("dry-run output exposed object data or identity: %q", dryOutput.String())
	}
	if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, content) {
		t.Fatalf("dry-run object = %q, %v; want unchanged", got, err)
	}

	var actualOutput bytes.Buffer
	args = []string{"gc", "--data-dir", dataDir, "--grace-period", "24h"}
	if err := run(t.Context(), args, strings.NewReader(""), &actualOutput, io.Discard); err != nil {
		t.Fatalf("run gc: %v", err)
	}
	if actualOutput.String() != dryOutput.String() {
		t.Fatalf("actual output = %q, want dry-run %q", actualOutput.String(), dryOutput.String())
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("collected object Stat error = %v, want not exist", err)
	}
}

func TestRunGarbageCollectionArgumentsAndLockExclusion(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := storage.Init(t.Context(), dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	for _, test := range []struct {
		args []string
		want string
	}{
		{args: []string{"gc"}, want: "usage: filecloud gc"},
		{args: []string{"gc", "--data-dir", dataDir, "--grace-period", "23h59m59s"}, want: "gc grace period must be at least 24h0m0s"},
		{args: []string{"gc", "--data-dir", dataDir, "extra"}, want: "usage: filecloud gc"},
	} {
		err := run(t.Context(), test.args, strings.NewReader(""), io.Discard, io.Discard)
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("run(%q) error = %v, want %q", test.args, err, test.want)
		}
	}

	serving, err := storage.OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenForServe: %v", err)
	}
	for _, dryRun := range []bool{false, true} {
		args := []string{"gc", "--data-dir", dataDir}
		if dryRun {
			args = append(args, "--dry-run")
		}
		err := run(t.Context(), args, strings.NewReader(""), io.Discard, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "locked by another process") {
			t.Errorf("run(%q) error = %v, want lock conflict", args, err)
		}
	}
	if err := serving.Close(); err != nil {
		t.Fatalf("close serving store: %v", err)
	}

	collector, err := storage.OpenGarbageCollector(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenGarbageCollector: %v", err)
	}
	serveErr := run(t.Context(), []string{"serve", "--data-dir", dataDir, "--listen", "127.0.0.1:0"}, strings.NewReader(""), io.Discard, io.Discard)
	if serveErr == nil || !strings.Contains(serveErr.Error(), "locked by another process") {
		t.Fatalf("serve while GC locked error = %v, want lock conflict", serveErr)
	}
	if err := collector.Close(); err != nil {
		t.Fatalf("close collector: %v", err)
	}
}
