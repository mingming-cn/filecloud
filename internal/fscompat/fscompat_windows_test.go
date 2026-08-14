//go:build windows

package fscompat

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestNormalizeErrorMapsNTStatus(t *testing.T) {
	for _, test := range []struct {
		name   string
		status windows.NTStatus
		want   error
	}{
		{name: "not found", status: windows.STATUS_OBJECT_NAME_NOT_FOUND, want: windows.ERROR_FILE_NOT_FOUND},
		{name: "collision", status: windows.STATUS_OBJECT_NAME_COLLISION, want: windows.ERROR_FILE_EXISTS},
		{name: "sharing violation", status: windows.STATUS_SHARING_VIOLATION, want: windows.ERROR_SHARING_VIOLATION},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := NormalizeError(test.status); !errors.Is(got, test.want) {
				t.Fatalf("NormalizeError(%#x) = %v, want %v", uint32(test.status), got, test.want)
			}
		})
	}
}

func TestOpenDirectoryEnumerationStartsFresh(t *testing.T) {
	path := t.TempDir()
	if err := os.WriteFile(filepath.Join(path, "first"), []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "second"), []byte("second"), 0o600); err != nil {
		t.Fatal(err)
	}
	fd, err := Open(path, O_RDONLY|O_DIRECTORY|O_NOFOLLOW, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer Close(fd)
	for attempt := 0; attempt < 2; attempt++ {
		directory, err := OpenDirectoryEnumeration(fd, path)
		if err != nil {
			t.Fatal(err)
		}
		names, readErr := directory.Readdirnames(-1)
		closeErr := directory.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("read fresh directory attempt %d: %v", attempt, errors.Join(readErr, closeErr))
		}
		found := map[string]bool{}
		for _, name := range names {
			found[name] = true
		}
		if !found["first"] || !found["second"] {
			t.Fatalf("fresh directory attempt %d names=%v", attempt, names)
		}
	}
}
