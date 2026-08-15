//go:build !windows

package storage

import "os"

func testModeMatches(info os.FileInfo, want os.FileMode) bool {
	return info.Mode().Perm() == want
}
