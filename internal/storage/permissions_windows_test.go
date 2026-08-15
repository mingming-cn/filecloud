//go:build windows

package storage

import "os"

// Windows reports synthesized POSIX mode bits; NTFS access control is defined
// by the inherited DACL and cannot be verified with os.FileMode.
func testModeMatches(os.FileInfo, os.FileMode) bool {
	return true
}
