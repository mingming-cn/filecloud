//go:build !windows

package main

import "os"

func testModeMatches(info os.FileInfo, want os.FileMode) bool {
	return info.Mode().Perm() == want
}
