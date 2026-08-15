//go:build windows

package main

import "os"

// Windows has no filesystem FIFO. A reparse point exercises the identical
// scanner and checkout rejection boundary without requiring developer-mode
// named-pipe privileges.
func testHasFIFO() bool                { return false }
func createTestFIFO(path string) error { return createTestSymlink("missing", path) }
func isTestFIFO(info os.FileInfo) bool { return isTestReparse(info) }
