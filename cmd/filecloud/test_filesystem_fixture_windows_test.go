//go:build windows

package main

import "os"

// Windows has no filesystem FIFO. A reparse point exercises the identical
// scanner and checkout rejection boundary without requiring developer-mode
// named-pipe privileges.
func createTestFIFO(path string) error { return os.Symlink("missing", path) }
func isTestFIFO(info os.FileInfo) bool { return info.Mode()&os.ModeSymlink != 0 }
