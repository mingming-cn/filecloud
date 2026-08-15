//go:build linux || darwin

package main

import (
	"os"
	"syscall"
)

func testHasFIFO() bool                { return true }
func createTestFIFO(path string) error { return syscall.Mkfifo(path, 0o600) }
func isTestFIFO(info os.FileInfo) bool { return info.Mode()&os.ModeNamedPipe != 0 }
