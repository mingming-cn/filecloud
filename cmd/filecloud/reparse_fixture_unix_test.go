//go:build !windows

package main

import "os"

func testCanRenameReparsePoint() bool     { return true }
func testCanTraverseSymlinkAlias() bool   { return true }
func isTestReparse(info os.FileInfo) bool { return info.Mode()&os.ModeSymlink != 0 }

func createTestSymlink(target, link string) error {
	return os.Symlink(target, link)
}
