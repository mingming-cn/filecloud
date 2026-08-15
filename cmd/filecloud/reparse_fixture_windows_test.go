//go:build windows

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

func testCanRenameReparsePoint() bool   { return false }
func testCanTraverseSymlinkAlias() bool { return false }
func isTestReparse(info os.FileInfo) bool {
	data, ok := info.Sys().(*syscall.Win32FileAttributeData)
	return info.Mode()&os.ModeSymlink != 0 || ok && data.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0
}

// createTestSymlink uses an NTFS junction because creating a symbolic link can
// require developer mode or SeCreateSymbolicLinkPrivilege. Both are reparse
// points and must be rejected before traversal.
func createTestSymlink(target, link string) error {
	resolved := target
	if !filepath.IsAbs(resolved) {
		resolved = filepath.Join(filepath.Dir(link), resolved)
	}
	if info, err := os.Stat(resolved); err != nil || !info.IsDir() {
		resolved = filepath.Dir(link)
	}
	output, err := exec.Command("cmd.exe", "/c", "mklink", "/J", link, resolved).CombinedOutput()
	if err != nil {
		return fmt.Errorf("create NTFS junction: %w: %s", err, output)
	}
	return nil
}
