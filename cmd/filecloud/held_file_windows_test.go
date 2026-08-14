//go:build windows

package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
)

// Windows does not support exec.Cmd.ExtraFiles. Reopen the tracked pathname in
// the helper before the journal operation; Go opens it with delete sharing, so
// the test still exercises an old handle across the subsequent rename.
func attachTestHeldFile(*exec.Cmd, *os.File) {}

func openTestHeldConflictFile() (*os.File, error) {
	worktree, relative := os.Getenv("FILECLOUD_PUBLIC_CRASH_WORKTREE"), os.Getenv("FILECLOUD_PUBLIC_MUTATION_PATH")
	if worktree == "" || relative == "" {
		return nil, errors.New("Windows held conflict path is absent")
	}
	return os.OpenFile(filepath.Join(worktree, filepath.FromSlash(relative)), os.O_RDWR, 0)
}
