//go:build linux || darwin

package main

import (
	"errors"
	"os"
	"os/exec"
)

func attachTestHeldFile(command *exec.Cmd, file *os.File) { command.ExtraFiles = []*os.File{file} }

func testCanInheritHeldFile() bool                   { return true }
func testCanRenameDirectoryWithHeldDescendant() bool { return true }

func openTestHeldFile(path string, write bool) (*os.File, error) {
	flag := os.O_RDONLY
	if write {
		flag = os.O_RDWR
	}
	return os.OpenFile(path, flag, 0)
}

func openTestHeldConflictFile() (*os.File, error) {
	file := os.NewFile(3, "held-conflict")
	if file == nil {
		return nil, errors.New("inherited conflict descriptor is absent")
	}
	return file, nil
}
