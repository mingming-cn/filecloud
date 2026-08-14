//go:build linux || darwin

package main

import (
	"errors"
	"os"
	"os/exec"
)

func attachTestHeldFile(command *exec.Cmd, file *os.File) { command.ExtraFiles = []*os.File{file} }

func openTestHeldConflictFile() (*os.File, error) {
	file := os.NewFile(3, "held-conflict")
	if file == nil {
		return nil, errors.New("inherited conflict descriptor is absent")
	}
	return file, nil
}
