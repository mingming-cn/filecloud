//go:build !windows

package storage

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
)

func killObjectTestProcess() error {
	if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
		return err
	}
	select {}
}

func assertObjectPublicationSIGKILL(t *testing.T, err error) {
	t.Helper()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("object publication process was not killed: %v", err)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("object publication process status=%v err=%v", status, err)
	}
}
