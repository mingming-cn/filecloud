//go:build !windows

package library

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
)

func killHeadTestProcess() error {
	return syscall.Kill(os.Getpid(), syscall.SIGKILL)
}

func assertHeadProcessSIGKILL(t *testing.T, err error) {
	t.Helper()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("Head process was not killed: %v", err)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("Head process status=%v err=%v", status, err)
	}
}
