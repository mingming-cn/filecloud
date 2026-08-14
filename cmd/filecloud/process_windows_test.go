//go:build windows

package main

import (
	"os"
	"testing"

	"golang.org/x/sys/windows"
)

var testCrossDevice = error(windows.ERROR_NOT_SAME_DEVICE)

// Windows does not report SIGKILL. Process.Kill terminates the child and the
// platform fault tests only require that the process did not reach a normal
// successful exit before restart recovery is exercised.
func assertProcessSIGKILL(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("process completed instead of being killed")
	}
}

func killTestProcess() error {
	process, err := os.FindProcess(os.Getpid())
	if err != nil {
		return err
	}
	return process.Kill()
}
