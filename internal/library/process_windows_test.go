//go:build windows

package library

import (
	"os"
	"testing"
)

func killHeadTestProcess() error {
	process, err := os.FindProcess(os.Getpid())
	if err != nil {
		return err
	}
	return process.Kill()
}

func assertHeadProcessSIGKILL(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("Head process completed instead of being killed")
	}
}
