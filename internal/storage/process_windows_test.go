//go:build windows

package storage

import (
	"os"
	"testing"
)

func killObjectTestProcess() error {
	process, err := os.FindProcess(os.Getpid())
	if err != nil {
		return err
	}
	return process.Kill()
}

func assertObjectPublicationSIGKILL(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("object publication process completed instead of being killed")
	}
}
