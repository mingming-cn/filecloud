package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSelectedTargets(t *testing.T) {
	all, err := selectedTargets("")
	if err != nil || len(all) != 3 {
		t.Fatalf("selectedTargets(empty) = %v, %v; want three targets", all, err)
	}
	windows, err := selectedTargets("windows/amd64")
	if err != nil || len(windows) != 1 || windows[0] != (releaseTarget{goos: "windows", goarch: "amd64"}) {
		t.Fatalf("selectedTargets(windows/amd64) = %v, %v", windows, err)
	}
	if _, err := selectedTargets("linux/arm64"); err == nil {
		t.Fatal("selectedTargets(linux/arm64) succeeded, want unsupported target error")
	}
}

func TestGenerateTargetLicenseMaterial(t *testing.T) {
	output := filepath.Join(t.TempDir(), "licenses")
	if err := generate(output, "linux/amd64"); err != nil {
		t.Fatalf("generate linux/amd64 notices: %v", err)
	}
	notices, err := os.ReadFile(filepath.Join(output, "THIRD_PARTY_NOTICES.md"))
	if err != nil {
		t.Fatalf("read notices: %v", err)
	}
	if !strings.Contains(string(notices), "`linux/amd64`") || !strings.Contains(string(notices), "modernc.org/sqlite") {
		t.Fatalf("notice content = %q", notices)
	}
	licenses, err := os.ReadDir(filepath.Join(output, "third_party_licenses"))
	if err != nil {
		t.Fatalf("read copied licenses: %v", err)
	}
	if len(licenses) == 0 {
		t.Fatal("generated license directory is empty")
	}
}
