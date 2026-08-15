package main

import (
	"strings"
	"testing"
)

func TestValidateSupportedFilesystemPolicy(t *testing.T) {
	for _, test := range []struct {
		platform   string
		filesystem string
		local      bool
		wantOK     bool
	}{
		{platform: "linux", filesystem: "ext4", local: true, wantOK: true},
		{platform: "darwin", filesystem: "apfs", local: true, wantOK: true},
		{platform: "windows", filesystem: "ntfs", local: true, wantOK: true},
		{platform: "linux", filesystem: "nfs", local: false},
		{platform: "linux", filesystem: "cifs", local: false},
		{platform: "darwin", filesystem: "smbfs", local: false},
		{platform: "windows", filesystem: "fat", local: true},
		{platform: "windows", filesystem: "exfat", local: true},
		{platform: "windows", filesystem: "ntfs", local: false},
	} {
		err := validateSupportedFilesystemPolicy(test.platform, test.filesystem, test.local)
		if test.wantOK {
			if err != nil {
				t.Fatalf("validateSupportedFilesystemPolicy(%q, %q, %t) error=%v", test.platform, test.filesystem, test.local, err)
			}
			continue
		}
		if err == nil {
			t.Fatalf("validateSupportedFilesystemPolicy(%q, %q, %t) succeeded", test.platform, test.filesystem, test.local)
		}
		message := strings.ToLower(err.Error())
		for _, boundary := range []string{"nfs", "smb", "fat/exfat", "network"} {
			if !strings.Contains(message, boundary) {
				t.Fatalf("unsupported filesystem error %q does not name %q boundary", err, boundary)
			}
		}
	}
}
