package main

import (
	"fmt"
	"strings"
)

func validateSupportedFilesystemPolicy(platform, filesystem string, local bool) error {
	filesystem = strings.ToLower(filesystem)
	supported := platform == "linux" && filesystem == "ext4" && local ||
		platform == "darwin" && filesystem == "apfs" && local ||
		platform == "windows" && filesystem == "ntfs" && local
	if supported {
		return nil
	}
	return fmt.Errorf("unsupported worktree filesystem %q on %s (local=%t); supported worktrees require local linux/ext4, macos/apfs, or windows/ntfs; nfs, smb, fat/exfat, and network drives are not supported",
		filesystem, platform, local)
}
