package acceptance

import (
	"strings"
	"testing"
)

func TestActivePlatformRequiresExplicitGate(t *testing.T) {
	t.Setenv("FILECLOUD_RUN_1A", "")
	t.Setenv("FILECLOUD_RUN_1B_APFS", "")
	if platform, filesystem, enabled := ActivePlatform(); enabled || platform != "" || filesystem != "" {
		t.Fatalf("inactive platform = %q/%q enabled=%t", platform, filesystem, enabled)
	}
	t.Setenv("FILECLOUD_RUN_1B_APFS", "1")
	if platform, filesystem, enabled := ActivePlatform(); !enabled || platform != "darwin" || filesystem != "apfs" {
		t.Fatalf("APFS platform = %q/%q enabled=%t", platform, filesystem, enabled)
	}
	t.Setenv("FILECLOUD_RUN_1B_APFS", "")
	t.Setenv("FILECLOUD_RUN_1B_NTFS", "1")
	if platform, filesystem, enabled := ActivePlatform(); !enabled || platform != "windows" || filesystem != "ntfs" {
		t.Fatalf("NTFS platform = %q/%q enabled=%t", platform, filesystem, enabled)
	}
}

func TestEncodeUsesSharedPrefixAndSchema(t *testing.T) {
	line, err := Encode(Attestation{Kind: "server-readability", Scenario: "point", Platform: "linux", Filesystem: "ext4"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(line, Prefix+`{"kind":"server-readability","scenario":"point"`) ||
		!strings.Contains(line, `"unregisteredInternalPaths":0`) ||
		!strings.Contains(line, `"residualJournalRows":0`) {
		t.Fatalf("encoded attestation=%q", line)
	}
}
