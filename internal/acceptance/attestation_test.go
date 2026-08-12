package acceptance

import (
	"strings"
	"testing"
)

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
