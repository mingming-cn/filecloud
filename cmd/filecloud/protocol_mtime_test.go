package main

import (
	"strings"
	"testing"
	"time"
)

func TestCanonicalProtocolMtime(t *testing.T) {
	for _, test := range []struct {
		name  string
		value time.Time
		want  string
	}{
		{"UTC", time.Date(2026, 2, 3, 4, 5, 6, 987654321, time.UTC), "2026-02-03T04:05:06Z"},
		{"offset", time.Date(2026, 2, 3, 12, 5, 6, 1, time.FixedZone("+8", 8*60*60)), "2026-02-03T04:05:06Z"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := canonicalProtocolMtime(test.value)
			parsed, err := parseCanonicalProtocolMtime(got)
			if err != nil || got != test.want || canonicalProtocolMtime(parsed) != got {
				t.Fatalf("canonical=%q parsed=%v err=%v want=%q", got, parsed, err, test.want)
			}
		})
	}
	for _, invalid := range []string{"2026-02-03T04:05:06.1Z", "2026-02-03T04:05:06+00:00", "2026-2-3T04:05:06Z"} {
		if _, err := parseCanonicalProtocolMtime(invalid); err == nil {
			t.Fatalf("noncanonical mtime %q accepted", invalid)
		}
	}
}

func TestCanonicalProtocolObjectIDs(t *testing.T) {
	directoryData, directoryID, err := canonicalDirectory("", []scanEntry{{name: "fixed.txt", kind: "File",
		id: strings.Repeat("a", 64), modified: canonicalProtocolMtime(time.Date(2026, 2, 3, 12, 5, 6, 900_000_000,
			time.FixedZone("+8", 8*60*60)))}})
	if err != nil {
		t.Fatal(err)
	}
	commitData, commitID, err := canonicalCommit(testClientUserID, testClientDeviceID, directoryID,
		[]string{strings.Repeat("b", 64)}, func() time.Time {
			return time.Date(2026, 2, 3, 4, 5, 7, 800_000_000, time.UTC)
		})
	if err != nil {
		t.Fatal(err)
	}
	const wantDirectoryID = "b53e9e0a56bec96806fb3df7c03b9ac609514a640f4959ff5e1d40b210049b39"
	const wantCommitID = "9fdc5eae97573252943f980fdeea40022078064e3c977d2d194561b8fbb0002b"
	if directoryID != wantDirectoryID || commitID != wantCommitID {
		t.Fatalf("fixed ids directory=%s commit=%s bytes=%q/%q", directoryID, commitID, directoryData, commitData)
	}
}
