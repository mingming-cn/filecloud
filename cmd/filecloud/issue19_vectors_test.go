package main

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mingming-cn/filecloud/internal/object"
)

func TestIssue19AttestedNormalConflictVectors(t *testing.T) {
	seed := object.Commit{AuthorUserID: testClientUserID, DeviceID: testClientDeviceID, CreatedAt: "2025-02-03T04:05:06Z"}
	want := map[int]string{
		1:   "界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界 (Filecloud conflict aaaaaaaa 20250203T040506Z).txt",
		2:   "界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界 (Filecloud conflict aaaaaaaa 20250203T040506Z) 2.txt",
		9:   "界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界 (Filecloud conflict aaaaaaaa 20250203T040506Z) 9.txt",
		10:  "界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界 (Filecloud conflict aaaaaaaa 20250203T040506Z) 10.txt",
		99:  "界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界 (Filecloud conflict aaaaaaaa 20250203T040506Z) 99.txt",
		100: "界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界 (Filecloud conflict aaaaaaaa 20250203T040506Z) 100.txt",
	}
	occupied := make([]string, 0, 100)
	entries := make([]scanEntry, 0, len(want))
	for ordinal := 1; ordinal <= 100; ordinal++ {
		name, err := _conflictCopyName(strings.Repeat("界", 80)+".txt", "", seed, occupied)
		if err != nil {
			t.Fatal(err)
		}
		occupied = append(occupied, name)
		if expected, ok := want[ordinal]; ok {
			if name != expected {
				t.Fatalf("ordinal %d name=%q want=%q", ordinal, name, expected)
			}
			entries = append(entries, scanEntry{name: name, kind: "File", id: fmt.Sprintf("%064x", ordinal), modified: "2026-02-03T04:05:06Z"})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
	_, directoryID, err := canonicalDirectory("", entries)
	if err != nil {
		t.Fatal(err)
	}
	commitData, commitID, err := canonicalCommit(testClientUserID, testClientDeviceID, directoryID,
		[]string{strings.Repeat("b", 64)}, func() time.Time { return time.Date(2026, 2, 3, 4, 5, 7, 0, time.UTC) })
	if err != nil {
		t.Fatal(err)
	}
	const wantDirectoryID = "ccd8e20f59ba49542bb39a4feb5aa8c32f8afeefeec66bef982e4187d81842cc"
	const wantCommit = `{"AuthorUserId":"01234567-89ab-4def-8123-456789abcdef","CreatedAt":"2026-02-03T04:05:07Z","DeviceId":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee","Message":"sync","Parents":["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"],"Root":"ccd8e20f59ba49542bb39a4feb5aa8c32f8afeefeec66bef982e4187d81842cc","Type":"Commit","Version":1}`
	const wantCommitID = "66a5d6cf323b308244b643766ef7e7bb8faa779b0904b5f064c5ebc706b47011"
	if directoryID != wantDirectoryID || string(commitData) != wantCommit || commitID != wantCommitID {
		t.Fatalf("fixed normal vector directory=%s commit=%q/%s", directoryID, commitData, commitID)
	}
}

func TestIssue19LongExtensionOrdinalOneFixedVector(t *testing.T) {
	seed := object.Commit{AuthorUserID: testClientUserID, DeviceID: testClientDeviceID, CreatedAt: "2025-02-03T04:05:06Z"}
	const wantName = "x (Filecloud conflict aaaaaaaa 20250203T040506Z).aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	name, err := _conflictCopyName("x."+strings.Repeat("a", 190), "", seed, nil)
	if err != nil || name != wantName {
		t.Fatalf("fixed long-extension name=%q err=%v", name, err)
	}
	_, directoryID, err := canonicalDirectory("", []scanEntry{{name: name, kind: "File", id: strings.Repeat("d", 64), modified: "2026-02-03T04:05:06Z"}})
	if err != nil {
		t.Fatal(err)
	}
	commitData, commitID, err := canonicalCommit(testClientUserID, testClientDeviceID, directoryID,
		[]string{strings.Repeat("e", 64)}, func() time.Time { return time.Date(2026, 2, 3, 4, 5, 9, 0, time.UTC) })
	if err != nil {
		t.Fatal(err)
	}
	const wantDirectoryID = "165d487d212140784566a279e3b3bb92b345338c5cad52bfadc2184e4f14ffed"
	const wantCommitID = "b8d525c45c363d22dc97d5a8214903df6ec867759a4b4ca98364e002fddcc5c9"
	if directoryID != wantDirectoryID || commitID != wantCommitID {
		t.Fatalf("fixed long-extension vector directory=%s commit=%q/%s", directoryID, commitData, commitID)
	}
}

func TestIssue19ReservedAndTrailingConflictNameBoundaries(t *testing.T) {
	seed := object.Commit{AuthorUserID: testClientUserID, DeviceID: testClientDeviceID, CreatedAt: "2025-02-03T04:05:06Z"}
	for _, leaf := range []string{strings.Repeat("n", 241), "CON", "name. ", "name."} {
		name, err := _conflictCopyName(leaf, "", seed, nil)
		if err != nil && !errors.Is(err, errConflictPathNeedsFallback) {
			t.Fatalf("leaf %q name=%q err=%v", leaf, name, err)
		}
		if err == nil && (len(name) > 240 || !validRecoveryVisibleName(name)) {
			t.Fatalf("leaf %q emitted invalid conflict name %q", leaf, name)
		}
	}
}

func TestIssue19FallbackSourceInsideConcurrentlyMergedRoot(t *testing.T) {
	emptyData, emptyID, err := canonicalEmptyDirectory()
	if err != nil {
		t.Fatal(err)
	}
	fallbackData, fallbackID, err := canonicalDirectory("Filecloud Conflicts", []scanEntry{{name: "remote", kind: "File",
		id: strings.Repeat("4", 64), modified: "2026-01-01T00:00:00Z"}})
	if err != nil {
		t.Fatal(err)
	}
	rootData, rootID, err := canonicalDirectory("", []scanEntry{{name: "Filecloud Conflicts", kind: "Directory",
		id: fallbackID, modified: "2026-01-01T00:00:00Z"}})
	if err != nil {
		t.Fatal(err)
	}
	lineage := map[string]_conflictPromotion{"Filecloud Conflicts/local": {source: "Filecloud Conflicts/local", target: "Filecloud Conflicts/local"}}
	merger := &_treeMerger{directories: map[string][]byte{emptyID: emptyData, fallbackID: fallbackData, rootID: rootData}, synthesized: make(map[string][]byte),
		active: make(map[string]bool), seen: make(map[string]bool), budget: _newReplayBudget(), localSeedID: strings.Repeat("5", 64), lineage: lineage,
		fallbacks: []_fallbackConflict{{source: "Filecloud Conflicts/local", leaf: "local", entry: object.DirectoryEntry{Name: "local", Type: "File",
			ID: strings.Repeat("6", 64), ModifiedAt: "2026-01-02T00:00:00Z"}}}}
	mergedID, err := merger.applyFallbacks(rootID)
	if err != nil {
		t.Fatal(err)
	}
	root, err := merger.loadDirectory(mergedID)
	if err != nil || len(root.Entries) != 1 || root.Entries[0].Name != "Filecloud Conflicts" {
		t.Fatalf("merged fallback root=%+v err=%v", root, err)
	}
	fallback, err := merger.loadDirectory(root.Entries[0].ID)
	if err != nil || len(fallback.Entries) != 2 || fallback.Entries[0].Name != "555555555555-local (Filecloud conflict 1)" ||
		fallback.Entries[1].Name != "remote" || lineage["Filecloud Conflicts/local"].target != "Filecloud Conflicts/555555555555-local (Filecloud conflict 1)" {
		t.Fatalf("merged fallback=%+v lineage=%+v err=%v", fallback, lineage, err)
	}
}

func TestIssue19AttestedFallbackVectors(t *testing.T) {
	const wantRoot = `{"Entries":[{"Id":"2ed3d5b84f7db1c1f72cf7a317f1c19de73f404e8c25d0c482f2809503355bf6","ModifiedAt":"2026-01-01T00:00:00Z","Name":"FILECLOUD CONFLICTS","Type":"Directory"},{"Id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","ModifiedAt":"2026-01-01T00:00:00Z","Name":"Filecloud Conflicts 2","Type":"File"},{"Id":"d60f61b25d6fb7b6cbda4eebc9bd9fe6b624b6d3262ac0757b218a320ab04637","ModifiedAt":"2026-01-03T00:00:00Z","Name":"Filecloud Conflicts 3","Type":"Directory"}],"Type":"Directory","Version":1}`
	const wantRootID = "0fc3d50e72b21f7a8b36e74b27590452da9c49a4f46068bc50ecf0753c748cd0"
	const wantFallback = `{"Entries":[{"Id":"3333333333333333333333333333333333333333333333333333333333333333","ModifiedAt":"2026-01-03T00:00:00Z","Name":"111111111111-界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界 (Filecloud conflict 1)","Type":"File"},{"Id":"2222222222222222222222222222222222222222222222222222222222222222","ModifiedAt":"2026-01-02T00:00:00Z","Name":"111111111111-界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界 (Filecloud conflict 2)","Type":"File"}],"Type":"Directory","Version":1}`
	const wantFallbackID = "d60f61b25d6fb7b6cbda4eebc9bd9fe6b624b6d3262ac0757b218a320ab04637"
	const wantCommit = `{"AuthorUserId":"01234567-89ab-4def-8123-456789abcdef","CreatedAt":"2026-02-03T04:05:08Z","DeviceId":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee","Message":"sync","Parents":["cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"],"Root":"0fc3d50e72b21f7a8b36e74b27590452da9c49a4f46068bc50ecf0753c748cd0","Type":"Commit","Version":1}`
	const wantCommitID = "f18dc9fdd3f68138a3427ab0e79c4849b1544e52d0d82e05d1447ce1910bd0f8"

	for _, reversed := range []bool{false, true} {
		emptyData, emptyID, err := canonicalEmptyDirectory()
		if err != nil {
			t.Fatal(err)
		}
		existingData, existingID, err := canonicalDirectory("", []scanEntry{
			{name: "FILECLOUD CONFLICTS", kind: "Directory", id: emptyID, modified: "2026-01-01T00:00:00Z"},
			{name: "Filecloud Conflicts 2", kind: "File", id: strings.Repeat("a", 64), modified: "2026-01-01T00:00:00Z"},
		})
		if err != nil {
			t.Fatal(err)
		}
		requests := []_fallbackConflict{
			{source: "z/f", leaf: strings.Repeat("界", 80), entry: object.DirectoryEntry{Name: "f", Type: "File", ID: strings.Repeat("2", 64), ModifiedAt: "2026-01-02T00:00:00Z"}},
			{source: "a/f", leaf: strings.Repeat("界", 80), entry: object.DirectoryEntry{Name: "f", Type: "File", ID: strings.Repeat("3", 64), ModifiedAt: "2026-01-03T00:00:00Z"}},
		}
		if reversed {
			requests[0], requests[1] = requests[1], requests[0]
		}
		merger := &_treeMerger{directories: map[string][]byte{emptyID: emptyData, existingID: existingData}, synthesized: make(map[string][]byte),
			active: make(map[string]bool), seen: make(map[string]bool), budget: _newReplayBudget(), localSeedID: strings.Repeat("1", 64),
			lineage: map[string]_conflictPromotion{"z/f": {source: "z/f", target: "z/f"}, "a/f": {source: "a/f", target: "a/f"}}, fallbacks: requests}
		rootID, err := merger.applyFallbacks(existingID)
		if err != nil {
			t.Fatal(err)
		}
		root, err := merger.loadDirectory(rootID)
		if err != nil {
			t.Fatal(err)
		}
		fallbackID := root.Entries[2].ID
		commitData, commitID, err := canonicalCommit(testClientUserID, testClientDeviceID, rootID,
			[]string{strings.Repeat("c", 64)}, func() time.Time { return time.Date(2026, 2, 3, 4, 5, 8, 0, time.UTC) })
		if err != nil {
			t.Fatal(err)
		}
		if string(merger.directories[rootID]) != wantRoot || rootID != wantRootID ||
			string(merger.directories[fallbackID]) != wantFallback || fallbackID != wantFallbackID ||
			string(commitData) != wantCommit || commitID != wantCommitID {
			t.Fatalf("fixed fallback vector reversed=%v root=%q/%s fallback=%q/%s commit=%q/%s", reversed,
				merger.directories[rootID], rootID, merger.directories[fallbackID], fallbackID, commitData, commitID)
		}
	}
}
