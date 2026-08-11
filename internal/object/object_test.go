package object_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/mingming-cn/filecloud/internal/object"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

func TestCanonicalMetadataVectors(t *testing.T) {
	tests := []struct {
		name, kind, input, canonical, id string
	}{
		{name: "empty file", kind: "files", input: `{"Version":1,"Size":"0","Blocks":[],"Type":"File"}`, canonical: `{"Blocks":[],"Size":"0","Type":"File","Version":1}`, id: "fe680f5ed33eb93ec5fb2eba2003164fe1d60401cc74edd895042aeb17220032"},
		{name: "one byte file", kind: "files", input: `{"Type":"File","Version":1,"Size":"1","Blocks":["ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"]}`, canonical: `{"Blocks":["ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"],"Size":"1","Type":"File","Version":1}`, id: "18d704e865da8f4112d96ee1a3e2a60ebe0c266c302188dda8ee1c662ec90ff6"},
		{name: "boundary minus one file", kind: "files", input: `{"Blocks":["475c02a4e3d98fe69daf9e9c9d78406169f788a52988042c3d32ba25ed4ce9bb"],"Size":"4194303","Type":"File","Version":1}`, canonical: `{"Blocks":["475c02a4e3d98fe69daf9e9c9d78406169f788a52988042c3d32ba25ed4ce9bb"],"Size":"4194303","Type":"File","Version":1}`, id: "af2527e8f3041e071f386cc99a723937529c5a808311ebf9eae26dce50bd86c0"},
		{name: "boundary file", kind: "files", input: `{"Blocks":["299285fc41a44cdb038b9fdaf494c76ca9d0c866672b2b266c1a0c17dda60a05"],"Size":"4194304","Type":"File","Version":1}`, canonical: `{"Blocks":["299285fc41a44cdb038b9fdaf494c76ca9d0c866672b2b266c1a0c17dda60a05"],"Size":"4194304","Type":"File","Version":1}`, id: "3866a97c3725f674345442e38763e27ad6802ff566c1754cd00541f9f8f7b071"},
		{name: "two block file", kind: "files", input: `{"Blocks":["299285fc41a44cdb038b9fdaf494c76ca9d0c866672b2b266c1a0c17dda60a05","ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"],"Size":"4194305","Type":"File","Version":1}`, canonical: `{"Blocks":["299285fc41a44cdb038b9fdaf494c76ca9d0c866672b2b266c1a0c17dda60a05","ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"],"Size":"4194305","Type":"File","Version":1}`, id: "28aea438519609c106cd850baa5d1553237346fe1994f2920a00e3950c5dbaa2"},
		{name: "empty directory", kind: "directories", input: `{"Version":1,"Entries":[],"Type":"Directory"}`, canonical: `{"Entries":[],"Type":"Directory","Version":1}`, id: "2ed3d5b84f7db1c1f72cf7a317f1c19de73f404e8c25d0c482f2809503355bf6"},
		{name: "unicode directory", kind: "directories", input: `{"Entries":[{"Type":"File","Name":"Å.txt","ModifiedAt":"2026-08-09T00:00:00Z","Id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}],"Type":"Directory","Version":1}`, canonical: `{"Entries":[{"Id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","ModifiedAt":"2026-08-09T00:00:00Z","Name":"Å.txt","Type":"File"}],"Type":"Directory","Version":1}`, id: "ed45fcac78692b7aef42e4a4cdd131b2fca7bde0f02b80c3fa6d5eaf5da27e92"},
		{name: "commit", kind: "commits", input: `{"Version":1,"Type":"Commit","Root":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","Parents":[],"Message":"sync","DeviceId":"01234567-89ab-4def-8123-456789abcdef","CreatedAt":"2026-08-09T00:00:00Z","AuthorUserId":"12345678-9abc-4def-8123-456789abcdef"}`, canonical: `{"AuthorUserId":"12345678-9abc-4def-8123-456789abcdef","CreatedAt":"2026-08-09T00:00:00Z","DeviceId":"01234567-89ab-4def-8123-456789abcdef","Message":"sync","Parents":[],"Root":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","Type":"Commit","Version":1}`, id: "ee29bc188da578df7cb218a5b292199200e3964b18a2f680e4f51e396f1f4290"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			canonical, id, err := object.Canonicalize(test.kind, []byte(test.input))
			if err != nil {
				t.Fatalf("Canonicalize: %v", err)
			}
			if string(canonical) != test.canonical || id != test.id {
				t.Fatalf("canonical/id = %s/%s, want %s/%s", canonical, id, test.canonical, test.id)
			}
		})
	}
}

func TestBlockVectors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		id   string
	}{
		{name: "one byte", data: []byte("a"), id: "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"},
		{name: "boundary minus one", data: []byte(strings.Repeat("a", object.MaxBlockSize-1)), id: "475c02a4e3d98fe69daf9e9c9d78406169f788a52988042c3d32ba25ed4ce9bb"},
		{name: "boundary", data: []byte(strings.Repeat("a", object.MaxBlockSize)), id: "299285fc41a44cdb038b9fdaf494c76ca9d0c866672b2b266c1a0c17dda60a05"},
		{name: "boundary plus one", data: []byte(strings.Repeat("a", object.MaxBlockSize+1)), id: "acd560a1e1d523c090ab93aed616d154b7b5e8206a153cced729d83f2c7dcfc3"},
	}
	for _, test := range tests {
		if digest := object.ID(test.data); digest != test.id {
			t.Fatalf("%s id = %s", test.name, digest)
		}
	}
}

func TestCanonicalMetadataRejectsInvalidInput(t *testing.T) {
	validID := strings.Repeat("a", 64)
	tests := []struct{ name, kind, input string }{
		{name: "unknown field", kind: "files", input: `{"Blocks":[],"Extra":1,"Size":"0","Type":"File","Version":1}`},
		{name: "duplicate key", kind: "files", input: `{"Blocks":[],"Size":"0","Size":"0","Type":"File","Version":1}`},
		{name: "wrong type", kind: "files", input: `{"Blocks":[],"Size":"0","Type":"Directory","Version":1}`},
		{name: "wrong version", kind: "files", input: `{"Blocks":[],"Size":"0","Type":"File","Version":2}`},
		{name: "float", kind: "files", input: `{"Blocks":[],"Size":"0","Type":"File","Version":1.0}`},
		{name: "leading zero size", kind: "files", input: `{"Blocks":[],"Size":"00","Type":"File","Version":1}`},
		{name: "nfd name", kind: "directories", input: `{"Entries":[{"Id":"` + validID + `","ModifiedAt":"2026-08-09T00:00:00Z","Name":"Å","Type":"File"}],"Type":"Directory","Version":1}`},
		{name: "path separator", kind: "directories", input: `{"Entries":[{"Id":"` + validID + `","ModifiedAt":"2026-08-09T00:00:00Z","Name":"a/b","Type":"File"}],"Type":"Directory","Version":1}`},
		{name: "case collision", kind: "directories", input: `{"Entries":[{"Id":"` + validID + `","ModifiedAt":"2026-08-09T00:00:00Z","Name":"a","Type":"File"},{"Id":"` + validID + `","ModifiedAt":"2026-08-09T00:00:00Z","Name":"A","Type":"File"}],"Type":"Directory","Version":1}`},
		{name: "unicode case-fold collision", kind: "directories", input: `{"Entries":[{"Id":"` + validID + `","ModifiedAt":"2026-08-09T00:00:00Z","Name":"s","Type":"File"},{"Id":"` + validID + `","ModifiedAt":"2026-08-09T00:00:00Z","Name":"ſ","Type":"File"}],"Type":"Directory","Version":1}`},
		{name: "unsorted", kind: "directories", input: `{"Entries":[{"Id":"` + validID + `","ModifiedAt":"2026-08-09T00:00:00Z","Name":"b","Type":"File"},{"Id":"` + validID + `","ModifiedAt":"2026-08-09T00:00:00Z","Name":"a","Type":"File"}],"Type":"Directory","Version":1}`},
		{name: "duplicate commit key", kind: "commits", input: `{"AuthorUserId":"12345678-9abc-4def-8123-456789abcdef","CreatedAt":"2026-08-09T00:00:00Z","DeviceId":"01234567-89ab-4def-8123-456789abcdef","Message":"sync","Message":"again","Parents":[],"Root":"` + validID + `","Type":"Commit","Version":1}`},
		{name: "unknown commit field", kind: "commits", input: `{"AuthorUserId":"12345678-9abc-4def-8123-456789abcdef","CreatedAt":"2026-08-09T00:00:00Z","DeviceId":"01234567-89ab-4def-8123-456789abcdef","Extra":true,"Message":"sync","Parents":[],"Root":"` + validID + `","Type":"Commit","Version":1}`},
		{name: "missing commit field", kind: "commits", input: `{"AuthorUserId":"12345678-9abc-4def-8123-456789abcdef","CreatedAt":"2026-08-09T00:00:00Z","DeviceId":"01234567-89ab-4def-8123-456789abcdef","Parents":[],"Root":"` + validID + `","Type":"Commit","Version":1}`},
		{name: "float commit version", kind: "commits", input: `{"AuthorUserId":"12345678-9abc-4def-8123-456789abcdef","CreatedAt":"2026-08-09T00:00:00Z","DeviceId":"01234567-89ab-4def-8123-456789abcdef","Message":"sync","Parents":[],"Root":"` + validID + `","Type":"Commit","Version":1.0}`},
		{name: "uppercase uuid", kind: "commits", input: `{"AuthorUserId":"12345678-9ABC-4def-8123-456789abcdef","CreatedAt":"2026-08-09T00:00:00Z","DeviceId":"01234567-89ab-4def-8123-456789abcdef","Message":"sync","Parents":[],"Root":"` + validID + `","Type":"Commit","Version":1}`},
		{name: "too many parents", kind: "commits", input: `{"AuthorUserId":"12345678-9abc-4def-8123-456789abcdef","CreatedAt":"2026-08-09T00:00:00Z","DeviceId":"01234567-89ab-4def-8123-456789abcdef","Message":"sync","Parents":["` + validID + `","` + validID + `","` + validID + `"],"Root":"` + validID + `","Type":"Commit","Version":1}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := object.Canonicalize(test.kind, []byte(test.input)); err == nil {
				t.Fatal("Canonicalize succeeded")
			}
		})
	}
}

func TestCanonicalMetadataRejectsCollectionPastLimit(t *testing.T) {
	validID := strings.Repeat("a", 64)

	oversizedFile := `{"Size":"1099511627777","Blocks":[{"not":"decoded"`
	if _, _, err := object.Canonicalize("files", []byte(oversizedFile)); !errors.Is(err, object.ErrPayloadTooLarge) {
		t.Fatalf("oversized File size error = %v, want ErrPayloadTooLarge", err)
	}

	var blocksFirst strings.Builder
	blocksFirst.WriteString(`{"Blocks":[`)
	for i := range 262144 {
		if i > 0 {
			blocksFirst.WriteByte(',')
		}
		fmt.Fprintf(&blocksFirst, "%q", validID)
	}
	blocksFirst.WriteString(`],"Size":"1099511627777","Type":{"not":"decoded"`)
	if _, _, err := object.Canonicalize("files", []byte(blocksFirst.String())); !errors.Is(err, object.ErrPayloadTooLarge) {
		t.Fatalf("blocks-first oversized File error = %v, want ErrPayloadTooLarge", err)
	}

	var file strings.Builder
	file.WriteString(`{"Blocks":[`)
	for i := 0; i < 262144; i++ {
		if i > 0 {
			file.WriteByte(',')
		}
		fmt.Fprintf(&file, "%q", validID)
	}
	// The over-limit value is malformed. ErrPayloadTooLarge proves it was not decoded.
	file.WriteString(`,{"not":"decoded"`)
	if _, _, err := object.Canonicalize("files", []byte(file.String())); !errors.Is(err, object.ErrPayloadTooLarge) {
		t.Fatalf("oversized File error = %v, want ErrPayloadTooLarge", err)
	}

	entry := `{"Id":"` + validID + `","ModifiedAt":"2026-08-09T00:00:00Z","Name":"a","Type":"File"}`
	var directory strings.Builder
	directory.WriteString(`{"Entries":[`)
	for i := 0; i < 100000; i++ {
		if i > 0 {
			directory.WriteByte(',')
		}
		directory.WriteString(entry)
	}
	// The over-limit value is malformed. ErrPayloadTooLarge proves it was not decoded.
	directory.WriteString(`,{"not":"decoded"`)
	if _, _, err := object.Canonicalize("directories", []byte(directory.String())); !errors.Is(err, object.ErrPayloadTooLarge) {
		t.Fatalf("oversized Directory error = %v, want ErrPayloadTooLarge", err)
	}

	commit := `{"AuthorUserId":"12345678-9abc-4def-8123-456789abcdef","CreatedAt":"2026-08-09T00:00:00Z","DeviceId":"01234567-89ab-4def-8123-456789abcdef","Message":"sync","Parents":["` + validID + `","` + validID + `",{"not":"decoded"`
	if _, _, err := object.Canonicalize("commits", []byte(commit)); !errors.Is(err, object.ErrPayloadTooLarge) {
		t.Fatalf("oversized Commit Parents error = %v, want ErrPayloadTooLarge", err)
	}
}

func TestMetadataJSONNestingBudget(t *testing.T) {
	validID := strings.Repeat("a", 64)
	tests := []struct {
		name, kind, prefix, suffix string
		boundaryContainers         int
	}{
		{name: "File field", kind: "files", prefix: `{"Blocks":[],"Size":`, suffix: `,"Type":"File","Version":1}`, boundaryContainers: 255},
		{name: "Directory field", kind: "directories", prefix: `{"Entries":[],"Type":`, suffix: `,"Version":1}`, boundaryContainers: 255},
		{name: "Directory Entry field", kind: "directories", prefix: `{"Entries":[{"Id":"` + validID + `","ModifiedAt":"2026-08-09T00:00:00Z","Name":`, suffix: `,"Type":"File"}],"Type":"Directory","Version":1}`, boundaryContainers: 253},
		{name: "Commit field", kind: "commits", prefix: `{"AuthorUserId":"12345678-9abc-4def-8123-456789abcdef","CreatedAt":"2026-08-09T00:00:00Z","DeviceId":"01234567-89ab-4def-8123-456789abcdef","Message":`, suffix: `,"Parents":[],"Root":"` + validID + `","Type":"Commit","Version":1}`, boundaryContainers: 255},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			boundary := test.prefix + strings.Repeat("[", test.boundaryContainers) + `null` + strings.Repeat("]", test.boundaryContainers) + test.suffix
			if _, _, err := object.Canonicalize(test.kind, []byte(boundary)); err == nil || errors.Is(err, object.ErrPayloadTooLarge) {
				t.Fatalf("nesting boundary error = %v, want ordinary validation error", err)
			}

			// The malformed token follows the over-budget opening delimiter. The budget
			// error proves the parser stops before consuming the remaining value.
			overLimit := test.prefix + strings.Repeat("[", test.boundaryContainers+1) + `not-json`
			if _, _, err := object.Canonicalize(test.kind, []byte(overLimit)); !errors.Is(err, object.ErrPayloadTooLarge) {
				t.Fatalf("nesting over limit error = %v, want ErrPayloadTooLarge", err)
			}
		})
	}

	for _, test := range []struct {
		name, input string
	}{
		{name: "unknown File field rejects before value", input: `{"Extra":` + strings.Repeat("[", 256) + `not-json`},
		{name: "duplicate File field rejects before value", input: `{"Type":"File","Type":` + strings.Repeat("[", 256) + `not-json`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := object.Canonicalize("files", []byte(test.input)); err == nil || errors.Is(err, object.ErrPayloadTooLarge) {
				t.Fatalf("Canonicalize error = %v, want ordinary field validation error", err)
			}
		})
	}
}

func TestUnicodeTablesRemainProtocolEquivalent(t *testing.T) {
	// Unicode 15.1 only added characters without NFC or default case-fold mappings,
	// so 15.0 tables are equivalent for the protocol. Fail on dependency/build drift.
	if norm.Version != "15.0.0" || cases.UnicodeVersion != "15.0.0" {
		t.Fatalf("Unicode tables = norm %s/cases %s, want protocol-equivalent 15.0.0 tables", norm.Version, cases.UnicodeVersion)
	}
}

func ExampleCanonicalize() {
	canonical, id, err := object.Canonicalize("files", []byte(`{"Version":1,"Size":"0","Blocks":[],"Type":"File"}`))
	fmt.Println(string(canonical))
	fmt.Println(id)
	fmt.Println(err)
	// Output:
	// {"Blocks":[],"Size":"0","Type":"File","Version":1}
	// fe680f5ed33eb93ec5fb2eba2003164fe1d60401cc74edd895042aeb17220032
	// <nil>
}
