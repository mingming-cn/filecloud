package object

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/mingming-cn/filecloud/internal/acceptance"
)

const (
	_performancePrefix           = "FILECLOUD_PERFORMANCE "
	_performanceDirectoryEntries = 100000
)

type directoryPerformanceResult struct {
	Scenario           string `json:"scenario"`
	Entries            int    `json:"entries"`
	ElapsedNanoseconds int64  `json:"elapsedNanoseconds"`
	PeakHeapBytes      uint64 `json:"peakHeapBytes"`
	EncodedBytes       int    `json:"encodedBytes"`
}

func TestPerformanceBaselineWideDirectory(t *testing.T) {
	if os.Getenv("FILECLOUD_RUN_1C") != "1" {
		t.Skip("set FILECLOUD_RUN_1C=1 to run deployment performance baselines")
	}
	input := wideDirectoryFixture(t, _performanceDirectoryEntries)
	var canonical []byte
	peak, elapsed, err := acceptance.MeasurePeakHeap(func() error {
		var canonicalErr error
		canonical, _, canonicalErr = Canonicalize("directories", input)
		return canonicalErr
	})
	if err != nil {
		t.Fatalf("canonicalize 100000 directory entries: %v", err)
	}
	result := directoryPerformanceResult{
		Scenario: "normalize-validate-100000-directory-entries", Entries: _performanceDirectoryEntries,
		ElapsedNanoseconds: elapsed.Nanoseconds(), PeakHeapBytes: peak, EncodedBytes: len(canonical),
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(_performancePrefix + string(data))
}

func wideDirectoryFixture(t *testing.T, entries int) []byte {
	t.Helper()
	var input bytes.Buffer
	input.Grow(entries * 150)
	input.WriteString(`{"Entries":[`)
	id := ID([]byte("wide-directory-entry"))
	for index := range entries {
		if index != 0 {
			input.WriteByte(',')
		}
		if _, err := fmt.Fprintf(&input,
			`{"Id":%q,"ModifiedAt":"2026-08-20T12:00:00Z","Name":"file-%06d","Type":"File"}`,
			id, index); err != nil {
			t.Fatal(err)
		}
	}
	input.WriteString(`],"Type":"Directory","Version":1}`)
	return input.Bytes()
}
