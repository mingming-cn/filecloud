package acceptance

import (
	"errors"
	"testing"
)

func TestMeasurePeakHeapPropagatesOperationError(t *testing.T) {
	want := errors.New("measurement failed")
	_, _, err := MeasurePeakHeap(func() error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("MeasurePeakHeap error = %v, want %v", err, want)
	}
}

func TestMeasurePeakHeapStopsSamplerBeforeRepanicking(t *testing.T) {
	const want = "measurement panic"
	deferred := false
	func() {
		defer func() {
			deferred = true
			if recovered := recover(); recovered != want {
				t.Fatalf("MeasurePeakHeap panic = %v, want %q", recovered, want)
			}
		}()
		_, _, _ = MeasurePeakHeap(func() error { panic(want) })
	}()
	if !deferred {
		t.Fatal("MeasurePeakHeap did not repanic after sampler cleanup")
	}
}
