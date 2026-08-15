package acceptance

import (
	"fmt"
	"runtime"
	"time"
)

// MeasurePeakHeap runs operation after a full GC and returns elapsed time and
// the peak HeapAlloc increase sampled once per millisecond.
func MeasurePeakHeap(operation func() error) (peak uint64, elapsed time.Duration, err error) {
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	type measurement struct {
		maximum   uint64
		recovered any
	}
	stop := make(chan struct{})
	done := make(chan measurement, 1)
	go func() {
		maximum := baseline.HeapAlloc
		defer func() {
			done <- measurement{maximum: maximum, recovered: recover()}
		}()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				var current runtime.MemStats
				runtime.ReadMemStats(&current)
				maximum = max(maximum, current.HeapAlloc)
			case <-stop:
				return
			}
		}
	}()
	started := time.Now()
	defer func() {
		elapsed = time.Since(started)
		close(stop)
		result := <-done
		peak = result.maximum - min(result.maximum, baseline.HeapAlloc)
		if result.recovered != nil {
			err = fmt.Errorf("heap sampler panic: %v", result.recovered)
		}
		if recovered := recover(); recovered != nil {
			panic(recovered)
		}
	}()
	err = operation()
	return peak, elapsed, err
}
