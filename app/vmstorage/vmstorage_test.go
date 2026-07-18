package vmstorage

import (
	"math"
	"strconv"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func TestCalculateMaxMetricsLimitByResource(t *testing.T) {
	f := func(maxConcurrentRequest, remainingMemory, expect int) {
		t.Helper()
		maxMetricsLimit := calculateMaxUniqueTimeseries(maxConcurrentRequest, remainingMemory)
		if maxMetricsLimit != expect {
			t.Fatalf("unexpected max metrics limit: got %d, want %d", maxMetricsLimit, expect)
		}
	}

	// 64-bit architectures support memory sizes > 4GB.
	if strconv.IntSize == 64 {
		// 8 CPU & 32 GiB
		f(16, int(math.Round(32*1024*1024*1024*0.4)), 4294967)
		// 4 CPU & 32 GiB
		f(8, int(math.Round(32*1024*1024*1024*0.4)), 8589934)
	}

	// 2 CPU & 4 GiB
	f(4, int(math.Round(4*1024*1024*1024*0.4)), 2147483)

	// other edge cases
	f(0, int(math.Round(4*1024*1024*1024*0.4)), 2e9)
	f(4, 0, 0)

}

func TestGetMaxMetrics(t *testing.T) {
	originalMaxUniqueTimeSeries := *maxUniqueTimeseries
	defer func() {
		*maxUniqueTimeseries = originalMaxUniqueTimeSeries
		fs.MustRemoveDir(t.Name())
	}()

	maxConcurrentRequests := 2 * cgroup.AvailableCPUs()
	f := func(searchQueryLimit, storageMaxUniqueTimeseries, expect int) {
		t.Helper()
		*maxUniqueTimeseries = storageMaxUniqueTimeseries
		s := storage.MustOpenStorage(t.Name(), storage.OpenOptions{})
		vms := newVMStorage(s, maxConcurrentRequests, func(mrs []storage.MetricRow) {})
		defer vms.Stop()
		maxMetrics := vms.getMaxMetrics(searchQueryLimit)
		if maxMetrics != expect {
			t.Fatalf("unexpected max metrics: got %d, want %d", maxMetrics, expect)
		}
	}

	f(0, 1e6, 1e6)
	f(2e6, 0, 2e6)
	f(2e6, 1e6, 1e6)
}

func TestTimestampMillisecondsToMicroseconds(t *testing.T) {
	testCases := []struct {
		name string
		in   int64
		want int64
	}{
		{name: "positive", in: 1_234, want: 1_234_000},
		{name: "negative", in: -1_234, want: -1_234_000},
		{name: "max", in: math.MaxInt64, want: math.MaxInt64},
		{name: "min", in: math.MinInt64, want: math.MinInt64},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := timestampMillisecondsToMicroseconds(tc.in)
			if got != tc.want {
				t.Fatalf("unexpected timestamp; got %d; want %d", got, tc.want)
			}
		})
	}
}
