package storage

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
)

// SetDedupInterval sets the deduplication interval, which is applied to raw samples during data ingestion and querying.
//
// De-duplication is disabled if dedupInterval is 0.
//
// This function must be called before initializing the storage.
func SetDedupInterval(dedupInterval time.Duration) {
	globalDedupInterval = dedupInterval.Microseconds()
}

// GetDedupInterval returns the dedup interval in microseconds, which has been set via SetDedupInterval.
func GetDedupInterval() int64 {
	return globalDedupInterval
}

// SetDownsamplingPeriod sets the offset and interval for downsampling.
//
// Downsampling is disabled if interval is 0.
//
// This function must be called before the storage starts accepting writes.
func SetDownsamplingPeriod(offset, interval time.Duration) {
	globalDownsamplingOffset.Store(offset.Microseconds())
	globalDownsamplingInterval.Store(interval.Microseconds())
}

// IsDownsamplingEnabled returns whether downsampling is enabled.
func IsDownsamplingEnabled() bool {
	return globalDownsamplingInterval.Load() > 0
}

// GetDownsamplingInterval returns the configured downsampling interval in microseconds.
func GetDownsamplingInterval() int64 {
	return globalDownsamplingInterval.Load()
}

// GetDedupIntervalForTimeRange returns the deduplication or downsampling interval in microseconds,
// which must be applied to a time range starting at minTimestamp at currentTimestamp.
func GetDedupIntervalForTimeRange(minTimestamp, currentTimestamp int64) int64 {
	return getDedupIntervalForTimestamp(minTimestamp, currentTimestamp)
}

// GetDownsamplingIntervalForTimeRange returns the downsampling interval in microseconds,
// which must be applied to a time range starting at minTimestamp at currentTimestamp.
// It returns 0 if the time range must use the base deduplication interval.
func GetDownsamplingIntervalForTimeRange(minTimestamp, currentTimestamp int64) int64 {
	return getDownsamplingIntervalForTimestamp(minTimestamp, currentTimestamp)
}

// getDedupIntervalForBlock returns the deduplication or downsampling interval in microseconds,
// which must be applied to a block with the given maximum timestamp at currentTimestamp.
func getDedupIntervalForBlock(maxTimestamp, currentTimestamp int64) int64 {
	return getDedupIntervalForTimestamp(maxTimestamp, currentTimestamp)
}

func getDedupIntervalForTimestamp(timestamp, currentTimestamp int64) int64 {
	if downsamplingInterval := getDownsamplingIntervalForTimestamp(timestamp, currentTimestamp); downsamplingInterval > 0 {
		return downsamplingInterval
	}
	return globalDedupInterval
}

func getDownsamplingIntervalForTimestamp(timestamp, currentTimestamp int64) int64 {
	downsamplingInterval := globalDownsamplingInterval.Load()
	if downsamplingInterval <= globalDedupInterval {
		return 0
	}
	if timestamp > getDownsamplingCutoff(currentTimestamp) {
		return 0
	}
	return downsamplingInterval
}

func getDownsamplingCutoff(currentTimestamp int64) int64 {
	return currentTimestamp - globalDownsamplingOffset.Load()
}

// GetDedupIntervalEnd returns the inclusive end of the epoch-aligned bucket
// containing timestamp. It returns timestamp if dedupInterval isn't positive
// and clamps the result to math.MaxInt64 on overflow.
func GetDedupIntervalEnd(timestamp, dedupInterval int64) int64 {
	if dedupInterval <= 0 {
		return timestamp
	}
	delta := dedupInterval - 1
	if timestamp > math.MaxInt64-delta {
		return math.MaxInt64
	}
	timestamp += delta
	return timestamp - timestamp%dedupInterval
}

func areBlockBoundariesInSameDedupInterval(pendingMaxTimestamp, nextMinTimestamp, dedupInterval int64) bool {
	return dedupInterval > 0 &&
		GetDedupIntervalEnd(pendingMaxTimestamp, dedupInterval) == GetDedupIntervalEnd(nextMinTimestamp, dedupInterval)
}

var (
	globalDedupInterval        int64
	globalDownsamplingOffset   atomic.Int64
	globalDownsamplingInterval atomic.Int64
)

func isDedupOrDownsamplingEnabled() bool {
	return globalDedupInterval > 0 || globalDownsamplingInterval.Load() > 0
}

// DeduplicateSamples removes samples from src* if they are closer to each other than dedupInterval in microseconds.
func DeduplicateSamples(srcTimestamps []int64, srcValues []float64, dedupInterval int64) ([]int64, []float64) {
	if !needsDedup(srcTimestamps, dedupInterval) {
		// Fast path - nothing to deduplicate
		return srcTimestamps, srcValues
	}
	tsNext := GetDedupIntervalEnd(srcTimestamps[0], dedupInterval)
	dstTimestamps := srcTimestamps[:0]
	dstValues := srcValues[:0]
	for i, ts := range srcTimestamps[1:] {
		if ts <= tsNext {
			continue
		}
		// Choose the maximum value with the timestamp equal to tsPrev.
		// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3333
		j := i
		tsPrev := srcTimestamps[j]
		vPrev := srcValues[j]
		// if multiple samples have the same timestamp, choose the maximum value, see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3333;
		// always prefer a non-decimal.StaleNaN value, see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10196
		for j > 0 && srcTimestamps[j-1] == tsPrev {
			j--
			if decimal.IsStaleNaN(srcValues[j]) {
				continue
			}
			if decimal.IsStaleNaN(vPrev) {
				vPrev = srcValues[j]
				continue
			}
			if srcValues[j] > vPrev {
				vPrev = srcValues[j]
			}
		}
		dstTimestamps = append(dstTimestamps, tsPrev)
		dstValues = append(dstValues, vPrev)
		tsNext += dedupInterval
		if tsNext < ts {
			tsNext = GetDedupIntervalEnd(ts, dedupInterval)
		}
	}
	j := len(srcTimestamps) - 1
	tsPrev := srcTimestamps[j]
	vPrev := srcValues[j]
	// if multiple samples have the same timestamp, choose the maximum value, see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3333;
	// always prefer a non-decimal.StaleNaN value, see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10196
	for j > 0 && srcTimestamps[j-1] == tsPrev {
		j--
		if decimal.IsStaleNaN(srcValues[j]) {
			continue
		}
		if decimal.IsStaleNaN(vPrev) {
			vPrev = srcValues[j]
			continue
		}
		if srcValues[j] > vPrev {
			vPrev = srcValues[j]
		}
	}
	dstTimestamps = append(dstTimestamps, tsPrev)
	dstValues = append(dstValues, vPrev)
	return dstTimestamps, dstValues
}

func deduplicateSamplesDuringMerge(srcTimestamps, srcValues []int64, dedupInterval int64) ([]int64, []int64) {
	if !needsDedup(srcTimestamps, dedupInterval) {
		// Fast path - nothing to deduplicate
		return srcTimestamps, srcValues
	}
	tsNext := GetDedupIntervalEnd(srcTimestamps[0], dedupInterval)
	dstTimestamps := srcTimestamps[:0]
	dstValues := srcValues[:0]
	for i, ts := range srcTimestamps[1:] {
		if ts <= tsNext {
			continue
		}
		// Choose the maximum value with the timestamp equal to tsPrev.
		// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3333
		j := i
		tsPrev := srcTimestamps[j]
		vPrev := srcValues[j]
		// if multiple samples have the same timestamp, choose the maximum value, see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3333;
		// always prefer a non-decimal.StaleNaN value, see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10196
		for j > 0 && srcTimestamps[j-1] == tsPrev {
			j--
			if decimal.IsStaleNaNInt64(srcValues[j]) {
				continue
			}
			if decimal.IsStaleNaNInt64(vPrev) {
				vPrev = srcValues[j]
				continue
			}
			if srcValues[j] > vPrev {
				vPrev = srcValues[j]
			}
		}
		dstTimestamps = append(dstTimestamps, tsPrev)
		dstValues = append(dstValues, vPrev)
		tsNext += dedupInterval
		if tsNext < ts {
			tsNext = GetDedupIntervalEnd(ts, dedupInterval)
		}
	}
	j := len(srcTimestamps) - 1
	tsPrev := srcTimestamps[j]
	vPrev := srcValues[j]
	// if multiple samples have the same timestamp, choose the maximum value, see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3333;
	// always prefer a non-decimal.StaleNaN value, see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10196
	for j > 0 && srcTimestamps[j-1] == tsPrev {
		j--
		if decimal.IsStaleNaNInt64(srcValues[j]) {
			continue
		}
		if decimal.IsStaleNaNInt64(vPrev) {
			vPrev = srcValues[j]
			continue
		}
		if srcValues[j] > vPrev {
			vPrev = srcValues[j]
		}
	}
	dstTimestamps = append(dstTimestamps, tsPrev)
	dstValues = append(dstValues, vPrev)
	return dstTimestamps, dstValues
}

func needsDedup(timestamps []int64, dedupInterval int64) bool {
	if len(timestamps) < 2 || dedupInterval <= 0 {
		return false
	}
	tsNext := GetDedupIntervalEnd(timestamps[0], dedupInterval)
	for _, ts := range timestamps[1:] {
		if ts <= tsNext {
			return true
		}
		tsNext += dedupInterval
		if tsNext < ts {
			tsNext = GetDedupIntervalEnd(ts, dedupInterval)
		}
	}
	return false
}
