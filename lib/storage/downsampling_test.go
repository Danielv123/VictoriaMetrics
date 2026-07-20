package storage

import (
	"slices"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
)

func TestIsDownsamplingEnabled(t *testing.T) {
	restore := setDownsamplingConfigForTest(2*time.Microsecond, 0, 0)
	defer restore()

	if IsDownsamplingEnabled() {
		t.Fatalf("downsampling must be disabled")
	}
	SetDownsamplingPeriod(100*time.Microsecond, 10*time.Microsecond)
	if !IsDownsamplingEnabled() {
		t.Fatalf("downsampling must be enabled")
	}
}

func TestGetDedupIntervalForTimeRange(t *testing.T) {
	restore := setDownsamplingConfigForTest(2*time.Microsecond, 100*time.Microsecond, 10*time.Microsecond)
	defer restore()

	const currentTimestamp = 1_000
	testCases := []struct {
		name             string
		minTimestamp     int64
		want             int64
		wantDownsampling int64
	}{
		{name: "newer than cutoff", minTimestamp: 901, want: 2},
		{name: "at cutoff", minTimestamp: 900, want: 10, wantDownsampling: 10},
		{name: "older than cutoff", minTimestamp: 899, want: 10, wantDownsampling: 10},
		{name: "future", minTimestamp: 1_001, want: 2},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := GetDedupIntervalForTimeRange(tc.minTimestamp, currentTimestamp)
			if got != tc.want {
				t.Fatalf("unexpected interval; got %d; want %d", got, tc.want)
			}
			gotDownsampling := GetDownsamplingIntervalForTimeRange(tc.minTimestamp, currentTimestamp)
			if gotDownsampling != tc.wantDownsampling {
				t.Fatalf("unexpected downsampling interval; got %d; want %d", gotDownsampling, tc.wantDownsampling)
			}
		})
	}
}

func TestGetDedupIntervalForBlock(t *testing.T) {
	restore := setDownsamplingConfigForTest(2*time.Microsecond, 100*time.Microsecond, 10*time.Microsecond)
	defer restore()

	if got := GetDedupInterval(); got != 2 {
		t.Fatalf("unexpected global dedup interval; got %d; want %d", got, 2)
	}

	const currentTimestamp = 1_000
	testCases := []struct {
		name         string
		maxTimestamp int64
		want         int64
	}{
		{name: "newer than offset", maxTimestamp: 901, want: 2},
		{name: "at offset", maxTimestamp: 900, want: 10},
		{name: "older than offset", maxTimestamp: 899, want: 10},
		{name: "future", maxTimestamp: 1_001, want: 2},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := getDedupIntervalForBlock(tc.maxTimestamp, currentTimestamp)
			if got != tc.want {
				t.Fatalf("unexpected interval; got %d; want %d", got, tc.want)
			}
		})
	}
}

func TestGetDedupIntervalEnd(t *testing.T) {
	testCases := []struct {
		name          string
		timestamp     int64
		dedupInterval int64
		want          int64
	}{
		{name: "positive timestamp", timestamp: 5, dedupInterval: 10, want: 10},
		{name: "bucket end", timestamp: 10, dedupInterval: 10, want: 10},
		{name: "negative timestamp", timestamp: -2, dedupInterval: 10, want: 0},
		{name: "max int64", timestamp: int64(^uint64(0) >> 1), dedupInterval: 10, want: int64(^uint64(0) >> 1)},
		{name: "disabled", timestamp: 5, dedupInterval: 0, want: 5},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := GetDedupIntervalEnd(tc.timestamp, tc.dedupInterval); got != tc.want {
				t.Fatalf("unexpected bucket end; got %d; want %d", got, tc.want)
			}
		})
	}
}

func TestBlockDeduplicateSamplesDuringMerge(t *testing.T) {
	tsid := &TSID{MetricID: 1}

	var baseBlock Block
	baseBlock.Init(tsid, []int64{801, 802, 809, 901}, []int64{1, 2, 3, 4}, 0, 64)
	dedups := baseBlock.deduplicateSamplesDuringMerge(2)
	if dedups != 1 {
		t.Fatalf("unexpected rows deduplicated at the base interval; got %d; want %d", dedups, 1)
	}
	if got := len(baseBlock.timestamps); got != 3 {
		t.Fatalf("unexpected rows count at the base interval; got %d; want %d", got, 3)
	}
	if got := baseBlock.RowsCount(); got != 3 {
		t.Fatalf("unexpected block header rows count at the base interval; got %d; want %d", got, 3)
	}

	var downsampledBlock Block
	downsampledBlock.Init(tsid, []int64{801, 802, 809, 901}, []int64{1, 2, 3, 4}, 0, 64)
	dedups = downsampledBlock.deduplicateSamplesDuringMerge(10)
	if dedups != 2 {
		t.Fatalf("unexpected rows deduplicated at the downsampling interval; got %d; want %d", dedups, 2)
	}
	if got := len(downsampledBlock.timestamps); got != 2 {
		t.Fatalf("unexpected rows count at the downsampling interval; got %d; want %d", got, 2)
	}
	if got := downsampledBlock.RowsCount(); got != 2 {
		t.Fatalf("unexpected block header rows count at the downsampling interval; got %d; want %d", got, 2)
	}
}

func TestInmemoryPartUsesBaseDedupInterval(t *testing.T) {
	restore := setDownsamplingConfigForTest(2*time.Microsecond, 100*time.Microsecond, 10*time.Microsecond)
	defer restore()

	rows := []rawRow{
		{TSID: TSID{MetricID: 1}, Timestamp: 801, Value: 1, PrecisionBits: 64},
		{TSID: TSID{MetricID: 1}, Timestamp: 802, Value: 2, PrecisionBits: 64},
		{TSID: TSID{MetricID: 1}, Timestamp: 809, Value: 3, PrecisionBits: 64},
	}
	var mp inmemoryPart
	mp.InitFromRows(rows)
	if got := mp.ph.RowsCount; got != 2 {
		t.Fatalf("raw part creation must apply only base deduplication; got %d rows; want %d", got, 2)
	}
	if got := mp.ph.MinDedupInterval; got != 2 {
		t.Fatalf("raw part creation must record the base interval; got %d; want %d", got, 2)
	}
}

func TestDownsamplingAcrossBlockBoundaries(t *testing.T) {
	restore := setDownsamplingConfigForTest(0, 0, 0)
	defer restore()

	testCases := []struct {
		name            string
		secondTimestamp int64
	}{
		{name: "equal timestamp stale winner", secondTimestamp: 801},
		{name: "adjacent timestamps in same bucket", secondTimestamp: 802},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			SetDownsamplingPeriod(0, 0)
			rows := make([]rawRow, maxRowsPerBlock)
			for i := range rows {
				rows[i] = rawRow{
					TSID:          TSID{MetricID: 1},
					Timestamp:     801,
					Value:         decimal.StaleNaN,
					PrecisionBits: 64,
				}
			}
			bsr1 := newTestBlockStreamReader(rows)
			bsr2 := newTestBlockStreamReader([]rawRow{{
				TSID:          TSID{MetricID: 1},
				Timestamp:     tc.secondTimestamp,
				Value:         42,
				PrecisionBits: 64,
			}})

			SetDownsamplingPeriod(100*time.Microsecond, 10*time.Microsecond)
			var mp inmemoryPart
			var bsw blockStreamWriter
			bsw.MustInitFromInmemoryPart(&mp, -5)
			var rowsMerged, rowsDeleted atomic.Uint64
			if err := mergeBlockStreams(&mp.ph, &bsw, []*blockStreamReader{bsr1, bsr2}, nil, &uint64set.Set{}, 0, 10, &rowsMerged, &rowsDeleted); err != nil {
				t.Fatalf("cannot merge block streams: %s", err)
			}
			if got := mp.ph.RowsCount; got != 1 {
				t.Fatalf("downsampling must retain one row across block boundaries; got %d; want %d", got, 1)
			}
			if got := mp.ph.MinDedupInterval; got != 10 {
				t.Fatalf("unexpected minimum dedup interval; got %d; want %d", got, 10)
			}
			if got := rowsMerged.Load(); got != maxRowsPerBlock+1 {
				t.Fatalf("unexpected merged rows count; got %d; want %d", got, maxRowsPerBlock+1)
			}

			var bsr blockStreamReader
			bsr.MustInitFromInmemoryPart(&mp)
			if !bsr.NextBlock() {
				t.Fatalf("missing merged block: %s", bsr.Error())
			}
			if err := bsr.Block.UnmarshalData(); err != nil {
				t.Fatalf("cannot unmarshal merged block: %s", err)
			}
			if got := bsr.Block.timestamps[0]; got != tc.secondTimestamp {
				t.Fatalf("unexpected retained timestamp; got %d; want %d", got, tc.secondTimestamp)
			}
			if got := decimal.ToFloat(bsr.Block.values[0], bsr.Block.bh.Scale); got != 42 {
				t.Fatalf("non-stale boundary sample must win; got %v; want %v", got, 42)
			}
			if bsr.NextBlock() {
				t.Fatalf("unexpected additional merged block")
			}
		})
	}
}

func TestMergeDownsamplingIndependentOfSourceBlockLayout(t *testing.T) {
	restore := setDownsamplingConfigForTest(0, 0, 0)
	defer restore()

	newInmemoryPart := func(timestamps ...int64) *inmemoryPart {
		rows := make([]rawRow, len(timestamps))
		for i, timestamp := range timestamps {
			rows[i] = rawRow{
				TSID:          TSID{MetricID: 1},
				Timestamp:     timestamp,
				Value:         float64(timestamp),
				PrecisionBits: 64,
			}
		}
		mp := &inmemoryPart{}
		mp.InitFromRows(rows)
		return mp
	}

	separateParts := []*inmemoryPart{
		newInmemoryPart(801, 802),
		newInmemoryPart(803, 804),
		newInmemoryPart(901),
	}
	mixedParts := []*inmemoryPart{
		newInmemoryPart(801, 802, 803, 804, 901),
	}

	SetDedupInterval(2 * time.Microsecond)
	SetDownsamplingPeriod(100*time.Microsecond, 10*time.Microsecond)

	merge := func(mps []*inmemoryPart) []int64 {
		pws := make([]*partWrapper, len(mps))
		bsrs := make([]*blockStreamReader, len(mps))
		for i, mp := range mps {
			pws[i] = &partWrapper{p: &part{ph: mp.ph}}
			bsrs[i] = &blockStreamReader{}
			bsrs[i].MustInitFromInmemoryPart(mp)
		}
		dedupInterval := getDedupIntervalForParts(pws, 1_000)
		if dedupInterval != 2 {
			t.Fatalf("the maximum input timestamp must select the base interval; got %d; want %d", dedupInterval, 2)
		}

		var mpOut inmemoryPart
		var bsw blockStreamWriter
		bsw.MustInitFromInmemoryPart(&mpOut, -5)
		var rowsMerged, rowsDeleted atomic.Uint64
		if err := mergeBlockStreams(&mpOut.ph, &bsw, bsrs, nil, &uint64set.Set{}, 0, dedupInterval, &rowsMerged, &rowsDeleted); err != nil {
			t.Fatalf("cannot merge block streams: %s", err)
		}
		if got := rowsMerged.Load(); got != 5 {
			t.Fatalf("unexpected merged rows count; got %d; want %d", got, 5)
		}
		if got := mpOut.ph.MinDedupInterval; got != 2 {
			t.Fatalf("unexpected output interval; got %d; want %d", got, 2)
		}

		var timestamps []int64
		var bsr blockStreamReader
		bsr.MustInitFromInmemoryPart(&mpOut)
		for bsr.NextBlock() {
			if err := bsr.Block.UnmarshalData(); err != nil {
				t.Fatalf("cannot unmarshal merged block: %s", err)
			}
			timestamps = append(timestamps, bsr.Block.timestamps...)
		}
		if err := bsr.Error(); err != nil {
			t.Fatalf("cannot read merged blocks: %s", err)
		}
		return timestamps
	}

	separateTimestamps := merge(separateParts)
	mixedTimestamps := merge(mixedParts)
	if !slices.Equal(separateTimestamps, mixedTimestamps) {
		t.Fatalf("merge result depends on source block layout; separate=%v; mixed=%v", separateTimestamps, mixedTimestamps)
	}
	want := []int64{802, 804, 901}
	if !slices.Equal(separateTimestamps, want) {
		t.Fatalf("unexpected merged timestamps; got %v; want %v", separateTimestamps, want)
	}
}

func TestMergeOversizedMarshaledBlockDeduplicatesBeforeSplit(t *testing.T) {
	const dedupInterval = int64(10)
	rowsCount := maxRowsPerBlock + 2
	timestamps := make([]int64, rowsCount)
	values := make([]int64, rowsCount)
	for i := range rowsCount {
		bucket := i
		timestamp := int64(bucket)*dedupInterval + 3
		if i == maxRowsPerBlock {
			bucket = i - 1
			timestamp = int64(bucket)*dedupInterval + 8
		} else if i > maxRowsPerBlock {
			bucket = i - 1
			timestamp = int64(bucket)*dedupInterval + 3
		}
		timestamps[i] = timestamp
		values[i] = timestamp
	}
	oldSplitBoundaryTimestamp := timestamps[maxRowsPerBlock-1]
	newSplitBoundaryTimestamp := timestamps[maxRowsPerBlock]

	var sourceBlock Block
	sourceBlock.Init(&TSID{MetricID: 1}, timestamps, values, 0, 64)
	var sourcePart inmemoryPart
	sourcePart.Reset()
	var sourceWriter blockStreamWriter
	sourceWriter.MustInitFromInmemoryPart(&sourcePart, -5)
	var sourceRowsMerged uint64
	sourceWriter.WriteExternalBlock(&sourceBlock, &sourcePart.ph, &sourceRowsMerged, 0)
	sourceWriter.MustClose()
	if got := sourcePart.ph.BlocksCount; got != 1 {
		t.Fatalf("test setup must create a single oversized marshaled block; got %d blocks; want %d", got, 1)
	}
	if got := sourcePart.ph.RowsCount; got != uint64(rowsCount) {
		t.Fatalf("unexpected source rows count; got %d; want %d", got, rowsCount)
	}

	var sourceReader blockStreamReader
	sourceReader.MustInitFromInmemoryPart(&sourcePart)
	var destinationPart inmemoryPart
	var destinationWriter blockStreamWriter
	destinationWriter.MustInitFromInmemoryPart(&destinationPart, -5)
	var rowsMerged, rowsDeleted atomic.Uint64
	if err := mergeBlockStreams(&destinationPart.ph, &destinationWriter, []*blockStreamReader{&sourceReader}, nil,
		&uint64set.Set{}, 0, dedupInterval, &rowsMerged, &rowsDeleted); err != nil {
		t.Fatalf("cannot merge oversized marshaled block: %s", err)
	}
	if got := rowsMerged.Load(); got != uint64(rowsCount) {
		t.Fatalf("unexpected merged rows accounting; got %d; want %d", got, rowsCount)
	}
	if got := destinationPart.ph.RowsCount; got != uint64(rowsCount-1) {
		t.Fatalf("unexpected destination rows count; got %d; want %d", got, rowsCount-1)
	}
	if got := destinationPart.ph.BlocksCount; got != 2 {
		t.Fatalf("deduplicated oversized block must be split into two output blocks; got %d; want %d", got, 2)
	}

	var got []int64
	var destinationReader blockStreamReader
	destinationReader.MustInitFromInmemoryPart(&destinationPart)
	for destinationReader.NextBlock() {
		if rows := destinationReader.Block.RowsCount(); rows > maxRowsPerBlock {
			t.Fatalf("output block contains too many rows; got %d; want at most %d", rows, maxRowsPerBlock)
		}
		if err := destinationReader.Block.UnmarshalData(); err != nil {
			t.Fatalf("cannot unmarshal destination block: %s", err)
		}
		got = append(got, destinationReader.Block.timestamps...)
	}
	if err := destinationReader.Error(); err != nil {
		t.Fatalf("cannot read destination blocks: %s", err)
	}
	if slices.Contains(got, oldSplitBoundaryTimestamp) {
		t.Fatalf("old split-boundary sample %d must be removed", oldSplitBoundaryTimestamp)
	}
	if !slices.Contains(got, newSplitBoundaryTimestamp) {
		t.Fatalf("new split-boundary winner %d must be retained", newSplitBoundaryTimestamp)
	}
}

func TestMergeDownsamplingAcrossThreeOverlappingLargeStreams(t *testing.T) {
	type streamConfig struct {
		start int64
		step  int64
	}
	testCases := []struct {
		name          string
		streams       []streamConfig
		dedupInterval int64
	}{
		{
			name: "overlap before the first safe output block",
			streams: []streamConfig{
				{start: 0, step: 3},
				{start: 1, step: 3},
				{start: 2, step: 3},
			},
			dedupInterval: 2,
		},
		{
			name: "flush a safe prefix before overlapping tail",
			streams: []streamConfig{
				{start: 0, step: 2},
				{start: 1, step: 2},
				{start: 10_000, step: 1},
			},
			dedupInterval: 1,
		},
		{
			name: "keep a pending block when the entire prefix is safe",
			streams: []streamConfig{
				{start: 0, step: 2},
				{start: 1, step: 2},
				{start: 20_000, step: 1},
			},
			dedupInterval: 1,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bsrs := make([]*blockStreamReader, 0, len(tc.streams))
			allTimestamps := make([]int64, 0, len(tc.streams)*maxRowsPerBlock)
			for _, stream := range tc.streams {
				rows := make([]rawRow, maxRowsPerBlock)
				for i := range rows {
					timestamp := stream.start + stream.step*int64(i)
					rows[i] = rawRow{
						TSID:          TSID{MetricID: 1},
						Timestamp:     timestamp,
						Value:         float64(timestamp),
						PrecisionBits: 64,
					}
					allTimestamps = append(allTimestamps, timestamp)
				}
				bsrs = append(bsrs, newTestBlockStreamReader(rows))
			}

			var mp inmemoryPart
			var bsw blockStreamWriter
			bsw.MustInitFromInmemoryPart(&mp, -5)
			var rowsMerged, rowsDeleted atomic.Uint64
			if err := mergeBlockStreams(&mp.ph, &bsw, bsrs, nil, &uint64set.Set{}, 0, tc.dedupInterval, &rowsMerged, &rowsDeleted); err != nil {
				t.Fatalf("cannot merge streams: %s", err)
			}
			if got, want := rowsMerged.Load(), uint64(len(allTimestamps)); got != want {
				t.Fatalf("unexpected merged rows count; got %d; want %d", got, want)
			}

			var got []int64
			var bsr blockStreamReader
			bsr.MustInitFromInmemoryPart(&mp)
			for bsr.NextBlock() {
				if got := bsr.Block.RowsCount(); got > maxRowsPerBlock {
					t.Fatalf("merged block contains too many rows; got %d; want at most %d", got, maxRowsPerBlock)
				}
				if err := bsr.Block.UnmarshalData(); err != nil {
					t.Fatalf("cannot unmarshal merged block: %s", err)
				}
				got = append(got, bsr.Block.timestamps...)
			}
			if err := bsr.Error(); err != nil {
				t.Fatalf("cannot read merged blocks: %s", err)
			}

			sort.Slice(allTimestamps, func(i, j int) bool {
				return allTimestamps[i] < allTimestamps[j]
			})
			allValues := append([]int64(nil), allTimestamps...)
			want, _ := deduplicateSamplesDuringMerge(allTimestamps, allValues, tc.dedupInterval)
			if gotRowsCount, wantRowsCount := mp.ph.RowsCount, uint64(len(want)); gotRowsCount != wantRowsCount {
				t.Fatalf("unexpected part rows count; got %d; want %d", gotRowsCount, wantRowsCount)
			}
			if got := rowsDeleted.Load(); got != 0 {
				t.Fatalf("unexpected deleted rows count; got %d; want 0", got)
			}
			if !slices.Equal(got, want) {
				firstDiff := min(len(got), len(want))
				for i := range firstDiff {
					if got[i] != want[i] {
						firstDiff = i
						break
					}
				}
				t.Fatalf("merge result depends on intermediate stream layout; got %d timestamps; want %d; first difference at %d", len(got), len(want), firstDiff)
			}
		})
	}
}

func TestIsFinalMergeNeededForDownsampling(t *testing.T) {
	restore := setDownsamplingConfigForTest(2*time.Microsecond, 100*time.Microsecond, 10*time.Microsecond)
	defer restore()

	newPart := func(maxTimestamp, minDedupInterval int64) *partWrapper {
		return &partWrapper{
			p: &part{
				ph: partHeader{
					MinTimestamp:     maxTimestamp,
					MaxTimestamp:     maxTimestamp,
					MinDedupInterval: minDedupInterval,
				},
			},
		}
	}

	const currentTimestamp = 1_000
	eligiblePart := newPart(900, 2)
	if !isFinalMergeNeededForParts([]*partWrapper{eligiblePart}, currentTimestamp) {
		t.Fatalf("a fully eligible part must be scheduled based on its actual maximum timestamp")
	}

	freshPart := newPart(901, 2)
	if isFinalMergeNeededForParts([]*partWrapper{freshPart}, currentTimestamp) {
		t.Fatalf("a fresh part must not trigger a final downsampling merge")
	}
	if isFinalMergeNeededForParts([]*partWrapper{eligiblePart, freshPart}, currentTimestamp) {
		t.Fatalf("downsampling must wait until every file part is fully eligible")
	}

	rebucketPart := newPart(900, 20)
	if !isFinalMergeNeededForParts([]*partWrapper{rebucketPart}, currentTimestamp) {
		t.Fatalf("an eligible part with a different MinDedupInterval must be re-bucketed")
	}

	processedPartA := newPart(899, 10)
	processedPartB := newPart(900, 10)
	if !isFinalMergeNeededForParts([]*partWrapper{processedPartA, processedPartB}, currentTimestamp) {
		t.Fatalf("multiple eligible parts must trigger one reconciliation merge")
	}
	if isFinalMergeNeededForParts([]*partWrapper{newPart(900, 10)}, currentTimestamp) {
		t.Fatalf("a single eligible part already marked with the downsampling interval is complete")
	}

	SetDownsamplingPeriod(0, 0)
	SetDedupInterval(5 * time.Microsecond)
	if !isFinalMergeNeededForParts([]*partWrapper{newPart(901, 2)}, currentTimestamp) {
		t.Fatalf("ordinary final deduplication must remain enabled when downsampling is disabled")
	}
	if isFinalMergeNeededForParts([]*partWrapper{newPart(901, 5)}, currentTimestamp) {
		t.Fatalf("a part already satisfying ordinary deduplication must not be merged")
	}
}

func TestPartitionFinalMergeEligibilityIncludesInmemoryParts(t *testing.T) {
	restore := setDownsamplingConfigForTest(2*time.Microsecond, 100*time.Microsecond, 10*time.Microsecond)
	defer restore()

	newPart := func(maxTimestamp, minDedupInterval int64) *partWrapper {
		pw := &partWrapper{
			p: &part{
				ph: partHeader{
					MinTimestamp:     maxTimestamp,
					MaxTimestamp:     maxTimestamp,
					MinDedupInterval: minDedupInterval,
				},
			},
		}
		pw.incRef()
		return pw
	}

	const currentTimestamp = 1_000
	processedFilePart := newPart(900, 10)
	freshInmemoryPart := newPart(901, 2)
	pt := &partition{
		name:          timestampToPartitionName(currentTimestamp),
		inmemoryParts: []*partWrapper{freshInmemoryPart},
		smallParts:    []*partWrapper{processedFilePart},
	}
	if pt.isFinalMergeNeeded(currentTimestamp) {
		t.Fatalf("final downsampling must wait until every in-memory and file part is fully eligible")
	}

	freshInmemoryPart.p.ph.MinTimestamp = 900
	freshInmemoryPart.p.ph.MaxTimestamp = 900
	freshInmemoryPart.p.ph.MinDedupInterval = 10
	if !pt.isFinalMergeNeeded(currentTimestamp) {
		t.Fatalf("a late second eligible part must trigger a reconciliation merge")
	}

	pt.inmemoryParts = nil
	pt.smallParts = []*partWrapper{processedFilePart}
	if pt.isFinalMergeNeeded(currentTimestamp) {
		t.Fatalf("the reconciled part must not trigger another final merge")
	}

	unprocessedCurrentMonthPart := newPart(900, 2)
	pt.smallParts = []*partWrapper{unprocessedCurrentMonthPart}
	if !pt.isFinalMergeNeeded(currentTimestamp) {
		t.Fatalf("an aged part in the current month must be checked for final downsampling")
	}
}

func TestSkipCurrentPartitionForFinalMerge(t *testing.T) {
	restore := setDownsamplingConfigForTest(2*time.Microsecond, 0, 0)
	defer restore()

	const currentPartitionName = "2026_07"
	if !skipCurrentPartitionForFinalMerge(currentPartitionName, currentPartitionName) {
		t.Fatalf("deduplication-only operation must skip the current-month partition")
	}
	if skipCurrentPartitionForFinalMerge("2026_06", currentPartitionName) {
		t.Fatalf("deduplication-only operation must not skip a historical partition")
	}

	SetDownsamplingPeriod(time.Hour, time.Minute)
	if skipCurrentPartitionForFinalMerge(currentPartitionName, currentPartitionName) {
		t.Fatalf("downsampling must include the current-month partition")
	}
}

func TestInitialRawBlockBoundaryDownsamplesOnlyDuringMerge(t *testing.T) {
	defer testRemoveAll(t)

	restore := setDownsamplingConfigForTest(0, 30*time.Minute, 10*time.Microsecond)
	defer restore()

	sampleTimestamp := time.Now().Add(-time.Hour).UnixMicro()
	s := newTestStorage()
	defer stopTestStorage(s)
	pt := testCreatePartition(t, sampleTimestamp, s)
	defer pt.MustClose()

	rows := make([]rawRow, maxRowsPerBlock+1)
	for i := range rows {
		rows[i] = rawRow{
			TSID:          TSID{MetricID: 1},
			Timestamp:     sampleTimestamp,
			Value:         float64(i),
			PrecisionBits: 64,
		}
	}
	mp := getInmemoryPart()
	mp.InitFromRows(rows)
	if got := mp.ph.RowsCount; got != maxRowsPerBlock+1 {
		t.Fatalf("raw part creation must preserve samples across the initial block boundary; got %d rows; want %d", got, maxRowsPerBlock+1)
	}
	if got := mp.ph.BlocksCount; got != 2 {
		t.Fatalf("unexpected raw blocks count; got %d; want %d", got, 2)
	}
	if got := mp.ph.MinDedupInterval; got != 0 {
		t.Fatalf("raw part creation must record only the base interval; got %d; want %d", got, 0)
	}

	pw := newPartWrapperFromInmemoryPart(mp, time.Time{})
	pt.partsLock.Lock()
	pt.inmemoryParts = append(pt.inmemoryParts, pw)
	pt.partsLock.Unlock()

	if err := pt.ForceMergeAllParts(nil); err != nil {
		t.Fatalf("cannot force-merge in-memory part: %s", err)
	}
	if got := pt.smallMergesCount.Load(); got != 1 {
		t.Fatalf("an old raw part must be physically downsampled during the merge; got %d block-stream merges", got)
	}
	if got := pt.smallRowsMerged.Load(); got != maxRowsPerBlock+1 {
		t.Fatalf("unexpected merged rows accounting; got %d; want %d", got, maxRowsPerBlock+1)
	}

	pt.partsLock.Lock()
	if got := len(pt.inmemoryParts); got != 0 {
		pt.partsLock.Unlock()
		t.Fatalf("unexpected in-memory parts count; got %d; want %d", got, 0)
	}
	if got := len(pt.smallParts); got != 1 {
		pt.partsLock.Unlock()
		t.Fatalf("unexpected small parts count; got %d; want %d", got, 1)
	}
	ph := pt.smallParts[0].p.ph
	pt.partsLock.Unlock()

	if got := ph.RowsCount; got != 1 {
		t.Fatalf("the merge must retain one physical winner; got %d rows; want %d", got, 1)
	}
	if got := ph.MinDedupInterval; got != 10 {
		t.Fatalf("unexpected interval after merge; got %d; want %d", got, 10)
	}
}

func TestSingleInmemoryPartWithDedupReconcilesBlockBoundary(t *testing.T) {
	defer testRemoveAll(t)

	restore := setDownsamplingConfigForTest(10*time.Microsecond, 0, 0)
	defer restore()

	now := time.Now().UnixMicro()
	s := newTestStorage()
	defer stopTestStorage(s)
	pt := testCreatePartition(t, now, s)
	defer pt.MustClose()

	rows := make([]rawRow, maxRowsPerBlock+1)
	for i := range rows {
		rows[i] = rawRow{
			TSID:          TSID{MetricID: 1},
			Timestamp:     now,
			Value:         float64(i),
			PrecisionBits: 64,
		}
	}
	mp := getInmemoryPart()
	mp.InitFromRows(rows)
	if got := mp.ph.RowsCount; got != 2 {
		t.Fatalf("raw block-local deduplication must leave one winner per source block; got %d rows; want %d", got, 2)
	}
	pw := newPartWrapperFromInmemoryPart(mp, time.Time{})
	pt.partsLock.Lock()
	pt.inmemoryParts = append(pt.inmemoryParts, pw)
	pt.partsLock.Unlock()

	if err := pt.ForceMergeAllParts(nil); err != nil {
		t.Fatalf("cannot force-merge in-memory part: %s", err)
	}
	if got := pt.smallMergesCount.Load(); got != 1 {
		t.Fatalf("deduplication must use the block-stream merge path; got %d merges; want %d", got, 1)
	}
	if got := pt.smallRowsMerged.Load(); got != 2 {
		t.Fatalf("unexpected merged rows accounting; got %d; want %d", got, 2)
	}

	pt.partsLock.Lock()
	if got := len(pt.smallParts); got != 1 {
		pt.partsLock.Unlock()
		t.Fatalf("unexpected small parts count; got %d; want %d", got, 1)
	}
	ph := pt.smallParts[0].p.ph
	pt.partsLock.Unlock()
	if got := ph.RowsCount; got != 1 {
		t.Fatalf("the merge must reconcile the deduplication bucket across source blocks; got %d rows; want %d", got, 1)
	}
	if got := ph.MinDedupInterval; got != 10 {
		t.Fatalf("unexpected interval after merge; got %d; want %d", got, 10)
	}
}

func TestSingleFreshInmemoryPartUsesFlushFastPathWithDownsampling(t *testing.T) {
	defer testRemoveAll(t)

	restore := setDownsamplingConfigForTest(0, 30*24*time.Hour, 10*time.Microsecond)
	defer restore()

	s := newTestStorage()
	defer stopTestStorage(s)
	now := time.Now().UnixMicro()
	pt := testCreatePartition(t, now, s)
	defer pt.MustClose()

	rows := []rawRow{{
		TSID:          TSID{MetricID: 1},
		Timestamp:     now,
		Value:         1,
		PrecisionBits: 64,
	}}
	mp := getInmemoryPart()
	mp.InitFromRows(rows)
	pw := newPartWrapperFromInmemoryPart(mp, time.Time{})
	pw.isInMerge = true
	pt.partsLock.Lock()
	pt.inmemoryParts = append(pt.inmemoryParts, pw)
	pt.partsLock.Unlock()

	if err := pt.mergeParts([]*partWrapper{pw}, nil, true, false); err != nil {
		t.Fatalf("cannot flush in-memory part: %s", err)
	}
	if got := pt.smallMergesCount.Load(); got != 0 {
		t.Fatalf("fresh single-part flush must use the direct path; got %d block-stream merges", got)
	}
}

func setDownsamplingConfigForTest(dedupInterval, offset, interval time.Duration) func() {
	prevDedupInterval := globalDedupInterval
	prevDownsamplingOffset := globalDownsamplingOffset.Load()
	prevDownsamplingInterval := globalDownsamplingInterval.Load()
	SetDedupInterval(dedupInterval)
	SetDownsamplingPeriod(offset, interval)
	return func() {
		SetDedupInterval(time.Duration(prevDedupInterval) * time.Microsecond)
		SetDownsamplingPeriod(
			time.Duration(prevDownsamplingOffset)*time.Microsecond,
			time.Duration(prevDownsamplingInterval)*time.Microsecond,
		)
	}
}
