package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestAddJitterOverflowSafe(t *testing.T) {
	const maxDuration = time.Duration(1<<63 - 1)
	if got := addJitter(maxDuration); got != maxDuration {
		t.Fatalf("jitter near MaxInt64 must be skipped; got %s; want %s", got, maxDuration)
	}

	const d = 4 * time.Hour
	got := addJitter(d)
	if got < d || got >= d+d/4 {
		t.Fatalf("unexpected jittered duration; got %s; want in [%s, %s)", got, d, d+d/4)
	}
}

func TestTableOpenClose(t *testing.T) {
	const path = "TestTableOpenClose"
	const retention = 123 * retention31Days

	fs.MustRemoveDir(path)
	defer fs.MustRemoveDir(path)

	// Create a new table
	strg := newTestStorage()
	strg.retentionUsecs = retention.Microseconds()
	tb := mustOpenTable(path, strg)

	// Close it
	tb.MustClose()

	// Re-open created table multiple times.
	for range 10 {
		tb := mustOpenTable(path, strg)
		tb.MustClose()
	}

	stopTestStorage(strg)
}

func TestGetPartition(t *testing.T) {
	defer testRemoveAll(t)

	s := MustOpenStorage(t.Name(), OpenOptions{})
	defer s.MustClose()

	var ptw *partitionWrapper
	timestamp := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()

	ptw = s.tb.GetPartition(timestamp)
	if ptw != nil {
		name := ptw.pt.name
		s.tb.PutPartition(ptw)
		t.Fatalf("GetPartition() unexpectedly returned a partition that should not exist: %s", name)
	}

	ptw = s.tb.MustGetPartition(timestamp)
	if ptw == nil {
		t.Fatalf("MustGetPartition() unexpectedly did not create a new partition")
	}
	s.tb.PutPartition(ptw)

	ptw = s.tb.GetPartition(timestamp)
	if ptw == nil {
		t.Fatalf("GetPartition() unexpectedly did not find partition")
	}
	s.tb.PutPartition(ptw)
}

func TestGetPartition_concurrent(t *testing.T) {
	defer testRemoveAll(t)

	s := MustOpenStorage(t.Name(), OpenOptions{})
	defer s.MustClose()

	begin := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	limit := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	for ts := begin; ts < limit; ts += usecPerDay {
		var wg sync.WaitGroup
		for range 100 {
			wg.Go(func() {
				ptw := s.tb.MustGetPartition(ts)
				s.tb.PutPartition(ptw)

				ptw = s.tb.GetPartition(ts)
				s.tb.PutPartition(ptw)
			})
		}
		wg.Wait()
	}
}
