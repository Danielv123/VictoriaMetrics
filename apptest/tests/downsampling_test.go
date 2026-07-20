package tests

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/VictoriaMetrics/VictoriaMetrics/apptest"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

const (
	testDownsamplingPeriod = "10y:10us"

	oldBaseUsecs = int64(946684800000000) // 2000-01-01T00:00:00Z

	oldRangeStart = "2000-01-01T00:00:00Z"
	oldRangeEnd   = "2000-01-01T00:00:01Z"
)

type downsamplingSample struct {
	Timestamp int64
	Value     float64
}

func TestSingleDownsamplingQueryMergeLateDataAndExports(t *testing.T) {
	testStart := time.Now().UTC()
	recentRangeStartTime := testStart.Add(-time.Hour).Truncate(time.Second)
	recentBaseUsecs := recentRangeStartTime.UnixMicro()
	recentRangeStart := recentRangeStartTime.Format(time.RFC3339Nano)
	recentRangeEnd := recentRangeStartTime.Add(time.Second).Format(time.RFC3339Nano)

	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	const (
		metric         = "single_downsampling_lifecycle"
		boundaryMetric = "single_downsampling_query_boundary"
	)
	storagePath := t.TempDir() + "/vmsingle"

	// Seed raw parts before enabling downsampling. This makes it possible to
	// verify query-time normalization independently from physical materialization.
	sut := tc.MustStartVmsingle("vmsingle", singleDownsamplingFlags(storagePath, ""))
	importVMRemoteWriteSamples(t, sut, metric, []downsamplingSample{
		{Timestamp: oldBaseUsecs + 1, Value: 1},
		{Timestamp: oldBaseUsecs + 2, Value: 2},
		{Timestamp: oldBaseUsecs + 3, Value: 3},
		{Timestamp: recentBaseUsecs + 1, Value: 10},
		{Timestamp: recentBaseUsecs + 2, Value: 11},
		{Timestamp: recentBaseUsecs + 3, Value: 12},
	})
	importVMRemoteWriteSamples(t, sut, boundaryMetric, []downsamplingSample{
		{Timestamp: oldBaseUsecs + 3, Value: 3},
		{Timestamp: oldBaseUsecs + 8, Value: 8},
	})
	sut.ForceFlush(t)
	tc.StopApp("vmsingle")

	sut = tc.MustStartVmsingle("vmsingle", singleDownsamplingFlags(storagePath, testDownsamplingPeriod))

	boundaryRangeEnd := time.UnixMicro(oldBaseUsecs + 5).UTC().Format(time.RFC3339Nano)
	assertDownsamplingSamples(t, "upper-boundary bucket is omitted before physical merge", nil,
		exportRawSamples(t, tc.Client(), sut, boundaryMetric, oldRangeStart, boundaryRangeEnd, false))
	assertDownsamplingSamples(t, "physical export still exposes the in-range sample before merge", []downsamplingSample{
		{Timestamp: oldBaseUsecs + 3, Value: 3},
	}, exportRawSamples(t, tc.Client(), sut, boundaryMetric, oldRangeStart, boundaryRangeEnd, true))

	oldRaw := []downsamplingSample{
		{Timestamp: oldBaseUsecs + 1, Value: 1},
		{Timestamp: oldBaseUsecs + 2, Value: 2},
		{Timestamp: oldBaseUsecs + 3, Value: 3},
	}
	oldNormalized := []downsamplingSample{
		{Timestamp: oldBaseUsecs + 3, Value: 3},
	}
	recentRaw := []downsamplingSample{
		{Timestamp: recentBaseUsecs + 1, Value: 10},
		{Timestamp: recentBaseUsecs + 2, Value: 11},
		{Timestamp: recentBaseUsecs + 3, Value: 12},
	}

	assertDownsamplingSamples(t, "reduce_mem_usage export is physical before merge", oldRaw,
		exportRawSamples(t, tc.Client(), sut, metric, oldRangeStart, oldRangeEnd, true))
	assertDownsamplingSamples(t, "old query-time normalization", oldNormalized,
		exportRawSamples(t, tc.Client(), sut, metric, oldRangeStart, oldRangeEnd, false))
	assertDownsamplingSamples(t, "recent-only query keeps raw resolution", recentRaw,
		exportRawSamples(t, tc.Client(), sut, metric, recentRangeStart, recentRangeEnd, false))
	assertDownsamplingSamples(t, "range crossing the cutoff uses one interval for the full fetch", []downsamplingSample{
		{Timestamp: oldBaseUsecs + 3, Value: 3},
		{Timestamp: recentBaseUsecs + 3, Value: 12},
	}, exportRawSamples(t, tc.Client(), sut, metric, oldRangeStart, recentRangeEnd, false))

	// Default JSON and CSV exports use normalized search results, while
	// reduce_mem_usage and native export expose the physical parts.
	assertDownsamplingSamples(t, "default CSV export is normalized", oldNormalized,
		exportCSVSamples(t, tc.Client(), sut, metric, oldRangeStart, oldRangeEnd, false))

	nativeData := sut.PrometheusAPIV1ExportNative(t, metric, apptest.QueryOpts{
		Start: oldRangeStart,
		End:   oldRangeEnd,
	})
	nativeReceiver := tc.MustStartVmsingle("native-receiver", singleDownsamplingFlags(t.TempDir()+"/native-receiver", ""))
	nativeReceiver.PrometheusAPIV1ImportNative(t, nativeData, apptest.QueryOpts{})
	nativeReceiver.ForceFlush(t)
	assertDownsamplingSamples(t, "native export is physical before merge", oldRaw,
		exportRawSamples(t, tc.Client(), nativeReceiver, metric, oldRangeStart, oldRangeEnd, true))

	forceMergeAndWaitForRawSamples(tc, sut, metric, oldRangeStart, oldRangeEnd, oldNormalized)
	assertDownsamplingSamples(t, "upper-boundary bucket is omitted after physical merge", nil,
		exportRawSamples(t, tc.Client(), sut, boundaryMetric, oldRangeStart, boundaryRangeEnd, false))
	assertDownsamplingSamples(t, "physical merge retains only the out-of-range bucket winner", nil,
		exportRawSamples(t, tc.Client(), sut, boundaryMetric, oldRangeStart, boundaryRangeEnd, true))
	assertDownsamplingSamples(t, "recent physical samples remain at raw resolution", recentRaw,
		exportRawSamples(t, tc.Client(), sut, metric, recentRangeStart, recentRangeEnd, true))

	// A late old part can overlap an already materialized bucket. Search-time
	// normalization must pick the newest sample before a reconciliation merge.
	importVMRemoteWriteSamples(t, sut, metric, []downsamplingSample{
		{Timestamp: oldBaseUsecs + 8, Value: 8},
	})
	sut.ForceFlush(t)

	lateWinner := []downsamplingSample{
		{Timestamp: oldBaseUsecs + 8, Value: 8},
	}
	assertDownsamplingSamples(t, "late old sample wins before reconciliation", lateWinner,
		exportRawSamples(t, tc.Client(), sut, metric, oldRangeStart, oldRangeEnd, false))
	assertDownsamplingSamples(t, "late old part is physically separate before reconciliation", []downsamplingSample{
		{Timestamp: oldBaseUsecs + 3, Value: 3},
		{Timestamp: oldBaseUsecs + 8, Value: 8},
	}, exportRawSamples(t, tc.Client(), sut, metric, oldRangeStart, oldRangeEnd, true))

	forceMergeAndWaitForRawSamples(tc, sut, metric, oldRangeStart, oldRangeEnd, lateWinner)
}

func TestSingleDownsamplingPolicyLatch(t *testing.T) {
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	storagePath := t.TempDir() + "/vmsingle"
	flags := singleDownsamplingFlags(storagePath, testDownsamplingPeriod)

	tc.MustStartVmsingle("lock-holder", singleDownsamplingFlags(storagePath, ""))
	assertVmsingleStartupFailure(t, storagePath, testDownsamplingPeriod, "make sure a single process has exclusive access")
	policyPath := filepath.Join(storagePath, "metadata", "downsampling-policy.json")
	if _, err := os.Stat(policyPath); !os.IsNotExist(err) {
		t.Fatalf("a process that fails to acquire the storage lock must not latch a downsampling policy; got err=%v", err)
	}
	tc.StopApp("lock-holder")

	tc.MustStartVmsingle("vmsingle", flags)
	tc.StopApp("vmsingle")
	tc.MustStartVmsingle("vmsingle", flags)
	tc.StopApp("vmsingle")

	assertVmsingleStartupFailure(t, storagePath, "", "-downsampling.period must be set to")
	assertVmsingleStartupFailure(t, storagePath, "10y:20us", "differs from the latched downsampling policy")
}

func TestSingleDownsamplingCacheSafety(t *testing.T) {
	testCases := []struct {
		name               string
		downsamplingPeriod string
		wantCacheRequests  bool
	}{
		{
			name:               "enabled",
			downsamplingPeriod: testDownsamplingPeriod,
		},
		{
			name:              "disabled",
			wantCacheRequests: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			testStart := time.Now().UTC()
			cacheBase := testStart.Add(-4 * time.Hour).Truncate(time.Hour)
			sampleTime := cacheBase.Add(30 * time.Minute)
			rangeStart := cacheBase.Add(time.Hour).Format(time.RFC3339Nano)
			rangeEnd := cacheBase.Add(3 * time.Hour).Format(time.RFC3339Nano)
			instantTime := cacheBase.Add(2 * time.Hour).Format(time.RFC3339Nano)

			tc := apptest.NewTestCase(t)
			defer tc.Stop()

			sut := tc.MustStartVmsingle("vmsingle", singleDownsamplingFlags(t.TempDir()+"/vmsingle", testCase.downsamplingPeriod))
			sut.PrometheusAPIV1ImportPrometheus(t, []string{
				"single_downsampling_cache 1 " + strconv.FormatInt(sampleTime.UnixMilli(), 10),
			}, apptest.QueryOpts{})
			sut.ForceFlush(t)

			const (
				cacheRequestsMetric = "vm_rollup_result_cache_requests_total"
				rangeRequestsMetric = `vm_http_requests_total{path="/api/v1/query_range"}`
				queryRequestsMetric = `vm_http_requests_total{path="/api/v1/query"}`
				query               = "avg_over_time(single_downsampling_cache[3h])"
			)
			requestsBefore := sut.GetIntMetric(t, cacheRequestsMetric)
			rangeRequestsBefore := sut.GetIntMetric(t, rangeRequestsMetric)

			for range 2 {
				sut.PrometheusAPIV1QueryRange(t, query, apptest.QueryOpts{
					Start: rangeStart,
					End:   rangeEnd,
					Step:  "1h",
				})
			}
			waitForMetricAtLeast(tc, sut, rangeRequestsMetric, rangeRequestsBefore+2)
			requestsAfterRange := sut.GetIntMetric(t, cacheRequestsMetric)
			assertCacheCounterChange(t, "range rollup cache requests", requestsBefore, requestsAfterRange, testCase.wantCacheRequests)

			queryRequestsBefore := sut.GetIntMetric(t, queryRequestsMetric)
			for range 2 {
				sut.PrometheusAPIV1Query(t, query, apptest.QueryOpts{
					Time: instantTime,
				})
			}
			waitForMetricAtLeast(tc, sut, queryRequestsMetric, queryRequestsBefore+2)
			requestsAfterInstant := sut.GetIntMetric(t, cacheRequestsMetric)
			assertCacheCounterChange(t, "instant rollup cache requests", requestsAfterRange, requestsAfterInstant, testCase.wantCacheRequests)
		})
	}
}

func TestSingleDownsamplingExpandedFetchRespectsDenyQueriesOutsideRetention(t *testing.T) {
	now := time.Now().UTC()
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	untilNoon := 12*time.Hour - now.Sub(midnight)
	if untilNoon < 0 {
		untilNoon += 24 * time.Hour
	}
	futureRetention := 48*time.Hour + untilNoon

	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	flags := singleDownsamplingFlags(t.TempDir()+"/vmsingle", "1d:1d")
	flags = append(flags,
		"-denyQueriesOutsideRetention",
		"-futureRetention="+futureRetention.String(),
	)
	sut := tc.MustStartVmsingle("vmsingle", flags)

	start := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	end := now.Add(futureRetention - time.Minute).Format(time.RFC3339Nano)
	assertDownsamplingSamples(t, "expanded final bucket remains within internal retention bounds", nil,
		exportRawSamples(t, tc.Client(), sut, "single_downsampling_retention_boundary", start, end, false))
}

func singleDownsamplingFlags(storagePath, downsamplingPeriod string) []string {
	flags := []string{
		"-storageDataPath=" + storagePath,
		"-retentionPeriod=100y",
		"-storage.finalDedupScheduleCheckInterval=1h",
	}
	if downsamplingPeriod != "" {
		flags = append(flags, "-downsampling.period="+downsamplingPeriod)
	}
	return flags
}

func importVMRemoteWriteSamples(t *testing.T, sut *apptest.Vmsingle, metric string, samples []downsamplingSample) {
	t.Helper()

	promSamples := make([]prompb.Sample, len(samples))
	for i, sample := range samples {
		promSamples[i] = prompb.Sample{
			Timestamp: sample.Timestamp,
			Value:     sample.Value,
		}
	}
	sut.VictoriaMetricsAPIV1Write(t, prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{{
			Labels:  []prompb.Label{{Name: "__name__", Value: metric}},
			Samples: promSamples,
		}},
	}, apptest.QueryOpts{})
}

func exportRawSamples(t *testing.T, cli *apptest.Client, sut *apptest.Vmsingle, metric, start, end string, reduceMemUsage bool) []downsamplingSample {
	t.Helper()

	values := url.Values{
		"match[]": {metric},
		"start":   {start},
		"end":     {end},
	}
	if reduceMemUsage {
		values.Set("reduce_mem_usage", "1")
	}
	response, statusCode := cli.PostForm(t, "http://"+sut.HTTPAddr()+"/prometheus/api/v1/export", values, nil)
	if statusCode != http.StatusOK {
		t.Fatalf("unexpected raw export status code: got %d; want %d; response=%q", statusCode, http.StatusOK, response)
	}

	var samples []downsamplingSample
	for _, line := range strings.Split(response, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var result struct {
			Values     []float64 `json:"values"`
			Timestamps []int64   `json:"timestamps"`
		}
		if err := json.Unmarshal([]byte(line), &result); err != nil {
			t.Fatalf("cannot unmarshal raw export line %q: %v", line, err)
		}
		if len(result.Values) != len(result.Timestamps) {
			t.Fatalf("raw export returned %d values and %d timestamps", len(result.Values), len(result.Timestamps))
		}
		for i, timestamp := range result.Timestamps {
			samples = append(samples, downsamplingSample{
				Timestamp: timestamp,
				Value:     result.Values[i],
			})
		}
	}
	sortDownsamplingSamples(samples)
	return samples
}

func exportCSVSamples(t *testing.T, cli *apptest.Client, sut *apptest.Vmsingle, metric, start, end string, reduceMemUsage bool) []downsamplingSample {
	t.Helper()

	values := url.Values{
		"match[]": {metric},
		"start":   {start},
		"end":     {end},
		"format":  {"__timestamp__,__value__"},
	}
	if reduceMemUsage {
		values.Set("reduce_mem_usage", "1")
	}
	response, statusCode := cli.PostForm(t, "http://"+sut.HTTPAddr()+"/prometheus/api/v1/export/csv", values, nil)
	if statusCode != http.StatusOK {
		t.Fatalf("unexpected CSV export status code: got %d; want %d; response=%q", statusCode, http.StatusOK, response)
	}

	records, err := csv.NewReader(strings.NewReader(response)).ReadAll()
	if err != nil {
		t.Fatalf("cannot parse CSV export %q: %v", response, err)
	}
	if len(records) == 0 || len(records[0]) != 2 || records[0][0] != "__timestamp__" || records[0][1] != "__value__" {
		t.Fatalf("unexpected CSV export header: %q", records)
	}
	var samples []downsamplingSample
	for _, record := range records[1:] {
		if len(record) != 2 {
			t.Fatalf("unexpected CSV export record: %q", record)
		}
		timestamp, err := strconv.ParseInt(record[0], 10, 64)
		if err != nil {
			t.Fatalf("cannot parse CSV timestamp %q: %v", record[0], err)
		}
		value, err := strconv.ParseFloat(record[1], 64)
		if err != nil {
			t.Fatalf("cannot parse CSV value %q: %v", record[1], err)
		}
		samples = append(samples, downsamplingSample{Timestamp: timestamp, Value: value})
	}
	sortDownsamplingSamples(samples)
	return samples
}

func forceMergeAndWaitForRawSamples(tc *apptest.TestCase, sut *apptest.Vmsingle, metric, start, end string, want []downsamplingSample) {
	t := tc.T()
	t.Helper()

	sut.ForceMerge(t)
	tc.Assert(&apptest.AssertOptions{
		Msg: "force merge didn't materialize downsampling",
		Got: func() any {
			return exportRawSamples(t, tc.Client(), sut, metric, start, end, true)
		},
		Want:    want,
		Retries: 100,
		Period:  20 * time.Millisecond,
		FailNow: true,
		CmpOpts: nil,
	})
	// The expected physical result proves that the asynchronous merge started,
	// so waiting for the active counter to reach zero cannot race with startup.
	tc.Assert(&apptest.AssertOptions{
		Msg: "force merge remains active",
		Got: func() any {
			return sut.GetIntMetric(t, "vm_active_force_merges")
		},
		Want:    0,
		Retries: 100,
		Period:  20 * time.Millisecond,
		FailNow: true,
	})
}

func assertDownsamplingSamples(t *testing.T, msg string, want, got []downsamplingSample) {
	t.Helper()
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("%s (-want, +got):\n%s", msg, diff)
	}
}

func sortDownsamplingSamples(samples []downsamplingSample) {
	sort.Slice(samples, func(i, j int) bool {
		if samples[i].Timestamp != samples[j].Timestamp {
			return samples[i].Timestamp < samples[j].Timestamp
		}
		return samples[i].Value < samples[j].Value
	})
}

func assertVmsingleStartupFailure(t *testing.T, storagePath, downsamplingPeriod, wantOutput string) {
	t.Helper()

	binary := os.Getenv("VMSINGLE_PATH")
	if binary == "" {
		binary = "../../bin/victoria-metrics-race"
	}
	flags := []string{
		"-storageDataPath=" + storagePath,
		"-retentionPeriod=100y",
		"-httpListenAddr=127.0.0.1:0",
		"-graphiteListenAddr=127.0.0.1:0",
		"-opentsdbListenAddr=127.0.0.1:0",
	}
	if downsamplingPeriod != "" {
		flags = append(flags, "-downsampling.period="+downsamplingPeriod)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, binary, flags...).CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("vmsingle didn't reject startup within the timeout; output=%q", output)
	}
	if err == nil {
		t.Fatalf("vmsingle unexpectedly started with -downsampling.period=%q; output=%q", downsamplingPeriod, output)
	}
	if !strings.Contains(string(output), wantOutput) {
		t.Fatalf("unexpected startup failure for -downsampling.period=%q; want output containing %q; got %q", downsamplingPeriod, wantOutput, output)
	}
}

func waitForMetricAtLeast(tc *apptest.TestCase, sut *apptest.Vmsingle, metric string, want int) {
	t := tc.T()
	t.Helper()

	tc.Assert(&apptest.AssertOptions{
		Msg: "metric didn't reach the expected value: " + metric,
		Got: func() any {
			return sut.GetIntMetric(t, metric) >= want
		},
		Want:    true,
		Retries: 100,
		Period:  20 * time.Millisecond,
		FailNow: true,
	})
}

func assertCacheCounterChange(t *testing.T, name string, before, after int, wantChange bool) {
	t.Helper()
	if wantChange {
		if after <= before {
			t.Fatalf("%s didn't increase; before=%d; after=%d", name, before, after)
		}
		return
	}
	if after != before {
		t.Fatalf("%s increased with downsampling enabled; before=%d; after=%d", name, before, after)
	}
}
