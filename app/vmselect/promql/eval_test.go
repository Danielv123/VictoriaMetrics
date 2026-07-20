package promql

import (
	"reflect"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/prometheus"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/metricsql"
)

func TestGetCommonLabelFilters(t *testing.T) {
	f := func(metrics string, lfsExpected string) {
		t.Helper()
		var tss []*timeseries
		var rows prometheus.Rows
		rows.UnmarshalWithErrLogger(metrics, func(errStr string) {
			t.Fatalf("unexpected error when parsing %s: %s", metrics, errStr)
		})
		for _, row := range rows.Rows {
			var tags []storage.Tag
			for _, tag := range row.Tags {
				tags = append(tags, storage.Tag{
					Key:   []byte(tag.Key),
					Value: []byte(tag.Value),
				})
			}
			var ts timeseries
			ts.MetricName.Tags = tags
			tss = append(tss, &ts)
		}
		lfs := getCommonLabelFilters(tss)
		var me metricsql.MetricExpr
		if len(lfs) > 0 {
			me.LabelFilterss = [][]metricsql.LabelFilter{lfs}
		}
		lfsMarshaled := me.AppendString(nil)
		if string(lfsMarshaled) != lfsExpected {
			t.Fatalf("unexpected common label filters;\ngot\n%s\nwant\n%s", lfsMarshaled, lfsExpected)
		}
	}
	f(``, `{}`)
	f(`m 1`, `{}`)
	f(`m{a="b"} 1`, `{a="b"}`)
	f(`m{c="d",a="b"} 1`, `{a="b",c="d"}`)
	f(`m1{a="foo"} 1
m2{a="bar"} 1`, `{a=~"bar|foo"}`)
	f(`m1{a="foo"} 1
m2{b="bar"} 1`, `{}`)
	f(`m1{a="foo",b="bar"} 1
m2{b="bar",c="x"} 1`, `{b="bar"}`)
}

func TestEvalConfigMayCacheWithDownsampling(t *testing.T) {
	if storage.IsDownsamplingEnabled() {
		t.Fatalf("downsampling must be disabled before the test")
	}
	prevDisableCache := *disableCache
	prevDedupInterval := storage.GetDedupInterval()
	*disableCache = false
	storage.SetDedupInterval(2 * time.Microsecond)
	defer func() {
		storage.SetDownsamplingPeriod(0, 0)
		storage.SetDedupInterval(time.Duration(prevDedupInterval) * time.Microsecond)
		*disableCache = prevDisableCache
	}()

	testCases := []struct {
		name string
		ec   *EvalConfig
		want bool
	}{
		{
			name: "instant query",
			ec:   &EvalConfig{Start: 1_000, End: 1_000, Step: 1_000, MayCache: true},
			want: true,
		},
		{
			name: "range query",
			ec:   &EvalConfig{Start: 1_000, End: 3_000, Step: 1_000, MayCache: true},
			want: true,
		},
		{
			name: "partial cache continuation",
			ec:   &EvalConfig{Start: 2_000, End: 3_000, Step: 1_000, MayCache: true},
			want: true,
		},
		{
			name: "unaligned range query",
			ec:   &EvalConfig{Start: 1_001, End: 3_000, Step: 1_000, MayCache: true},
			want: false,
		},
		{
			name: "cache disabled by evaluation config",
			ec:   &EvalConfig{Start: 1_000, End: 3_000, Step: 1_000},
			want: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name+" without downsampling", func(t *testing.T) {
			if got := tc.ec.mayCache(); got != tc.want {
				t.Fatalf("unexpected mayCache result; got %v; want %v", got, tc.want)
			}
		})
	}

	storage.SetDownsamplingPeriod(100*time.Microsecond, 10*time.Microsecond)
	for _, tc := range testCases {
		t.Run(tc.name+" with downsampling", func(t *testing.T) {
			if tc.ec.mayCache() {
				t.Fatalf("cache must be disabled when downsampling is enabled")
			}
		})
	}
}

func TestValidateMaxPointsPerSeriesFailure(t *testing.T) {
	f := func(start, end, step int64, maxPoints int) {
		t.Helper()
		if err := ValidateMaxPointsPerSeries(start, end, step, maxPoints); err == nil {
			t.Fatalf("expecting non-nil error for ValidateMaxPointsPerSeries(start=%d, end=%d, step=%d, maxPoints=%d)", start, end, step, maxPoints)
		}
	}
	// zero step
	f(0, 0, 0, 0)
	f(0, 0, 0, 1)
	// the maxPoints is smaller than the generated points
	f(0, 1, 1, 0)
	f(0, 1, 1, 1)
	f(1659962171908, 1659966077742, 5000, 700)
}

func TestValidateMaxPointsPerSeriesSuccess(t *testing.T) {
	f := func(start, end, step int64, maxPoints int) {
		t.Helper()
		if err := ValidateMaxPointsPerSeries(start, end, step, maxPoints); err != nil {
			t.Fatalf("unexpected error in ValidateMaxPointsPerSeries(start=%d, end=%d, step=%d, maxPoints=%d): %s", start, end, step, maxPoints, err)
		}
	}
	f(1, 1, 1, 2)
	f(1659962171908, 1659966077742, 5000, 800)
	f(1659962150000, 1659966070000, 10000, 393)
}

func TestCopyEvalConfigPreservesCurrentTimestamp(t *testing.T) {
	const currentTimestamp = int64(123_456_789)
	ec := &EvalConfig{
		CurrentTimestamp: currentTimestamp,
	}
	for range 2 {
		ecCopy := copyEvalConfig(ec)
		if got := ecCopy.CurrentTimestamp; got != currentTimestamp {
			t.Fatalf("unexpected current timestamp in copied config; got %d; want %d", got, currentTimestamp)
		}
		sq := storage.NewSearchQuery(1, 2, nil, 0)
		sq.SetDownsamplingCurrentTimestamp(ecCopy.CurrentTimestamp)
		if got := sq.DownsamplingCurrentTimestamp(); got != currentTimestamp {
			t.Fatalf("multiple fetches must use the same current timestamp; got %d; want %d", got, currentTimestamp)
		}
	}
}

func TestQueryStats_addSeriesFetched(t *testing.T) {
	qs := &QueryStats{}
	ec := &EvalConfig{
		QueryStats: qs,
	}
	ec.QueryStats.addSeriesFetched(1)

	if n := qs.SeriesFetched.Load(); n != 1 {
		t.Fatalf("expected to get 1; got %d instead", n)
	}

	ecNew := copyEvalConfig(ec)
	ecNew.QueryStats.addSeriesFetched(3)
	if n := qs.SeriesFetched.Load(); n != 4 {
		t.Fatalf("expected to get 4; got %d instead", n)
	}
}

func TestGetSumInstantValues(t *testing.T) {
	f := func(cached, start, end []*timeseries, timestamp int64, expectedResult []*timeseries) {
		t.Helper()

		result := getSumInstantValues(nil, cached, start, end, timestamp)
		if !reflect.DeepEqual(result, expectedResult) {
			t.Fatalf("unexpected result; got\n%v\nwant\n%v", result, expectedResult)
		}
	}
	ts := func(name string, timestamp int64, value float64) *timeseries {
		return &timeseries{
			MetricName: storage.MetricName{
				MetricGroup: []byte(name),
			},
			Timestamps: []int64{timestamp},
			Values:     []float64{value},
		}
	}

	// start - end + cached = 1
	f(
		nil,
		[]*timeseries{ts("foo", 42, 1)},
		nil,
		100,
		[]*timeseries{ts("foo", 100, 1)},
	)

	// start - end + cached = 0
	f(
		nil,
		[]*timeseries{ts("foo", 100, 1)},
		[]*timeseries{ts("foo", 10, 1)},
		100,
		[]*timeseries{ts("foo", 100, 0)},
	)

	// start - end + cached = 2
	f(
		[]*timeseries{ts("foo", 10, 1)},
		[]*timeseries{ts("foo", 100, 1)},
		nil,
		100,
		[]*timeseries{ts("foo", 100, 2)},
	)

	// start - end + cached = 1
	f(
		[]*timeseries{ts("foo", 50, 1)},
		[]*timeseries{ts("foo", 100, 1)},
		[]*timeseries{ts("foo", 10, 1)},
		100,
		[]*timeseries{ts("foo", 100, 1)},
	)

	// start - end + cached = 0
	f(
		[]*timeseries{ts("foo", 50, 1)},
		nil,
		[]*timeseries{ts("foo", 10, 1)},
		100,
		[]*timeseries{ts("foo", 100, 0)},
	)

	// start - end + cached = 1
	f(
		[]*timeseries{ts("foo", 50, 1)},
		nil,
		nil,
		100,
		[]*timeseries{ts("foo", 100, 1)},
	)
}

func TestShouldOptimizeRepeatedBinaryOpSubexprsGate(t *testing.T) {
	e, err := metricsql.Parse(`count(count(vm_requests_total) by (action,addr,cluster,endpoint)) by (action,addr,cluster) / count(count(vm_requests_total) by (action,addr,cluster,endpoint))`)
	if err != nil {
		t.Fatalf("unexpected error in metricsql.Parse(): %s", err)
	}
	be, ok := e.(*metricsql.BinaryOpExpr)
	if !ok {
		t.Fatalf("unexpected expr type; got %T; want *metricsql.BinaryOpExpr", e)
	}

	f := func(name string, ec *EvalConfig, resultExpected bool) {
		t.Helper()
		result := shouldOptimizeRepeatedBinaryOpSubexprs(ec, be.Left, be.Right)
		if result != resultExpected {
			t.Fatalf("unexpected result for %q; got %v; want %v", name, result, resultExpected)
		}
	}

	f("disabled optimization", &EvalConfig{
		Start: 1000,
		End:   2000,
		Step:  1000,
	}, false)
	f("disabled cache", &EvalConfig{
		Start:                            1000,
		End:                              2000,
		Step:                             1000,
		OptimizeRepeatedBinaryOpSubexprs: true,
	}, false)
	f("instant query", &EvalConfig{
		Start:                            1000,
		End:                              1000,
		Step:                             1000,
		MayCache:                         true,
		OptimizeRepeatedBinaryOpSubexprs: true,
	}, false)
	f("repeated cacheable aggregate subexpression", &EvalConfig{
		Start:                            1000,
		End:                              2000,
		Step:                             1000,
		MayCache:                         true,
		OptimizeRepeatedBinaryOpSubexprs: true,
	}, true)
	f("unaligned range query", &EvalConfig{
		Start:                            1001,
		End:                              2000,
		Step:                             1000,
		MayCache:                         true,
		OptimizeRepeatedBinaryOpSubexprs: true,
	}, false)
}

func TestShouldOptimizeRepeatedBinaryOpSubexprsExpressions(t *testing.T) {
	f := func(name, q string, resultExpected bool) {
		t.Helper()
		e, err := metricsql.Parse(q)
		if err != nil {
			t.Fatalf("unexpected error in metricsql.Parse(%q) for %q: %s", q, name, err)
		}
		be, ok := e.(*metricsql.BinaryOpExpr)
		if !ok {
			t.Fatalf("unexpected expr type for %q; got %T; want *metricsql.BinaryOpExpr", name, e)
		}
		ec := &EvalConfig{Start: 1000, End: 2000, Step: 1000, MayCache: true, OptimizeRepeatedBinaryOpSubexprs: true}
		result := shouldOptimizeRepeatedBinaryOpSubexprs(ec, be.Left, be.Right)
		if result != resultExpected {
			t.Fatalf("unexpected result for %q; got %v; want %v; query: %q", name, result, resultExpected, q)
		}
	}

	f("original issue query", `count(count(vm_requests_total) by (action,addr,cluster,endpoint)) by (action,addr,cluster) / count(count(vm_requests_total) by (action,addr,cluster,endpoint))`, true)
	f("right side contains repeated count aggregate", `count(foo) by (job) / (count(foo) by (job) + 1)`, true)
	f("same sum aggregate", `sum(rate(foo[5m])) by (job) / sum(rate(foo[5m])) by (job)`, true)
	f("same inner rollup but different aggregates", `sum(rate(foo[5m])) by (job) / count(rate(foo[5m])) by (job)`, false)
	f("different count aggregates", `count(foo) by (job) / count(bar) by (job)`, false)
	f("bare metric selector", `foo / foo`, false)
	f("bare rollup function", `rate(a[5m]) / rate(a[5m])`, false)
	f("now at modifier", `sum(rate(foo[5m] @ now())) by (job) / sum(rate(foo[5m] @ now())) by (job)`, false)
	f("unseeded rand at modifier", `sum(rate(foo[5m] @ rand())) by (job) / sum(rate(foo[5m] @ rand())) by (job)`, false)
	f("unseeded rand_normal at modifier", `sum(rate(foo[5m] @ rand_normal())) by (job) / sum(rate(foo[5m] @ rand_normal())) by (job)`, false)
	f("unseeded rand_exponential at modifier", `sum(rate(foo[5m] @ rand_exponential())) by (job) / sum(rate(foo[5m] @ rand_exponential())) by (job)`, false)
	f("seeded rand at modifier", `sum(rate(foo[5m] @ rand(1))) by (job) / sum(rate(foo[5m] @ rand(1))) by (job)`, true)
}
