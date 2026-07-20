package vmstorage

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseDownsamplingConfigSuccess(t *testing.T) {
	testCases := []struct {
		name          string
		period        string
		dedupInterval time.Duration
		wantOffset    int64
		wantInterval  int64
	}{
		{
			name:          "Go microsecond duration",
			period:        "2ms:250us",
			dedupInterval: 50 * time.Microsecond,
			wantOffset:    2_000,
			wantInterval:  250,
		},
		{
			name:          "VictoriaMetrics day duration",
			period:        "2d:1h",
			dedupInterval: 5 * time.Minute,
			wantOffset:    (48 * time.Hour).Microseconds(),
			wantInterval:  time.Hour.Microseconds(),
		},
		{
			name:          "VictoriaMetrics week duration",
			period:        "2w:1d",
			dedupInterval: time.Hour,
			wantOffset:    (14 * 24 * time.Hour).Microseconds(),
			wantInterval:  (24 * time.Hour).Microseconds(),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config, err := parseDownsamplingConfig(tc.period, tc.dedupInterval)
			if err != nil {
				t.Fatalf("cannot parse config: %s", err)
			}
			if config == nil {
				t.Fatalf("unexpected nil config")
			}
			if config.offsetUsecs != tc.wantOffset {
				t.Fatalf("unexpected offset; got %d; want %d", config.offsetUsecs, tc.wantOffset)
			}
			if config.intervalUsecs != tc.wantInterval {
				t.Fatalf("unexpected interval; got %d; want %d", config.intervalUsecs, tc.wantInterval)
			}
		})
	}

	config, err := parseDownsamplingConfig("", 0)
	if err != nil {
		t.Fatalf("unexpected error for empty config: %s", err)
	}
	if config != nil {
		t.Fatalf("unexpected non-nil config for empty period: %+v", config)
	}
}

func TestParseDownsamplingConfigFailure(t *testing.T) {
	testCases := []struct {
		name          string
		period        string
		dedupInterval time.Duration
	}{
		{name: "missing separator", period: "1d"},
		{name: "multiple rules", period: "1d:1h,2d:2h"},
		{name: "missing offset", period: ":1h"},
		{name: "missing interval", period: "1d:"},
		{name: "negative offset", period: "-1h:1m"},
		{name: "zero offset", period: "0s:1m"},
		{name: "zero interval", period: "1h:0s"},
		{name: "sub-microsecond interval", period: "2us:500ns"},
		{name: "offset not divisible by interval", period: "90m:1h"},
		{name: "interval does not divide day", period: "21h:7h"},
		{name: "interval can cross calendar partitions", period: "4d:2d"},
		{name: "sub-microsecond dedup interval", period: "100us:10us", dedupInterval: 500 * time.Nanosecond},
		{name: "fractional-microsecond dedup interval", period: "100us:10us", dedupInterval: 1500 * time.Nanosecond},
		{name: "interval equals dedup interval", period: "2h:1h", dedupInterval: time.Hour},
		{name: "interval below dedup interval", period: "2h:1h", dedupInterval: 2 * time.Hour},
		{name: "incompatible dedup interval", period: "2h:1h", dedupInterval: 40 * time.Minute},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if config, err := parseDownsamplingConfig(tc.period, tc.dedupInterval); err == nil {
				t.Fatalf("unexpected success: %+v", config)
			}
		})
	}
}

func TestDownsamplingPeriodFlagValueRejectsRepeatedSet(t *testing.T) {
	var period string
	v := &downsamplingPeriodFlagValue{value: &period}
	if got := v.String(); got != "" {
		t.Fatalf("unexpected initial flag value; got %q", got)
	}
	if err := v.Set("30d:1h"); err != nil {
		t.Fatalf("cannot set the flag value: %s", err)
	}
	if got := v.String(); got != "30d:1h" {
		t.Fatalf("unexpected flag value; got %q; want %q", got, "30d:1h")
	}
	if err := v.Set("60d:2h"); err == nil {
		t.Fatalf("a repeated flag occurrence must be rejected")
	}
	if got := v.String(); got != "30d:1h" {
		t.Fatalf("a rejected repeated occurrence changed the flag value; got %q; want %q", got, "30d:1h")
	}
}

func TestLoadDownsamplingConfigDoesNotLatchPolicy(t *testing.T) {
	storageDataPath := t.TempDir()
	config, err := loadDownsamplingConfig(storageDataPath, "2d:1h", 5*time.Minute)
	if err != nil {
		t.Fatalf("cannot load downsampling config: %s", err)
	}
	if config == nil {
		t.Fatalf("unexpected nil config")
	}
	policyPath := getDownsamplingPolicyPath(storageDataPath)
	if _, err := os.Stat(policyPath); !os.IsNotExist(err) {
		t.Fatalf("policy must not be latched during startup validation; got err=%v", err)
	}
}

func TestLoadDownsamplingConfigRejectsEqualDedupWithoutLatch(t *testing.T) {
	storageDataPath := t.TempDir()
	if config, err := loadDownsamplingConfig(storageDataPath, "2h:1h", time.Hour); err == nil {
		t.Fatalf("equal downsampling and dedup intervals must be rejected; got config %+v", config)
	}
	policyPath := getDownsamplingPolicyPath(storageDataPath)
	if _, err := os.Stat(policyPath); !os.IsNotExist(err) {
		t.Fatalf("a no-op downsampling config must not latch a policy; got err=%v", err)
	}
}

func TestEnsureDownsamplingPolicy(t *testing.T) {
	storageDataPath := t.TempDir()
	policyPath := getDownsamplingPolicyPath(storageDataPath)
	config := &downsamplingConfig{
		offsetUsecs:   (48 * time.Hour).Microseconds(),
		intervalUsecs: time.Hour.Microseconds(),
	}

	if err := ensureDownsamplingPolicy(storageDataPath, nil); err != nil {
		t.Fatalf("unexpected error without config: %s", err)
	}
	if _, err := os.Stat(policyPath); !os.IsNotExist(err) {
		t.Fatalf("policy file must not exist without config; got err=%v", err)
	}

	if err := ensureDownsamplingPolicy(storageDataPath, config); err != nil {
		t.Fatalf("cannot latch policy: %s", err)
	}
	data, err := os.ReadFile(policyPath)
	if err != nil {
		t.Fatalf("cannot read policy: %s", err)
	}
	if got, want := string(data), string(marshalDownsamplingPolicy(config.policy())); got != want {
		t.Fatalf("unexpected policy contents; got %q; want %q", got, want)
	}

	equivalentConfig := &downsamplingConfig{
		offsetUsecs:   (2 * 24 * time.Hour).Microseconds(),
		intervalUsecs: (60 * time.Minute).Microseconds(),
	}
	if err := ensureDownsamplingPolicy(storageDataPath, equivalentConfig); err != nil {
		t.Fatalf("equivalent config must match latched policy: %s", err)
	}
	if err := ensureDownsamplingPolicy(storageDataPath, nil); err == nil {
		t.Fatalf("missing config must be rejected after policy is latched")
	}

	changedConfig := &downsamplingConfig{
		offsetUsecs:   config.offsetUsecs,
		intervalUsecs: 2 * config.intervalUsecs,
	}
	if err := ensureDownsamplingPolicy(storageDataPath, changedConfig); err == nil {
		t.Fatalf("changed config must be rejected after policy is latched")
	}
}

func TestEnsureDownsamplingPolicyRejectsInvalidLatch(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{
			name: "unsupported version",
			data: []byte(`{"version":2,"offsetUsecs":7200000000,"intervalUsecs":3600000000}` + "\n"),
		},
		{
			name: "non-canonical",
			data: []byte("{\n  \"version\": 1,\n  \"offsetUsecs\": 7200000000,\n  \"intervalUsecs\": 3600000000\n}\n"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storageDataPath := t.TempDir()
			policyPath := getDownsamplingPolicyPath(storageDataPath)
			if err := os.MkdirAll(filepath.Dir(policyPath), 0o755); err != nil {
				t.Fatalf("cannot create metadata directory: %s", err)
			}
			if err := os.WriteFile(policyPath, tc.data, 0o600); err != nil {
				t.Fatalf("cannot write policy: %s", err)
			}
			config := &downsamplingConfig{
				offsetUsecs:   7_200_000_000,
				intervalUsecs: 3_600_000_000,
			}
			if err := ensureDownsamplingPolicy(storageDataPath, config); err == nil {
				t.Fatalf("invalid latched policy must be rejected")
			}
		})
	}
}
