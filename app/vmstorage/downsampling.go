package vmstorage

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
)

var (
	downsamplingPeriod    string
	useGlobalDownsampling bool
)

type downsamplingPeriodFlagValue struct {
	value *string
	set   bool
}

func (v *downsamplingPeriodFlagValue) String() string {
	if v.value == nil {
		return ""
	}
	return *v.value
}

func (v *downsamplingPeriodFlagValue) Set(s string) error {
	if v.set {
		return fmt.Errorf("-downsampling.period cannot be specified more than once")
	}
	v.set = true
	*v.value = s
	return nil
}

func init() {
	if flag.Lookup("downsampling.period") != nil {
		// Enterprise builds already register this flag with filter-aware, multi-rule semantics.
		return
	}
	flag.Var(&downsamplingPeriodFlagValue{value: &downsamplingPeriod}, "downsampling.period", "Vmsingle-only global downsampling period in the form offset:interval. The offset and interval must be positive, and the interval must divide 24h exactly. For example, '30d:1h' instructs VictoriaMetrics to leave a single sample per hour for samples older than 30 days. The flag may be specified only once")
	useGlobalDownsampling = true
}

const (
	downsamplingPolicyVersion  = 1
	downsamplingPolicyFilename = "downsampling-policy.json"
	metadataDirname            = "metadata"
)

type downsamplingConfig struct {
	offsetUsecs   int64
	intervalUsecs int64
}

type downsamplingPolicy struct {
	Version       int   `json:"version"`
	OffsetUsecs   int64 `json:"offsetUsecs"`
	IntervalUsecs int64 `json:"intervalUsecs"`
}

func loadDownsamplingConfig(storageDataPath, period string, dedupInterval time.Duration) (*downsamplingConfig, error) {
	config, err := parseDownsamplingConfig(period, dedupInterval)
	if err != nil {
		return nil, err
	}
	if err := validateDownsamplingPolicy(storageDataPath, config); err != nil {
		return nil, err
	}
	return config, nil
}

func setDownsamplingConfig(config *downsamplingConfig) {
	if config == nil {
		storage.SetDownsamplingPeriod(0, 0)
		return
	}
	storage.SetDownsamplingPeriod(
		time.Duration(config.offsetUsecs)*time.Microsecond,
		time.Duration(config.intervalUsecs)*time.Microsecond,
	)
}

func activateDownsamplingConfig(storageDataPath string, config *downsamplingConfig) error {
	if err := ensureDownsamplingPolicy(storageDataPath, config); err != nil {
		return err
	}
	setDownsamplingConfig(config)
	return nil
}

func parseDownsamplingConfig(s string, dedupInterval time.Duration) (*downsamplingConfig, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	if strings.Count(s, ":") != 1 {
		return nil, fmt.Errorf("invalid -downsampling.period=%q; want a single offset:interval rule", s)
	}
	offsetStr, intervalStr, _ := strings.Cut(s, ":")
	offsetStr = strings.TrimSpace(offsetStr)
	intervalStr = strings.TrimSpace(intervalStr)
	if offsetStr == "" || intervalStr == "" {
		return nil, fmt.Errorf("invalid -downsampling.period=%q; both offset and interval must be set", s)
	}

	offset, err := parseDownsamplingDuration(offsetStr)
	if err != nil {
		return nil, fmt.Errorf("cannot parse downsampling offset %q: %w", offsetStr, err)
	}
	interval, err := parseDownsamplingDuration(intervalStr)
	if err != nil {
		return nil, fmt.Errorf("cannot parse downsampling interval %q: %w", intervalStr, err)
	}
	if offset <= 0 {
		return nil, fmt.Errorf("downsampling offset must be positive; got %s", offset)
	}
	if interval <= 0 {
		return nil, fmt.Errorf("downsampling interval must be positive; got %s", interval)
	}
	if offset%time.Microsecond != 0 {
		return nil, fmt.Errorf("downsampling offset must be a whole number of microseconds; got %s", offset)
	}
	if interval%time.Microsecond != 0 {
		return nil, fmt.Errorf("downsampling interval must be a whole number of microseconds; got %s", interval)
	}
	if (24*time.Hour)%interval != 0 {
		return nil, fmt.Errorf("downsampling interval %s must divide 24h exactly", interval)
	}

	offsetUsecs := offset.Microseconds()
	intervalUsecs := interval.Microseconds()
	if offsetUsecs%intervalUsecs != 0 {
		return nil, fmt.Errorf("downsampling offset %s must be divisible by interval %s", offset, interval)
	}
	if dedupInterval > 0 && dedupInterval%time.Microsecond != 0 {
		return nil, fmt.Errorf("-dedup.minScrapeInterval must be a whole number of microseconds when downsampling is enabled; got %s", dedupInterval)
	}
	dedupIntervalUsecs := dedupInterval.Microseconds()
	if dedupIntervalUsecs > 0 && intervalUsecs <= dedupIntervalUsecs {
		return nil, fmt.Errorf("downsampling interval %s must be greater than -dedup.minScrapeInterval=%s", interval, dedupInterval)
	}
	if dedupIntervalUsecs > 0 && intervalUsecs%dedupIntervalUsecs != 0 {
		return nil, fmt.Errorf("downsampling interval %s must be divisible by -dedup.minScrapeInterval=%s", interval, dedupInterval)
	}

	return &downsamplingConfig{
		offsetUsecs:   offsetUsecs,
		intervalUsecs: intervalUsecs,
	}, nil
}

func parseDownsamplingDuration(s string) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err == nil {
		return d, nil
	}
	d, fallbackErr := timeutil.ParseDuration(s)
	if fallbackErr == nil {
		return d, nil
	}
	return 0, fmt.Errorf("cannot parse as Go duration (%v) or VictoriaMetrics duration (%v)", err, fallbackErr)
}

func validateDownsamplingPolicy(storageDataPath string, config *downsamplingConfig) error {
	policy, policyPath, err := readDownsamplingPolicy(storageDataPath)
	if err != nil || policy == nil {
		return err
	}
	return compareDownsamplingPolicy(policyPath, *policy, config)
}

func ensureDownsamplingPolicy(storageDataPath string, config *downsamplingConfig) error {
	policy, policyPath, err := readDownsamplingPolicy(storageDataPath)
	if err != nil {
		return err
	}
	if policy != nil {
		return compareDownsamplingPolicy(policyPath, *policy, config)
	}
	if config == nil {
		return nil
	}
	policyPath = getDownsamplingPolicyPath(storageDataPath)
	fs.MustMkdirIfNotExist(filepath.Dir(policyPath))
	fs.MustWriteAtomic(policyPath, marshalDownsamplingPolicy(config.policy()), false)
	return nil
}

func getDownsamplingPolicyPath(storageDataPath string) string {
	return filepath.Join(storageDataPath, metadataDirname, downsamplingPolicyFilename)
}

func readDownsamplingPolicy(storageDataPath string) (*downsamplingPolicy, string, error) {
	policyPath := getDownsamplingPolicyPath(storageDataPath)
	data, err := os.ReadFile(policyPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, policyPath, nil
		}
		return nil, policyPath, fmt.Errorf("cannot read downsampling policy at %q: %w", policyPath, err)
	}

	var policy downsamplingPolicy
	if err := json.Unmarshal(data, &policy); err != nil {
		return nil, policyPath, fmt.Errorf("cannot parse downsampling policy at %q: %w", policyPath, err)
	}
	if policy.Version != downsamplingPolicyVersion {
		return nil, policyPath, fmt.Errorf("unsupported downsampling policy version %d at %q; want %d", policy.Version, policyPath, downsamplingPolicyVersion)
	}
	if !bytes.Equal(data, marshalDownsamplingPolicy(policy)) {
		return nil, policyPath, fmt.Errorf("downsampling policy at %q isn't in the canonical format", policyPath)
	}
	return &policy, policyPath, nil
}

func compareDownsamplingPolicy(policyPath string, policy downsamplingPolicy, config *downsamplingConfig) error {
	if config == nil {
		return fmt.Errorf("-downsampling.period must be set to %s because the storage has a latched downsampling policy", policy.period())
	}
	if policy != config.policy() {
		return fmt.Errorf("-downsampling.period=%s differs from the latched downsampling policy %s at %q", config.policy().period(), policy.period(), policyPath)
	}
	return nil
}

func (config *downsamplingConfig) policy() downsamplingPolicy {
	return downsamplingPolicy{
		Version:       downsamplingPolicyVersion,
		OffsetUsecs:   config.offsetUsecs,
		IntervalUsecs: config.intervalUsecs,
	}
}

func (policy downsamplingPolicy) period() string {
	return fmt.Sprintf("%dus:%dus", policy.OffsetUsecs, policy.IntervalUsecs)
}

func marshalDownsamplingPolicy(policy downsamplingPolicy) []byte {
	data, err := json.Marshal(&policy)
	if err != nil {
		panic(fmt.Errorf("BUG: cannot marshal downsampling policy: %w", err))
	}
	return append(data, '\n')
}
