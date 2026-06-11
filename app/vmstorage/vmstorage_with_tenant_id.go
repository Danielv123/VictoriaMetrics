package vmstorage

import (
	"flag"
	"fmt"
	"math"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/querytracer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage/metricnamestats"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage/metricsmetadata"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/vmselectapi"
)

var (
	accountID = flag.Uint64("accountID", 0, "The accountID of the stored data")
	projectID = flag.Uint64("projectID", 0, "The projectID of the stored data")
)

func newVMStorageWithTenantID(vms *VMStorage) *VMStorageWithTenantID {
	if *accountID > math.MaxUint32 {
		logger.Fatalf("-clusternative.accountID must to be in the range [0, %d], got %d", uint32(math.MaxUint32), *accountID)
	}
	if *projectID > math.MaxUint32 {
		logger.Fatalf("-clusternative.projectID must to be in the range [0, %d], got %d", uint32(math.MaxUint32), *projectID)
	}
	return &VMStorageWithTenantID{
		vms:       vms,
		accountID: uint32(*accountID),
		projectID: uint32(*projectID),
	}
}

// VMStorageWithTenantID is a thin wrapper around VMStorage type that overrides
// its methods to properly serve requests coming from a vmselect (require
// tenantID).
//
// The type does not take ownership of vms.
type VMStorageWithTenantID struct {
	vms *VMStorage

	accountID uint32
	projectID uint32
}

func (vmst *VMStorageWithTenantID) InitSearch(qt *querytracer.Tracer, sq *storage.SearchQuery, deadline uint64) (vmselectapi.BlockIterator, error) {
	if !sq.IsMultiTenant && (sq.AccountID != vmst.accountID || sq.ProjectID != vmst.projectID) {
		return emptyBI, nil
	}
	return vmst.vms.initSearch(qt, sq, vmst.marshalMetricBlock, deadline)
}

// emptyBlockIterator is an implementation of vmselectapi.BlockIterator that
// always returns no data.
type emptyBlockIterator struct{}

func (*emptyBlockIterator) MustClose() {}

func (*emptyBlockIterator) NextBlock(dst []byte) ([]byte, bool) {
	return dst, false
}

func (*emptyBlockIterator) Error() error {
	return nil
}

var emptyBI = &emptyBlockIterator{}

// marshalMetricBlock serializes a metric block in the format expected by
// vmselect.
//
// vmselect expects metric names and data blocks to have the tenantID but
// vmsingle does not have it. Therefore the tenantID needs to be included to
// every metric name and block.
func (vmst *VMStorageWithTenantID) marshalMetricBlock(dst []byte, src *storage.MetricBlock) []byte {
	// Marshal metric name:
	// 1. Marshal metric name length + accountID length + projectID length (in
	//    bytes).
	// 2. Append accountID and projectID bytes
	// 3. Finally append metric name bytes
	dst = encoding.MarshalVarUint64(dst, uint64(len(src.MetricName))+8)
	dst = encoding.MarshalUint32(dst, vmst.accountID)
	dst = encoding.MarshalUint32(dst, vmst.projectID)
	dst = append(dst, src.MetricName...)

	// Marshal data block.
	dst = encoding.MarshalUint32(dst, vmst.accountID)
	dst = encoding.MarshalUint32(dst, vmst.projectID)
	dst = storage.MarshalBlock(dst, &src.Block)

	return dst
}

func (vmst *VMStorageWithTenantID) SearchMetricNames(qt *querytracer.Tracer, sq *storage.SearchQuery, deadline uint64) ([]string, error) {
	if !sq.IsMultiTenant && (sq.AccountID != vmst.accountID || sq.ProjectID != vmst.projectID) {
		return nil, nil
	}

	metricNames, err := vmst.vms.SearchMetricNames(qt, sq, deadline)
	if err != nil {
		return nil, err
	}

	// vmselect expects metric names to have the tenantID but vmsingle does not
	// have it. Therefore the tenantID needs to be appended to every metric
	// name.
	dst := make([]byte, 0, 8)
	dst = encoding.MarshalUint32(dst, sq.AccountID)
	dst = encoding.MarshalUint32(dst, sq.ProjectID)
	tenantID := string(dst)

	for i, metricName := range metricNames {
		metricNames[i] = tenantID + metricName
	}
	return metricNames, nil
}

func (vmst *VMStorageWithTenantID) LabelValues(qt *querytracer.Tracer, sq *storage.SearchQuery, labelName string, maxLabelValues int, deadline uint64) ([]string, error) {
	if !sq.IsMultiTenant && (sq.AccountID != vmst.accountID || sq.ProjectID != vmst.projectID) {
		return nil, nil
	}
	return vmst.vms.LabelValues(qt, sq, labelName, maxLabelValues, deadline)
}

func (vmst *VMStorageWithTenantID) TagValueSuffixes(qt *querytracer.Tracer, accountID, projectID uint32, tr storage.TimeRange, tagKey, tagValuePrefix string, delimiter byte, maxSuffixes int, deadline uint64) ([]string, error) {
	if accountID != vmst.accountID || projectID != vmst.projectID {
		return nil, nil
	}
	return vmst.vms.TagValueSuffixes(qt, accountID, projectID, tr, tagKey, tagValuePrefix, delimiter, maxSuffixes, deadline)
}

func (vmst *VMStorageWithTenantID) LabelNames(qt *querytracer.Tracer, sq *storage.SearchQuery, maxLabelNames int, deadline uint64) ([]string, error) {
	if !sq.IsMultiTenant && (sq.AccountID != vmst.accountID || sq.ProjectID != vmst.projectID) {
		return nil, nil
	}
	return vmst.vms.LabelNames(qt, sq, maxLabelNames, deadline)
}

func (vmst *VMStorageWithTenantID) SeriesCount(qt *querytracer.Tracer, accountID, projectID uint32, deadline uint64) (uint64, error) {
	if accountID != vmst.accountID || projectID != vmst.projectID {
		return 0, nil
	}
	return vmst.vms.SeriesCount(qt, accountID, projectID, deadline)
}

func (vmst *VMStorageWithTenantID) Tenants(qt *querytracer.Tracer, tr storage.TimeRange, deadline uint64) ([]string, error) {
	tenantID := fmt.Sprintf("%d:%d", vmst.accountID, vmst.projectID)
	return []string{tenantID}, nil
}

func (vmst *VMStorageWithTenantID) TSDBStatus(qt *querytracer.Tracer, sq *storage.SearchQuery, focusLabel string, topN int, deadline uint64) (*storage.TSDBStatus, error) {
	if !sq.IsMultiTenant && (sq.AccountID != vmst.accountID || sq.ProjectID != vmst.projectID) {
		return &storage.TSDBStatus{}, nil
	}
	return vmst.vms.TSDBStatus(qt, sq, focusLabel, topN, deadline)
}

func (vmst *VMStorageWithTenantID) DeleteSeries(qt *querytracer.Tracer, sq *storage.SearchQuery, deadline uint64) (int, error) {
	if !sq.IsMultiTenant && (sq.AccountID != vmst.accountID || sq.ProjectID != vmst.projectID) {
		return 0, nil
	}
	return vmst.vms.DeleteSeries(qt, sq, deadline)
}

func (vmst *VMStorageWithTenantID) RegisterMetricNames(qt *querytracer.Tracer, mrs []storage.MetricRow, deadline uint64) error {
	return vmst.vms.RegisterMetricNames(qt, mrs, deadline)
}

func (vmst *VMStorageWithTenantID) GetMetricNamesUsageStats(qt *querytracer.Tracer, tt *storage.TenantToken, limit, le int, matchPattern string, deadline uint64) (metricnamestats.StatsResult, error) {
	if tt != nil && (tt.AccountID != vmst.accountID || tt.ProjectID != vmst.projectID) {
		return metricnamestats.StatsResult{}, nil
	}
	return vmst.vms.GetMetricNamesUsageStats(qt, tt, limit, le, matchPattern, deadline)
}

func (vmst *VMStorageWithTenantID) ResetMetricNamesUsageStats(qt *querytracer.Tracer, deadline uint64) error {
	return vmst.vms.ResetMetricNamesUsageStats(qt, deadline)
}

func (vmst *VMStorageWithTenantID) GetMetadataRecords(qt *querytracer.Tracer, tt *storage.TenantToken, limit int, metricName string, deadline uint64) ([]*metricsmetadata.Row, error) {
	if tt != nil && (tt.AccountID != vmst.accountID || tt.ProjectID != vmst.projectID) {
		return nil, nil
	}
	return vmst.vms.GetMetadataRecords(qt, tt, limit, metricName, deadline)
}
