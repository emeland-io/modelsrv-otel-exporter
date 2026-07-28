package emelandexporter

import (
	"time"

	"go.uber.org/zap"
)

// MetricMapper is exported for testing.
type MetricMapper = metricMapper

// NewMetricMapperForTest creates a MetricMapper with the given threshold.
func NewMetricMapperForTest(threshold time.Duration, log *zap.SugaredLogger) *MetricMapper {
	return newMetricMapper(threshold, log)
}
