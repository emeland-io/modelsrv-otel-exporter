package emelandexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// MetricMapper is exported for testing.
type MetricMapper = metricMapper

// NewMetricMapperForTest creates a MetricMapper with the given threshold.
func NewMetricMapperForTest(threshold time.Duration, log *zap.SugaredLogger) *MetricMapper {
	return newMetricMapper(threshold, log)
}

// TestableExporter wraps the internal exporter for integration tests.
type TestableExporter struct {
	exp *emelandExporter
}

// NewExporterForTest creates an exporter with test settings (no OTel infra needed).
func NewExporterForTest(cfg *Config) *TestableExporter {
	exp := &emelandExporter{
		config:          cfg,
		logger:          zap.NewNop(),
		endpointMapping: cfg.ParsedEndpointMapping(),
	}
	return &TestableExporter{exp: exp}
}

func (t *TestableExporter) Start(ctx context.Context) error {
	return t.exp.start(ctx, nil)
}

func (t *TestableExporter) Shutdown(ctx context.Context) error {
	return t.exp.shutdown(ctx)
}

func (t *TestableExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	return t.exp.consumeMetrics(ctx, md)
}
