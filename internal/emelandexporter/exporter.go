package emelandexporter

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"emeland.io/modelsrv-otel-exporter/internal/sensor"
)

const (
	// metricCertRemaining is the OTel httpcheck receiver metric that reports
	// seconds until TLS certificate expiry. Negative values mean already expired.
	metricCertRemaining = "httpcheck.tls.cert_remaining"

	// attrHTTPURL is the data-point attribute the httpcheck receiver uses to
	// identify which endpoint was probed.
	attrHTTPURL = "http.url"

	// attrAPIInstanceID is a custom attribute that can optionally be injected
	// (via an attributes processor) as a fallback when endpoint_mapping is not used.
	attrAPIInstanceID = "emeland.api_instance_id"
)

// emelandExporter receives OTel metrics, maps them to Findings, and applies
// them to an in-memory modelsrv instance that exposes the standard modelsrv
// HTTP API for downstream subscribers to consume.
type emelandExporter struct {
	config          *Config
	logger          *zap.Logger
	server          *sensor.Server
	mapper          *metricMapper
	endpointMapping map[string]uuid.UUID
}

func newEmelandExporter(settings exporter.Settings, cfg *Config) (*emelandExporter, error) {
	return &emelandExporter{
		config:          cfg,
		logger:          settings.Logger,
		endpointMapping: cfg.ParsedEndpointMapping(),
	}, nil
}

func (e *emelandExporter) start(_ context.Context, _ component.Host) error {
	slog := e.logger.Sugar()

	srv, err := sensor.New(e.config.ListenAddr, e.config.Subscribers, slog)
	if err != nil {
		return fmt.Errorf("start modelsrv sensor: %w", err)
	}
	e.server = srv
	e.mapper = newMetricMapper(e.config.ExpiryThreshold, slog)

	e.logger.Info("emeland exporter started",
		zap.String("listen", e.config.ListenAddr),
		zap.Int("subscribers", len(e.config.Subscribers)),
		zap.Int("endpoint_mappings", len(e.endpointMapping)),
	)
	return nil
}

func (e *emelandExporter) shutdown(_ context.Context) error {
	if e.server != nil {
		return e.server.Close()
	}
	return nil
}

// consumeMetrics is called by the Collector pipeline for each batch of metrics.
// It walks the payload looking for httpcheck.tls.cert_remaining data points,
// resolves the ApiInstance UUID (via endpoint_mapping or attribute), maps them
// to Finding events, and emits them into the local modelsrv.
func (e *emelandExporter) consumeMetrics(_ context.Context, md pmetric.Metrics) error {
	var firstErr error

	for ri := range md.ResourceMetrics().Len() {
		rm := md.ResourceMetrics().At(ri)
		for si := range rm.ScopeMetrics().Len() {
			sm := rm.ScopeMetrics().At(si)
			for mi := range sm.Metrics().Len() {
				metric := sm.Metrics().At(mi)
				if metric.Name() != metricCertRemaining {
					continue
				}
				if err := e.processCertMetric(metric, rm); err != nil && firstErr == nil {
					firstErr = err
				}
			}
		}
	}

	return firstErr
}

// processCertMetric handles a single httpcheck.tls.cert_remaining gauge.
func (e *emelandExporter) processCertMetric(metric pmetric.Metric, rm pmetric.ResourceMetrics) error {
	if metric.Type() != pmetric.MetricTypeGauge {
		return nil
	}

	var firstErr error
	dps := metric.Gauge().DataPoints()

	for i := range dps.Len() {
		dp := dps.At(i)
		apiInstanceID := e.resolveAPIInstanceID(dp, rm)
		if apiInstanceID == uuid.Nil {
			e.logger.Debug("skipping data point: cannot resolve api_instance_id",
				zap.String("metric", metric.Name()),
			)
			continue
		}

		remainingSecs := dp.DoubleValue()
		remaining := time.Duration(remainingSecs * float64(time.Second))

		events := e.mapper.Reconcile(apiInstanceID, remaining)
		for _, ev := range events {
			if err := e.server.Emit(ev); err != nil {
				e.logger.Error("failed to emit finding event",
					zap.String("resourceId", ev.ResourceId.String()),
					zap.Error(err),
				)
				if firstErr == nil {
					firstErr = err
				}
			}
		}
	}

	return firstErr
}

// resolveAPIInstanceID determines the ApiInstance UUID for a data point.
// Resolution order:
//  1. endpoint_mapping: look up http.url attribute in the configured map
//  2. Explicit attribute: emeland.api_instance_id on data point or resource
func (e *emelandExporter) resolveAPIInstanceID(dp pmetric.NumberDataPoint, rm pmetric.ResourceMetrics) uuid.UUID {
	// 1. Try endpoint_mapping via http.url
	if len(e.endpointMapping) > 0 {
		if v, ok := dp.Attributes().Get(attrHTTPURL); ok {
			if id, found := e.endpointMapping[v.Str()]; found {
				return id
			}
			e.logger.Warn("http.url not found in endpoint_mapping (check for trailing slash or port mismatch)",
				zap.String("http.url", v.Str()),
			)
		}
	}

	// 2. Fall back to explicit emeland.api_instance_id attribute
	if v, ok := dp.Attributes().Get(attrAPIInstanceID); ok {
		if id, err := uuid.Parse(v.Str()); err == nil {
			return id
		}
	}
	if v, ok := rm.Resource().Attributes().Get(attrAPIInstanceID); ok {
		if id, err := uuid.Parse(v.Str()); err == nil {
			return id
		}
	}

	return uuid.Nil
}
