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
	"go.emeland.io/modelsrv/pkg/events"
)

const (
	// metricCertRemaining is the OTel httpcheck receiver metric that reports
	// seconds until TLS certificate expiry. Negative values mean already expired.
	metricCertRemaining = "httpcheck.tls.cert_remaining"

	// metricHTTPCheckError is emitted by the httpcheck receiver when a probe
	// fails (connection error, timeout, TLS handshake failure, etc.).
	metricHTTPCheckError = "httpcheck.error"

	// attrHTTPURL is the data-point attribute the httpcheck receiver uses to
	// identify which endpoint was probed.
	attrHTTPURL = "http.url"

	// attrErrorMessage is set on httpcheck.error data points with the failure reason.
	attrErrorMessage = "error.message"

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
// It processes httpcheck.error (probe failures → CertificateProbeFailed) first,
// then httpcheck.tls.cert_remaining (success path). Cert metrics are applied
// second so a successful TLS observation in the same batch wins over an error.
func (e *emelandExporter) consumeMetrics(_ context.Context, md pmetric.Metrics) error {
	var firstErr error

	for _, name := range []string{metricHTTPCheckError, metricCertRemaining} {
		for ri := range md.ResourceMetrics().Len() {
			rm := md.ResourceMetrics().At(ri)
			for si := range rm.ScopeMetrics().Len() {
				sm := rm.ScopeMetrics().At(si)
				for mi := range sm.Metrics().Len() {
					metric := sm.Metrics().At(mi)
					if metric.Name() != name {
						continue
					}
					var err error
					switch name {
					case metricHTTPCheckError:
						err = e.processErrorMetric(metric, rm)
					case metricCertRemaining:
						err = e.processCertMetric(metric, rm)
					}
					if err != nil && firstErr == nil {
						firstErr = err
					}
				}
			}
		}
	}

	return firstErr
}

// processErrorMetric handles httpcheck.error sum data points (value > 0 → probe failed).
func (e *emelandExporter) processErrorMetric(metric pmetric.Metric, rm pmetric.ResourceMetrics) error {
	dps, ok := numberDataPoints(metric)
	if !ok {
		return nil
	}

	var firstErr error
	for i := range dps.Len() {
		dp := dps.At(i)
		if numberValue(dp) <= 0 {
			continue
		}

		apiInstanceID := e.resolveAPIInstanceID(dp, rm)
		if apiInstanceID == uuid.Nil {
			e.logger.Debug("skipping data point: cannot resolve api_instance_id",
				zap.String("metric", metric.Name()),
			)
			continue
		}

		errMsg := ""
		if v, ok := dp.Attributes().Get(attrErrorMessage); ok {
			errMsg = v.Str()
		}

		if err := e.emitEvents(e.mapper.ReconcileProbeFailed(apiInstanceID, errMsg)); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// processCertMetric handles a single httpcheck.tls.cert_remaining gauge.
func (e *emelandExporter) processCertMetric(metric pmetric.Metric, rm pmetric.ResourceMetrics) error {
	dps, ok := numberDataPoints(metric)
	if !ok {
		return nil
	}

	var firstErr error
	for i := range dps.Len() {
		dp := dps.At(i)
		apiInstanceID := e.resolveAPIInstanceID(dp, rm)
		if apiInstanceID == uuid.Nil {
			e.logger.Debug("skipping data point: cannot resolve api_instance_id",
				zap.String("metric", metric.Name()),
			)
			continue
		}

		remaining := time.Duration(numberValue(dp) * float64(time.Second))
		if err := e.emitEvents(e.mapper.Reconcile(apiInstanceID, remaining)); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func (e *emelandExporter) emitEvents(evs []events.Event) error {
	var firstErr error
	for _, ev := range evs {
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
	return firstErr
}

// numberDataPoints returns gauge or sum data points from a metric.
func numberDataPoints(metric pmetric.Metric) (pmetric.NumberDataPointSlice, bool) {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		return metric.Gauge().DataPoints(), true
	case pmetric.MetricTypeSum:
		return metric.Sum().DataPoints(), true
	default:
		return pmetric.NumberDataPointSlice{}, false
	}
}

// numberValue reads an int or double datapoint value (httpcheck uses ints).
func numberValue(dp pmetric.NumberDataPoint) float64 {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		return float64(dp.IntValue())
	case pmetric.NumberDataPointValueTypeDouble:
		return dp.DoubleValue()
	default:
		return 0
	}
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
