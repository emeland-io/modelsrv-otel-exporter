package emelandexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	typeStr             = "emeland"
	stability           = component.StabilityLevelAlpha
	defaultListenAddr   = "localhost:24200"
	defaultExpiryThresh = 30 * 24 * time.Hour
)

// NewFactory returns a new exporter.Factory for the EmELand exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		exporter.WithMetrics(createMetricsExporter, stability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		ListenAddr:      defaultListenAddr,
		ExpiryThreshold: defaultExpiryThresh,
	}
}

func createMetricsExporter(
	ctx context.Context,
	settings exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	eCfg := cfg.(*Config)

	exp, err := newEmelandExporter(settings, eCfg)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewMetrics(
		ctx,
		settings,
		cfg,
		exp.consumeMetrics,
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
	)
}
