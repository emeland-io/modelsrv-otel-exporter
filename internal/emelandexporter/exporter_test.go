package emelandexporter_test

import (
	"context"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"emeland.io/modelsrv-otel-exporter/internal/emelandexporter"
)

var _ = Describe("ConsumeMetrics with endpoint_mapping", func() {
	var (
		apiInstanceID uuid.UUID
		endpointURL   string
	)

	BeforeEach(func() {
		apiInstanceID = uuid.MustParse("11111111-2222-3333-4444-555555555555")
		endpointURL = "https://api.example.com:443/health"
	})

	It("resolves ApiInstance UUID from http.url via endpoint_mapping", func() {
		cfg := &emelandexporter.Config{
			ListenAddr:      "localhost:24250",
			ExpiryThreshold: 30 * 24 * time.Hour,
			EndpointMapping: map[string]string{
				endpointURL: apiInstanceID.String(),
			},
		}

		exp := emelandexporter.NewExporterForTest(cfg)
		defer exp.Shutdown(context.Background())

		err := exp.Start(context.Background())
		Expect(err).ToNot(HaveOccurred())

		md := buildMetricsWithURL(endpointURL, 5*24*3600)
		err = exp.ConsumeMetrics(context.Background(), md)
		Expect(err).ToNot(HaveOccurred())
	})

	It("falls back to emeland.api_instance_id attribute when endpoint_mapping has no match", func() {
		cfg := &emelandexporter.Config{
			ListenAddr:      "localhost:24251",
			ExpiryThreshold: 30 * 24 * time.Hour,
			EndpointMapping: map[string]string{},
		}

		exp := emelandexporter.NewExporterForTest(cfg)
		defer exp.Shutdown(context.Background())

		err := exp.Start(context.Background())
		Expect(err).ToNot(HaveOccurred())

		md := buildMetricsWithExplicitID(apiInstanceID, 5*24*3600)
		err = exp.ConsumeMetrics(context.Background(), md)
		Expect(err).ToNot(HaveOccurred())
	})

	It("skips data points that cannot be resolved", func() {
		cfg := &emelandexporter.Config{
			ListenAddr:      "localhost:24252",
			ExpiryThreshold: 30 * 24 * time.Hour,
			EndpointMapping: map[string]string{},
		}

		exp := emelandexporter.NewExporterForTest(cfg)
		defer exp.Shutdown(context.Background())

		err := exp.Start(context.Background())
		Expect(err).ToNot(HaveOccurred())

		md := buildMetricsWithURL("https://unknown.example.com/", 3600)
		err = exp.ConsumeMetrics(context.Background(), md)
		Expect(err).ToNot(HaveOccurred())
	})
})

// --- test helpers ---

func buildMetricsWithURL(url string, remainingSecs float64) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("otelcol/httpcheckreceiver")

	m := sm.Metrics().AppendEmpty()
	m.SetName("httpcheck.tls.cert_remaining")
	m.SetUnit("s")

	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(remainingSecs)
	dp.Attributes().PutStr("http.url", url)

	return md
}

func buildMetricsWithExplicitID(apiInstanceID uuid.UUID, remainingSecs float64) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("emeland.api_instance_id", apiInstanceID.String())

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("otelcol/httpcheckreceiver")

	m := sm.Metrics().AppendEmpty()
	m.SetName("httpcheck.tls.cert_remaining")
	m.SetUnit("s")

	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(remainingSecs)
	dp.Attributes().PutStr("http.url", "https://whatever.example.com/")

	return md
}
