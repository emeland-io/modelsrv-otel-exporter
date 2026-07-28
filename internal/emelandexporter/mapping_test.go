package emelandexporter_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"emeland.io/modelsrv-otel-exporter/internal/emelandexporter"
	"go.emeland.io/modelsrv/pkg/events"
	"go.emeland.io/modelsrv/pkg/model/finding"
)

func TestEmelandExporter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EmeLand Exporter Suite")
}

var _ = Describe("MetricMapper.Reconcile", func() {
	var (
		mapper        *emelandexporter.MetricMapper
		apiInstanceID uuid.UUID
		threshold     = 30 * 24 * time.Hour // 30 days
	)

	BeforeEach(func() {
		apiInstanceID = uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
		mapper = emelandexporter.NewMetricMapperForTest(threshold, nil)
	})

	It("produces CertificateExpiringSoon when 0 < remaining <= threshold", func() {
		remaining := 5 * 24 * time.Hour

		evs := mapper.Reconcile(apiInstanceID, remaining)

		Expect(evs).ToNot(BeEmpty())
		upserts, deletes := splitEvents(evs)
		Expect(upserts).To(HaveLen(1))
		Expect(upserts[0].ResourceType).To(Equal(events.FindingResource))
		f := upserts[0].Objects[0].(finding.Finding)
		Expect(f.GetFindingTypeId()).To(Equal(finding.TypeIDForKind(finding.CertificateExpiringSoon)))
		Expect(deletes).To(HaveLen(2))
	})

	It("produces CertificateExpired when remaining <= 0", func() {
		remaining := -1 * time.Hour

		evs := mapper.Reconcile(apiInstanceID, remaining)

		upserts, _ := splitEvents(evs)
		Expect(upserts).To(HaveLen(1))
		f := upserts[0].Objects[0].(finding.Finding)
		Expect(f.GetFindingTypeId()).To(Equal(finding.TypeIDForKind(finding.CertificateExpired)))
	})

	It("deletes all findings when remaining > threshold (resolved)", func() {
		remaining := 90 * 24 * time.Hour

		evs := mapper.Reconcile(apiInstanceID, remaining)

		for _, ev := range evs {
			Expect(ev.Operation).To(Equal(events.DeleteOperation))
		}
		Expect(evs).To(HaveLen(3))
	})

	It("produces deterministic finding IDs matching endpointprobe", func() {
		remaining := 5 * 24 * time.Hour

		evs := mapper.Reconcile(apiInstanceID, remaining)

		expectedID := expectedFindingID(apiInstanceID, finding.CertificateExpiringSoon)
		var found bool
		for _, ev := range evs {
			if ev.Operation == events.CreateOperation && ev.ResourceId == expectedID {
				found = true
			}
		}
		Expect(found).To(BeTrue(), "expected finding ID %s in events", expectedID)
	})

	It("links findings to the correct ApiInstance via ResourceRef", func() {
		remaining := 5 * 24 * time.Hour

		evs := mapper.Reconcile(apiInstanceID, remaining)

		upserts, _ := splitEvents(evs)
		Expect(upserts).To(HaveLen(1))
		f := upserts[0].Objects[0].(finding.Finding)
		refs := f.GetResources()
		Expect(refs).To(HaveLen(1))
		Expect(refs[0].ResourceId).To(Equal(apiInstanceID))
		Expect(refs[0].ResourceType).To(Equal(events.APIInstanceResource))
	})
})

// --- helpers ---

func splitEvents(evs []events.Event) (upserts, deletes []events.Event) {
	for _, ev := range evs {
		if ev.Operation == events.CreateOperation {
			upserts = append(upserts, ev)
		} else {
			deletes = append(deletes, ev)
		}
	}
	return
}

func expectedFindingID(apiInstanceID uuid.UUID, kind finding.FindingKind) uuid.UUID {
	ns := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
	key := append(apiInstanceID[:], []byte(kind)...)
	return uuid.NewSHA1(ns, key)
}
