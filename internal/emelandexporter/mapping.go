// Package emelandexporter implements the OTel Collector exporter component that
// maps httpcheck TLS metrics into EmELand Finding events.
//
// This file contains the pure mapping logic: given a probe outcome (failure or
// seconds until certificate expiry) and an ApiInstance UUID, produce the correct
// Finding events. It has no dependency on OTel pdata types so it can be tested
// in isolation.
package emelandexporter

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"go.emeland.io/modelsrv/pkg/events"
	"go.emeland.io/modelsrv/pkg/model/common"
	"go.emeland.io/modelsrv/pkg/model/finding"
)

// certFindingNamespace mirrors the endpointprobe namespace so finding IDs
// match between the native certprobe and the OTel exporter path.
var certFindingNamespace = uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

// metricMapper converts certificate-remaining durations into modelsrv Finding events.
type metricMapper struct {
	threshold time.Duration
	log       *zap.SugaredLogger
}

func newMetricMapper(threshold time.Duration, log *zap.SugaredLogger) *metricMapper {
	if log == nil {
		log = zap.NewNop().Sugar()
	}
	return &metricMapper{threshold: threshold, log: log}
}

// Reconcile applies the success/has-cert branch of endpointprobe.reconcileFinding:
//
//	remaining > threshold   -> delete all cert findings (resolved)
//	0 < remaining <= thresh -> CertificateExpiringSoon
//	remaining <= 0          -> CertificateExpired
func (m *metricMapper) Reconcile(apiInstanceID uuid.UUID, remaining time.Duration) []events.Event {
	switch {
	case remaining > m.threshold:
		// All clear: delete any existing findings for this instance.
		return m.deleteAllCertFindings(apiInstanceID)

	case remaining > 0:
		desc := fmt.Sprintf(
			"CertificateExpiringSoon: ApiInstance %s certificate expires in %s (threshold %s)",
			apiInstanceID, remaining.Round(time.Second), m.threshold,
		)
		return append(
			m.upsertFinding(apiInstanceID, finding.CertificateExpiringSoon, desc),
			m.deleteFinding(apiInstanceID, finding.CertificateExpired),
			m.deleteFinding(apiInstanceID, finding.CertificateProbeFailed),
		)

	default:
		desc := fmt.Sprintf(
			"CertificateExpired: ApiInstance %s certificate expired %s ago",
			apiInstanceID, (-remaining).Round(time.Second),
		)
		return append(
			m.upsertFinding(apiInstanceID, finding.CertificateExpired, desc),
			m.deleteFinding(apiInstanceID, finding.CertificateExpiringSoon),
			m.deleteFinding(apiInstanceID, finding.CertificateProbeFailed),
		)
	}
}

// ReconcileProbeFailed applies the !Success branch of endpointprobe.reconcileFinding:
// upsert CertificateProbeFailed and delete ExpiringSoon/Expired.
// errMsg is optional detail from the httpcheck error.message attribute.
func (m *metricMapper) ReconcileProbeFailed(apiInstanceID uuid.UUID, errMsg string) []events.Event {
	desc := fmt.Sprintf("CertificateProbeFailed: probe of ApiInstance %s failed", apiInstanceID)
	if errMsg != "" {
		desc = fmt.Sprintf("CertificateProbeFailed: probe of ApiInstance %s failed: %s", apiInstanceID, errMsg)
	}
	return append(
		m.upsertFinding(apiInstanceID, finding.CertificateProbeFailed, desc),
		m.deleteFinding(apiInstanceID, finding.CertificateExpiringSoon),
		m.deleteFinding(apiInstanceID, finding.CertificateExpired),
	)
}

func (m *metricMapper) upsertFinding(apiInstanceID uuid.UUID, kind finding.FindingKind, description string) []events.Event {
	fID := findingID(apiInstanceID, kind)
	f := finding.NewFinding(fID)
	f.SetFindingTypeById(finding.TypeIDForKind(kind))
	f.SetDisplayName("Certificate expiry check")
	f.SetDescription(description)
	f.SetResources([]*common.ResourceRef{
		{ResourceId: apiInstanceID, ResourceType: events.APIInstanceResource},
	})

	return []events.Event{{
		ResourceType: events.FindingResource,
		Operation:    events.CreateOperation,
		ResourceId:   fID,
		Objects:      []any{f},
	}}
}

func (m *metricMapper) deleteFinding(apiInstanceID uuid.UUID, kind finding.FindingKind) events.Event {
	return events.Event{
		ResourceType: events.FindingResource,
		Operation:    events.DeleteOperation,
		ResourceId:   findingID(apiInstanceID, kind),
	}
}

func (m *metricMapper) deleteAllCertFindings(apiInstanceID uuid.UUID) []events.Event {
	return []events.Event{
		m.deleteFinding(apiInstanceID, finding.CertificateExpiringSoon),
		m.deleteFinding(apiInstanceID, finding.CertificateExpired),
		m.deleteFinding(apiInstanceID, finding.CertificateProbeFailed),
	}
}

// findingID produces the same deterministic UUID as endpointprobe.findingID
// so that findings from the native certprobe and the OTel exporter path are
// treated as the same resource.
func findingID(apiInstanceID uuid.UUID, kind finding.FindingKind) uuid.UUID {
	key := append(apiInstanceID[:], []byte(kind)...)
	return uuid.NewSHA1(certFindingNamespace, key)
}
