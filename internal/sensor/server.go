// Package sensor implements an in-memory modelsrv instance that exposes the
// standard modelsrv HTTP API. Other modelsrv instances can subscribe and
// receive replicated events -- exactly the same pattern used by git-sensor,
// helm-sensor, etc.
//
// The OTel exporter logic (added in a later commit) will call [Server.Emit] to
// apply Finding events into this model.
package sensor

import (
	"fmt"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"go.emeland.io/modelsrv/pkg/endpoint"
	"go.emeland.io/modelsrv/pkg/events"
	"go.emeland.io/modelsrv/pkg/model"
	"go.emeland.io/modelsrv/pkg/model/finding"
	"go.emeland.io/modelsrv/pkg/model/node"
)

// otelExporterNodeTypeID is the stable identity for the "otel-exporter" NodeType.
// All instances of this sensor share this UUID.
var otelExporterNodeTypeID = uuid.MustParse("f7a1b2c3-d4e5-6f78-9012-abcdef345678")

// Server runs a modelsrv web endpoint backed by a local in-memory model and
// forwards changes to subscribers. It is the modelsrv "shell" that the OTel
// exporter logic emits findings into.
type Server struct {
	events *eventManager
	model  model.Model
	nodeID uuid.UUID
	log    *zap.SugaredLogger
}

// New starts a sensor server bound to listenAddr, pre-registering any provided
// subscriber URLs. It registers the well-known certificate FindingTypes so that
// findings produced by the mapper are immediately valid.
func New(listenAddr string, subscribers []string, log *zap.SugaredLogger) (*Server, error) {
	if log == nil {
		log = zap.NewNop().Sugar()
	}

	em := newEventManager(log)

	sink, err := em.GetSink()
	if err != nil {
		return nil, err
	}

	m, err := model.NewModel(sink)
	if err != nil {
		return nil, err
	}

	// Register our node type + node so this sensor is visible in the model.
	nt := node.NewNodeType(otelExporterNodeTypeID)
	nt.SetDisplayName("otel-exporter")
	if err := m.AddNodeType(nt); err != nil {
		return nil, fmt.Errorf("register node type: %w", err)
	}

	nodeID := uuid.New()
	n := node.NewNode(nodeID)
	n.SetDisplayName("otel-exporter")
	n.SetNodeTypeByRef(nt)
	if err := m.AddNode(n); err != nil {
		return nil, fmt.Errorf("register node: %w", err)
	}

	// Ensure well-known certificate FindingTypes exist in the model.
	ensureCertFindingTypes(m, log)

	// Register downstream subscribers (they receive all events from here on).
	for _, s := range subscribers {
		if err := em.AddSubscriber(s); err != nil {
			return nil, err
		}
	}

	if err := endpoint.StartWebListener(m, em, listenAddr, endpoint.WebListenerOptions{}); err != nil {
		return nil, err
	}

	log.Infow("otel-exporter sensor started",
		"nodeTypeId", otelExporterNodeTypeID,
		"nodeId", nodeID,
		"listen", listenAddr,
	)

	return &Server{events: em, model: m, nodeID: nodeID, log: log}, nil
}

// Close shuts down the sensor's HTTP listener and removes its Node from the model.
func (s *Server) Close() error {
	if err := s.model.DeleteNodeById(s.nodeID); err != nil {
		s.log.Warnw("failed to delete node on shutdown", "nodeId", s.nodeID, "error", err)
	}
	endpoint.StopWebListener()
	return nil
}

// Emit applies the event to this process's model and forwards it through the
// event manager to all registered subscribers.
func (s *Server) Emit(ev events.Event) error {
	return s.model.Apply(ev)
}

// ensureCertFindingTypes registers the well-known certificate finding types so
// that findings reference valid types from the start.
func ensureCertFindingTypes(m model.Model, log *zap.SugaredLogger) {
	kinds := []finding.FindingKind{
		finding.CertificateExpiringSoon,
		finding.CertificateExpired,
		finding.CertificateProbeFailed,
	}
	for _, kind := range kinds {
		id := finding.TypeIDForKind(kind)
		if ft := m.GetFindingTypeById(id); ft != nil {
			continue
		}
		ft := finding.NewFindingType(id)
		ft.SetDisplayName(string(kind))
		if desc := finding.DescriptionForKind(kind); desc != "" {
			ft.SetDescription(desc)
		}
		if err := m.AddFindingType(ft); err != nil {
			log.Warnw("failed to register FindingType", "kind", kind, "error", err)
		}
	}
}
