package sensor

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"go.emeland.io/modelsrv/pkg/client"
	"go.emeland.io/modelsrv/pkg/events"
)

// eventManager records all events in a master list and replicates them to
// registered subscribers. It implements [events.EventManager].
type eventManager struct {
	mu          sync.Mutex
	log         *zap.SugaredLogger
	sequence    uint64
	subscribers []events.Subscriber
	master      *events.ListSink
	sink        events.EventSink
}

var _ events.EventManager = (*eventManager)(nil)

func newEventManager(log *zap.SugaredLogger) *eventManager {
	if log == nil {
		log = zap.NewNop().Sugar()
	}
	return &eventManager{
		log:         log,
		subscribers: make([]events.Subscriber, 0),
		master:      events.NewListSink(),
	}
}

func (e *eventManager) GetCurrentSequenceId(_ context.Context) (uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.sequence, nil
}

func (e *eventManager) IncrementSequenceId(_ context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sequence++
	return nil
}

func (e *eventManager) SetSinkFactory(_ func() (events.EventSink, error)) {}

func (e *eventManager) GetSink() (events.EventSink, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.sink == nil {
		e.sink = &recordingSink{mgr: e}
	}
	return e.sink, nil
}

func (e *eventManager) GetSubscribers() []events.Subscriber {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]events.Subscriber, len(e.subscribers))
	copy(out, e.subscribers)
	return out
}

func (e *eventManager) AddSubscriber(subURL string) error {
	subURL = strings.TrimSpace(subURL)
	if subURL == "" {
		return fmt.Errorf("empty subscriber url")
	}
	if _, err := url.Parse(subURL); err != nil {
		return fmt.Errorf("invalid subscriber url %q: %w", subURL, err)
	}

	e.mu.Lock()
	for _, sub := range e.subscribers {
		if sub.GetURL() == subURL {
			e.mu.Unlock()
			e.log.Infow("subscriber already registered", "subscriber", subURL)
			return nil
		}
	}
	e.mu.Unlock()

	sub, err := newSubscriber(subURL, e.log)
	if err != nil {
		return err
	}

	e.mu.Lock()
	for _, existing := range e.subscribers {
		if existing.GetURL() == subURL {
			e.mu.Unlock()
			return nil
		}
	}
	e.subscribers = append(e.subscribers, sub)
	rawPast := e.master.GetEvents()
	past := make([]events.Event, len(rawPast))
	copy(past, rawPast)
	e.mu.Unlock()

	e.log.Infow("subscriber registered", "subscriber", sub.GetURL())

	// Replay past events to new subscriber so it catches up.
	if len(past) == 0 {
		return nil
	}
	var okCount, errCount int
	for _, ev := range past {
		evCopy := ev
		if err := sub.Notify(context.Background(), &evCopy); err != nil {
			errCount++
			continue
		}
		okCount++
	}
	e.log.Infow("subscriber replay complete",
		"subscriber", sub.GetURL(),
		"replayed", len(past), "ok", okCount, "failed", errCount,
	)
	return nil
}

func (e *eventManager) RemoveSubscriber(url string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, sub := range e.subscribers {
		if sub.GetURL() == url {
			e.subscribers = append(e.subscribers[:i], e.subscribers[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("subscriber %s not found", url)
}

func (e *eventManager) QueryEvents(_ context.Context, _ events.EventQuery) ([]events.StoredEvent, error) {
	return nil, nil
}

// recordingSink records events in the master list and notifies subscribers.
type recordingSink struct {
	mgr *eventManager
}

var _ events.EventSink = (*recordingSink)(nil)

func (r *recordingSink) Receive(resType events.ResourceType, op events.Operation, resourceId uuid.UUID, objects ...any) error {
	ev := events.Event{
		ResourceType: resType,
		Operation:    op,
		ResourceId:   resourceId,
		Objects:      objects,
	}
	r.mgr.mu.Lock()
	_ = r.mgr.master.Receive(resType, op, resourceId, objects...)
	r.mgr.sequence++
	subs := make([]events.Subscriber, len(r.mgr.subscribers))
	copy(subs, r.mgr.subscribers)
	r.mgr.mu.Unlock()

	for _, sub := range subs {
		if err := sub.Notify(context.Background(), &ev); err != nil {
			r.mgr.log.Errorw("subscriber notify failed",
				"subscriber", sub.GetURL(),
				"error", err,
			)
		}
	}
	return nil
}

// subscriber wraps a downstream modelsrv client.
type subscriber struct {
	url    string
	id     uuid.UUID
	status string
	c      *client.ModelSrvClient
	log    *zap.SugaredLogger
}

var _ events.Subscriber = (*subscriber)(nil)

func newSubscriber(url string, log *zap.SugaredLogger) (*subscriber, error) {
	c, err := client.NewModelSrvClient(url)
	if err != nil {
		return nil, err
	}
	return &subscriber{
		url:    url,
		id:     uuid.New(),
		status: "active",
		c:      c,
		log:    log,
	}, nil
}

func (s *subscriber) Notify(ctx context.Context, event *events.Event) error {
	if event == nil {
		return fmt.Errorf("nil event")
	}
	return s.c.PostEvent(ctx, event)
}

func (s *subscriber) GetURL() string    { return s.url }
func (s *subscriber) GetId() uuid.UUID  { return s.id }
func (s *subscriber) GetStatus() string { return s.status }
