package events

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type EventType string

const (
	EventInsert EventType = "INSERT"
	EventUpdate EventType = "UPDATE"
	EventDelete EventType = "DELETE"
)

type Event struct {
	ID         string                 `json:"id"`
	Type       EventType              `json:"type"`
	Collection string                 `json:"collection"`
	Payload    map[string]interface{} `json:"payload"`
	OldImage   map[string]interface{} `json:"old_image,omitempty"` // For updates
	Timestamp  time.Time              `json:"timestamp"`
}

type SinkType string

const (
	SinkWebhook  SinkType = "WEBHOOK"
	SinkRedpanda SinkType = "REDPANDA"
)

type TriggerConfig struct {
	Name       string
	Collection string
	Events     []EventType
	Sink       SinkType
	Config     map[string]string // URL for webhook, Topic for redpanda
}

type TriggerManager struct {
	logger   *zap.Logger
	triggers map[string][]TriggerConfig // collection -> triggers
	mu       sync.RWMutex
	client   *http.Client
	redpanda *kgo.Client // Franz-go client
}

func NewTriggerManager(logger *zap.Logger, redpandaBrokers []string) *TriggerManager {
	tm := &TriggerManager{
		logger:   logger,
		triggers: make(map[string][]TriggerConfig),
		client:   &http.Client{Timeout: 5 * time.Second},
	}

	if len(redpandaBrokers) > 0 {
		opts := []kgo.Opt{
			kgo.SeedBrokers(redpandaBrokers...),
		}
		client, err := kgo.NewClient(opts...)
		if err != nil {
			logger.Error("Failed to create Redpanda client", zap.Error(err))
		} else {
			tm.redpanda = client
			logger.Info("Connected to Redpanda", zap.Strings("brokers", redpandaBrokers))
		}
	}

	return tm
}

func (tm *TriggerManager) AddTrigger(config TriggerConfig) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.triggers[config.Collection] = append(tm.triggers[config.Collection], config)
}

func (tm *TriggerManager) Fire(ctx context.Context, collection string, eventType EventType, payload, oldImage map[string]interface{}) {
	tm.mu.RLock()
	triggers, ok := tm.triggers[collection]
	tm.mu.RUnlock()

	if !ok {
		return
	}

	event := Event{
		ID:         fmt.Sprintf("%d", time.Now().UnixNano()),
		Type:       eventType,
		Collection: collection,
		Payload:    payload,
		OldImage:   oldImage,
		Timestamp:  time.Now(),
	}

	for _, t := range triggers {
		// Check if trigger cares about this event type
		shouldFire := false
		for _, et := range t.Events {
			if et == eventType {
				shouldFire = true
				break
			}
		}

		if shouldFire {
			go tm.executeTrigger(ctx, t, event)
		}
	}
}

func (tm *TriggerManager) executeTrigger(ctx context.Context, t TriggerConfig, event Event) {
	switch t.Sink {
	case SinkWebhook:
		url := t.Config["url"]
		if url == "" {
			return
		}
		data, _ := json.Marshal(event)
		resp, err := tm.client.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			tm.logger.Error("Webhook failed", zap.String("trigger", t.Name), zap.Error(err))
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			tm.logger.Error("Webhook error response", zap.String("trigger", t.Name), zap.Int("status", resp.StatusCode))
		}

	case SinkRedpanda:
		if tm.redpanda == nil {
			return
		}
		topic := t.Config["topic"]
		if topic == "" {
			topic = "events_" + t.Collection
		}

		val, _ := json.Marshal(event)
		record := &kgo.Record{
			Topic: topic,
			Key:   []byte(event.Collection),
			Value: val,
		}

		if err := tm.redpanda.ProduceSync(ctx, record).FirstErr(); err != nil {
			tm.logger.Error("Failed to produce to Redpanda", zap.String("trigger", t.Name), zap.Error(err))
		}
	}
}

func (tm *TriggerManager) Close() {
	if tm.redpanda != nil {
		tm.redpanda.Close()
	}
}
