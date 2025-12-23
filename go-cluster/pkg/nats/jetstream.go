package nats

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Interfaces for Storage used by JetStream
// This allows plugging in LumaDB's actual storage engine later.

type StorageEngine interface {
	// Placeholder for storage engine methods
}

type MessageStore struct {
	storage StorageEngine
	stream  string
	msgs    []*Message // In-memory fallback
	mu      sync.RWMutex
}

func NewMessageStore(storage StorageEngine, stream string) *MessageStore {
	return &MessageStore{
		storage: storage,
		stream:  stream,
		msgs:    make([]*Message, 0),
	}
}

func (ms *MessageStore) Store(msg *Message) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.msgs = append(ms.msgs, msg)
}

func (ms *MessageStore) DeleteFirst() int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.msgs) == 0 {
		return 0
	}
	size := len(ms.msgs[0].Data)
	ms.msgs = ms.msgs[1:]
	return size
}

func (ms *MessageStore) GetFirst() *Message {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if len(ms.msgs) == 0 {
		return nil
	}
	return ms.msgs[0]
}

// KVBucket represents a Key-Value bucket in JetStream
type KVBucket struct {
	Name    string
	streams map[string][]byte
	mu      sync.RWMutex
}

func (b *KVBucket) Put(key string, value []byte, headers map[string]string) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.streams[key] = value
	return 1, nil // Mock revision
}

func (b *KVBucket) Get(key string) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if v, ok := b.streams[key]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("key not found: %s", key)
}

func (b *KVBucket) Delete(key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.streams, key)
	return nil
}

func (b *KVBucket) Purge(key string) error {
	return b.Delete(key)
}

type ObjBucket struct {
	Name string
}

// JetStreamEngine manages streams, consumers, and persistence
type JetStreamEngine struct {
	storage    StorageEngine
	streams    sync.Map // name -> *Stream
	consumers  sync.Map // streamName.consumerName -> *Consumer
	kvBuckets  sync.Map // name -> *KVBucket
	objBuckets sync.Map // name -> *ObjBucket
	mu         sync.RWMutex
}

func (js *JetStreamEngine) getOrCreateKVBucket(name string) (*KVBucket, error) {
	if v, ok := js.kvBuckets.Load(name); ok {
		return v.(*KVBucket), nil
	}
	b := &KVBucket{
		Name:    name,
		streams: make(map[string][]byte),
	}
	js.kvBuckets.Store(name, b)
	return b, nil
}

// Stream configuration matching NATS JetStream
type StreamConfig struct {
	Name         string          `json:"name"`
	Description  string          `json:"description,omitempty"`
	Subjects     []string        `json:"subjects"`
	Retention    RetentionPolicy `json:"retention"`
	MaxConsumers int             `json:"max_consumers"`
	MaxMsgs      int64           `json:"max_msgs"`
	MaxBytes     int64           `json:"max_bytes"`
	MaxAge       time.Duration   `json:"max_age"`
	MaxMsgSize   int32           `json:"max_msg_size"`
	Storage      StorageType     `json:"storage"`
	Replicas     int             `json:"num_replicas"`
	NoAck        bool            `json:"no_ack"`
	Duplicates   time.Duration   `json:"duplicate_window"`
	Placement    *Placement      `json:"placement,omitempty"`
	Mirror       *StreamSource   `json:"mirror,omitempty"`
	Sources      []*StreamSource `json:"sources,omitempty"`
	Discard      DiscardPolicy   `json:"discard"`
	DenyDelete   bool            `json:"deny_delete"`
	DenyPurge    bool            `json:"deny_purge"`
	AllowRollup  bool            `json:"allow_rollup_hdrs"`
	AllowDirect  bool            `json:"allow_direct"`
	MirrorDirect bool            `json:"mirror_direct"`
}

type Placement struct {
	Cluster string   `json:"cluster,omitempty"`
	Tags    []string `json:"tags,omitempty"`
}

type StreamSource struct {
	Name          string     `json:"name"`
	OptStartSeq   uint64     `json:"opt_start_seq,omitempty"`
	OptStartTime  *time.Time `json:"opt_start_time,omitempty"`
	FilterSubject string     `json:"filter_subject,omitempty"`
}

type RetentionPolicy int

const (
	LimitsPolicy RetentionPolicy = iota
	InterestPolicy
	WorkQueuePolicy
)

type StorageType int

const (
	FileStorage StorageType = iota
	MemoryStorage
)

type DiscardPolicy int

const (
	DiscardOld DiscardPolicy = iota
	DiscardNew
)

// Stream represents a JetStream stream
type Stream struct {
	config    StreamConfig
	state     StreamState
	messages  *MessageStore
	consumers sync.Map
	mu        sync.RWMutex
}

type StreamState struct {
	Msgs      uint64    `json:"messages"`
	Bytes     uint64    `json:"bytes"`
	FirstSeq  uint64    `json:"first_seq"`
	LastSeq   uint64    `json:"last_seq"`
	FirstTime time.Time `json:"first_ts"`
	LastTime  time.Time `json:"last_ts"`
}

// ConsumerConfig matching NATS JetStream
type ConsumerConfig struct {
	Durable           string          `json:"durable_name,omitempty"`
	Name              string          `json:"name,omitempty"`
	Description       string          `json:"description,omitempty"`
	DeliverPolicy     DeliverPolicy   `json:"deliver_policy"`
	OptStartSeq       uint64          `json:"opt_start_seq,omitempty"`
	OptStartTime      *time.Time      `json:"opt_start_time,omitempty"`
	AckPolicy         AckPolicy       `json:"ack_policy"`
	AckWait           time.Duration   `json:"ack_wait"`
	MaxDeliver        int             `json:"max_deliver"`
	BackOff           []time.Duration `json:"backoff,omitempty"`
	FilterSubject     string          `json:"filter_subject,omitempty"`
	FilterSubjects    []string        `json:"filter_subjects,omitempty"`
	ReplayPolicy      ReplayPolicy    `json:"replay_policy"`
	RateLimit         uint64          `json:"rate_limit_bps,omitempty"`
	SampleFrequency   string          `json:"sample_freq,omitempty"`
	MaxWaiting        int             `json:"max_waiting"`
	MaxAckPending     int             `json:"max_ack_pending"`
	Heartbeat         time.Duration   `json:"idle_heartbeat,omitempty"`
	FlowControl       bool            `json:"flow_control"`
	HeadersOnly       bool            `json:"headers_only"`
	MaxBatch          int             `json:"max_batch"`
	MaxExpires        time.Duration   `json:"max_expires"`
	InactiveThreshold time.Duration   `json:"inactive_threshold"`
	Replicas          int             `json:"num_replicas"`
	MemStorage        bool            `json:"mem_storage"`
}

type DeliverPolicy int

const (
	DeliverAll DeliverPolicy = iota
	DeliverLast
	DeliverNew
	DeliverByStartSequence
	DeliverByStartTime
	DeliverLastPerSubject
)

type AckPolicy int

const (
	AckExplicit AckPolicy = iota
	AckNone
	AckAll
)

type ReplayPolicy int

const (
	ReplayInstant ReplayPolicy = iota
	ReplayOriginal
)

// Consumer represents a JetStream consumer
type Consumer struct {
	stream     *Stream
	config     ConsumerConfig
	state      ConsumerState
	ackPending sync.Map // seq -> *PendingAck
	mu         sync.RWMutex
}

type ConsumerState struct {
	Delivered      SequenceInfo `json:"delivered"`
	AckFloor       SequenceInfo `json:"ack_floor"`
	NumAckPending  int          `json:"num_ack_pending"`
	NumRedelivered int          `json:"num_redelivered"`
	NumWaiting     int          `json:"num_waiting"`
	NumPending     uint64       `json:"num_pending"`
}

type SequenceInfo struct {
	Consumer uint64    `json:"consumer_seq"`
	Stream   uint64    `json:"stream_seq"`
	Last     time.Time `json:"last_active,omitempty"`
}

func NewJetStreamEngine(storage StorageEngine) *JetStreamEngine {
	return &JetStreamEngine{
		storage: storage,
	}
}

// HandleStreamAPI processes stream API calls
func (js *JetStreamEngine) HandleStreamAPI(action string, args []string, payload []byte) ([]byte, error) {
	switch action {
	case "CREATE":
		return js.createStream(payload)
	case "UPDATE":
		if len(args) < 1 {
			return nil, fmt.Errorf("stream name required")
		}
		return js.updateStream(args[0], payload)
	case "DELETE":
		if len(args) < 1 {
			return nil, fmt.Errorf("stream name required")
		}
		return js.deleteStream(args[0])
	case "INFO":
		if len(args) < 1 {
			return nil, fmt.Errorf("stream name required")
		}
		return js.streamInfo(args[0])
	case "LIST":
		return js.listStreams()
	case "PURGE":
		if len(args) < 1 {
			return nil, fmt.Errorf("stream name required")
		}
		return js.purgeStream(args[0], payload)
	case "NAMES":
		return js.streamNames()
	default:
		return nil, fmt.Errorf("unknown stream action: %s", action)
	}
}

func (js *JetStreamEngine) createStream(payload []byte) ([]byte, error) {
	var config StreamConfig
	if err := json.Unmarshal(payload, &config); err != nil {
		return nil, err
	}

	if _, exists := js.streams.Load(config.Name); exists {
		return nil, fmt.Errorf("stream already exists: %s", config.Name)
	}

	stream := &Stream{
		config:   config,
		messages: NewMessageStore(js.storage, config.Name),
		state: StreamState{
			FirstSeq: 1,
			LastSeq:  0,
		},
	}

	js.streams.Store(config.Name, stream)

	// Return stream info
	return js.streamInfo(config.Name)
}

func (js *JetStreamEngine) updateStream(name string, payload []byte) ([]byte, error) {
	// Simple update logic
	val, ok := js.streams.Load(name)
	if !ok {
		return nil, fmt.Errorf("stream not found: %s", name)
	}
	stream := val.(*Stream)

	var config StreamConfig
	if err := json.Unmarshal(payload, &config); err != nil {
		return nil, err
	}

	stream.mu.Lock()
	stream.config = config
	stream.mu.Unlock()

	return js.streamInfo(name)
}

func (js *JetStreamEngine) deleteStream(name string) ([]byte, error) {
	if _, ok := js.streams.LoadAndDelete(name); !ok {
		return nil, fmt.Errorf("stream not found: %s", name)
	}
	return []byte(`{"result": true}`), nil
}

func (js *JetStreamEngine) listStreams() ([]byte, error) {
	var streams []string
	js.streams.Range(func(key, value any) bool {
		streams = append(streams, key.(string))
		return true
	})
	return json.Marshal(map[string]interface{}{"streams": streams})
}

func (js *JetStreamEngine) purgeStream(name string, payload []byte) ([]byte, error) {
	// Mock purge
	return []byte(`{"purged": 0}`), nil
}

func (js *JetStreamEngine) streamNames() ([]byte, error) {
	return js.listStreams()
}

func (js *JetStreamEngine) streamInfo(name string) ([]byte, error) {
	val, ok := js.streams.Load(name)
	if !ok {
		return nil, fmt.Errorf("stream not found: %s", name)
	}

	stream := val.(*Stream)
	stream.mu.RLock()
	defer stream.mu.RUnlock()

	info := map[string]interface{}{
		"type":    "io.nats.jetstream.api.v1.stream_info_response",
		"config":  stream.config,
		"state":   stream.state,
		"created": time.Now(), // Should be actual creation time
	}

	return json.Marshal(info)
}

// HandleConsumerAPI processes consumer API calls
func (js *JetStreamEngine) HandleConsumerAPI(action string, args []string, payload []byte) ([]byte, error) {
	switch action {
	case "CREATE":
		if len(args) < 1 {
			return nil, fmt.Errorf("stream name required")
		}
		return js.createConsumer(args[0], payload)
	case "DELETE":
		if len(args) < 2 {
			return nil, fmt.Errorf("stream and consumer name required")
		}
		return js.deleteConsumer(args[0], args[1])
	case "INFO":
		if len(args) < 2 {
			return nil, fmt.Errorf("stream and consumer name required")
		}
		return js.consumerInfo(args[0], args[1])
	case "LIST":
		if len(args) < 1 {
			return nil, fmt.Errorf("stream name required")
		}
		return js.listConsumers(args[0])
	case "NAMES":
		if len(args) < 1 {
			return nil, fmt.Errorf("stream name required")
		}
		return js.consumerNames(args[0])
	default:
		return nil, fmt.Errorf("unknown consumer action: %s", action)
	}
}

func (js *JetStreamEngine) createConsumer(streamName string, payload []byte) ([]byte, error) {
	val, ok := js.streams.Load(streamName)
	if !ok {
		return nil, fmt.Errorf("stream not found: %s", streamName)
	}

	stream := val.(*Stream)

	var config ConsumerConfig
	if err := json.Unmarshal(payload, &config); err != nil {
		return nil, err
	}

	name := config.Durable
	if name == "" {
		name = config.Name
	}
	if name == "" {
		name = fmt.Sprintf("ephemeral_%d", time.Now().UnixNano())
	}

	consumer := &Consumer{
		stream: stream,
		config: config,
		state: ConsumerState{
			Delivered: SequenceInfo{},
			AckFloor:  SequenceInfo{},
		},
	}

	key := fmt.Sprintf("%s.%s", streamName, name)
	js.consumers.Store(key, consumer)
	stream.consumers.Store(name, consumer)

	return js.consumerInfo(streamName, name)
}

func (js *JetStreamEngine) deleteConsumer(stream, consumer string) ([]byte, error) {
	key := fmt.Sprintf("%s.%s", stream, consumer)
	js.consumers.Delete(key)
	return []byte(`{"result": true}`), nil
}

func (js *JetStreamEngine) consumerInfo(stream, consumer string) ([]byte, error) {
	key := fmt.Sprintf("%s.%s", stream, consumer)
	val, ok := js.consumers.Load(key)
	if !ok {
		return nil, fmt.Errorf("consumer not found")
	}
	c := val.(*Consumer)
	return json.Marshal(map[string]interface{}{
		"type":   "io.nats.jetstream.api.v1.consumer_info_response",
		"config": c.config,
		"state":  c.state,
	})
}

func (js *JetStreamEngine) listConsumers(stream string) ([]byte, error) {
	// Filter consumers by stream... keeping simple for now
	return []byte(`{"consumers": []}`), nil
}

func (js *JetStreamEngine) consumerNames(stream string) ([]byte, error) {
	return []byte(`{"consumers": []}`), nil
}

func (js *JetStreamEngine) HandleMessageAPI(action string, args []string, payload []byte) ([]byte, error) {
	// Implement MSG GET, etc.
	return []byte("{}"), nil
}

func (js *JetStreamEngine) HandleInfo() ([]byte, error) {
	return []byte(`{"account_info": {}}`), nil
}

// ProcessMessage handles incoming messages for streams
func (js *JetStreamEngine) ProcessMessage(msg *Message) {
	js.streams.Range(func(key, value any) bool {
		stream := value.(*Stream)

		// Check if any subject pattern matches
		for _, pattern := range stream.config.Subjects {
			if subjectMatches(pattern, msg.Subject) {
				stream.addMessage(msg)
				break
			}
		}

		return true
	})
}

func (s *Stream) addMessage(msg *Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state.LastSeq++
	msg.Seq = s.state.LastSeq

	if s.state.Msgs == 0 {
		s.state.FirstSeq = msg.Seq
		s.state.FirstTime = msg.Time
	}

	s.state.Msgs++
	s.state.Bytes += uint64(len(msg.Data))
	s.state.LastTime = msg.Time

	// Store message
	s.messages.Store(msg)

	// Apply retention limits
	s.applyLimits()
}

func (s *Stream) applyLimits() {
	// Apply max messages limit
	for s.config.MaxMsgs > 0 && int64(s.state.Msgs) > s.config.MaxMsgs {
		s.messages.DeleteFirst()
		s.state.Msgs--
		s.state.FirstSeq++
	}

	// Apply max bytes limit
	for s.config.MaxBytes > 0 && int64(s.state.Bytes) > s.config.MaxBytes {
		size := s.messages.DeleteFirst()
		s.state.Msgs--
		s.state.Bytes -= uint64(size)
		s.state.FirstSeq++
	}

	// Apply max age limit
	if s.config.MaxAge > 0 {
		cutoff := time.Now().Add(-s.config.MaxAge)
		for {
			msg := s.messages.GetFirst()
			if msg == nil || msg.Time.After(cutoff) {
				break
			}
			s.messages.DeleteFirst()
			s.state.Msgs--
			s.state.Bytes -= uint64(len(msg.Data))
			s.state.FirstSeq++
		}
	}
}

// KV Store operations
func (js *JetStreamEngine) KVPut(bucket, key string, value []byte, headers map[string]string) ([]byte, error) {
	bkt, err := js.getOrCreateKVBucket(bucket)
	if err != nil {
		return nil, err
	}

	revision, err := bkt.Put(key, value, headers)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]interface{}{
		"seq": revision,
	})
}

func (js *JetStreamEngine) KVGet(bucket, key string) ([]byte, error) {
	val, ok := js.kvBuckets.Load(bucket)
	if !ok {
		return nil, fmt.Errorf("bucket not found: %s", bucket)
	}

	bkt := val.(*KVBucket)
	return bkt.Get(key)
}

func (js *JetStreamEngine) KVDelete(bucket, key string) error {
	val, ok := js.kvBuckets.Load(bucket)
	if !ok {
		return fmt.Errorf("bucket not found: %s", bucket)
	}

	bkt := val.(*KVBucket)
	return bkt.Delete(key)
}

func (js *JetStreamEngine) KVPurge(bucket, key string) error {
	val, ok := js.kvBuckets.Load(bucket)
	if !ok {
		return fmt.Errorf("bucket not found: %s", bucket)
	}

	bkt := val.(*KVBucket)
	return bkt.Purge(key)
}

// Helper functions
func subjectMatches(pattern, subject string) bool {
	switch pattern {
	case ">":
		return true
	case "*":
		return true // technically * only matches one token, but simplifying for MVP
	}
	// TODO: Full wildcard matching logic
	return pattern == subject
}
