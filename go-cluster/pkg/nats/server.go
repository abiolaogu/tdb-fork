package nats

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Server implements NATS protocol server
type Server struct {
	listener   net.Listener
	jetstream  *JetStreamEngine
	clients    sync.Map
	nextCID    uint64
	opts       *Options
	running    atomic.Bool
	shutdownCh chan struct{}
}

type Options struct {
	Host             string
	Port             int
	MaxPayload       int64
	WriteDeadline    time.Duration
	ReadDeadline     time.Duration
	JetStreamEnabled bool
}

func NewServer(opts *Options, js *JetStreamEngine) *Server {
	return &Server{
		jetstream:  js,
		opts:       opts,
		shutdownCh: make(chan struct{}),
	}
}

func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.opts.Host, s.opts.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = listener
	s.running.Store(true)

	go s.acceptLoop()
	return nil
}

func (s *Server) acceptLoop() {
	for s.running.Load() {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.running.Load() {
				continue
			}
			return
		}

		cid := atomic.AddUint64(&s.nextCID, 1)
		client := newClient(cid, conn, s)
		s.clients.Store(cid, client)
		go client.process()
	}
}

func (s *Server) routeMessage(msg *Message) {
	// Route to interested subscribers (simple O(N) loop for MVP)
	s.clients.Range(func(key, value any) bool {
		c := value.(*Client)
		c.subs.Range(func(sid, subVal any) bool {
			sub := subVal.(*Subscription)
			if sub.subject == msg.Subject || sub.subject == ">" {
				c.sendMessage(sub.subject, msg.Data)
			}
			return true
		})
		return true
	})
}

func (s *Server) addSubscription(sub *Subscription) {
	// In a real implementation, you'd add to a radix tree or similar structure
}

func (s *Server) removeSubscription(sub *Subscription) {
	// Remove from tracking
}

// Client represents a NATS client connection
type Client struct {
	cid    uint64
	conn   net.Conn
	server *Server
	reader *bufio.Reader
	writer *bufio.Writer
	subs   sync.Map // sid -> subscription
	mu     sync.Mutex
	closed atomic.Bool
}

func newClient(cid uint64, conn net.Conn, server *Server) *Client {
	return &Client{
		cid:    cid,
		conn:   conn,
		server: server,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}
}

func (c *Client) process() {
	defer c.close()

	// Send INFO on connect
	c.sendInfo()

	for !c.closed.Load() {
		line, err := c.reader.ReadString('\n')
		if err != nil {
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		c.processLine(line)
	}
}

func (c *Client) processLine(line string) {
	parts := strings.SplitN(line, " ", 2)
	cmd := strings.ToUpper(parts[0])

	switch cmd {
	case "CONNECT":
		// Ignore connect payload for now
	case "PING":
		c.sendPong()
	case "PONG":
		// Client pong, ignore
	case "PUB", "HPUB":
		c.handlePublish(line)
	case "SUB":
		if len(strings.Fields(line)) > 1 {
			c.handleSubscribe(strings.Fields(line))
		}
	case "UNSUB":
		if len(strings.Fields(line)) > 1 {
			c.handleUnsubscribe(strings.Fields(line))
		}
	case "MSG":
		// Server sends MSG, client shouldn't
	default:
		c.sendError(fmt.Sprintf("Unknown Protocol Operation: %s", cmd))
	}
}

func (c *Client) handlePublish(line string) {
	// Parse: PUB <subject> [reply-to] <#bytes>
	parts := strings.Fields(line)

	// Simplified parsing logic
	var subject, replyTo string
	var payloadSize int

	idx := 1
	subject = parts[idx]
	idx++

	if len(parts) == 4 {
		replyTo = parts[idx]
		idx++
	}

	if idx < len(parts) {
		payloadSize, _ = strconv.Atoi(parts[idx])
	}

	// Read payload
	payload := make([]byte, payloadSize)
	io.ReadFull(c.reader, payload)
	c.reader.ReadString('\n') // Consume trailing \r\n

	// Check if JetStream subject
	if strings.HasPrefix(subject, "$JS.API.") {
		// Mock headers for now
		headers := make(map[string]string)
		c.handleJetStreamAPI(subject, replyTo, headers, payload)
		return
	}

	// Check if KV subject
	if strings.HasPrefix(subject, "$KV.") {
		headers := make(map[string]string)
		c.handleKVOperation(subject, replyTo, headers, payload)
		return
	}

	// Regular publish
	msg := &Message{
		Subject: subject,
		ReplyTo: replyTo,
		Data:    payload,
		Time:    time.Now(),
	}

	if c.server.jetstream != nil {
		c.server.jetstream.ProcessMessage(msg)
	}

	c.server.routeMessage(msg)

	if replyTo != "" {
		c.sendOK()
	}
}

func (c *Client) handleJetStreamAPI(subject, replyTo string, headers map[string]string, payload []byte) {
	// Route JetStream API calls
	parts := strings.Split(subject, ".")
	if len(parts) < 4 {
		c.sendJSError(replyTo, "invalid api subject")
		return
	}

	apiType := parts[2] // STREAM, CONSUMER, MSG, etc.
	action := parts[3]  // CREATE, DELETE, INFO, etc.

	var response []byte
	var err error

	if c.server.jetstream != nil {
		switch apiType {
		case "STREAM":
			response, err = c.server.jetstream.HandleStreamAPI(action, parts[4:], payload)
		case "CONSUMER":
			response, err = c.server.jetstream.HandleConsumerAPI(action, parts[4:], payload)
		case "MSG":
			response, err = c.server.jetstream.HandleMessageAPI(action, parts[4:], payload)
		case "INFO":
			response, err = c.server.jetstream.HandleInfo()
		default:
			err = fmt.Errorf("unknown api type: %s", apiType)
		}
	} else {
		err = fmt.Errorf("jetstream not enabled")
	}

	if err != nil {
		c.sendJSError(replyTo, err.Error())
		return
	}

	c.sendMessage(replyTo, response)
}

func (c *Client) handleKVOperation(subject, replyTo string, headers map[string]string, payload []byte) {
	// $KV.<bucket>.<key>
	parts := strings.SplitN(strings.TrimPrefix(subject, "$KV."), ".", 2)
	if len(parts) < 2 {
		c.sendError("invalid kv subject")
		return
	}

	bucket := parts[0]
	key := parts[1]

	op := headers["KV-Operation"]
	if op == "" {
		op = "PUT"
	}

	var response []byte
	var err error

	if c.server.jetstream != nil {
		switch op {
		case "PUT":
			response, err = c.server.jetstream.KVPut(bucket, key, payload, headers)
		case "GET":
			response, err = c.server.jetstream.KVGet(bucket, key)
		case "DEL":
			err = c.server.jetstream.KVDelete(bucket, key)
		case "PURGE":
			err = c.server.jetstream.KVPurge(bucket, key)
		default:
			err = fmt.Errorf("unknown kv operation: %s", op)
		}
	} else {
		err = fmt.Errorf("jetstream not enabled")
	}

	if err != nil {
		c.sendError(err.Error())
		return
	}

	if replyTo != "" && response != nil {
		c.sendMessage(replyTo, response)
	} else {
		c.sendOK()
	}
}

func (c *Client) handleSubscribe(args []string) {
	// SUB <subject> [queue group] <sid>
	// Args includes command name, so index 1..

	var subject, queue, sid string

	// args[0] is SUB
	if len(args) == 3 {
		subject, sid = args[1], args[2]
	} else if len(args) == 4 {
		subject, queue, sid = args[1], args[2], args[3]
	} else {
		c.sendError("invalid subscription")
		return
	}

	sub := &Subscription{
		sid:     sid,
		subject: subject,
		queue:   queue,
		client:  c,
	}

	c.subs.Store(sid, sub)
	c.server.addSubscription(sub)
	c.sendOK()
}

func (c *Client) handleUnsubscribe(args []string) {
	// UNSUB <sid> [max-msgs]
	if len(args) < 2 {
		return
	}
	sid := args[1]

	if sub, ok := c.subs.LoadAndDelete(sid); ok {
		c.server.removeSubscription(sub.(*Subscription))
	}
	c.sendOK()
}

func (c *Client) sendInfo() {
	info := fmt.Sprintf(`INFO {"server_id":"lumadb-nats","server_name":"lumadb","version":"2.10.0","proto":1,"go":"go1.21","host":"%s","port":%d,"headers":true,"max_payload":%d,"jetstream":%t}`,
		c.server.opts.Host,
		c.server.opts.Port,
		c.server.opts.MaxPayload,
		c.server.opts.JetStreamEnabled,
	)
	c.write(info + "\r\n")
}

func (c *Client) sendOK() {
	c.write("+OK\r\n")
}

func (c *Client) sendPong() {
	c.write("PONG\r\n")
}

func (c *Client) sendError(msg string) {
	c.write(fmt.Sprintf("-ERR '%s'\r\n", msg))
}

func (c *Client) sendJSError(replyTo, msg string) {
	errResp := fmt.Sprintf(`{"type":"io.nats.jetstream.api.v1.error","error":{"code":500,"description":"%s"}}`, msg)
	c.sendMessage(replyTo, []byte(errResp))
}

func (c *Client) sendMessage(subject string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.writer.WriteString(fmt.Sprintf("MSG %s 0 %d\r\n", subject, len(data)))
	c.writer.Write(data)
	c.writer.WriteString("\r\n")
	c.writer.Flush()
}

func (c *Client) write(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writer.WriteString(s)
	c.writer.Flush()
}

func (c *Client) close() {
	if c.closed.Swap(true) {
		return
	}

	c.subs.Range(func(key, value any) bool {
		c.server.removeSubscription(value.(*Subscription))
		return true
	})

	c.conn.Close()
	c.server.clients.Delete(c.cid)
}

// Message represents a NATS message
type Message struct {
	Subject string
	ReplyTo string
	Headers map[string]string
	Data    []byte
	Time    time.Time
	Seq     uint64
}

// Subscription represents a client subscription
type Subscription struct {
	sid     string
	subject string
	queue   string
	client  *Client
}
