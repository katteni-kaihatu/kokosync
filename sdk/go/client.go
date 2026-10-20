// Package kokosync provides a QUIC client for the local development relay.
package kokosync

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/katteni-kaihatu/kokosync/protocol"
	"github.com/quic-go/quic-go"
)

type Message = protocol.Message
type Options struct {
	Address, Room, Peer string
	State               bool
	// InsecureDevelopment permits the relay's ephemeral certificate on a literal loopback IP only.
	InsecureDevelopment bool
	TLSConfig           *tls.Config
}
type Error struct{ Code, Message string }

func (e *Error) Error() string { return e.Code + ": " + e.Message }

type Client struct {
	conn      *quic.Conn
	stream    *quic.Stream
	writeMu   sync.Mutex
	mu        sync.Mutex
	pending   map[string]chan Message
	transfers map[string]chan Message
	state     map[string]Message
	latest    map[string]uint64
	messages  chan Message
	done      chan struct{}
	once      sync.Once
	info      Message
}

func id() string { var b [12]byte; _, _ = rand.Read(b[:]); return hex.EncodeToString(b[:]) }
func Connect(ctx context.Context, o Options) (*Client, error) {
	if o.Address == "" {
		o.Address = "127.0.0.1:8081"
	}
	if o.Peer == "" {
		o.Peer = "go-" + id()
	}
	cfg := o.TLSConfig
	if cfg == nil {
		cfg = &tls.Config{MinVersion: tls.VersionTLS13}
	} else {
		cfg = cfg.Clone()
	}
	cfg.NextProtos = []string{"kokosync/1"}
	if o.InsecureDevelopment {
		host, _, e := net.SplitHostPort(o.Address)
		if e != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			return nil, errors.New("insecure development requires a literal loopback IP")
		}
		cfg.InsecureSkipVerify = true // Explicit, loopback-only prototype certificate opt-in.
	}
	conn, e := quic.DialAddr(ctx, o.Address, cfg, &quic.Config{EnableDatagrams: true, KeepAlivePeriod: 10 * time.Second})
	if e != nil {
		return nil, e
	}
	stream, e := conn.OpenStreamSync(ctx)
	if e != nil {
		_ = conn.CloseWithError(0, "failed")
		return nil, e
	}
	c := &Client{conn: conn, stream: stream, pending: map[string]chan Message{}, transfers: map[string]chan Message{}, state: map[string]Message{}, latest: map[string]uint64{}, messages: make(chan Message, 256), done: make(chan struct{})}
	go c.read()
	go c.datagrams()
	info, e := c.Request(ctx, Message{"type": "join", "room": o.Room, "peer": o.Peer, "state": o.State})
	if e != nil {
		c.Close()
		return nil, e
	}
	c.mu.Lock()
	c.info = info
	c.mu.Unlock()
	return c, nil
}
func (c *Client) Info() Message            { c.mu.Lock(); defer c.mu.Unlock(); return c.info }
func (c *Client) Messages() <-chan Message { return c.messages }
func (c *Client) Done() <-chan struct{}    { return c.done }
func (c *Client) Close() {
	c.once.Do(func() { close(c.done); _ = c.conn.CloseWithError(0, "client closed") })
}
func (c *Client) read() {
	defer c.Close()
	for {
		b, e := protocol.ReadFrame(c.stream)
		if e != nil {
			return
		}
		if !c.accept(b) {
			return
		}
	}
}
func (c *Client) datagrams() {
	for {
		b, e := c.conn.ReceiveDatagram(context.Background())
		if e != nil {
			return
		}
		if len(b) <= protocol.MaxRealtimeFrame {
			c.accept(b)
		}
	}
}
func (c *Client) accept(b []byte) bool {
	m, e := protocol.Decode(b)
	if e != nil {
		c.Close()
		return false
	}
	kind := protocol.String(m, "type")
	c.mu.Lock()
	if kind == "latest" || kind == "live" {
		key := protocol.String(m, "peer") + "\x00" + protocol.String(m, "publisherEpoch") + "\x00" + kind + "\x00" + protocol.String(m, "key") + protocol.String(m, "stream")
		n := protocol.Counter(m, "seq")
		old, exists := c.latest[key]
		if exists && n <= old {
			c.mu.Unlock()
			return true
		}
		if len(c.latest) >= 1024 {
			for k := range c.latest {
				delete(c.latest, k)
				break
			}
		}
		c.latest[key] = n
	}
	if kind == "state" {
		c.state[protocol.String(m, "key")] = m
	}
	if kind == "joined" || kind == "snapshot" {
		if a, ok := m["entries"].([]any); ok {
			c.state = map[string]Message{}
			for _, v := range a {
				if entry, ok := v.(map[string]any); ok {
					c.state[protocol.String(entry, "key")] = entry
				}
			}
		}
	}
	if kind == "blob_open" {
		tid := protocol.String(m, "transfer")
		if len(c.transfers) < 4 {
			c.transfers[tid] = make(chan Message, 16)
		}
	}
	tid := protocol.String(m, "transfer")
	transfer := c.transfers[tid]
	isReply := kind == "ack" || kind == "error" || kind == "joined" || kind == "snapshot" || kind == "catchup" || kind == "pong" || kind == "stats"
	pending := c.pending[protocol.String(m, "id")]
	if isReply && pending != nil {
		delete(c.pending, protocol.String(m, "id"))
	}
	c.mu.Unlock()
	if isReply && pending != nil {
		pending <- m
		return true
	}
	if transfer != nil && kind != "blob_open" {
		select {
		case transfer <- m:
			return true
		default:
			c.Close()
			return false
		}
	}
	select {
	case c.messages <- m:
		return true
	case <-c.done:
		return false
	default:
		if kind == "latest" || kind == "live" {
			return true
		}
		c.Close()
		return false
	}
}

// Send transmits a protocol envelope. Request adds request tracking and server-error handling.
func (c *Client) Send(ctx context.Context, m Message) error {
	m["v"] = 1
	if v, ok := m["value"]; ok {
		if e := protocol.ValidateValue(v); e != nil {
			return e
		}
	}
	b, e := protocol.Encode(m)
	if e != nil {
		return e
	}
	select {
	case <-c.done:
		return errors.New("TransportClosed")
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	kind := protocol.String(m, "type")
	if kind == "latest" || kind == "live" {
		if len(b) > protocol.MaxRealtimeFrame-128 {
			return errors.New("PayloadTooLarge: realtime relay metadata reserve")
		}
		return c.conn.SendDatagram(b)
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	deadline := time.Now().Add(10 * time.Second)
	if d, ok := ctx.Deadline(); ok {
		deadline = d
	}
	_ = c.stream.SetWriteDeadline(deadline)
	return protocol.WriteFrame(c.stream, b)
}
func (c *Client) Request(ctx context.Context, m Message) (Message, error) {
	if protocol.String(m, "id") == "" {
		m["id"] = id()
	}
	requestID := protocol.String(m, "id")
	ch := make(chan Message, 1)
	c.mu.Lock()
	if len(c.pending) >= 256 {
		c.mu.Unlock()
		return nil, errors.New("Backpressure: too many pending requests")
	}
	if c.pending[requestID] != nil {
		c.mu.Unlock()
		return nil, errors.New("request ID already in flight")
	}
	c.pending[requestID] = ch
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.pending, requestID); c.mu.Unlock() }()
	if e := c.Send(ctx, m); e != nil {
		return nil, e
	}
	select {
	case result := <-ch:
		if protocol.String(result, "type") == "error" {
			return nil, &Error{Code: protocol.String(result, "code"), Message: protocol.String(result, "message")}
		}
		return result, nil
	case <-c.done:
		return nil, errors.New("TransportClosed: request outcome unknown")
	case <-ctx.Done():
		return nil, fmt.Errorf("request %s outcome unknown: %w", requestID, ctx.Err())
	}
}
func (c *Client) Publish(ctx context.Context, topic string, value any) (Message, error) {
	return c.Request(ctx, Message{"type": "message", "topic": topic, "value": value})
}
func (c *Client) Event(ctx context.Context, topic string, value any) (Message, error) {
	return c.Request(ctx, Message{"type": "event", "topic": topic, "value": value})
}
func (c *Client) Set(ctx context.Context, key string, value any, expected *uint64) (Message, error) {
	m := Message{"type": "set", "key": key, "value": value}
	if expected != nil {
		m["expected"] = *expected
	}
	return c.Request(ctx, m)
}
func (c *Client) Delete(ctx context.Context, key string, expected *uint64) (Message, error) {
	m := Message{"type": "delete", "key": key}
	if expected != nil {
		m["expected"] = *expected
	}
	return c.Request(ctx, m)
}
func (c *Client) Get(key string) (Message, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, ok := c.state[key]
	return m, ok
}
func (c *Client) Checkpoint(ctx context.Context, seq uint64, value any) error {
	_, e := c.Request(ctx, Message{"type": "checkpoint", "seq": seq, "value": value})
	return e
}
func (c *Client) CatchUp(ctx context.Context) (Message, error) {
	return c.Request(ctx, Message{"type": "catchup"})
}
func (c *Client) Latest(ctx context.Context, key string, seq uint64, value any) error {
	return c.Send(ctx, Message{"type": "latest", "key": key, "seq": seq, "value": value})
}
func (c *Client) Live(ctx context.Context, stream, codec string, seq uint64, ttlMs uint64, data []byte) error {
	return c.Send(ctx, Message{"type": "live", "stream": stream, "codec": codec, "seq": seq, "ttlMs": ttlMs, "value": data})
}
func (c *Client) Stats(ctx context.Context) (Message, error) {
	return c.Request(ctx, Message{"type": "stats"})
}

// SendBlob streams exactly size bytes. Completion means receiver SHA256 and sink validation succeeded.
func (c *Client) SendBlob(ctx context.Context, to string, source io.Reader, size uint64) error {
	if size > 64<<20 {
		return errors.New("PayloadTooLarge")
	}
	tid := id()
	ch := make(chan Message, 16)
	c.mu.Lock()
	if len(c.transfers) >= 4 {
		c.mu.Unlock()
		return errors.New("Backpressure")
	}
	c.transfers[tid] = ch
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.transfers, tid); c.mu.Unlock() }()
	success := false
	defer func() {
		if !success {
			cancelCtx, done := context.WithTimeout(context.Background(), time.Second)
			defer done()
			_ = c.Send(cancelCtx, Message{"type": "blob_cancel", "transfer": tid})
		}
	}()
	if _, e := c.Request(ctx, Message{"type": "blob_open", "transfer": tid, "to": to, "size": size}); e != nil {
		return e
	}
	hash := sha256.New()
	var offset, credit uint64
	wait := func() (Message, error) {
		select {
		case m := <-ch:
			if protocol.String(m, "type") == "blob_cancel" {
				return nil, errors.New("Cancelled")
			}
			return m, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.done:
			return nil, errors.New("TransportClosed")
		}
	}
	for offset < size {
		if credit == 0 {
			m, e := wait()
			if e != nil {
				return e
			}
			if protocol.String(m, "type") == "blob_credit" {
				credit += protocol.Counter(m, "credit")
			}
			continue
		}
		n := uint64(12288)
		if n > credit {
			n = credit
		}
		if n > size-offset {
			n = size - offset
		}
		b := make([]byte, n)
		if _, e := io.ReadFull(source, b); e != nil {
			return e
		}
		_, _ = hash.Write(b)
		if e := c.Send(ctx, Message{"type": "blob_chunk", "transfer": tid, "offset": offset, "value": b}); e != nil {
			return e
		}
		offset += n
		credit -= n
	}
	if e := c.Send(ctx, Message{"type": "blob_end", "transfer": tid, "sha256": hex.EncodeToString(hash.Sum(nil))}); e != nil {
		return e
	}
	for {
		m, e := wait()
		if e != nil {
			return e
		}
		if protocol.String(m, "type") == "blob_done" {
			success = true
			return nil
		}
	}
}

// ReceiveBlob should be called once for an incoming blob_open message. Writes are incremental.
func (c *Client) ReceiveBlob(ctx context.Context, open Message, sink io.Writer) error {
	tid := protocol.String(open, "transfer")
	size := protocol.Counter(open, "size")
	c.mu.Lock()
	ch := c.transfers[tid]
	c.mu.Unlock()
	if ch == nil {
		return errors.New("unknown transfer")
	}
	defer func() { c.mu.Lock(); delete(c.transfers, tid); c.mu.Unlock() }()
	success := false
	defer func() {
		if !success {
			cancelCtx, done := context.WithTimeout(context.Background(), time.Second)
			defer done()
			_ = c.Send(cancelCtx, Message{"type": "blob_cancel", "transfer": tid})
		}
	}()
	if e := c.Send(ctx, Message{"type": "blob_credit", "transfer": tid, "credit": uint64(24576)}); e != nil {
		return e
	}
	hash := sha256.New()
	var offset uint64
	for {
		var m Message
		select {
		case m = <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			return errors.New("TransportClosed")
		}
		switch protocol.String(m, "type") {
		case "blob_chunk":
			b, ok := m["value"].([]byte)
			if !ok || protocol.Counter(m, "offset") != offset || offset+uint64(len(b)) > size {
				return errors.New("invalid blob offset")
			}
			n, e := sink.Write(b)
			if e != nil {
				return e
			}
			if n != len(b) {
				return io.ErrShortWrite
			}
			_, _ = hash.Write(b)
			offset += uint64(n)
			if e := c.Send(ctx, Message{"type": "blob_credit", "transfer": tid, "credit": uint64(n)}); e != nil {
				return e
			}
		case "blob_end":
			if offset != size || hex.EncodeToString(hash.Sum(nil)) != protocol.String(m, "sha256") {
				return errors.New("blob size or SHA256 mismatch")
			}
			if e := c.Send(ctx, Message{"type": "blob_done", "transfer": tid}); e != nil {
				return e
			}
			success = true
			return nil
		case "blob_cancel":
			return errors.New("Cancelled")
		}
	}
}
