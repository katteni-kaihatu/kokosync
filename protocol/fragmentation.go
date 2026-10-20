package protocol

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Limits bound logical envelopes independently from transport frames.
type Limits struct {
	LogicalBytes    int `json:"logicalBytes"`
	ChunkBytes      int `json:"chunkBytes"`
	ReassemblyBytes int `json:"reassemblyBytes"`
	InflightBytes   int `json:"inflightBytes"`
	Concurrent      int `json:"concurrent"`
	TimeoutMs       int `json:"timeoutMs"`
	QueueBytes      int `json:"queueBytes"`
}

func DefaultLimits() Limits { return Limits{4 << 20, 12288, 8 << 20, 65536, 4, 30000, 8 << 20} }
func (l Limits) Map() Message {
	return Message{"logicalBytes": l.LogicalBytes, "chunkBytes": l.ChunkBytes, "reassemblyBytes": l.ReassemblyBytes, "inflightBytes": l.InflightBytes, "concurrent": l.Concurrent, "timeoutMs": l.TimeoutMs, "queueBytes": l.QueueBytes}
}
func (l Limits) Normalize() (Limits, error) {
	d := DefaultLimits()
	a := []*int{&l.LogicalBytes, &l.ChunkBytes, &l.ReassemblyBytes, &l.InflightBytes, &l.Concurrent, &l.TimeoutMs, &l.QueueBytes}
	b := []int{d.LogicalBytes, d.ChunkBytes, d.ReassemblyBytes, d.InflightBytes, d.Concurrent, d.TimeoutMs, d.QueueBytes}
	for i, p := range a {
		if *p == 0 {
			*p = b[i]
		}
		if *p < 1 {
			return l, errors.New("fragment limits must be positive")
		}
	}
	if l.LogicalBytes > 64<<20 || l.ChunkBytes > 16384 || l.ReassemblyBytes > 256<<20 || l.QueueBytes > 256<<20 || l.InflightBytes > 1<<20 || l.Concurrent > 32 || l.TimeoutMs > 300000 {
		return l, errors.New("fragment limits exceed hard safety ceiling")
	}
	if l.ReassemblyBytes < l.LogicalBytes || l.QueueBytes < l.LogicalBytes {
		return l, errors.New("logicalBytes must fit reassemblyBytes and queueBytes")
	}
	return l, nil
}
func NegotiateLimits(local Limits, m Message) (Limits, error) {
	remote := Limits{}
	for key, p := range map[string]*int{"logicalBytes": &remote.LogicalBytes, "chunkBytes": &remote.ChunkBytes, "reassemblyBytes": &remote.ReassemblyBytes, "inflightBytes": &remote.InflightBytes, "concurrent": &remote.Concurrent, "timeoutMs": &remote.TimeoutMs, "queueBytes": &remote.QueueBytes} {
		n, ok := Uint(m[key])
		if !ok || n == 0 || n > 1<<30 {
			return remote, fmt.Errorf("invalid fragment limit %s", key)
		}
		*p = int(n)
	}
	a := []*int{&local.LogicalBytes, &local.ChunkBytes, &local.ReassemblyBytes, &local.InflightBytes, &local.Concurrent, &local.TimeoutMs, &local.QueueBytes}
	b := []int{remote.LogicalBytes, remote.ChunkBytes, remote.ReassemblyBytes, remote.InflightBytes, remote.Concurrent, remote.TimeoutMs, remote.QueueBytes}
	for i, p := range a {
		if b[i] < *p {
			*p = b[i]
		}
	}
	if local.LogicalBytes > local.ReassemblyBytes {
		local.LogicalBytes = local.ReassemblyBytes
	}
	if local.LogicalBytes > local.QueueBytes {
		local.LogicalBytes = local.QueueBytes
	}
	return local.Normalize()
}
func Lane(m Message) string {
	kind := String(m, "type")
	if IsPeerDelivery(kind) {
		if kind == "peer_chunk" {
			if bulk, _ := m["bulk"].(bool); bulk {
				return "peer_bulk"
			}
			return "peer_small"
		}
		return "peer_control"
	}
	switch String(m, "type") {
	case "message", "catchup", "checkpoint":
		return "message"
	case "set", "delete", "state", "snapshot":
		return "state"
	case "event":
		return "event"
	case "peer_joined", "peer_left", "route_signal":
		return "membership"
	case "joined":
		return "barrier"
	case "ack":
		if _, ok := m["revision"]; ok {
			return "state"
		}
		if _, ok := m["seq"]; ok {
			return "message"
		}
	}
	return "control"
}

type outgoing struct {
	data           []byte
	id             string
	offset, credit int
	started, ended bool
	deadline       time.Time
	lane           string
	inline         bool
	expires        time.Time
}
type incoming struct {
	data           []byte
	offset, credit int
	hash           string
	deadline       time.Time
}

// Link schedules complete logical envelopes. Transport writes happen only in Run,
// outside the application/core lock. Callbacks must not call Accept recursively.
type Link struct {
	mu               sync.Mutex
	limits           Limits
	enabled          bool
	queues           map[string][]*outgoing
	out              map[string]*outgoing
	in               map[string]*incoming
	queued, reserved int
	control          [][]byte
	wake             chan struct{}
	done             chan struct{}
	once             sync.Once
	send             func([]byte) error
	deliver          func(Message)
	fail             func(error)
	next             int
	budget           *Budget
}
type Budget struct {
	mu            sync.Mutex
	used, maximum int
}

func NewBudget(maximum int) *Budget { return &Budget{maximum: maximum} }
func (b *Budget) change(n int) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > 0 && b.used+n > b.maximum {
		return false
	}
	b.used += n
	return true
}
func (l *Link) SetBudget(b *Budget) { l.mu.Lock(); l.budget = b; l.mu.Unlock() }
func NewLink(l Limits, enabled bool, send func([]byte) error, deliver func(Message), fail func(error)) *Link {
	return &Link{limits: l, enabled: enabled, queues: map[string][]*outgoing{}, out: map[string]*outgoing{}, in: map[string]*incoming{}, wake: make(chan struct{}, 1), done: make(chan struct{}), send: send, deliver: deliver, fail: fail}
}
func (l *Link) Configure(limits Limits, enabled bool) {
	l.mu.Lock()
	l.limits = limits
	l.enabled = enabled
	l.mu.Unlock()
	l.signal()
}
func (l *Link) Limits() (Limits, bool) { l.mu.Lock(); defer l.mu.Unlock(); return l.limits, l.enabled }
func (l *Link) signal() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}
func (l *Link) Close() {
	l.once.Do(func() {
		close(l.done)
		l.mu.Lock()
		l.budget.change(-l.queued - l.reserved)
		l.in = map[string]*incoming{}
		l.out = map[string]*outgoing{}
		l.queues = map[string][]*outgoing{}
		l.control = nil
		l.queued = 0
		l.reserved = 0
		l.mu.Unlock()
	})
}
func (l *Link) Queue(m Message) error {
	return l.QueueTTL(m, 0)
}
func (l *Link) QueueTTL(m Message, ttl uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	select {
	case <-l.done:
		return errors.New("TransportClosed")
	default:
	}
	max := MaxFrame
	if l.enabled && !IsPeerDelivery(String(m, "type")) {
		max = l.limits.LogicalBytes
	}
	m["v"] = 1
	b, e := EncodeLimit(m, max)
	if e != nil {
		return e
	}
	if l.queued+len(b) > l.limits.QueueBytes {
		return errors.New("Backpressure: reliable byte queue")
	}
	n := 0
	for _, q := range l.queues {
		n += len(q)
	}
	if n >= 256 {
		return errors.New("Backpressure: reliable envelope queue")
	}
	if !l.budget.change(len(b)) {
		return errors.New("Backpressure: server transfer budget")
	}
	lane := Lane(m)
	t := &outgoing{data: b, lane: lane, inline: len(b) <= MaxFrame && len(String(m, "type")) >= 5 && String(m, "type")[:5] == "blob_"}
	if IsPeerDelivery(String(m, "type")) {
		t.inline = true
	}
	if lane == "event" && ttl > 0 {
		t.expires = time.Now().Add(time.Duration(ttl) * time.Millisecond)
	}
	l.queues[lane] = append(l.queues[lane], t)
	l.queued += len(b)
	l.signal()
	return nil
}
func (l *Link) ctrl(m Message) error {
	if len(l.control) >= 1024 {
		return errors.New("Backpressure: fragment control queue")
	}
	m["v"] = 1
	b, e := Encode(m)
	if e == nil {
		l.control = append(l.control, b)
		l.signal()
	}
	return e
}
func (l *Link) Accept(m Message) error {
	kind := String(m, "type")
	if len(kind) < 5 || kind[:5] != "frag_" {
		l.deliver(m)
		return nil
	}
	l.mu.Lock()
	var complete Message
	var completedID string
	e := func() error {
		if !l.enabled {
			return errors.New("UnsupportedCapability: fragmentation")
		}
		id := String(m, "transfer")
		if len(id) == 0 || len(id) > 128 {
			return errors.New("invalid fragment transfer")
		}
		switch kind {
		case "frag_open":
			size, ok := Uint(m["size"])
			hash := String(m, "sha256")
			raw, e := hex.DecodeString(hash)
			if !ok || size == 0 || size > uint64(l.limits.LogicalBytes) || e != nil || len(raw) != 32 || hash != hex.EncodeToString(raw) || l.in[id] != nil {
				return errors.New("invalid fragment open")
			}
			if len(l.in) >= l.limits.Concurrent || uint64(l.reserved)+size > uint64(l.limits.ReassemblyBytes) {
				return errors.New("Backpressure: reassembly budget")
			}
			n := int(size)
			credit := min(n, l.limits.InflightBytes)
			if !l.budget.change(n) {
				return errors.New("Backpressure: server transfer budget")
			}
			l.in[id] = &incoming{data: make([]byte, n), hash: hash, credit: credit, deadline: time.Now().Add(time.Duration(l.limits.TimeoutMs) * time.Millisecond)}
			l.reserved += n
			return l.ctrl(Message{"type": "frag_credit", "transfer": id, "credit": credit})
		case "frag_credit":
			t := l.out[id]
			if t == nil {
				return nil
			}
			n, ok := Uint(m["credit"])
			if !ok || n == 0 || n > uint64(l.limits.InflightBytes) || uint64(t.credit)+n > uint64(l.limits.InflightBytes) {
				return errors.New("invalid fragment credit")
			}
			t.credit += int(n)
			l.signal()
		case "frag_chunk":
			t := l.in[id]
			b, ok := m["value"].([]byte)
			off, valid := Uint(m["offset"])
			if t == nil || !ok || !valid || off != uint64(t.offset) || len(b) == 0 || len(b) > l.limits.ChunkBytes || len(b) > t.credit || len(b) > len(t.data)-t.offset {
				return errors.New("invalid fragment chunk")
			}
			copy(t.data[t.offset:], b)
			t.offset += len(b)
			// Credit is replenished only after a bounded copy into reserved storage.
			return l.ctrl(Message{"type": "frag_credit", "transfer": id, "credit": len(b)})
		case "frag_end":
			t := l.in[id]
			if t == nil || t.offset != len(t.data) {
				return errors.New("incomplete fragment")
			}
			h := sha256.Sum256(t.data)
			if hex.EncodeToString(h[:]) != t.hash {
				return errors.New("fragment SHA256 mismatch")
			}
			var e error
			complete, e = DecodeLimit(t.data, l.limits.LogicalBytes)
			if e != nil {
				return e
			}
			k := String(complete, "type")
			if len(k) >= 5 && k[:5] == "frag_" || k == "latest" || k == "live" || IsPeerDelivery(k) {
				return errors.New("invalid fragmented logical kind")
			}
			delete(l.in, id)
			l.reserved -= len(t.data)
			l.budget.change(-len(t.data))
			completedID = id
			return nil
		case "frag_done":
			t := l.out[id]
			if t == nil {
				return nil
			}
			if !t.ended {
				return errors.New("premature fragment done")
			}
			delete(l.out, id)
			l.pop(t.lane)
			l.signal()
		case "frag_cancel":
			return errors.New("Cancelled: reliable fragment")
		default:
			return errors.New("unknown fragmentation control")
		}
		return nil
	}()
	l.mu.Unlock()
	if e == nil && complete != nil {
		l.deliver(complete)
		l.mu.Lock()
		e = l.ctrl(Message{"type": "frag_done", "transfer": completedID})
		l.mu.Unlock()
	}
	return e
}
func (l *Link) pop(lane string) {
	q := l.queues[lane]
	if len(q) > 0 {
		l.queued -= len(q[0].data)
		l.budget.change(-len(q[0].data))
		l.queues[lane] = q[1:]
	}
}
func (l *Link) packet() ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	for _, t := range l.in {
		if now.After(t.deadline) {
			return nil, errors.New("DeadlineExceeded: fragment receive")
		}
	}
	for _, t := range l.out {
		if now.After(t.deadline) {
			return nil, errors.New("DeadlineExceeded: fragment send")
		}
	}
	if len(l.control) > 0 {
		b := l.control[0]
		l.control = l.control[1:]
		return b, nil
	}
	lanes := []string{"control", "barrier", "membership", "peer_control", "peer_small", "state", "event", "message", "peer_bulk"}
	barrier := len(l.queues["barrier"]) > 0
	for i := 0; i < len(lanes); i++ {
		index := (l.next + i) % len(lanes)
		lane := lanes[index]
		if barrier && lane != "control" && lane != "barrier" {
			continue
		}
		q := l.queues[lane]
		if len(q) == 0 {
			continue
		}
		t := q[0]
		if !t.started && !t.expires.IsZero() && now.After(t.expires) {
			l.pop(lane)
			l.signal()
			continue
		}
		if !l.enabled || len(t.data) <= l.limits.ChunkBytes || t.inline {
			l.pop(lane)
			l.next = (index + 1) % len(lanes)
			return t.data, nil
		}
		var m Message
		if !t.started {
			if len(l.out) >= l.limits.Concurrent {
				continue
			}
			var id [16]byte
			_, _ = rand.Read(id[:])
			t.id = hex.EncodeToString(id[:])
			h := sha256.Sum256(t.data)
			t.started = true
			t.deadline = now.Add(time.Duration(l.limits.TimeoutMs) * time.Millisecond)
			l.out[t.id] = t
			m = Message{"type": "frag_open", "transfer": t.id, "size": len(t.data), "sha256": hex.EncodeToString(h[:])}
		} else if t.offset < len(t.data) && t.credit > 0 {
			n := min(l.limits.ChunkBytes, t.credit, len(t.data)-t.offset)
			m = Message{"type": "frag_chunk", "transfer": t.id, "offset": t.offset, "value": t.data[t.offset : t.offset+n]}
			t.offset += n
			t.credit -= n
		} else if t.offset == len(t.data) && !t.ended {
			t.ended = true
			m = Message{"type": "frag_end", "transfer": t.id}
		} else {
			continue
		}
		m["v"] = 1
		l.next = (index + 1) % len(lanes)
		return Encode(m)
	}
	return nil, nil
}
func (l *Link) Run() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-l.done:
			return
		default:
		}
		b, e := l.packet()
		if e == nil && b != nil {
			e = l.send(b)
		}
		if e != nil {
			l.Close()
			l.fail(e)
			return
		}
		if b != nil {
			continue
		}
		select {
		case <-l.done:
			return
		case <-l.wake:
		case <-ticker.C:
		}
	}
}

func (l *Link) CanQueue(n int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.queued+n <= l.limits.QueueBytes
}

func (l *Link) BarrierPending() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.queues["barrier"]) > 0
}
func (l *Link) QueuedBytes() int { l.mu.Lock(); defer l.mu.Unlock(); return l.queued }
