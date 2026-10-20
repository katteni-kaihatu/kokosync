// Package core is an in-memory relay. It has no application-specific data model.
package core

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/katteni-kaihatu/kokosync/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type M = protocol.Message
type packet struct {
	data    []byte
	lossy   bool
	expires time.Time
}
type Peer struct {
	mu                              sync.Mutex
	queue                           chan packet
	realtime                        chan packet
	done                            chan struct{}
	once                            sync.Once
	queued, sent, received, dropped uint64
	send                            func([]byte, bool) error
	close                           func()
	room                            *room
	id, transport                   string
	publisherEpoch                  string
	state, datagram                 bool
}
type record struct {
	request M
	reply   M
}
type transfer struct {
	from, to             *Peer
	size, offset, credit uint64
	ended                bool
}
type room struct {
	name, epoch   string
	peers         map[string]*Peer
	seq, revision uint64
	entries       map[string]M
	log           []M
	checkpoint    M
	dedup         map[string]record
	dedupOrder    []string
	latest        map[string]uint64
	transfers     map[string]*transfer
}
type Core struct {
	mu    sync.Mutex
	rooms map[string]*room
}

func New() *Core    { return &Core{rooms: map[string]*room{}} }
func token() string { var b [12]byte; _, _ = rand.Read(b[:]); return hex.EncodeToString(b[:]) }
func (c *Core) NewPeer(transport string, datagram bool, send func([]byte, bool) error, closeFn func()) *Peer {
	p := &Peer{transport: transport, datagram: datagram, publisherEpoch: token(), queue: make(chan packet, 256), realtime: make(chan packet, 64), done: make(chan struct{}), send: send, close: closeFn}
	go func() {
		defer c.Disconnect(p)
		for {
			var item packet
			select {
			case <-p.done:
				return
			case item = <-p.queue:
			default:
				select {
				case <-p.done:
					return
				case item = <-p.queue:
				case item = <-p.realtime:
				}
			}
			p.mu.Lock()
			p.queued -= uint64(len(item.data))
			p.mu.Unlock()
			if !item.expires.IsZero() && time.Now().After(item.expires) {
				p.mu.Lock()
				p.dropped++
				p.mu.Unlock()
				continue
			}
			if e := p.send(item.data, item.lossy); e != nil {
				if item.lossy {
					p.mu.Lock()
					p.dropped++
					p.mu.Unlock()
					continue
				}
				return
			}
			p.mu.Lock()
			p.sent += uint64(len(item.data))
			p.mu.Unlock()
		}
	}()
	return p
}
func (p *Peer) stop() { p.once.Do(func() { close(p.done); go p.close() }) }
func (c *Core) Disconnect(p *Peer) {
	p.stop()
	c.mu.Lock()
	defer c.mu.Unlock()
	r := p.room
	if r == nil {
		return
	}
	if r.peers[p.id] == p {
		delete(r.peers, p.id)
	}
	for k := range r.latest {
		if len(k) > len(p.id) && k[:len(p.id)+1] == p.id+"\x00" {
			delete(r.latest, k)
		}
	}
	for id, t := range r.transfers {
		if t.from == p || t.to == p {
			other := t.from
			if other == p {
				other = t.to
			}
			c.deliver(other, M{"type": "blob_cancel", "transfer": id, "reason": "TransportClosed"}, false, 0)
			delete(r.transfers, id)
		}
	}
	p.room = nil
}
func (c *Core) deliver(p *Peer, m M, lossy bool, ttl uint64) bool {
	m["v"] = 1
	data, e := protocol.Encode(m)
	if e != nil {
		return false
	}
	if lossy && len(data) > protocol.MaxRealtimeFrame {
		return false
	}
	item := packet{data: data, lossy: lossy}
	if ttl > 0 {
		item.expires = time.Now().Add(time.Duration(ttl) * time.Millisecond)
	}
	p.mu.Lock()
	select {
	case <-p.done:
		p.mu.Unlock()
		return false
	default:
	}
	if p.queued+uint64(len(data)) > 1<<20 {
		p.dropped++
		p.mu.Unlock()
		if !lossy {
			p.stop()
		}
		return false
	}
	q := p.queue
	if lossy {
		q = p.realtime
	}
	select {
	case q <- item:
		p.queued += uint64(len(data))
		p.mu.Unlock()
		return true
	default:
		p.dropped++
		p.mu.Unlock()
		if !lossy {
			p.stop()
		}
		return false
	}
}
func (c *Core) fail(p *Peer, id, code, message string) {
	c.deliver(p, M{"type": "error", "id": id, "code": code, "message": message}, false, 0)
}
func (c *Core) Handle(p *Peer, data []byte) {
	m, e := protocol.Decode(data)
	if e != nil {
		c.fail(p, "", "InvalidMessage", e.Error())
		p.stop()
		return
	}
	p.mu.Lock()
	p.received += uint64(len(data))
	p.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	id := protocol.String(m, "id")
	kind := protocol.String(m, "type")
	if protocol.Counter(m, "v") != 1 {
		c.fail(p, id, "UnsupportedVersion", "v must be 1")
		return
	}
	if len(id) > 128 {
		c.fail(p, id, "InvalidMessage", "id too long")
		return
	}
	if kind == "join" {
		c.join(p, m)
		return
	}
	r := p.room
	if r == nil {
		c.fail(p, id, "NotReady", "join first")
		return
	}
	select {
	case <-p.done:
		return
	default:
	}
	ack := M{"type": "ack", "id": id}
	needsID := kind == "message" || kind == "event" || kind == "set" || kind == "delete"
	dedupKey := p.id + "\x00" + id
	if needsID {
		if id == "" {
			c.fail(p, id, "InvalidMessage", "id required")
			return
		}
		if old, ok := r.dedup[dedupKey]; ok {
			if reflect.DeepEqual(old.request, m) {
				c.deliver(p, old.reply, false, 0)
			} else {
				c.fail(p, id, "DuplicateMismatch", "operation ID reused with different contents")
			}
			return
		}
	}
	if value, ok := m["value"]; ok {
		if e := protocol.ValidateValue(value); e != nil {
			c.fail(p, id, "InvalidValue", e.Error())
			return
		}
		b, _ := msgpack.Marshal(value)
		if len(b) > 16384 {
			c.fail(p, id, "PayloadTooLarge", "value limit 16384")
			return
		}
	}
	switch kind {
	case "message", "event":
		topic := protocol.String(m, "topic")
		if topic == "" || len(topic) > 256 {
			c.fail(p, id, "InvalidMessage", "topic required, max 256")
			return
		}
		out := M{"type": kind, "id": id, "peer": p.id, "topic": topic, "value": m["value"]}
		if kind == "message" {
			r.seq++
			out["seq"] = r.seq
			ack["seq"] = r.seq
			r.log = append(r.log, out)
			if len(r.log) > 256 {
				r.log = r.log[1:]
			}
		}
		ttl := protocol.Counter(m, "ttlMs")
		if ttl > 60000 {
			ttl = 60000
		}
		for _, other := range r.peers {
			c.deliver(other, out, false, ttl)
		}
	case "set", "delete":
		if !p.state {
			c.fail(p, id, "UnsupportedCapability", "join with state:true")
			return
		}
		key := protocol.String(m, "key")
		if key == "" || len(key) > 256 {
			c.fail(p, id, "InvalidMessage", "key required, max 256")
			return
		}
		entry, exists := r.entries[key]
		if !exists && len(r.entries) >= 256 {
			c.fail(p, id, "Backpressure", "state key limit")
			return
		}
		if expected, ok := m["expected"]; ok {
			n, valid := protocol.Uint(expected)
			if !valid || n != protocol.Counter(entry, "revision") {
				c.fail(p, id, "Conflict", "expected revision does not match")
				return
			}
		}
		r.revision++
		entry = M{"type": "state", "key": key, "revision": r.revision, "deleted": kind == "delete"}
		if kind == "set" {
			entry["value"] = m["value"]
		}
		r.entries[key] = entry
		ack["revision"] = r.revision
		for _, other := range r.peers {
			if other.state {
				c.deliver(other, entry, false, 0)
			}
		}
	case "snapshot":
		if !p.state {
			c.fail(p, id, "UnsupportedCapability", "join with state:true")
			return
		}
		out := M{"type": "snapshot", "id": id, "revision": r.revision, "entries": entries(r)}
		if !c.deliver(p, out, false, 0) {
			c.fail(p, id, "PayloadTooLarge", "snapshot exceeds frame limit")
		}
		return
	case "checkpoint":
		seq, ok := protocol.Uint(m["seq"])
		if !ok || seq > r.seq || !covers(r, seq) {
			c.fail(p, id, "ResyncRequired", "checkpoint outside retained operation history")
			return
		}
		if r.checkpoint != nil && seq < protocol.Counter(r.checkpoint, "seq") {
			c.fail(p, id, "Conflict", "older checkpoint")
			return
		}
		r.checkpoint = M{"seq": seq, "value": m["value"]}
	case "catchup":
		cp := r.checkpoint
		if cp == nil {
			cp = M{"seq": uint64(0), "value": nil}
		}
		start := protocol.Counter(cp, "seq")
		if !covers(r, start) {
			c.fail(p, id, "ResyncRequired", "application checkpoint too old")
			return
		}
		log := []M{}
		for _, event := range r.log {
			if protocol.Counter(event, "seq") > start {
				log = append(log, event)
			}
		}
		out := M{"type": "catchup", "id": id, "seq": r.seq, "checkpoint": cp, "messages": log}
		if !c.deliver(p, out, false, 0) {
			c.fail(p, id, "PayloadTooLarge", "catchup exceeds frame limit; publish newer checkpoint")
		}
		return
	case "latest", "live":
		if len(data) > protocol.MaxRealtimeFrame-128 {
			c.fail(p, id, "PayloadTooLarge", "realtime payload needs room for relay metadata")
			return
		}
		key := protocol.String(m, "key")
		if kind == "live" {
			key = protocol.String(m, "stream")
		}
		seq, ok := protocol.Uint(m["seq"])
		if key == "" || len(key) > 128 || !ok {
			c.fail(p, id, "InvalidMessage", "key/stream and uint64 seq required")
			return
		}
		latestKey := p.id + "\x00" + kind + "\x00" + key
		old, exists := r.latest[latestKey]
		if exists && seq <= old {
			return
		}
		if !exists && len(r.latest) >= 1024 {
			c.fail(p, id, "Backpressure", "latest key limit")
			return
		}
		out := M{"type": kind, "peer": p.id, "publisherEpoch": p.publisherEpoch, "seq": seq, "value": m["value"]}
		ttl := protocol.Counter(m, "ttlMs")
		if kind == "latest" {
			out["key"] = key
		} else {
			if _, ok := m["value"].([]byte); !ok || ttl == 0 {
				c.fail(p, id, "InvalidMessage", "live requires bytes and positive ttlMs")
				return
			}
			out["stream"] = key
			out["codec"] = m["codec"]
			out["ttlMs"] = ttl
		}
		if ttl > 60000 {
			ttl = 60000
		}
		out["v"] = 1
		encoded, err := protocol.Encode(out)
		if err != nil || len(encoded) > protocol.MaxRealtimeFrame {
			c.fail(p, id, "PayloadTooLarge", "realtime frame including peer metadata exceeds 1000 bytes")
			return
		}
		r.latest[latestKey] = seq
		for _, other := range r.peers {
			c.deliver(other, out, true, ttl)
		}
		return
	case "blob_open":
		tid := protocol.String(m, "transfer")
		to := r.peers[protocol.String(m, "to")]
		size, ok := protocol.Uint(m["size"])
		if tid == "" || len(tid) > 128 || to == nil || to == p || !ok || size > 64<<20 {
			c.fail(p, id, "InvalidMessage", "invalid transfer, recipient or size")
			return
		}
		if len(r.transfers) >= 4 {
			c.fail(p, id, "Backpressure", "4 active transfers per room")
			return
		}
		if _, exists := r.transfers[tid]; exists {
			c.fail(p, id, "Conflict", "transfer already exists")
			return
		}
		r.transfers[tid] = &transfer{from: p, to: to, size: size}
		c.deliver(to, M{"type": kind, "transfer": tid, "peer": p.id, "size": size}, false, 0)
	case "blob_credit", "blob_chunk", "blob_end", "blob_done", "blob_cancel":
		tid := protocol.String(m, "transfer")
		t := r.transfers[tid]
		if t == nil || (p != t.from && p != t.to) {
			c.fail(p, id, "Forbidden", "unknown transfer")
			return
		}
		out := M{"type": kind, "transfer": tid, "peer": p.id}
		target := t.to
		switch kind {
		case "blob_credit":
			if p != t.to {
				c.fail(p, id, "Forbidden", "only receiver grants credit")
				return
			}
			credit, ok := protocol.Uint(m["credit"])
			if !ok || credit > 65536 || t.credit+credit > 65536 {
				c.fail(p, id, "Backpressure", "credit window 65536")
				return
			}
			t.credit += credit
			out["credit"] = credit
			target = t.from
		case "blob_chunk":
			value, ok := m["value"].([]byte)
			if p != t.from || !ok || len(value) == 0 || len(value) > 12288 || t.ended || protocol.Counter(m, "offset") != t.offset || uint64(len(value)) > t.credit || t.offset+uint64(len(value)) > t.size {
				c.fail(p, id, "InvalidMessage", "invalid chunk/offset/credit")
				return
			}
			out["value"] = value
			out["offset"] = t.offset
			t.offset += uint64(len(value))
			t.credit -= uint64(len(value))
		case "blob_end":
			hash := protocol.String(m, "sha256")
			decoded, e := hex.DecodeString(hash)
			if p != t.from || t.offset != t.size || t.ended || e != nil || len(decoded) != 32 {
				c.fail(p, id, "InvalidMessage", "incomplete transfer or invalid SHA256")
				return
			}
			t.ended = true
			out["sha256"] = hash
		case "blob_done":
			if p != t.to || !t.ended {
				c.fail(p, id, "InvalidMessage", "receiver completes ended transfer")
				return
			}
			target = t.from
			delete(r.transfers, tid)
		case "blob_cancel":
			if p == t.to {
				target = t.from
			}
			delete(r.transfers, tid)
		}
		c.deliver(target, out, false, 0)
		return
	case "ping":
		c.deliver(p, M{"type": "pong", "id": id}, false, 0)
		return
	case "stats":
		p.mu.Lock()
		out := M{"type": "stats", "id": id, "queuedBytes": float64(p.queued), "sentBytes": float64(p.sent), "receivedBytes": float64(p.received), "dropped": float64(p.dropped), "transport": p.transport, "rttMs": nil}
		p.mu.Unlock()
		c.deliver(p, out, false, 0)
		return
	default:
		c.fail(p, id, "UnsupportedOperation", fmt.Sprintf("unknown operation %q", kind))
		return
	}
	if needsID {
		r.dedup[dedupKey] = record{request: m, reply: ack}
		r.dedupOrder = append(r.dedupOrder, dedupKey)
		if len(r.dedupOrder) > 512 {
			delete(r.dedup, r.dedupOrder[0])
			r.dedupOrder = r.dedupOrder[1:]
		}
	}
	c.deliver(p, ack, false, 0)
}
func covers(r *room, seq uint64) bool {
	return seq == r.seq || (len(r.log) > 0 && seq+1 >= protocol.Counter(r.log[0], "seq"))
}
func entries(r *room) []M {
	out := make([]M, 0, len(r.entries))
	for _, v := range r.entries {
		out = append(out, v)
	}
	return out
}
func (c *Core) join(p *Peer, m M) {
	id := protocol.String(m, "id")
	if p.room != nil {
		c.fail(p, id, "Conflict", "already joined")
		return
	}
	name := protocol.String(m, "room")
	peerID := protocol.String(m, "peer")
	if name == "" || len(name) > 128 || peerID == "" || len(peerID) > 128 {
		c.fail(p, id, "InvalidMessage", "room and peer required, max 128")
		return
	}
	r := c.rooms[name]
	if r == nil {
		if len(c.rooms) >= 64 {
			c.fail(p, id, "Backpressure", "room limit")
			return
		}
		r = &room{name: name, epoch: token(), peers: map[string]*Peer{}, entries: map[string]M{}, dedup: map[string]record{}, latest: map[string]uint64{}, transfers: map[string]*transfer{}}
		c.rooms[name] = r
	}
	if len(r.peers) >= 32 || r.peers[peerID] != nil {
		c.fail(p, id, "Conflict", "room full or peer already connected")
		return
	}
	state, _ := m["state"].(bool)
	out := M{"type": "joined", "id": id, "room": name, "peer": peerID, "epoch": r.epoch, "seq": r.seq, "revision": r.revision, "durability": "memory", "capabilities": M{"transport": p.transport, "reliableOrdered": true, "datagram": p.datagram, "boundedLatest": false, "insecureDevelopment": true, "maxFrame": protocol.MaxFrame, "maxRealtimeFrame": protocol.MaxRealtimeFrame, "blobResume": false}}
	out["publisherEpoch"] = p.publisherEpoch
	if state {
		out["entries"] = entries(r)
	}
	if _, e := protocol.Encode(out); e != nil {
		c.fail(p, id, "PayloadTooLarge", "initial snapshot exceeds frame limit")
		return
	}
	p.id = peerID
	p.room = r
	p.state = state
	r.peers[peerID] = p
	c.deliver(p, out, false, 0)
}
