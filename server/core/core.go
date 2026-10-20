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
	link                            *protocol.Link
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
	directProtocol                  string
	maxDirectPeers                  int
	signalWindow                    time.Time
	signals                         int
	peerDelivery                    *protocol.PeerLimits
}
type record struct {
	bytes   int
	request M
	reply   M
}
type transfer struct {
	from, to             *Peer
	size, offset, credit uint64
	ended                bool
}
type room struct {
	historyBytes, stateBytes, dedupBytes, checkpointBytes int
	name, epoch                                           string
	peers                                                 map[string]*Peer
	seq, revision                                         uint64
	entries                                               map[string]M
	log                                                   []M
	checkpoint                                            M
	dedup                                                 map[string]record
	dedupOrder                                            []string
	latest                                                map[string]uint64
	transfers                                             map[string]*transfer
	peerTransfers                                         map[string]*peerRelayTransfer
}
type Core struct {
	mu     sync.Mutex
	rooms  map[string]*room
	Config Config
	stored int
	budget *protocol.Budget
}

type Config struct {
	Fragmentation   protocol.Limits     `json:"fragmentation"`
	StateBytes      int                 `json:"stateBytes"`
	HistoryBytes    int                 `json:"historyBytes"`
	DedupBytes      int                 `json:"dedupBytes"`
	CheckpointBytes int                 `json:"checkpointBytes"`
	StorageBytes    int                 `json:"storageBytes"`
	TransferBytes   int                 `json:"transferBytes"`
	PeerDelivery    protocol.PeerLimits `json:"peerDelivery"`
}

func DefaultConfig() Config {
	return Config{protocol.DefaultLimits(), 2 << 20, 4 << 20, 4 << 20, 2 << 20, 64 << 20, 64 << 20, protocol.DefaultPeerLimits()}
}
func New() *Core { c, _ := NewWithConfig(DefaultConfig()); return c }
func NewWithConfig(cfg Config) (*Core, error) {
	var e error
	cfg.Fragmentation, e = cfg.Fragmentation.Normalize()
	if e != nil {
		return nil, e
	}
	if cfg.PeerDelivery.Version == 0 {
		cfg.PeerDelivery = protocol.DefaultPeerLimits()
	}
	if e := cfg.PeerDelivery.Validate(); e != nil {
		return nil, e
	}
	for _, n := range []int{cfg.StateBytes, cfg.HistoryBytes, cfg.DedupBytes, cfg.CheckpointBytes, cfg.StorageBytes, cfg.TransferBytes} {
		if n < 1 || n > 1<<30 {
			return nil, fmt.Errorf("invalid core byte budget")
		}
	}
	return &Core{rooms: map[string]*room{}, Config: cfg, budget: protocol.NewBudget(cfg.TransferBytes)}, nil
}
func token() string { var b [12]byte; _, _ = rand.Read(b[:]); return hex.EncodeToString(b[:]) }
func (c *Core) NewPeer(transport string, datagram bool, send func([]byte, bool) error, closeFn func()) *Peer {
	p := &Peer{transport: transport, datagram: datagram, publisherEpoch: token(), queue: make(chan packet, 256), realtime: make(chan packet, 64), done: make(chan struct{}), send: send, close: closeFn}
	p.link = protocol.NewLink(c.Config.Fragmentation, false, func(b []byte) error {
		e := p.send(b, false)
		p.mu.Lock()
		p.sent += uint64(len(b))
		p.mu.Unlock()
		return e
	}, func(m M) { c.handle(p, m) }, func(error) { c.Disconnect(p) })
	p.link.SetBudget(c.budget)
	go p.link.Run()
	go func() {
		defer c.Disconnect(p)
		for {
			select {
			case <-p.done:
				return
			case item := <-p.realtime:
				p.mu.Lock()
				p.queued -= uint64(len(item.data))
				p.mu.Unlock()
				if !item.expires.IsZero() && time.Now().After(item.expires) {
					continue
				}
				if e := p.send(item.data, true); e != nil {
					p.mu.Lock()
					p.dropped++
					p.mu.Unlock()
				}
			}
		}
	}()

	return p
}
func (p *Peer) stop() { p.once.Do(func() { close(p.done); p.link.Close(); go p.close() }) }
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
		for _, other := range r.peers {
			c.deliver(other, M{"type": "peer_left", "epoch": r.epoch, "peer": p.id, "publisherEpoch": p.publisherEpoch}, false, 0)
		}
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
	for id, t := range r.peerTransfers {
		if t.from == p || t.to == p {
			delete(r.peerTransfers, id)
		}
	}
	p.room = nil
}
func (c *Core) deliver(p *Peer, m M, lossy bool, ttl uint64) bool {
	if lossy && p.link.BarrierPending() {
		return true
	}
	m["v"] = 1
	if !lossy {
		if e := p.link.QueueTTL(m, ttl); e != nil {
			p.stop()
			return false
		}
		return true
	}
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
	if e := p.link.Accept(m); e != nil {
		c.fail(p, "", "InvalidMessage", e.Error())
		p.stop()
	}
}
func (c *Core) handle(p *Peer, m M) {
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
	if protocol.IsPeerDelivery(kind) {
		c.relayPeerDelivery(p, m)
		return
	}
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
		if len(b) > c.Config.Fragmentation.LogicalBytes-1024 {
			c.fail(p, id, "PayloadTooLarge", "value must leave 1024 bytes of logical envelope headroom")
			return
		}
	}
	if needsID || kind == "checkpoint" {
		raw, _ := protocol.EncodeLimit(m, c.Config.Fragmentation.LogicalBytes)
		if needsID && len(raw) > c.Config.DedupBytes {
			c.fail(p, id, "PayloadTooLarge", "operation exceeds dedup byte budget")
			return
		}
		if c.stored+3*len(raw)+2048 > c.Config.StorageBytes {
			c.fail(p, id, "Backpressure", "server storage byte budget")
			return
		}
	}
	switch kind {
	case "route_signal":
		c.routeSignal(p, m)
		return
	case "message", "event":
		topic := protocol.String(m, "topic")
		if topic == "" || len(topic) > 256 {
			c.fail(p, id, "InvalidMessage", "topic required, max 256")
			return
		}
		out := M{"type": kind, "id": id, "peer": p.id, "topic": topic, "value": m["value"]}
		if kind == "message" {
			out["seq"] = r.seq + 1
		}
		if !c.compatible(r, out, false) {
			c.fail(p, id, "UnsupportedCapability", "recipient logical or queue limit")
			return
		}
		if kind == "message" {
			if sizeOf(out) > c.Config.HistoryBytes {
				c.fail(p, id, "PayloadTooLarge", "message exceeds history byte budget")
				return
			}
			r.seq++
			out["seq"] = r.seq
			ack["seq"] = r.seq
			r.log = append(r.log, out)
			n := sizeOf(out)
			r.historyBytes += n
			c.stored += n
			for len(r.log) > 256 || r.historyBytes > c.Config.HistoryBytes {
				n = sizeOf(r.log[0])
				r.historyBytes -= n
				c.stored -= n
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
		oldSize := 0
		if exists {
			oldSize = sizeOf(entry)
		}
		entry = M{"type": "state", "key": key, "revision": r.revision + 1, "deleted": kind == "delete"}
		if kind == "set" {
			entry["value"] = m["value"]
		}
		nextSize := r.stateBytes - oldSize + sizeOf(entry)
		if nextSize > c.Config.StateBytes {
			c.fail(p, id, "Backpressure", "room state byte budget")
			return
		}
		if !c.compatible(r, entry, true) {
			c.fail(p, id, "UnsupportedCapability", "state recipient logical or queue limit")
			return
		}
		c.stored += nextSize - r.stateBytes
		r.stateBytes = nextSize
		r.revision++
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
		if !replyFits(p, out) {
			c.fail(p, id, "PayloadTooLarge", "snapshot exceeds frame limit")
		} else {
			c.deliver(p, out, false, 0)
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
		cp := M{"seq": seq, "value": m["value"]}
		n := sizeOf(cp)
		if n > c.Config.CheckpointBytes {
			c.fail(p, id, "PayloadTooLarge", "checkpoint byte budget")
			return
		}
		c.stored += n - r.checkpointBytes
		r.checkpointBytes = n
		r.checkpoint = cp
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
		if !replyFits(p, out) {
			c.fail(p, id, "PayloadTooLarge", "catchup exceeds frame limit; publish newer checkpoint")
		} else {
			c.deliver(p, out, false, 0)
		}
		return
	case "latest", "live":
		recipients, recipientErr := realtimeRecipients(r, m)
		if recipientErr != nil {
			c.fail(p, id, "InvalidMessage", recipientErr.Error())
			return
		}
		encodedInput, _ := protocol.Encode(m)
		if len(encodedInput) > protocol.MaxRealtimeFrame-128 {
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
		for _, other := range recipients {
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
		out := M{"type": "stats", "id": id, "queuedBytes": float64(p.queued) + float64(p.link.QueuedBytes()), "sentBytes": float64(p.sent), "receivedBytes": float64(p.received), "dropped": float64(p.dropped), "transport": p.transport, "rttMs": nil}
		p.mu.Unlock()
		c.deliver(p, out, false, 0)
		return
	default:
		c.fail(p, id, "UnsupportedOperation", fmt.Sprintf("unknown operation %q", kind))
		return
	}
	if needsID {
		n := sizeOf(m) + sizeOf(ack)
		r.dedup[dedupKey] = record{request: m, reply: ack, bytes: n}
		r.dedupOrder = append(r.dedupOrder, dedupKey)
		r.dedupBytes += n
		c.stored += n
		for len(r.dedupOrder) > 512 || r.dedupBytes > c.Config.DedupBytes {
			key := r.dedupOrder[0]
			n = r.dedup[key].bytes
			r.dedupBytes -= n
			c.stored -= n
			delete(r.dedup, key)
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
	if advertised, ok := m["fragmentation"].(map[string]any); ok {
		selected, e := protocol.NegotiateLimits(c.Config.Fragmentation, advertised)
		if e != nil {
			c.fail(p, id, "InvalidMessage", e.Error())
			return
		}
		p.link.Configure(selected, true)
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
	var peerDelivery *protocol.PeerLimits
	if raw, exists := m["peerDelivery"]; exists {
		caps, ok := raw.(map[string]any)
		if !ok {
			c.fail(p, id, "InvalidMessage", "peerDelivery must be a map")
			return
		}
		limits, e := protocol.ParsePeerLimits(caps)
		if e != nil {
			c.fail(p, id, "InvalidMessage", e.Error())
			return
		}
		limits = protocol.MinPeerLimits(limits, c.Config.PeerDelivery)
		peerDelivery = &limits
	}
	directProtocol, maxDirectPeers, e := routingCapability(m)
	if e != nil {
		c.fail(p, id, "InvalidMessage", e.Error())
		return
	}
	out := M{"type": "joined", "id": id, "room": name, "peer": peerID, "epoch": r.epoch, "seq": r.seq, "revision": r.revision, "durability": "memory", "capabilities": M{"transport": p.transport, "reliableOrdered": true, "datagram": p.datagram, "boundedLatest": false, "insecureDevelopment": true, "maxFrame": protocol.MaxFrame, "maxRealtimeFrame": protocol.MaxRealtimeFrame, "blobResume": false}}
	out["capabilities"].(M)["routeSignaling"] = true
	out["capabilities"].(M)["peerDelivery"] = c.Config.PeerDelivery.Map()
	peers := make([]M, 0, len(r.peers))
	for _, other := range r.peers {
		peers = append(peers, peerInfo(other))
	}
	out["peers"] = peers
	out["publisherEpoch"] = p.publisherEpoch
	if state {
		out["entries"] = entries(r)
	}
	limits, enabled := p.link.Limits()
	if enabled {
		out["capabilities"].(M)["fragmentation"] = limits.Map()
	}
	out["capabilities"].(M)["stateBytes"] = c.Config.StateBytes
	max := protocol.MaxFrame
	if enabled {
		max = limits.LogicalBytes
	}
	if _, e := protocol.EncodeLimit(out, max); e != nil {
		c.fail(p, id, "PayloadTooLarge", "initial snapshot exceeds frame limit")
		return
	}
	p.id = peerID
	p.room = r
	p.state = state
	p.peerDelivery = peerDelivery
	p.directProtocol = directProtocol
	p.maxDirectPeers = maxDirectPeers
	r.peers[peerID] = p
	c.deliver(p, out, false, 0)
	for _, other := range r.peers {
		if other == p {
			continue
		}
		joined := peerInfo(p)
		joined["type"] = "peer_joined"
		joined["epoch"] = r.epoch
		c.deliver(other, joined, false, 0)
	}
}

func sizeOf(m M) int {
	if m == nil {
		return 0
	}
	b, _ := msgpack.Marshal(m)
	return len(b)
}
func replyFits(p *Peer, m M) bool {
	l, enabled := p.link.Limits()
	maximum := protocol.MaxFrame
	if enabled {
		maximum = l.LogicalBytes
	}
	return sizeOf(m)+16 <= maximum
}
func (c *Core) compatible(r *room, m M, stateOnly bool) bool {
	m["v"] = 1
	n := sizeOf(m)
	for _, p := range r.peers {
		if stateOnly && !p.state {
			continue
		}
		limits, enabled := p.link.Limits()
		maximum := protocol.MaxFrame
		if enabled {
			maximum = limits.LogicalBytes
		}
		if n > maximum || !p.link.CanQueue(n) {
			return false
		}
	}
	return true
}
