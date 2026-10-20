package kokosync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/katteni-kaihatu/kokosync/protocol"
)

type DeliveryReceipt struct {
	Peer, PublisherEpoch, Topic, MessageID string
	Seq                                    uint64
	Route                                  string
	RTTMs                                  *float64
}
type DeliveryError struct{ Code, Outcome, Message string }

func (e *DeliveryError) Error() string { return e.Code + " (" + e.Outcome + "): " + e.Message }
func deliveryCode(code string) string {
	switch code {
	case "UnsupportedCapability", "PeerUnavailable", "InvalidArgument", "InvalidValue", "PayloadTooLarge", "Backpressure", "Timeout", "Cancelled", "Closed", "TransportFailed", "LaneFailed", "ProtocolError":
		return code
	case "Unsupported":
		return "UnsupportedCapability"
	case "PeerMissing", "PeerLeft", "PeerNotFound", "TargetChanged", "GenerationChanged", "StaleGeneration":
		return "PeerUnavailable"
	case "Capacity":
		return "Backpressure"
	case "DeadlineExceeded":
		return "Timeout"
	case "TransportClosed":
		return "Closed"
	case "TransportError":
		return "TransportFailed"
	default:
		return "ProtocolError"
	}
}
func deliveryContextCode(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "Timeout"
	}
	return "Cancelled"
}

type DeliveryStat struct {
	Peer             string   `json:"peer"`
	PublisherEpoch   string   `json:"publisherEpoch"`
	Route            string   `json:"route"`
	Kind             string   `json:"kind"`
	SentFrames       uint64   `json:"sentFrames"`
	ReceivedFrames   uint64   `json:"receivedFrames"`
	SentBytes        uint64   `json:"sentBytes"`
	ReceivedBytes    uint64   `json:"receivedBytes"`
	SentMessages     uint64   `json:"sentMessages"`
	ReceivedMessages uint64   `json:"receivedMessages"`
	Retries          uint64   `json:"retries"`
	UnackedMessages  int      `json:"unackedMessages"`
	QueuedMessages   int      `json:"queuedMessages"`
	QueuedBytes      int      `json:"queuedBytes"`
	Reason           string   `json:"reason"`
	RTTMs            *float64 `json:"rttMs"`
}
type deliveryMember struct {
	epoch  string
	limits protocol.PeerLimits
}
type deliveryResult struct {
	receipt DeliveryReceipt
	err     error
}
type deliverySend struct {
	peer, epoch, topic, id, hash                  string
	data                                          []byte
	limits                                        protocol.PeerLimits
	seq                                           uint64
	ack, sent, maxSent, limit, retries            int
	active, complete, completeEver, retransmitted bool
	deadline, nextRetry, opened                   time.Time
	result                                        chan deliveryResult
	lane                                          *deliveryTXLane
}
type deliveryTXLane struct {
	seq    uint64
	failed bool
	queue  []*deliverySend
}
type deliveryRX struct {
	id, hash, peer, epoch, topic string
	seq                          uint64
	data                         []byte
	offset                       int
	deadline                     time.Time
	limits                       protocol.PeerLimits
}
type deliveryRXLane struct {
	high     uint64
	id, hash string
	size     int
	failed   bool
	active   *deliveryRX
}
type peerDelivery struct {
	mu                                                       sync.Mutex
	limits                                                   protocol.PeerLimits
	peer, epoch, room                                        string
	ready, closed                                            bool
	members                                                  map[string]deliveryMember
	tx                                                       map[string]*deliveryTXLane
	rx                                                       map[string]*deliveryRXLane
	pending                                                  map[string]*deliverySend
	stats                                                    map[string]*DeliveryStat
	queuedBytes, bulkBytes, bulkQueued, rxBytes, rxBulkBytes int
	send                                                     func(Message) error
	admit                                                    func(Message) bool
	done, wake                                               chan struct{}
}

func newPeerDelivery(limits protocol.PeerLimits, send func(Message) error, admit func(Message) bool) *peerDelivery {
	d := &peerDelivery{limits: limits, members: map[string]deliveryMember{}, tx: map[string]*deliveryTXLane{}, rx: map[string]*deliveryRXLane{}, pending: map[string]*deliverySend{}, stats: map[string]*DeliveryStat{}, send: send, admit: admit, done: make(chan struct{}), wake: make(chan struct{}, 1)}
	go d.run()
	return d
}
func deliveryLane(epoch, topic string) string { return epoch + "\x00" + topic }
func deliveryKind(size int) string {
	if size > 16384 {
		return "peer_bulk"
	}
	return "peer_message"
}
func (d *peerDelivery) stat(peer, epoch, kind string) *DeliveryStat {
	key := peer + "\x00" + epoch + "\x00" + kind
	s := d.stats[key]
	if s == nil {
		s = &DeliveryStat{Peer: peer, PublisherEpoch: epoch, Route: "relay", Kind: kind, Reason: "local-adapter-unavailable"}
		d.stats[key] = s
	}
	return s
}
func (d *peerDelivery) metadata(m Message) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	add := func(m Message) {
		peer, epoch := protocol.String(m, "peer"), protocol.String(m, "publisherEpoch")
		if peer == "" || epoch == "" {
			return
		}
		member := deliveryMember{epoch: epoch}
		if raw, ok := m["peerDelivery"].(map[string]any); ok {
			if caps, err := protocol.ParsePeerLimits(raw); err == nil {
				member.limits = caps
			}
		}
		d.members[peer] = member
	}
	switch protocol.String(m, "type") {
	case "joined":
		d.peer = protocol.String(m, "peer")
		d.epoch = protocol.String(m, "publisherEpoch")
		d.room = protocol.String(m, "epoch")
		if caps, ok := m["capabilities"].(map[string]any); ok {
			if raw, ok := caps["peerDelivery"].(map[string]any); ok {
				limits, e := protocol.ParsePeerLimits(raw)
				if e == nil {
					d.limits = protocol.MinPeerLimits(d.limits, limits)
					d.ready = true
				}
			}
		}
		if peers, ok := m["peers"].([]any); ok {
			for _, v := range peers {
				if m, ok := v.(map[string]any); ok {
					add(m)
				}
			}
		}
	case "peer_joined":
		if protocol.String(m, "epoch") == d.room {
			peer := protocol.String(m, "peer")
			if _, exists := d.members[peer]; !exists {
				add(m)
			}
		}
	case "peer_left":
		if protocol.String(m, "epoch") != d.room {
			return
		}
		peer, epoch := protocol.String(m, "peer"), protocol.String(m, "publisherEpoch")
		if member, ok := d.members[peer]; !ok || member.epoch != epoch {
			return
		}
		delete(d.members, peer)
		for _, s := range d.pending {
			if s.peer == peer && s.epoch == epoch {
				d.fail(s, "PeerUnavailable", "recipient left", s.active)
			}
		}
		for key := range d.tx {
			if len(key) > len(epoch) && key[:len(epoch)+1] == epoch+"\x00" {
				delete(d.tx, key)
			}
		}
		prefix := peer + "\x00" + epoch + "\x00"
		for key := range d.stats {
			if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
				delete(d.stats, key)
			}
		}
		for key, lane := range d.rx {
			if lane.active != nil && lane.active.peer == peer && lane.active.epoch == epoch {
				d.releaseRX(lane)
			}
			if len(key) > len(epoch) && key[:len(epoch)+1] == epoch+"\x00" {
				delete(d.rx, key)
			}
		}
	}
}
func (d *peerDelivery) notify() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}
func (d *peerDelivery) enqueue(ctx context.Context, peer, topic string, value any) (DeliveryReceipt, error) {
	if peer == "" || len(peer) > 128 || !utf8.ValidString(peer) || topic == "" || len(topic) > 256 || !utf8.ValidString(topic) {
		return DeliveryReceipt{}, &DeliveryError{"InvalidArgument", "not-sent", "peer/topic required; max128/max256 UTF-8 bytes"}
	}
	d.mu.Lock()
	member, ok := d.members[peer]
	ready, closed := d.ready, d.closed
	local := d.limits
	d.mu.Unlock()
	if closed {
		return DeliveryReceipt{}, &DeliveryError{"Closed", "not-sent", "connection closed"}
	}
	if !ready {
		return DeliveryReceipt{}, &DeliveryError{"UnsupportedCapability", "not-sent", "core does not support peer delivery"}
	}
	if peer == d.peer {
		return DeliveryReceipt{}, &DeliveryError{"InvalidArgument", "not-sent", "cannot send to self"}
	}
	if !ok {
		return DeliveryReceipt{}, &DeliveryError{"PeerUnavailable", "not-sent", "recipient is not present"}
	}
	if member.limits.Version != 1 {
		return DeliveryReceipt{}, &DeliveryError{"UnsupportedCapability", "not-sent", "recipient does not support peer delivery"}
	}
	limits := protocol.MinPeerLimits(local, member.limits)
	data, e := protocol.EncodeValue(value, limits.MessageBytes)
	if e != nil {
		code := "InvalidValue"
		if e.Error() == "PayloadTooLarge" {
			code = "PayloadTooLarge"
		}
		return DeliveryReceipt{}, &DeliveryError{code, "not-sent", e.Error()}
	}
	select {
	case <-ctx.Done():
		return DeliveryReceipt{}, &DeliveryError{deliveryContextCode(ctx.Err()), "not-sent", ctx.Err().Error()}
	default:
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return DeliveryReceipt{}, &DeliveryError{"Closed", "not-sent", "connection closed"}
	}
	if current, ok := d.members[peer]; !ok || current.epoch != member.epoch {
		d.mu.Unlock()
		return DeliveryReceipt{}, &DeliveryError{"PeerUnavailable", "not-sent", "recipient changed"}
	}
	bulk := len(data) > 16384
	if len(d.pending) >= d.limits.Queued || d.queuedBytes+len(data) > d.limits.QueueBytes || bulk && (d.bulkQueued >= max(1, d.limits.Queued-1) || d.bulkBytes+len(data) > d.limits.QueueBytes-16384) {
		d.mu.Unlock()
		return DeliveryReceipt{}, &DeliveryError{"Backpressure", "not-sent", "outgoing delivery budget"}
	}
	key := deliveryLane(member.epoch, topic)
	lane := d.tx[key]
	if lane == nil {
		if len(d.tx)+len(d.rx) >= d.limits.Lanes {
			d.mu.Unlock()
			return DeliveryReceipt{}, &DeliveryError{"Backpressure", "not-sent", "ordering lane limit"}
		}
		lane = &deliveryTXLane{}
		d.tx[key] = lane
	}
	if lane.failed {
		d.mu.Unlock()
		return DeliveryReceipt{}, &DeliveryError{"LaneFailed", "not-sent", "rejoin required for failed lane"}
	}
	hash := sha256.Sum256(data)
	deadline := time.Now().Add(time.Duration(limits.TimeoutMs) * time.Millisecond)
	if end, ok := ctx.Deadline(); ok && end.Before(deadline) {
		deadline = end
	}
	s := &deliverySend{peer: peer, epoch: member.epoch, topic: topic, id: id(), hash: hex.EncodeToString(hash[:]), data: data, limits: limits, deadline: deadline, result: make(chan deliveryResult, 1), lane: lane}
	lane.queue = append(lane.queue, s)
	d.pending[s.id] = s
	d.stat(peer, member.epoch, deliveryKind(len(data)))
	d.queuedBytes += len(data)
	if bulk {
		d.bulkBytes += len(data)
		d.bulkQueued++
	}
	d.notify()
	d.mu.Unlock()
	select {
	case result := <-s.result:
		return result.receipt, result.err
	case <-ctx.Done():
		d.mu.Lock()
		if d.pending[s.id] != nil {
			frames := []Message{}
			if s.active {
				frames = append(frames, d.frame(s, "peer_cancel"))
				frames[0]["code"] = "Cancelled"
			}
			d.fail(s, deliveryContextCode(ctx.Err()), ctx.Err().Error(), s.active)
			d.mu.Unlock()
			d.transmit(frames)
		} else {
			d.mu.Unlock()
		}
		result := <-s.result
		return result.receipt, result.err
	}
}
func (d *peerDelivery) frame(s *deliverySend, kind string) Message {
	return Message{"v": 1, "type": kind, "peer": d.peer, "publisherEpoch": d.epoch, "epoch": d.room, "to": s.peer, "targetEpoch": s.epoch, "topic": s.topic, "messageId": s.id, "seq": s.seq}
}
func (d *peerDelivery) response(m Message, kind string) Message {
	return Message{"v": 1, "type": kind, "peer": d.peer, "publisherEpoch": d.epoch, "epoch": d.room, "to": protocol.String(m, "peer"), "targetEpoch": protocol.String(m, "publisherEpoch"), "topic": m["topic"], "messageId": m["messageId"], "seq": m["seq"]}
}
func (d *peerDelivery) remove(s *deliverySend) {
	delete(d.pending, s.id)
	d.queuedBytes -= len(s.data)
	if len(s.data) > 16384 {
		d.bulkBytes -= len(s.data)
		d.bulkQueued--
	}
	for i, item := range s.lane.queue {
		if item == s {
			copy(s.lane.queue[i:], s.lane.queue[i+1:])
			s.lane.queue[len(s.lane.queue)-1] = nil
			s.lane.queue = s.lane.queue[:len(s.lane.queue)-1]
			break
		}
	}
	if s.lane.seq == 0 && !s.lane.failed && len(s.lane.queue) == 0 {
		key := deliveryLane(s.epoch, s.topic)
		if d.tx[key] == s.lane {
			delete(d.tx, key)
		}
	}
	s.data = nil
}
func (d *peerDelivery) fail(s *deliverySend, code, reason string, poison bool) {
	if d.pending[s.id] == nil {
		return
	}
	if poison {
		s.lane.failed = true
	}
	outcome := "not-sent"
	if s.active {
		outcome = "unknown"
	}
	d.remove(s)
	s.result <- deliveryResult{err: &DeliveryError{deliveryCode(code), outcome, reason}}
	if poison {
		for len(s.lane.queue) > 0 {
			next := s.lane.queue[0]
			d.remove(next)
			next.result <- deliveryResult{err: &DeliveryError{"LaneFailed", "not-sent", "prior sequence failed"}}
		}
	}
	d.notify()
}
func (d *peerDelivery) releaseRX(lane *deliveryRXLane) {
	if lane.active == nil {
		return
	}
	n := len(lane.active.data)
	d.rxBytes -= n
	if n > 16384 {
		d.rxBulkBytes -= n
	}
	lane.active = nil
}
func (d *peerDelivery) transmit(frames []Message) {
	for _, m := range frames {
		d.mu.Lock()
		if d.closed {
			d.mu.Unlock()
			return
		}
		kind := protocol.String(m, "type")
		if kind == "peer_open" || kind == "peer_chunk" || kind == "peer_complete" {
			s := d.pending[protocol.String(m, "messageId")]
			if s == nil {
				d.mu.Unlock()
				continue
			}
			if kind == "peer_chunk" {
				offset, _ := protocol.Uint(m["offset"])
				data, _ := m["value"].([]byte)
				if int(offset) < s.maxSent {
					s.retransmitted = true
				}
				s.maxSent = max(s.maxSent, int(offset)+len(data))
			}
			if kind == "peer_complete" {
				if s.completeEver {
					s.retransmitted = true
				}
				s.completeEver = true
			}
		}
		d.mu.Unlock()
		e := d.send(m)
		d.mu.Lock()
		peer, epoch := protocol.String(m, "to"), protocol.String(m, "targetEpoch")
		kind = "peer_message"
		if s := d.pending[protocol.String(m, "messageId")]; s != nil {
			kind = deliveryKind(len(s.data))
			if e != nil {
				code := "TransportFailed"
				if strings.Contains(e.Error(), "Backpressure") {
					code = "Backpressure"
				} else if strings.Contains(e.Error(), "Closed") {
					code = "Closed"
				}
				d.fail(s, code, e.Error(), true)
			}
		}
		if member, exists := d.members[peer]; e == nil && !d.closed && exists && member.epoch == epoch {
			if lane := d.rx[deliveryLane(epoch, protocol.String(m, "topic"))]; lane != nil {
				if lane.active != nil {
					kind = deliveryKind(len(lane.active.data))
				} else if lane.id == protocol.String(m, "messageId") {
					kind = deliveryKind(lane.size)
				}
			}
			raw, _ := protocol.Encode(m)
			stat := d.stat(peer, epoch, kind)
			stat.SentFrames++
			stat.SentBytes += uint64(len(raw))
		}
		d.mu.Unlock()
	}
}
func (d *peerDelivery) run() {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-d.done:
			return
		case <-d.wake:
		case <-ticker.C:
		}
		d.mu.Lock()
		if d.closed {
			d.mu.Unlock()
			return
		}
		now := time.Now()
		frames := []Message{}
		for _, lane := range d.rx {
			if t := lane.active; t != nil && now.After(t.deadline) {
				m := Message{"peer": t.peer, "publisherEpoch": t.epoch, "topic": t.topic, "messageId": t.id, "seq": t.seq}
				cancel := d.response(m, "peer_cancel")
				cancel["code"] = "DeadlineExceeded"
				frames = append(frames, cancel)
				lane.failed = true
				d.releaseRX(lane)
			}
		}
		active, bulkActive := 0, 0
		for _, s := range d.pending {
			if s.active {
				active++
				if len(s.data) > 16384 {
					bulkActive++
				}
			}
		}
		for _, lane := range d.tx {
			if len(lane.queue) == 0 || lane.failed {
				continue
			}
			s := lane.queue[0]
			bulk := len(s.data) > 16384
			if now.After(s.deadline) {
				if s.active {
					cancel := d.frame(s, "peer_cancel")
					cancel["code"] = "DeadlineExceeded"
					frames = append(frames, cancel)
				}
				d.fail(s, "DeadlineExceeded", "delivery deadline", s.active)
				continue
			}
			if !s.active {
				if active >= d.limits.Concurrent || bulk && bulkActive >= max(1, d.limits.Concurrent-1) {
					continue
				}
				lane.seq++
				s.seq = lane.seq
				s.active = true
				active++
				if bulk {
					bulkActive++
				}
				s.opened = now
				s.nextRetry = now.Add(500 * time.Millisecond)
				open := d.frame(s, "peer_open")
				open["size"] = len(s.data)
				open["sha256"] = s.hash
				frames = append(frames, open)
				continue
			}
			if now.After(s.nextRetry) {
				if s.retries >= s.limits.Retries {
					cancel := d.frame(s, "peer_cancel")
					cancel["code"] = "DeadlineExceeded"
					frames = append(frames, cancel)
					d.fail(s, "DeadlineExceeded", "receipt retry limit", true)
					continue
				}
				s.retries++
				s.sent = s.ack
				s.limit = s.ack
				s.complete = false
				s.opened = now
				s.nextRetry = now.Add(time.Duration(min(2000, 500<<min(s.retries, 2))) * time.Millisecond)
				open := d.frame(s, "peer_open")
				open["size"] = len(s.data)
				open["sha256"] = s.hash
				frames = append(frames, open)
				d.stat(s.peer, s.epoch, deliveryKind(len(s.data))).Retries++
				continue
			}
			if s.sent < len(s.data) && s.sent < s.limit {
				n := min(s.limits.ChunkBytes, len(s.data)-s.sent, s.limit-s.sent)
				chunk := d.frame(s, "peer_chunk")
				chunk["offset"] = s.sent
				chunk["value"] = s.data[s.sent : s.sent+n]
				chunk["bulk"] = bulk
				s.sent += n
				frames = append(frames, chunk)
			}
			if s.sent == len(s.data) && !s.complete {
				s.complete = true
				frames = append(frames, d.frame(s, "peer_complete"))
			}
		}
		d.mu.Unlock()
		d.transmit(frames)
	}
}
func (d *peerDelivery) accept(m Message) {
	d.mu.Lock()
	if d.closed || !d.ready || protocol.String(m, "epoch") != d.room || protocol.String(m, "to") != d.peer || protocol.String(m, "targetEpoch") != d.epoch {
		d.mu.Unlock()
		return
	}
	peer, epoch := protocol.String(m, "peer"), protocol.String(m, "publisherEpoch")
	member, exists := d.members[peer]
	kind := protocol.String(m, "type")
	if !exists || member.epoch != epoch || member.limits.Version != 1 {
		if kind != "peer_relay_error" {
			d.mu.Unlock()
			return
		}
	}
	id, topic, seq := protocol.String(m, "messageId"), protocol.String(m, "topic"), protocol.Counter(m, "seq")
	if id == "" || len(id) > 128 || topic == "" || len(topic) > 256 || seq == 0 {
		d.mu.Unlock()
		return
	}
	frames := []Message{}
	cancel := func(code string) { f := d.response(m, "peer_cancel"); f["code"] = code; frames = append(frames, f) }
	s := d.pending[id]
	if kind == "peer_credit" || kind == "peer_done" || kind == "peer_relay_error" || kind == "peer_cancel" && s != nil {
		if s == nil || !s.active || s.seq != seq || s.peer != peer || s.epoch != epoch || s.topic != topic {
			d.mu.Unlock()
			return
		}
		stat := d.stat(peer, epoch, deliveryKind(len(s.data)))
		raw, _ := protocol.Encode(m)
		stat.ReceivedFrames++
		stat.ReceivedBytes += uint64(len(raw))
		switch kind {
		case "peer_credit":
			offset, ok := protocol.Uint(m["offset"])
			limit, valid := protocol.Uint(m["limit"])
			if !ok || !valid || offset > uint64(s.maxSent) || limit < offset || limit > uint64(len(s.data)) || limit-offset > uint64(s.limits.WindowBytes) {
				d.fail(s, "InvalidMessage", "invalid credit", true)
				break
			}
			progress := int(offset) > s.ack
			if progress {
				s.ack = int(offset)
				s.nextRetry = time.Now().Add(500 * time.Millisecond)
			}
			if int(offset) >= s.ack {
				s.limit = max(s.limit, int(limit))
				if retry, _ := m["retry"].(bool); retry && int(offset) >= s.ack {
					s.sent = max(s.ack, int(offset))
					s.complete = false
				}
			}
			d.notify()
		case "peer_done":
			if !s.completeEver {
				d.fail(s, "InvalidMessage", "premature receipt", true)
				break
			}
			var rtt *float64
			if s.retries == 0 && !s.retransmitted {
				measured := float64(time.Since(s.opened).Microseconds()) / 1000
				rtt = &measured
			}
			receipt := DeliveryReceipt{peer, epoch, topic, id, seq, "relay", rtt}
			stat.SentMessages++
			stat.RTTMs = rtt
			d.remove(s)
			s.result <- deliveryResult{receipt: receipt}
			d.notify()
		default:
			d.fail(s, protocol.String(m, "code"), "peer delivery rejected", true)
		}
		d.mu.Unlock()
		d.transmit(frames)
		return
	}
	key := deliveryLane(epoch, topic)
	lane := d.rx[key]
	limits := protocol.MinPeerLimits(d.limits, member.limits)
	if kind == "peer_open" {
		size, ok := protocol.Uint(m["size"])
		hash := protocol.String(m, "sha256")
		h, e := hex.DecodeString(hash)
		if !ok || size == 0 || size > uint64(limits.MessageBytes) || e != nil || len(h) != 32 || hash != hex.EncodeToString(h) {
			cancel("InvalidMessage")
		} else {
			if lane == nil {
				if len(d.rx)+len(d.tx) >= d.limits.Lanes {
					cancel("Backpressure")
				} else {
					lane = &deliveryRXLane{}
					d.rx[key] = lane
				}
			}
			if lane != nil {
				if seq <= lane.high {
					if seq == lane.high && (lane.id != id || lane.hash != hash || lane.size != int(size)) {
						cancel("DuplicateMismatch")
					} else {
						frames = append(frames, d.response(m, "peer_done"))
					}
				} else if lane.failed || seq != lane.high+1 {
					cancel("LaneFailed")
				} else if t := lane.active; t != nil {
					if t.id != id || t.seq != seq || t.hash != hash || len(t.data) != int(size) {
						lane.failed = true
						d.releaseRX(lane)
						cancel("DuplicateMismatch")
					} else {
						frames = append(frames, d.credit(m, t, false))
					}
				} else {
					active, bulkActive := 0, 0
					for _, l := range d.rx {
						if l.active != nil {
							active++
							if len(l.active.data) > 16384 {
								bulkActive++
							}
						}
					}
					bulk := size > 16384
					if active >= d.limits.Concurrent || d.rxBytes+int(size) > d.limits.ReceiveBytes || bulk && (bulkActive >= max(1, d.limits.Concurrent-1) || d.rxBulkBytes+int(size) > d.limits.ReceiveBytes-16384) {
						cancel("Backpressure")
						lane.failed = true
					} else {
						t := &deliveryRX{id, hash, peer, epoch, topic, seq, make([]byte, int(size)), 0, time.Now().Add(time.Duration(limits.AssemblyMs) * time.Millisecond), limits}
						lane.active = t
						d.rxBytes += int(size)
						if bulk {
							d.rxBulkBytes += int(size)
						}
						frames = append(frames, d.credit(m, t, false))
					}
				}
			}
		}
	} else if lane != nil && seq <= lane.high {
		if seq == lane.high && id != lane.id {
			cancel("DuplicateMismatch")
		} else {
			frames = append(frames, d.response(m, "peer_done"))
		}
	} else if kind == "peer_cancel" {
		if lane == nil && len(d.rx)+len(d.tx) < d.limits.Lanes {
			lane = &deliveryRXLane{}
			d.rx[key] = lane
		}
		if lane != nil {
			lane.failed = true
			d.releaseRX(lane)
		}
	} else if lane == nil || lane.active == nil || lane.active.id != id || lane.active.seq != seq {
		cancel("UnknownTransfer")
	} else {
		t := lane.active
		switch kind {
		case "peer_chunk":
			data, ok := m["value"].([]byte)
			offset, valid := protocol.Uint(m["offset"])
			if !ok || !valid || len(data) == 0 || len(data) > t.limits.ChunkBytes || offset > uint64(len(t.data)) || uint64(len(data)) > uint64(len(t.data))-offset {
				lane.failed = true
				d.releaseRX(lane)
				cancel("InvalidMessage")
			} else if int(offset) > t.offset {
				frames = append(frames, d.credit(m, t, true))
			} else if int(offset) < t.offset {
				if int(offset)+len(data) > t.offset || !bytes.Equal(t.data[int(offset):int(offset)+len(data)], data) {
					lane.failed = true
					d.releaseRX(lane)
					cancel("DuplicateMismatch")
				} else {
					frames = append(frames, d.credit(m, t, false))
				}
			} else {
				copy(t.data[t.offset:], data)
				t.offset += len(data)
				frames = append(frames, d.credit(m, t, false))
			}
		case "peer_complete":
			if t.offset != len(t.data) {
				frames = append(frames, d.credit(m, t, true))
			} else {
				hash := sha256.Sum256(t.data)
				var value any
				var e error
				if hex.EncodeToString(hash[:]) == t.hash {
					value, e = protocol.DecodeValue(t.data, t.limits.MessageBytes)
				}
				if hex.EncodeToString(hash[:]) != t.hash || e != nil {
					lane.failed = true
					d.releaseRX(lane)
					cancel("InvalidMessage")
				} else {
					event := Message{"type": "peer_message", "peer": peer, "publisherEpoch": epoch, "targetEpoch": d.epoch, "topic": topic, "messageId": id, "seq": seq, "value": value}
					if !d.admit(event) {
						lane.failed = true
						d.releaseRX(lane)
						cancel("Backpressure")
					} else {
						lane.high = seq
						lane.id = id
						lane.hash = t.hash
						lane.size = len(t.data)
						d.stat(peer, epoch, deliveryKind(len(t.data))).ReceivedMessages++
						d.releaseRX(lane)
						frames = append(frames, d.response(m, "peer_done"))
					}
				}
			}
		case "peer_cancel":
			lane.failed = true
			d.releaseRX(lane)
		default:
			cancel("InvalidMessage")
		}
	}
	size := 0
	if lane != nil {
		size = lane.size
		if lane.active != nil {
			size = len(lane.active.data)
		}
	}
	stat := d.stat(peer, epoch, deliveryKind(size))
	raw, _ := protocol.Encode(m)
	stat.ReceivedFrames++
	stat.ReceivedBytes += uint64(len(raw))
	d.mu.Unlock()
	d.transmit(frames)
}
func (d *peerDelivery) credit(m Message, t *deliveryRX, retry bool) Message {
	f := d.response(m, "peer_credit")
	f["offset"] = t.offset
	f["limit"] = min(len(t.data), t.offset+t.limits.WindowBytes)
	if retry {
		f["retry"] = true
	}
	return f
}
func (d *peerDelivery) close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	d.closed = true
	close(d.done)
	for _, s := range d.pending {
		d.fail(s, "TransportClosed", "connection closed", s.active)
	}
	for _, lane := range d.rx {
		d.releaseRX(lane)
	}
	d.members = map[string]deliveryMember{}
	d.rx = map[string]*deliveryRXLane{}
	d.tx = map[string]*deliveryTXLane{}
	d.stats = map[string]*DeliveryStat{}
}
func (d *peerDelivery) snapshot() []DeliveryStat {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := []DeliveryStat{}
	for _, stat := range d.stats {
		copy := *stat
		copy.QueuedMessages = 0
		copy.QueuedBytes = 0
		copy.UnackedMessages = 0
		for _, s := range d.pending {
			if s.peer == copy.Peer && s.epoch == copy.PublisherEpoch && deliveryKind(len(s.data)) == copy.Kind {
				copy.QueuedMessages++
				copy.QueuedBytes += len(s.data)
				if s.active {
					copy.UnackedMessages++
				}
			}
		}
		out = append(out, copy)
	}
	return out
}
func (c *Client) SendTo(ctx context.Context, peer, topic string, value any) (DeliveryReceipt, error) {
	select {
	case <-c.done:
		return DeliveryReceipt{}, &DeliveryError{"Closed", "not-sent", "connection closed"}
	default:
	}
	if c.delivery == nil {
		return DeliveryReceipt{}, &DeliveryError{"UnsupportedCapability", "not-sent", "peer delivery disabled"}
	}
	return c.delivery.enqueue(ctx, peer, topic, value)
}
func (c *Client) DeliveryStats() []DeliveryStat {
	if c.delivery == nil {
		return nil
	}
	return c.delivery.snapshot()
}
func (d *peerDelivery) realtime(m Message, sending bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	raw, _ := protocol.Encode(m)
	kind := protocol.String(m, "type")
	if !sending {
		peer, epoch := protocol.String(m, "peer"), protocol.String(m, "publisherEpoch")
		if member, exists := d.members[peer]; exists && member.epoch == epoch {
			stat := d.stat(peer, epoch, kind)
			stat.ReceivedFrames++
			stat.ReceivedMessages++
			stat.ReceivedBytes += uint64(len(raw))
		}
		return
	}
	contains := func(v any, peer string) bool {
		switch names := v.(type) {
		case []string:
			for _, name := range names {
				if name == peer {
					return true
				}
			}
		case []any:
			for _, name := range names {
				if name == peer {
					return true
				}
			}
		}
		return false
	}
	to, hasTo := m["to"]
	exclude, hasExclude := m["exclude"]
	for peer, member := range d.members {
		if hasTo && !contains(to, peer) || hasExclude && contains(exclude, peer) {
			continue
		}
		stat := d.stat(peer, member.epoch, kind)
		stat.SentFrames++
		stat.SentMessages++
		stat.SentBytes += uint64(len(raw))
	}
}
