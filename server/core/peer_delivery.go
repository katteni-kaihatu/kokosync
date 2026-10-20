package core

import (
	"encoding/hex"
	"fmt"
	"github.com/katteni-kaihatu/kokosync/protocol"
	"time"
)

type peerRelayTransfer struct {
	from, to        *Peer
	topic, id, hash string
	seq, size       uint64
	deadline        time.Time
}

func relayTransferKey(from, to *Peer, id string) string {
	return from.publisherEpoch + "\x00" + to.publisherEpoch + "\x00" + id
}
func (c *Core) relayPeerDelivery(p *Peer, m M) {
	r := p.room
	kind := protocol.String(m, "type")
	errReply := func(code, reason string) {
		c.deliver(p, M{"type": "peer_relay_error", "epoch": r.epoch, "peer": protocol.String(m, "to"), "publisherEpoch": protocol.String(m, "targetEpoch"), "to": p.id, "targetEpoch": p.publisherEpoch, "topic": m["topic"], "messageId": m["messageId"], "seq": m["seq"], "code": code, "message": reason}, false, 0)
	}
	if kind == "peer_relay_error" {
		return
	}
	if protocol.String(m, "peer") != p.id || protocol.String(m, "publisherEpoch") != p.publisherEpoch || protocol.String(m, "epoch") != r.epoch {
		errReply("StaleGeneration", "source generation mismatch")
		return
	}
	to := r.peers[protocol.String(m, "to")]
	if to == nil || to == p || protocol.String(m, "targetEpoch") != to.publisherEpoch {
		errReply("PeerUnavailable", "target generation unavailable")
		return
	}
	if p.peerDelivery == nil || to.peerDelivery == nil {
		errReply("UnsupportedCapability", "peer delivery unavailable")
		return
	}
	limits := protocol.MinPeerLimits(*p.peerDelivery, *to.peerDelivery)
	id, topic := protocol.String(m, "messageId"), protocol.String(m, "topic")
	seq, valid := protocol.Uint(m["seq"])
	if id == "" || len(id) > 128 || topic == "" || len(topic) > 256 || !valid || seq == 0 {
		errReply("InvalidMessage", "invalid peer delivery identity")
		return
	}
	if r.peerTransfers == nil {
		r.peerTransfers = map[string]*peerRelayTransfer{}
	}
	now := time.Now()
	for key, t := range r.peerTransfers {
		if now.After(t.deadline) {
			delete(r.peerTransfers, key)
		}
	}
	key := relayTransferKey(p, to, id)
	reverse := relayTransferKey(to, p, id)
	t := r.peerTransfers[key]
	out := M{"type": kind, "epoch": r.epoch, "peer": p.id, "publisherEpoch": p.publisherEpoch, "to": to.id, "targetEpoch": to.publisherEpoch, "topic": topic, "messageId": id, "seq": seq}
	switch kind {
	case "peer_open":
		size, ok := protocol.Uint(m["size"])
		hash := protocol.String(m, "sha256")
		decoded, e := hex.DecodeString(hash)
		if ok && size > uint64(limits.MessageBytes) {
			errReply("PayloadTooLarge", "peer value exceeds message limit")
			return
		}
		if !ok || size == 0 || e != nil || len(decoded) != 32 || hash != hex.EncodeToString(decoded) {
			errReply("InvalidMessage", "invalid peer open size/hash")
			return
		}
		if t != nil {
			if t.topic != topic || t.seq != seq || t.size != size || t.hash != hash {
				errReply("DuplicateMismatch", "open metadata changed")
				return
			}
		} else {
			active, bulk := 0, 0
			for _, old := range r.peerTransfers {
				if old.from == p {
					active++
					if old.size > 16384 {
						bulk++
					}
				}
			}
			if active >= limits.Concurrent || size > 16384 && bulk >= max(1, limits.Concurrent-1) {
				errReply("Backpressure", "peer relay active-transfer reserve")
				return
			}
			t = &peerRelayTransfer{p, to, topic, id, hash, seq, size, now.Add(time.Duration(limits.TimeoutMs) * time.Millisecond)}
			r.peerTransfers[key] = t
		}
		out["size"] = size
		out["sha256"] = hash
	case "peer_chunk":
		data, ok := m["value"].([]byte)
		offset, valid := protocol.Uint(m["offset"])
		if t == nil || t.topic != topic || t.seq != seq {
			errReply("UnknownTransfer", "chunk has no current relay open")
			return
		}
		if !ok || !valid || len(data) == 0 || len(data) > limits.ChunkBytes || offset > t.size || uint64(len(data)) > t.size-offset {
			errReply("InvalidMessage", "invalid peer chunk")
			return
		}
		out["offset"] = offset
		out["value"] = data
		out["bulk"] = t.size > 16384
	case "peer_credit":
		offset, ok := protocol.Uint(m["offset"])
		limit, valid := protocol.Uint(m["limit"])
		if !ok || !valid || limit < offset || limit-offset > uint64(limits.WindowBytes) {
			errReply("InvalidMessage", "invalid cumulative credit")
			return
		}
		out["offset"] = offset
		out["limit"] = limit
		if retry, ok := m["retry"].(bool); ok {
			out["retry"] = retry
		}
	case "peer_complete":
		if t != nil && (t.topic != topic || t.seq != seq) {
			errReply("DuplicateMismatch", "completion metadata mismatch")
			return
		}
	case "peer_done":
		delete(r.peerTransfers, reverse)
	case "peer_cancel":
		code := protocol.String(m, "code")
		reason := protocol.String(m, "message")
		if code == "" || len(code) > 64 || len(reason) > 256 {
			errReply("InvalidMessage", "invalid peer cancellation")
			return
		}
		out["code"] = code
		out["message"] = reason
		delete(r.peerTransfers, key)
		delete(r.peerTransfers, reverse)
	default:
		errReply("InvalidMessage", fmt.Sprintf("unsupported peer frame %s", kind))
		return
	}
	if !c.deliver(to, out, false, 0) {
		errReply("Backpressure", "target reliable queue unavailable")
	}
}
