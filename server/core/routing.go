package core

import (
	"errors"
	"time"

	"github.com/katteni-kaihatu/kokosync/protocol"
)

func peerInfo(p *Peer) M {
	direct := p.directProtocol
	if direct == "" {
		direct = "none"
	}
	out := M{"peer": p.id, "publisherEpoch": p.publisherEpoch, "directProtocol": direct, "maxDirectPeers": p.maxDirectPeers}
	if p.peerDelivery != nil {
		out["peerDelivery"] = p.peerDelivery.Map()
	}
	return out
}
func routingCapability(m M) (string, int, error) {
	raw, exists := m["routing"]
	if !exists {
		return "none", 0, nil
	}
	v, ok := raw.(map[string]any)
	if !ok {
		return "", 0, errors.New("routing must be a map")
	}
	direct := protocol.String(v, "directProtocol")
	n, valid := protocol.Uint(v["maxDirectPeers"])
	if !valid || n > 8 || (direct != "none" && direct != "webrtc") || (direct == "none" && n != 0) {
		return "", 0, errors.New("invalid direct routing capability")
	}
	return direct, int(n), nil
}
func (c *Core) routeSignal(p *Peer, m M) {
	id := protocol.String(m, "id")
	fail := func(code, reason string) { c.fail(p, id, code, reason) }
	raw, e := protocol.EncodeLimit(m, 16384)
	if e != nil || len(raw) > 16384 {
		fail("PayloadTooLarge", "route signal limit 16384")
		return
	}
	to := p.room.peers[protocol.String(m, "to")]
	if to == nil || to == p || protocol.String(m, "targetEpoch") != to.publisherEpoch {
		fail("InvalidMessage", "unknown or stale route target")
		return
	}
	if p.directProtocol != "webrtc" || to.directProtocol != "webrtc" {
		fail("UnsupportedCapability", "both peers require the same direct adapter")
		return
	}
	signal, ok := m["signal"].(map[string]any)
	if !ok {
		fail("InvalidMessage", "signal must be a map")
		return
	}
	connection := protocol.String(signal, "connection")
	if len(connection) == 0 || len(connection) > 64 {
		fail("InvalidMessage", "signal requires a bounded connection attempt ID")
		return
	}
	switch protocol.String(signal, "type") {
	case "offer", "answer":
		sdp := protocol.String(signal, "sdp")
		if len(sdp) == 0 || len(sdp) > 15000 {
			fail("InvalidMessage", "invalid route SDP")
			return
		}
	case "candidate":
		// Browser ICECandidate.toJSON supplies these fields. No arbitrary nested payload.
		candidate, ok := signal["candidate"].(map[string]any)
		if !ok || len(protocol.String(candidate, "candidate")) > 4096 || len(protocol.String(candidate, "sdpMid")) > 256 || len(protocol.String(candidate, "usernameFragment")) > 256 {
			fail("InvalidMessage", "invalid route candidate")
			return
		}
		if n, exists := candidate["sdpMLineIndex"]; exists && n != nil {
			value, valid := protocol.Uint(n)
			if !valid || value > 65535 {
				fail("InvalidMessage", "invalid candidate line index")
				return
			}
		}
	case "close":
		if len(protocol.String(signal, "reason")) > 128 {
			fail("InvalidMessage", "route reason too long")
			return
		}
	default:
		fail("InvalidMessage", "unknown route signal")
		return
	}
	if e := protocol.ValidateValue(signal); e != nil {
		fail("InvalidValue", e.Error())
		return
	}
	now := time.Now()
	if p.signalWindow.IsZero() || now.Sub(p.signalWindow) >= time.Minute {
		p.signalWindow = now
		p.signals = 0
	}
	if p.signals >= 60 {
		fail("Backpressure", "route signal rate limit 60 per minute")
		return
	}
	p.signals++
	out := M{"type": "route_signal", "to": to.id, "targetEpoch": to.publisherEpoch, "peer": p.id, "publisherEpoch": p.publisherEpoch, "epoch": p.room.epoch, "signal": signal}
	if !c.deliver(to, out, false, 0) {
		fail("TransportClosed", "route target unavailable")
		return
	}
	if id != "" {
		c.deliver(p, M{"type": "ack", "id": id}, false, 0)
	}
}
func realtimeRecipients(r *room, m M) ([]*Peer, error) {
	to, hasTo := m["to"]
	exclude, hasExclude := m["exclude"]
	if hasTo && hasExclude {
		return nil, errors.New("to and exclude are mutually exclusive")
	}
	selected := map[string]bool{}
	if hasTo || hasExclude {
		raw := to
		if hasExclude {
			raw = exclude
		}
		var names []string
		switch v := raw.(type) {
		case []any:
			for _, item := range v {
				s, ok := item.(string)
				if !ok {
					return nil, errors.New("recipient must be a string")
				}
				names = append(names, s)
			}
		case []string:
			names = v
		default:
			return nil, errors.New("recipient selector must be an array")
		}
		if len(names) > 32 {
			return nil, errors.New("recipient selector limit 32")
		}
		for _, name := range names {
			if name == "" || len(name) > 128 || selected[name] || hasTo && r.peers[name] == nil {
				return nil, errors.New("duplicate or unknown recipient")
			}
			selected[name] = true
		}
	}
	out := make([]*Peer, 0, len(r.peers))
	for name, p := range r.peers {
		if hasTo && !selected[name] || hasExclude && selected[name] {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}
