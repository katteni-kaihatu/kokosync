package core

import (
	"strings"
	"testing"
	"time"

	"github.com/katteni-kaihatu/kokosync/protocol"
)

func routingPeer(t *testing.T, c *Core, room, id, direct string) (*Peer, chan M) {
	t.Helper()
	out := make(chan M, 256)
	p := c.NewPeer("websocket", false, func(b []byte, _ bool) error {
		m, e := protocol.Decode(b)
		if e == nil {
			out <- m
		}
		return e
	}, func() {})
	t.Cleanup(func() { c.Disconnect(p) })
	n := 0
	if direct == "webrtc" {
		n = 8
	}
	request(t, c, p, M{"type": "join", "id": "join", "room": room, "peer": id, "routing": M{"directProtocol": direct, "maxDirectPeers": n}})
	reply := nextKind(t, out, "joined")
	if reply["peers"] == nil {
		t.Fatal("missing joined membership")
	}
	return p, out
}
func request(t *testing.T, c *Core, p *Peer, m M) {
	t.Helper()
	m["v"] = 1
	b, e := protocol.Encode(m)
	if e != nil {
		t.Fatal(e)
	}
	c.Handle(p, b)
}
func nextKind(t *testing.T, ch <-chan M, kind string) M {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case m := <-ch:
			if protocol.String(m, "type") == kind {
				return m
			}
		case <-timer.C:
			t.Fatalf("waiting for %s", kind)
			return nil
		}
	}
}
func assertNoKind(t *testing.T, ch <-chan M, kind string) {
	t.Helper()
	timer := time.NewTimer(15 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case m := <-ch:
			if protocol.String(m, "type") == kind {
				t.Fatalf("unexpected %s", kind)
			}
		case <-timer.C:
			return
		}
	}
}
func TestRoutingMembershipAndBoundSignaling(t *testing.T) {
	c := New()
	a, aout := routingPeer(t, c, "room", "a", "webrtc")
	b, bout := routingPeer(t, c, "room", "b", "webrtc")
	native, _ := routingPeer(t, c, "room", "native", "none")
	member := nextKind(t, aout, "peer_joined")
	if protocol.String(member, "peer") != "b" || protocol.String(member, "publisherEpoch") != b.publisherEpoch {
		t.Fatal("membership identity mismatch")
	}
	request(t, c, a, M{"type": "route_signal", "id": "offer", "to": "b", "targetEpoch": b.publisherEpoch, "peer": "forged", "publisherEpoch": "forged", "epoch": "forged", "signal": M{"connection": "attempt-1", "type": "offer", "sdp": "v=0"}})
	signal := nextKind(t, bout, "route_signal")
	if protocol.String(signal, "peer") != "a" || protocol.String(signal, "publisherEpoch") != a.publisherEpoch || protocol.String(signal, "epoch") != a.room.epoch {
		t.Fatal("signal identity was not assigned by core")
	}
	request(t, c, a, M{"type": "route_signal", "id": "stale", "to": "b", "targetEpoch": "old", "signal": M{"connection": "attempt-1", "type": "offer", "sdp": "v=0"}})
	if protocol.String(nextKind(t, aout, "error"), "code") != "InvalidMessage" {
		t.Fatal("stale route signal accepted")
	}
	request(t, c, a, M{"type": "route_signal", "id": "native", "to": native.id, "targetEpoch": native.publisherEpoch, "signal": M{"connection": "attempt-1", "type": "offer", "sdp": "v=0"}})
	if protocol.String(nextKind(t, aout, "error"), "code") != "UnsupportedCapability" {
		t.Fatal("incompatible target accepted")
	}
	request(t, c, a, M{"type": "route_signal", "id": "large", "to": "b", "targetEpoch": b.publisherEpoch, "signal": M{"connection": "attempt-1", "type": "offer", "sdp": strings.Repeat("x", 17000)}})
	if protocol.String(nextKind(t, aout, "error"), "code") != "PayloadTooLarge" {
		t.Fatal("unbounded signaling")
	}
	for i := 0; i < 59; i++ {
		request(t, c, a, M{"type": "route_signal", "to": "b", "targetEpoch": b.publisherEpoch, "signal": M{"connection": "attempt-1", "type": "candidate", "candidate": M{"candidate": "candidate:1"}}})
	}
	request(t, c, a, M{"type": "route_signal", "id": "rate", "to": "b", "targetEpoch": b.publisherEpoch, "signal": M{"connection": "attempt-1", "type": "candidate", "candidate": M{"candidate": "candidate:1"}}})
	if protocol.String(nextKind(t, aout, "error"), "code") != "Backpressure" {
		t.Fatal("signal rate was not bounded")
	}
	oldEpoch := b.publisherEpoch
	c.Disconnect(b)
	left := nextKind(t, aout, "peer_left")
	if protocol.String(left, "publisherEpoch") != oldEpoch {
		t.Fatal("departure epoch mismatch")
	}
	replacement, _ := routingPeer(t, c, "room", "b", "webrtc")
	if replacement.publisherEpoch == oldEpoch {
		t.Fatal("rejoined peer retained old generation")
	}
}
func TestDirectedRealtimeDoesNotLeakOrAdvanceRejectedSequence(t *testing.T) {
	c := New()
	a, aout := routingPeer(t, c, "room", "a", "webrtc")
	_, bout := routingPeer(t, c, "room", "b", "webrtc")
	_, cout := routingPeer(t, c, "room", "c", "none")
	routingPeer(t, c, "other", "outsider", "none")
	request(t, c, a, M{"type": "latest", "key": "x", "seq": uint64(1), "value": "one", "to": []string{"b"}})
	first := nextKind(t, bout, "latest")
	if _, exists := first["to"]; exists {
		t.Fatal("relay leaked recipient selector")
	}
	assertNoKind(t, aout, "latest")
	assertNoKind(t, cout, "latest")
	request(t, c, a, M{"type": "latest", "key": "x", "seq": uint64(2), "value": "two", "exclude": []string{"b"}})
	nextKind(t, aout, "latest")
	nextKind(t, cout, "latest")
	assertNoKind(t, bout, "latest")
	for _, selector := range []M{{"to": []string{"b", "b"}}, {"to": []string{"outsider"}}, {"to": []string{"b"}, "exclude": []string{"c"}}} {
		m := M{"type": "latest", "id": "bad", "key": "x", "seq": uint64(3), "value": "bad"}
		for k, v := range selector {
			m[k] = v
		}
		request(t, c, a, m)
		if protocol.String(nextKind(t, aout, "error"), "code") != "InvalidMessage" {
			t.Fatal("invalid selector accepted")
		}
	}
	request(t, c, a, M{"type": "latest", "key": "x", "seq": uint64(3), "value": "valid", "to": []string{"b"}})
	if protocol.String(nextKind(t, bout, "latest"), "value") != "valid" {
		t.Fatal("rejected selector advanced sequence")
	}
	request(t, c, a, M{"type": "latest", "key": "x", "seq": uint64(4), "value": "departed-exclusion", "exclude": []string{"departed"}})
	for _, out := range []chan M{aout, bout, cout} {
		if protocol.String(nextKind(t, out, "latest"), "value") != "departed-exclusion" {
			t.Fatal("departed exclusion suppressed healthy recipient")
		}
	}
}
