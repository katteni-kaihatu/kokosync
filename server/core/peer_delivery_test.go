package core

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/katteni-kaihatu/kokosync/protocol"
	"testing"
)

func TestPeerDeliveryRelayIdentityCapabilityAndRoomIsolation(t *testing.T) {
	c := New()
	a, ao := routingPeer(t, c, "room", "a", "none")
	b, bo := routingPeer(t, c, "room", "b", "none")
	old, _ := routingPeer(t, c, "room", "old", "none")
	limits := protocol.DefaultPeerLimits()
	a.peerDelivery = &limits
	b.peerDelivery = &limits
	base := func(kind string) M {
		return M{"type": kind, "peer": "a", "publisherEpoch": a.publisherEpoch, "epoch": a.room.epoch, "to": "b", "targetEpoch": b.publisherEpoch, "topic": "drawing", "messageId": "stable", "seq": uint64(1)}
	}
	data := make([]byte, 20000)
	hash := sha256.Sum256(data)
	open := base("peer_open")
	open["size"] = len(data)
	open["sha256"] = hex.EncodeToString(hash[:])
	request(t, c, a, open)
	got := nextKind(t, bo, "peer_open")
	if protocol.String(got, "peer") != "a" || protocol.String(got, "publisherEpoch") != a.publisherEpoch {
		t.Fatal("sender identity changed")
	}
	chunk := base("peer_chunk")
	chunk["offset"] = uint64(0)
	chunk["value"] = data[:100]
	chunk["bulk"] = false
	request(t, c, a, chunk)
	if nextKind(t, bo, "peer_chunk")["bulk"] != true {
		t.Fatal("untrusted chunk marker bypassed bulk classification")
	}
	stale := base("peer_complete")
	stale["publisherEpoch"] = "old"
	request(t, c, a, stale)
	if protocol.String(nextKind(t, ao, "peer_relay_error"), "code") != "StaleGeneration" {
		t.Fatal("stale source was rewritten")
	}
	incompatible := base("peer_open")
	incompatible["to"] = old.id
	incompatible["targetEpoch"] = old.publisherEpoch
	incompatible["size"] = len(data)
	incompatible["sha256"] = hex.EncodeToString(hash[:])
	request(t, c, a, incompatible)
	if protocol.String(nextKind(t, ao, "peer_relay_error"), "code") != "UnsupportedCapability" {
		t.Fatal("legacy recipient silently accepted")
	}
	if a.room.seq != 0 || a.room.revision != 0 || len(a.room.log) != 0 || len(a.room.dedup) != 0 {
		t.Fatal("addressed relay changed room ordered/state history")
	}
}

func TestPeerRelayReservesSmallTransferSlot(t *testing.T) {
	c := New()
	a, ao := routingPeer(t, c, "room", "a", "none")
	b, bo := routingPeer(t, c, "room", "b", "none")
	limits := protocol.DefaultPeerLimits()
	a.peerDelivery = &limits
	b.peerDelivery = &limits
	hash := sha256.Sum256([]byte("value"))
	send := func(id string, size int) {
		request(t, c, a, M{"type": "peer_open", "peer": "a", "publisherEpoch": a.publisherEpoch, "epoch": a.room.epoch, "to": "b", "targetEpoch": b.publisherEpoch, "topic": id, "messageId": id, "seq": uint64(1), "size": size, "sha256": hex.EncodeToString(hash[:])})
	}
	for _, id := range []string{"bulk1", "bulk2", "bulk3"} {
		send(id, 20000)
		nextKind(t, bo, "peer_open")
	}
	send("bulk4", 20000)
	if protocol.String(nextKind(t, ao, "peer_relay_error"), "code") != "Backpressure" {
		t.Fatal("bulk consumed small reserve")
	}
	send("small", 10)
	if protocol.String(nextKind(t, bo, "peer_open"), "topic") != "small" {
		t.Fatal("small blocked behind bulk")
	}
	if len(a.room.peerTransfers) != 4 {
		t.Fatal("unexpected transfer count")
	}
	c.Disconnect(a)
	if len(b.room.peerTransfers) != 0 {
		t.Fatal("departure retained relay transfers")
	}
}
