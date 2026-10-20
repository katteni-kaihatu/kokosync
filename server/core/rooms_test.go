package core

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/katteni-kaihatu/kokosync/protocol"
)

func pruneAt(c *Core, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneIdleRoomsLocked(now)
}

func TestRoomLimitAndExistingRoomAdmission(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRooms = 2
	c, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := routingPeer(t, c, "one", "a", "none")
	routingPeer(t, c, "two", "b", "none")
	c.Disconnect(a)
	out := make(chan M, 256)
	p := c.NewPeer("websocket", false, func(b []byte, _ bool) error {
		m, err := protocol.Decode(b)
		if err == nil {
			out <- m
		}
		return err
	}, func() {})
	t.Cleanup(func() { c.Disconnect(p) })
	request(t, c, p, M{"type": "join", "id": "third", "room": "three", "peer": "c"})
	if protocol.String(nextKind(t, out, "error"), "code") != "Backpressure" {
		t.Fatal("room limit not enforced")
	}
	// Empty rooms remain reserved during the reconnect grace period.
	routingPeer(t, c, "one", "rejoin", "none")
	routingPeer(t, c, "two", "second-peer", "none")
	if got := c.RoomStats(); got.Count != 2 || got.Active != 2 || got.Empty != 0 || got.Peers != 3 {
		t.Fatalf("stats: %+v", got)
	}
}

func TestDefaultRoomLimitExceedsPrevious64(t *testing.T) {
	c := New()
	for i := 0; i < 65; i++ {
		p, _ := routingPeer(t, c, fmt.Sprintf("room-%d", i), "a", "none")
		c.Disconnect(p)
	}
	if got := c.RoomStats(); got.Count != 65 || got.Empty != 65 || got.Max != 1024 {
		t.Fatalf("stats: %+v", got)
	}
}

func TestIdleRoomRetentionExpiryAndStorageRelease(t *testing.T) {
	c := New()
	p, out := routingPeer(t, c, "stored", "a", "none")
	c.mu.Lock()
	p.state = true
	c.mu.Unlock()
	for _, m := range []M{
		{"type": "set", "id": "set", "key": "x", "value": "state"},
		{"type": "message", "id": "message", "topic": "x", "value": "history"},
		{"type": "checkpoint", "id": "checkpoint", "seq": uint64(1), "value": "checkpoint"},
	} {
		request(t, c, p, m)
		nextKind(t, out, "ack")
	}
	r := p.room
	if r.stateBytes == 0 || r.historyBytes == 0 || r.dedupBytes == 0 || r.checkpointBytes == 0 {
		t.Fatal("test did not retain all storage types")
	}
	stored := c.RoomStats().StoredBytes
	c.Disconnect(p)
	ttl := time.Duration(c.Config.RoomIdleTimeoutMs) * time.Millisecond
	pruneAt(c, r.idleSince.Add(ttl-time.Nanosecond))
	if c.RoomStats().StoredBytes != stored {
		t.Fatal("room was removed before timeout")
	}
	rejoined, _ := routingPeer(t, c, "stored", "a", "none")
	if rejoined.room != r || !r.idleSince.IsZero() {
		t.Fatal("rejoin lost retained room")
	}
	// A connected room must survive any cleanup time.
	pruneAt(c, time.Now().Add(2*ttl))
	if c.RoomStats().Count != 1 {
		t.Fatal("active room was deleted")
	}
	c.Disconnect(rejoined)
	routingPeer(t, c, "active", "b", "none")
	pruneAt(c, r.idleSince.Add(ttl))
	if got := c.RoomStats(); got.Count != 1 || got.Active != 1 || got.StoredBytes != 0 {
		t.Fatalf("storage not released or active room affected: %+v", got)
	}
	recreated, _ := routingPeer(t, c, "stored", "a", "none")
	if recreated.room.epoch == r.epoch || recreated.room.seq != 0 || len(recreated.room.entries) != 0 {
		t.Fatal("expired room was not recreated cleanly")
	}
}

func TestJoinReclaimsExpiredRoomAtCapacity(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRooms = 1
	c, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := routingPeer(t, c, "old", "a", "none")
	r := p.room
	c.Disconnect(p)
	c.mu.Lock()
	r.idleSince = time.Now().Add(-time.Duration(cfg.RoomIdleTimeoutMs) * time.Millisecond)
	c.mu.Unlock()
	routingPeer(t, c, "new", "a", "none")
	if got := c.RoomStats(); got.Count != 1 || got.Empty != 0 {
		t.Fatalf("expired room blocked admission: %+v", got)
	}
}

func TestPeriodicRoomCleanupAndCancellation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RoomIdleTimeoutMs = 10
	c, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := routingPeer(t, c, "idle", "a", "none")
	c.Disconnect(p)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { c.RunRoomCleanup(ctx); close(done) }()
	deadline := time.Now().Add(time.Second)
	for c.RoomStats().Count != 0 {
		if time.Now().After(deadline) {
			t.Fatal("idle room not cleaned without new joins")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not stop")
	}
}

func TestRoomConfigValidationAndLegacyLimits(t *testing.T) {
	for _, cfg := range []Config{
		func() Config { c := DefaultConfig(); c.MaxRooms = 0; return c }(),
		func() Config { c := DefaultConfig(); c.MaxRooms = -1; return c }(),
		func() Config { c := DefaultConfig(); c.RoomIdleTimeoutMs = 0; return c }(),
		func() Config { c := DefaultConfig(); c.RoomIdleTimeoutMs = -1; return c }(),
	} {
		if _, err := NewWithConfig(cfg); err == nil {
			t.Fatal("invalid room config accepted")
		}
	}
	cfg := DefaultConfig()
	if err := json.Unmarshal([]byte(`{"storageBytes":67108864}`), &cfg); err != nil {
		t.Fatal(err)
	}
	c, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.RoomStats(); got.Max != 1024 || got.IdleTimeoutMs != 600000 {
		t.Fatalf("legacy limits lost defaults: %+v", got)
	}
}
