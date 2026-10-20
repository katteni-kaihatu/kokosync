package kokosync_test

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/katteni-kaihatu/kokosync/protocol"
	kokosync "github.com/katteni-kaihatu/kokosync/sdk/go"
	"github.com/katteni-kaihatu/kokosync/server/gateway"
)

func TestQUICRawOptionalStateAndBlob(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	reservation, e := net.ListenPacket("udp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	addr := reservation.LocalAddr().String()
	_ = reservation.Close()
	server := gateway.New("../..", "test")
	go func() { _ = server.ServeQUIC(ctx, addr) }()
	var a *kokosync.Client
	for i := 0; i < 20; i++ {
		attempt, end := context.WithTimeout(ctx, 200*time.Millisecond)
		a, e = kokosync.Connect(attempt, kokosync.Options{Address: addr, Room: "test", Peer: "a", InsecureDevelopment: true})
		end()
		if e == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	b, e := kokosync.Connect(ctx, kokosync.Options{Address: addr, Room: "test", Peer: "b", State: true, InsecureDevelopment: true})
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	isolated, e := kokosync.Connect(ctx, kokosync.Options{Address: addr, Room: "other-room", Peer: "isolated", InsecureDevelopment: true})
	if e != nil {
		t.Fatal(e)
	}
	defer isolated.Close()
	if _, exists := a.Info()["entries"]; exists {
		t.Fatal("raw join unexpectedly initialized state")
	}
	ack, e := a.Publish(ctx, "draw", map[string]any{"x": 1, "y": 2})
	if e != nil {
		t.Fatal(e)
	}
	first := wait(t, ctx, b, "message")
	if protocol.Counter(first, "seq") != protocol.Counter(ack, "seq") {
		t.Fatal("ordered seq mismatch")
	}
	select {
	case leaked := <-isolated.Messages():
		t.Fatalf("room isolation failed: %#v", leaked)
	case <-time.After(25 * time.Millisecond):
	}
	if e = a.Checkpoint(ctx, protocol.Counter(ack, "seq"), map[string]any{"points": 1}); e != nil {
		t.Fatal(e)
	}
	_, e = a.Publish(ctx, "draw", map[string]any{"x": 3, "y": 4})
	if e != nil {
		t.Fatal(e)
	}
	catchup, e := b.CatchUp(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if len(catchup["messages"].([]any)) != 1 {
		t.Fatal("checkpoint boundary replay incorrect")
	}
	dedup := kokosync.Message{"type": "message", "id": "stable-operation", "topic": "once", "value": "same"}
	once, e := a.Request(ctx, dedup)
	if e != nil {
		t.Fatal(e)
	}
	twice, e := a.Request(ctx, dedup)
	if e != nil || protocol.Counter(once, "seq") != protocol.Counter(twice, "seq") {
		t.Fatal("retry duplicated operation", e)
	}
	if _, e = a.Request(ctx, kokosync.Message{"type": "message", "id": "stable-operation", "topic": "once", "value": "changed"}); e == nil {
		t.Fatal("duplicate ID payload mismatch accepted")
	}
	zero := uint64(0)
	set, e := b.Set(ctx, "key", "one", &zero)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = b.Set(ctx, "key", "conflict", &zero); e == nil {
		t.Fatal("CAS conflict accepted")
	}
	rev := protocol.Counter(set, "revision")
	deleted, e := b.Delete(ctx, "key", &rev)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = b.Set(ctx, "key", "stale", &zero); e == nil {
		t.Fatal("tombstone revision was lost")
	}
	entry, ok := b.Get("key")
	if !ok || entry["deleted"] != true || protocol.Counter(entry, "revision") != protocol.Counter(deleted, "revision") {
		t.Fatal("ack returned before local state applied")
	}
	if e = a.Latest(ctx, "cursor", 9007199254740993, map[string]any{"x": 2}); e != nil {
		t.Fatal(e)
	}
	latest := wait(t, ctx, b, "latest")
	if protocol.Counter(latest, "seq") != 9007199254740993 {
		t.Fatal("datagram uint64 mismatch")
	}
	source := bytes.Repeat([]byte("portable blobこんにちは"), 8000)
	sent := make(chan error, 1)
	go func() { sent <- a.SendBlob(ctx, "b", bytes.NewReader(source), uint64(len(source))) }()
	open := wait(t, ctx, b, "blob_open")
	var sink bytes.Buffer
	if e = b.ReceiveBlob(ctx, open, &sink); e != nil {
		t.Fatal(e)
	}
	if e = <-sent; e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(source, sink.Bytes()) {
		t.Fatal("blob contents differ")
	}
}
func wait(t *testing.T, ctx context.Context, c *kokosync.Client, kind string) kokosync.Message {
	t.Helper()
	for {
		select {
		case m := <-c.Messages():
			if protocol.String(m, "type") == kind {
				return m
			}
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", kind, ctx.Err())
		case <-c.Done():
			t.Fatal("client unexpectedly disconnected")
		}
	}
}
