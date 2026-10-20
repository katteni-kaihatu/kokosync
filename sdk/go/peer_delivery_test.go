package kokosync

import (
	"bytes"
	"context"
	"fmt"
	"github.com/katteni-kaihatu/kokosync/protocol"
	"sync/atomic"
	"testing"
	"time"
)

func deliveryPair(t *testing.T, filter func(string, Message) bool) (*peerDelivery, *peerDelivery, chan Message) {
	t.Helper()
	var a, b *peerDelivery
	got := make(chan Message, 32)
	limits := protocol.DefaultPeerLimits()
	a = newPeerDelivery(limits, func(m Message) error {
		if filter == nil || filter("a", m) {
			b.accept(m)
		}
		return nil
	}, func(m Message) bool { got <- m; return true })
	b = newPeerDelivery(limits, func(m Message) error {
		if filter == nil || filter("b", m) {
			a.accept(m)
		}
		return nil
	}, func(m Message) bool { got <- m; return true })
	a.metadata(Message{"type": "joined", "peer": "a", "publisherEpoch": "ae", "epoch": "r", "capabilities": Message{"peerDelivery": limits.Map()}, "peers": []any{Message{"peer": "b", "publisherEpoch": "be", "peerDelivery": limits.Map()}}})
	b.metadata(Message{"type": "joined", "peer": "b", "publisherEpoch": "be", "epoch": "r", "capabilities": Message{"peerDelivery": limits.Map()}, "peers": []any{Message{"peer": "a", "publisherEpoch": "ae", "peerDelivery": limits.Map()}}})
	t.Cleanup(func() { a.close(); b.close() })
	return a, b, got
}
func eventually(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatal("condition timed out")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestPeerDeliveryLostReceiptDoesNotRedispatch(t *testing.T) {
	var dropped atomic.Bool
	a, _, got := deliveryPair(t, func(side string, m Message) bool {
		return !(side == "b" && protocol.String(m, "type") == "peer_done" && !dropped.Swap(true))
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	payload := bytes.Repeat([]byte{1, 2, 255}, 40000)
	receipt, e := a.enqueue(ctx, "b", "document", payload)
	if e != nil {
		t.Fatal(e)
	}
	if receipt.Seq != 1 || receipt.Route != "relay" || receipt.RTTMs != nil {
		t.Fatal("invalid receipt")
	}
	event := <-got
	if !bytes.Equal(event["value"].([]byte), payload) {
		t.Fatal("changed bytes")
	}
	select {
	case <-got:
		t.Fatal("duplicate application delivery")
	default:
	}
	receipt, e = a.enqueue(ctx, "b", "document", "next")
	if e != nil || receipt.Seq != 2 {
		t.Fatal("same-topic order", e)
	}
	stats := a.snapshot()
	retried := false
	for _, s := range stats {
		if s.Retries > 0 {
			retried = true
		}
	}
	if !retried {
		t.Fatal("missing retry diagnostics")
	}
}
func TestPeerDeliveryQueuedCancelAndActiveLaneFailure(t *testing.T) {
	a, _, _ := deliveryPair(t, func(side string, m Message) bool {
		return !(side == "b" && protocol.String(m, "type") == "peer_credit" && protocol.String(m, "topic") == "blocked")
	})
	activeCtx, stopActive := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { _, e := a.enqueue(activeCtx, "b", "blocked", bytes.Repeat([]byte{1}, 20000)); first <- e }()
	eventually(t, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		for _, s := range a.pending {
			if s.active {
				return true
			}
		}
		return false
	})
	queuedCtx, stopQueued := context.WithCancel(context.Background())
	second := make(chan error, 1)
	go func() { _, e := a.enqueue(queuedCtx, "b", "blocked", "later"); second <- e }()
	eventually(t, func() bool { a.mu.Lock(); defer a.mu.Unlock(); return len(a.pending) == 2 })
	stopQueued()
	if e := (<-second).(*DeliveryError); e.Outcome != "not-sent" {
		t.Fatal(e)
	}
	stopActive()
	if e := (<-first).(*DeliveryError); e.Outcome != "unknown" {
		t.Fatal(e)
	}
	if _, e := a.enqueue(context.Background(), "b", "blocked", "skip"); e == nil || e.(*DeliveryError).Code != "LaneFailed" {
		t.Fatal("failed lane skipped sequence")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, e := a.enqueue(ctx, "b", "healthy", "works"); e != nil {
		t.Fatal(e)
	}
}
func TestPeerDeliveryReservesSmallProgressWhenBulkSaturated(t *testing.T) {
	a, _, got := deliveryPair(t, func(side string, m Message) bool {
		return !(side == "b" && protocol.String(m, "type") == "peer_credit" && protocol.String(m, "topic") != "small")
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, topic := range []string{"bulk1", "bulk2", "bulk3", "bulk4"} {
		go func(topic string) { _, _ = a.enqueue(ctx, "b", topic, bytes.Repeat([]byte{1}, 20000)) }(topic)
	}
	eventually(t, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		n := 0
		for _, s := range a.pending {
			if s.active {
				n++
			}
		}
		return n == 3 && len(a.pending) == 4
	})
	smallCtx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	if _, e := a.enqueue(smallCtx, "b", "small", "priority"); e != nil {
		t.Fatal(e)
	}
	if event := <-got; protocol.String(event, "topic") != "small" {
		t.Fatal("bulk bypassed withheld credit")
	}
}
func TestPeerDeliveryPreOpenCancelPoisonsReceiver(t *testing.T) {
	_, b, got := deliveryPair(t, nil)
	cancel := Message{"v": 1, "type": "peer_cancel", "peer": "a", "publisherEpoch": "ae", "epoch": "r", "to": "b", "targetEpoch": "be", "topic": "late", "messageId": "one", "seq": uint64(1), "code": "Cancelled"}
	b.accept(cancel)
	b.mu.Lock()
	lane := b.rx[deliveryLane("ae", "late")]
	failed := lane != nil && lane.failed
	b.mu.Unlock()
	if !failed {
		t.Fatal("pre-open cancellation did not retain failed lane")
	}
	select {
	case <-got:
		t.Fatal("unexpected delivery")
	default:
	}
}

func TestPeerDeliveryRejectsImpossibleProgress(t *testing.T) {
	for _, kind := range []string{"peer_credit", "peer_done"} {
		t.Run(kind, func(t *testing.T) {
			opened := make(chan Message, 1)
			a, _, _ := deliveryPair(t, func(side string, m Message) bool {
				if side == "a" && protocol.String(m, "type") == "peer_open" {
					select {
					case opened <- m:
					default:
					}
					return false
				}
				return true
			})
			result := make(chan error, 1)
			go func() { _, err := a.enqueue(context.Background(), "b", "bad", "payload"); result <- err }()
			open := <-opened
			frame := Message{"type": kind, "peer": "b", "publisherEpoch": "be", "epoch": "r", "to": "a", "targetEpoch": "ae", "topic": "bad", "messageId": open["messageId"], "seq": open["seq"], "offset": 1, "limit": 1}
			a.accept(frame)
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("impossible progress succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("did not reject impossible progress")
			}
			a.mu.Lock()
			defer a.mu.Unlock()
			lane := a.tx[deliveryLane("be", "bad")]
			if !lane.failed || len(lane.queue) != 0 || a.queuedBytes != 0 {
				t.Fatal("terminal transfer retained")
			}
			if cap(lane.queue) > 0 && lane.queue[:cap(lane.queue)][0] != nil {
				t.Fatal("backing array retains payload")
			}
		})
	}
}
func TestPeerDeliveryLateSendDoesNotRecreateDepartedStats(t *testing.T) {
	a, _, _ := deliveryPair(t, nil)
	a.send = func(m Message) error {
		a.metadata(Message{"type": "peer_left", "epoch": "r", "peer": "b", "publisherEpoch": "be"})
		return nil
	}
	a.transmit([]Message{{"type": "peer_cancel", "to": "b", "targetEpoch": "be", "topic": "test", "messageId": "old"}})
	if len(a.snapshot()) != 0 {
		t.Fatal("late send recreated departed stats")
	}
	a.close()
	if len(a.snapshot()) != 0 {
		t.Fatal("close retained stats")
	}
}

func TestQueuedCancelledTopicsDoNotConsumeRetainedLanes(t *testing.T) {
	a, _, _ := deliveryPair(t, func(string, Message) bool { return false })
	a.mu.Lock()
	a.limits.Concurrent = 1
	a.limits.Lanes = 2
	a.mu.Unlock()
	activeCtx, activeCancel := context.WithCancel(context.Background())
	defer activeCancel()
	activeDone := make(chan error, 1)
	go func() { _, err := a.enqueue(activeCtx, "b", "active", "value"); activeDone <- err }()
	eventually(t, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		for _, s := range a.pending {
			if s.active {
				return true
			}
		}
		return false
	})
	for i := 0; i < 6; i++ {
		topic := fmt.Sprintf("cancel-%d", i)
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() { _, err := a.enqueue(ctx, "b", topic, "value"); result <- err }()
		eventually(t, func() bool { a.mu.Lock(); defer a.mu.Unlock(); return a.tx[deliveryLane("be", topic)] != nil })
		cancel()
		err := <-result
		if failure, ok := err.(*DeliveryError); !ok || failure.Outcome != "not-sent" {
			t.Fatal("queued cancellation result", err)
		}
		a.mu.Lock()
		retained := len(a.tx)
		a.mu.Unlock()
		if retained != 1 {
			t.Fatal("cancelled unsent topic retained lane", retained)
		}
	}
	activeCancel()
	<-activeDone
}

func TestPeerDeliveryPublicErrorConformance(t *testing.T) {
	cases := []struct {
		name, code string
		prepare    func(*peerDelivery) (context.Context, string, string, any)
	}{
		{"missing", "PeerUnavailable", func(d *peerDelivery) (context.Context, string, string, any) {
			return context.Background(), "missing", "topic", nil
		}},
		{"unsupported", "UnsupportedCapability", func(d *peerDelivery) (context.Context, string, string, any) {
			d.metadata(Message{"type": "peer_joined", "epoch": "r", "peer": "legacy", "publisherEpoch": "old"})
			return context.Background(), "legacy", "topic", nil
		}},
		{"empty-topic", "InvalidArgument", func(d *peerDelivery) (context.Context, string, string, any) {
			return context.Background(), "b", "", nil
		}},
		{"self", "InvalidArgument", func(d *peerDelivery) (context.Context, string, string, any) {
			return context.Background(), "a", "topic", nil
		}},
		{"invalid-value", "InvalidValue", func(d *peerDelivery) (context.Context, string, string, any) {
			return context.Background(), "b", "topic", func() {}
		}},
		{"size", "PayloadTooLarge", func(d *peerDelivery) (context.Context, string, string, any) {
			d.mu.Lock()
			d.limits.MessageBytes = 8
			d.mu.Unlock()
			return context.Background(), "b", "topic", make([]byte, 32)
		}},
		{"cancelled", "Cancelled", func(d *peerDelivery) (context.Context, string, string, any) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx, "b", "topic", nil
		}},
		{"expired", "Timeout", func(d *peerDelivery) (context.Context, string, string, any) {
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			t.Cleanup(cancel)
			return ctx, "b", "topic", nil
		}},
		{"closed", "Closed", func(d *peerDelivery) (context.Context, string, string, any) {
			d.close()
			return context.Background(), "b", "topic", nil
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, _, _ := deliveryPair(t, nil)
			ctx, peer, topic, value := tc.prepare(a)
			_, err := a.enqueue(ctx, peer, topic, value)
			failure, ok := err.(*DeliveryError)
			if !ok || failure.Code != tc.code || failure.Outcome != "not-sent" {
				t.Fatalf("got %v, want %s/not-sent", err, tc.code)
			}
		})
	}
	for _, code := range []string{"InvalidMessage", "DuplicateMismatch", "UnknownTransfer"} {
		if deliveryCode(code) != "ProtocolError" {
			t.Fatal("wire protocol category", code)
		}
	}
}

func TestPeerDeliveryInvalidOptionsBeforeDial(t *testing.T) {
	limits := protocol.DefaultPeerLimits()
	limits.ChunkBytes = 0
	_, err := Connect(context.Background(), Options{Address: "not-an-address", PeerDelivery: &limits})
	failure, ok := err.(*DeliveryError)
	if !ok || failure.Code != "InvalidArgument" || failure.Outcome != "not-sent" {
		t.Fatal("invalid options did not fail before dial", err)
	}
}
