package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

func TestFragmentationIntegrityAndIndependentLanes(t *testing.T) {
	limits := DefaultLimits()
	got := make(chan Message, 16)
	errs := make(chan error, 4)
	var a, b *Link
	a = NewLink(limits, true, func(raw []byte) error {
		m, e := Decode(raw)
		if e != nil {
			return e
		}
		return b.Accept(m)
	}, func(Message) {}, func(e error) { errs <- e })
	b = NewLink(limits, true, func(raw []byte) error {
		m, e := Decode(raw)
		if e != nil {
			return e
		}
		return a.Accept(m)
	}, func(m Message) { got <- m }, func(e error) { errs <- e })
	defer a.Close()
	defer b.Close()
	payload := bytes.Repeat([]byte{1, 3, 5, 7}, 256<<10)
	if e := a.Queue(Message{"type": "message", "seq": uint64(1<<63 + 1), "value": payload}); e != nil {
		t.Fatal(e)
	}
	if e := a.Queue(Message{"type": "message", "seq": uint64(1<<63 + 2), "value": "next"}); e != nil {
		t.Fatal(e)
	}
	if e := a.Queue(Message{"type": "state", "key": "small", "value": 1}); e != nil {
		t.Fatal(e)
	}
	if e := a.Queue(Message{"type": "pong"}); e != nil {
		t.Fatal(e)
	}
	go a.Run()
	go b.Run()
	seen := []Message{}
	for len(seen) < 4 {
		select {
		case m := <-got:
			seen = append(seen, m)
		case e := <-errs:
			t.Fatal(e)
		case <-time.After(3 * time.Second):
			t.Fatal("timeout")
		}
	}
	var seq []uint64
	for i, m := range seen {
		switch String(m, "type") {
		case "pong", "state":
			if i >= 2 {
				t.Fatal("small independent lane blocked behind bulk")
			}
		case "message":
			seq = append(seq, Counter(m, "seq"))
			if v, ok := m["value"].([]byte); ok && !bytes.Equal(v, payload) {
				t.Fatal("payload changed")
			}
		}
	}
	if len(seq) != 2 || seq[0] != 1<<63+1 || seq[1] != 1<<63+2 {
		t.Fatalf("message order/uint64 lost: %v", seq)
	}
}

func TestFragmentationValidationAndRelease(t *testing.T) {
	limits := DefaultLimits()
	budget := NewBudget(200000)
	l := NewLink(limits, true, func([]byte) error { return nil }, func(Message) { t.Fatal("partial dispatch") }, func(error) {})
	l.SetBudget(budget)
	raw, _ := EncodeLimit(Message{"type": "message", "value": strings.Repeat("a", 70000)}, limits.LogicalBytes)
	h := sha256.Sum256(raw)
	open := Message{"type": "frag_open", "transfer": "x", "size": len(raw), "sha256": hex.EncodeToString(h[:])}
	if e := l.Accept(open); e != nil {
		t.Fatal(e)
	}
	if e := l.Accept(open); e == nil {
		t.Fatal("duplicate transfer accepted")
	}
	if e := l.Accept(Message{"type": "frag_chunk", "transfer": "x", "offset": 1, "value": []byte{1}}); e == nil {
		t.Fatal("wrong offset accepted")
	}
	for offset := 0; offset < len(raw); {
		end := min(offset+limits.ChunkBytes, len(raw))
		part := append([]byte(nil), raw[offset:end]...)
		if offset == 0 {
			part[0] ^= 1
		}
		if e := l.Accept(Message{"type": "frag_chunk", "transfer": "x", "offset": offset, "value": part}); e != nil {
			t.Fatal(e)
		}
		offset = end
	}
	if e := l.Accept(Message{"type": "frag_end", "transfer": "x"}); e == nil {
		t.Fatal("hash mismatch accepted")
	}
	l.Close()
	if budget.used != 0 || l.reserved != 0 || l.queued != 0 {
		t.Fatal("close leaked budget")
	}
}

func TestFragmentationTimeout(t *testing.T) {
	limits := DefaultLimits()
	limits.TimeoutMs = 1
	errs := make(chan error, 1)
	l := NewLink(limits, true, func([]byte) error { return nil }, func(Message) {}, func(e error) { errs <- e })
	defer l.Close()
	if e := l.Queue(Message{"type": "message", "value": strings.Repeat("x", 70000)}); e != nil {
		t.Fatal(e)
	}
	go l.Run()
	select {
	case <-errs:
	case <-time.After(time.Second):
		t.Fatal("transfer did not expire")
	}
	if l.queued != 0 {
		t.Fatal("timeout retained queue")
	}
}

func TestPeerFramesRemainPhysicalAndCannotNest(t *testing.T) {
	limits := DefaultLimits()
	limits.LogicalBytes = 100
	l := NewLink(limits, true, func([]byte) error { return nil }, func(Message) { t.Fatal("nested peer frame dispatched") }, func(error) {})
	defer l.Close()
	if err := l.Queue(Message{"type": "peer_chunk", "value": make([]byte, 12288), "bulk": true}); err != nil {
		t.Fatal("peer physical frame used generic logical cap", err)
	}
	raw, _ := Encode(Message{"type": "peer_done"})
	hash := sha256.Sum256(raw)
	if err := l.Accept(Message{"type": "frag_open", "transfer": "nested", "size": len(raw), "sha256": hex.EncodeToString(hash[:])}); err != nil {
		t.Fatal(err)
	}
	if err := l.Accept(Message{"type": "frag_chunk", "transfer": "nested", "offset": 0, "value": raw}); err != nil {
		t.Fatal(err)
	}
	if err := l.Accept(Message{"type": "frag_end", "transfer": "nested"}); err == nil {
		t.Fatal("nested peer frame accepted")
	}
}
