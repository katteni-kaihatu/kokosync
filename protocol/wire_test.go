package protocol

import (
	"bytes"
	"math"
	"testing"
)

func TestPortableEnvelope(t *testing.T) {
	want := uint64(9007199254740993)
	b, e := Encode(Message{"v": 1, "type": "latest", "seq": want, "value": Message{"unicode": "こんにちは", "bytes": []byte{0, 255}, "null": nil, "array": []any{true, 1.5}}})
	if e != nil {
		t.Fatal(e)
	}
	got, e := Decode(b)
	if e != nil {
		t.Fatal(e)
	}
	if got["seq"] != want {
		t.Fatalf("lost uint64 precision: %#v", got["seq"])
	}
	if !bytes.Contains(b, []byte{0xcf, 0, 0x20, 0, 0, 0, 0, 0, 1}) {
		t.Fatal("uint64 wire encoding missing")
	}
	if e = ValidateValue(got["value"]); e != nil {
		t.Fatal(e)
	}
}
func TestRejectMalformedOrUnsafe(t *testing.T) {
	for _, b := range [][]byte{
		{0x81, 0xa1, 'x', 0xdd, 0xff, 0xff, 0xff, 0xff},
		{0x82, 0xa1, 'x', 0, 0xa1, 'x', 1},
		{0x81, 1, 2},
		{0x81, 0xa1, 'x', 0xd4, 0, 1},
		{0x80, 0},
	} {
		if _, e := Decode(b); e == nil {
			t.Fatalf("accepted malformed %x", b)
		}
	}
	for _, v := range []any{math.NaN(), math.Inf(1), uint64(9007199254740993), map[int]any{1: "bad"}} {
		if e := ValidateValue(v); e == nil {
			t.Fatalf("accepted unsafe %#v", v)
		}
	}
}
