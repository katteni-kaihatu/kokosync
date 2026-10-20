package protocol

import (
	"bytes"
	"errors"

	"github.com/vmihailenco/msgpack/v5"
)

type PeerLimits struct {
	Version      int `json:"version"`
	MessageBytes int `json:"messageBytes"`
	ChunkBytes   int `json:"chunkBytes"`
	WindowBytes  int `json:"windowBytes"`
	QueueBytes   int `json:"queueBytes"`
	ReceiveBytes int `json:"receiveBytes"`
	Concurrent   int `json:"concurrent"`
	Queued       int `json:"queued"`
	Lanes        int `json:"lanes"`
	TimeoutMs    int `json:"timeoutMs"`
	AssemblyMs   int `json:"assemblyMs"`
	Retries      int `json:"retries"`
}

func DefaultPeerLimits() PeerLimits {
	return PeerLimits{1, 4 << 20, 12288, 65536, 8 << 20, 8 << 20, 4, 128, 64, 30000, 30000, 5}
}
func (l PeerLimits) Map() Message {
	return Message{"version": l.Version, "messageBytes": l.MessageBytes, "chunkBytes": l.ChunkBytes, "windowBytes": l.WindowBytes, "queueBytes": l.QueueBytes, "receiveBytes": l.ReceiveBytes, "concurrent": l.Concurrent, "queued": l.Queued, "lanes": l.Lanes, "timeoutMs": l.TimeoutMs, "assemblyMs": l.AssemblyMs, "retries": l.Retries}
}
func (l PeerLimits) Validate() error {
	values := []int{l.MessageBytes, l.ChunkBytes, l.WindowBytes, l.QueueBytes, l.ReceiveBytes, l.Concurrent, l.Queued, l.Lanes, l.TimeoutMs, l.AssemblyMs}
	maxima := []int{64 << 20, 16384, 256 << 10, 256 << 20, 256 << 20, 32, 4096, 1024, 300000, 300000}
	if l.Version != 1 || l.Retries < 0 || l.Retries > 20 {
		return errors.New("invalid peer delivery version or retries")
	}
	for i, v := range values {
		if v < 1 || v > maxima[i] {
			return errors.New("invalid peer delivery limit")
		}
	}
	if l.QueueBytes < 16384 || l.ReceiveBytes < 16384 {
		return errors.New("peer budgets must reserve at least 16384 bytes")
	}
	return nil
}
func ParsePeerLimits(m Message) (PeerLimits, error) {
	l := PeerLimits{}
	pointers := map[string]*int{"version": &l.Version, "messageBytes": &l.MessageBytes, "chunkBytes": &l.ChunkBytes, "windowBytes": &l.WindowBytes, "queueBytes": &l.QueueBytes, "receiveBytes": &l.ReceiveBytes, "concurrent": &l.Concurrent, "queued": &l.Queued, "lanes": &l.Lanes, "timeoutMs": &l.TimeoutMs, "assemblyMs": &l.AssemblyMs, "retries": &l.Retries}
	for k, p := range pointers {
		n, ok := Uint(m[k])
		if !ok || n > 1<<30 {
			return l, errors.New("invalid peer delivery capability")
		}
		*p = int(n)
	}
	return l, l.Validate()
}
func MinPeerLimits(a, b PeerLimits) PeerLimits {
	return PeerLimits{1, min(a.MessageBytes, b.MessageBytes), min(a.ChunkBytes, b.ChunkBytes), min(a.WindowBytes, b.WindowBytes), min(a.QueueBytes, b.QueueBytes), min(a.ReceiveBytes, b.ReceiveBytes), min(a.Concurrent, b.Concurrent), min(a.Queued, b.Queued), min(a.Lanes, b.Lanes), min(a.TimeoutMs, b.TimeoutMs), min(a.AssemblyMs, b.AssemblyMs), min(a.Retries, b.Retries)}
}
func EncodeValue(v any, limit int) ([]byte, error) {
	if e := ValidateValue(v); e != nil {
		return nil, e
	}
	b, e := msgpack.Marshal(v)
	if len(b) > limit {
		return nil, errors.New("PayloadTooLarge")
	}
	return b, e
}
func DecodeValue(b []byte, limit int) (any, error) {
	if len(b) == 0 || len(b) > limit {
		return nil, errors.New("invalid peer value size")
	}
	p := parser{b: b}
	if _, e := p.value(0, false); e != nil {
		return nil, e
	}
	if p.i != len(b) {
		return nil, errors.New("trailing value data")
	}
	d := msgpack.NewDecoder(bytes.NewReader(b))
	d.SetMapDecoder(func(d *msgpack.Decoder) (any, error) {
		n, e := d.DecodeMapLen()
		if e != nil {
			return nil, e
		}
		m := make(Message, n)
		for i := 0; i < n; i++ {
			k, e := d.DecodeString()
			if e != nil {
				return nil, e
			}
			v, e := d.DecodeInterface()
			if e != nil {
				return nil, e
			}
			m[k] = v
		}
		return m, nil
	})
	v, e := d.DecodeInterface()
	if e != nil {
		return nil, e
	}
	return v, ValidateValue(v)
}
func IsPeerDelivery(kind string) bool {
	switch kind {
	case "peer_open", "peer_credit", "peer_chunk", "peer_complete", "peer_done", "peer_cancel", "peer_relay_error":
		return true
	}
	return false
}
