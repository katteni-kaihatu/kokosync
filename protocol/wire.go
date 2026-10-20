// Package protocol defines the small, portable MessagePack subset used by the prototype.
package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"unicode/utf8"

	"github.com/vmihailenco/msgpack/v5"
)

const MaxFrame = 65536
const MaxRealtimeFrame = 1000

type Message = map[string]any

func Encode(m Message) ([]byte, error) {
	return EncodeLimit(m, MaxFrame)
}
func EncodeLimit(m Message, limit int) ([]byte, error) {
	b, err := msgpack.Marshal(m)
	if len(b) > limit {
		return nil, errors.New("PayloadTooLarge")
	}
	return b, err
}

// Decode validates lengths, nesting, map keys and extension absence before decoding.
// This prevents tiny hostile frames claiming multi-gigabyte arrays from allocating.
func Decode(b []byte) (Message, error) {
	return DecodeLimit(b, MaxFrame)
}
func DecodeLimit(b []byte, limit int) (Message, error) {
	if len(b) == 0 || len(b) > limit {
		return nil, errors.New("invalid frame size")
	}
	p := parser{b: b}
	if _, err := p.value(0, false); err != nil {
		return nil, err
	}
	if p.i != len(b) {
		return nil, errors.New("trailing MessagePack data")
	}
	var m Message
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
	if err := d.Decode(&m); err != nil {
		return nil, err
	}
	if m == nil {
		return nil, errors.New("envelope must be a map")
	}
	return m, nil
}

type parser struct {
	b []byte
	i int
}

func (p *parser) take(n uint64) ([]byte, error) {
	if n > uint64(len(p.b)-p.i) {
		return nil, io.ErrUnexpectedEOF
	}
	b := p.b[p.i : p.i+int(n)]
	p.i += int(n)
	return b, nil
}
func (p *parser) length(n int) (uint64, error) {
	b, e := p.take(uint64(n))
	if e != nil {
		return 0, e
	}
	switch n {
	case 1:
		return uint64(b[0]), nil
	case 2:
		return uint64(binary.BigEndian.Uint16(b)), nil
	default:
		return uint64(binary.BigEndian.Uint32(b)), nil
	}
}
func (p *parser) value(depth int, key bool) (string, error) {
	if depth > 34 {
		return "", errors.New("nesting limit")
	}
	b, e := p.take(1)
	if e != nil {
		return "", e
	}
	c := b[0]
	var n uint64
	var kind byte
	switch {
	case c >= 0xa0 && c <= 0xbf:
		n = uint64(c & 31)
		kind = 's'
	case c >= 0x90 && c <= 0x9f:
		n = uint64(c & 15)
		kind = 'a'
	case c >= 0x80 && c <= 0x8f:
		n = uint64(c & 15)
		kind = 'm'
	case c <= 0x7f || c >= 0xe0 || c == 0xc0 || c == 0xc2 || c == 0xc3:
		if key {
			return "", errors.New("map key must be string")
		}
		return "", nil
	default:
		switch c {
		case 0xd9, 0xda, 0xdb:
			kind = 's'
			n, e = p.length(1 << uint(c-0xd9))
		case 0xc4, 0xc5, 0xc6:
			kind = 'b'
			n, e = p.length(1 << uint(c-0xc4))
		case 0xdc, 0xdd:
			kind = 'a'
			n, e = p.length(2 << uint(c-0xdc))
		case 0xde, 0xdf:
			kind = 'm'
			n, e = p.length(2 << uint(c-0xde))
		case 0xcc, 0xd0:
			n = 1
		case 0xcd, 0xd1:
			n = 2
		case 0xce, 0xd2, 0xca:
			n = 4
		case 0xcf, 0xd3, 0xcb:
			n = 8
		default:
			return "", errors.New("unsupported MessagePack code")
		}
	}
	if e != nil {
		return "", e
	}
	if key && kind != 's' {
		return "", errors.New("map key must be string")
	}
	if kind == 'm' || kind == 'a' {
		if n > 4096 {
			return "", errors.New("container element limit")
		}
		keys := map[string]bool{}
		for i := uint64(0); i < n; i++ {
			if kind == 'm' {
				k, e := p.value(depth+1, true)
				if e != nil {
					return "", e
				}
				if keys[k] {
					return "", errors.New("duplicate map key")
				}
				keys[k] = true
			}
			if _, e := p.value(depth+1, false); e != nil {
				return "", e
			}
		}
		return "", nil
	}
	raw, e := p.take(n)
	if e != nil {
		return "", e
	}
	if kind == 's' {
		if !utf8.Valid(raw) {
			return "", errors.New("invalid UTF-8")
		}
		return string(raw), nil
	}
	return "", nil
}

func Uint(v any) (uint64, bool) {
	switch x := v.(type) {
	case uint64:
		return x, true
	case uint32:
		return uint64(x), true
	case uint16:
		return uint64(x), true
	case uint8:
		return uint64(x), true
	case uint:
		return uint64(x), true
	case int64:
		if x >= 0 {
			return uint64(x), true
		}
	case int32:
		if x >= 0 {
			return uint64(x), true
		}
	case int16:
		if x >= 0 {
			return uint64(x), true
		}
	case int8:
		if x >= 0 {
			return uint64(x), true
		}
	case int:
		if x >= 0 {
			return uint64(x), true
		}
	}
	return 0, false
}
func String(m Message, k string) string  { s, _ := m[k].(string); return s }
func Counter(m Message, k string) uint64 { n, _ := Uint(m[k]); return n }

func ValidateValue(v any) error { return validate(reflect.ValueOf(v), 0) }
func validate(v reflect.Value, depth int) error {
	if depth > 32 {
		return errors.New("value nesting exceeds 32")
	}
	if !v.IsValid() {
		return nil
	}
	if v.Kind() == reflect.Interface {
		return validate(v.Elem(), depth)
	}
	switch v.Kind() {
	case reflect.Bool, reflect.String:
		return nil
	case reflect.Float32, reflect.Float64:
		x := v.Float()
		if math.IsNaN(x) || math.IsInf(x, 0) || math.Abs(x) > 9007199254740991 {
			return errors.New("unsafe numeric value")
		}
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		x := v.Int()
		if x > 9007199254740991 || x < -9007199254740991 {
			return errors.New("unsafe integer")
		}
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if v.Uint() > 9007199254740991 {
			return errors.New("unsafe integer")
		}
		return nil
	case reflect.Slice, reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return nil
		}
		if v.Len() > 4096 {
			return errors.New("array limit")
		}
		for i := 0; i < v.Len(); i++ {
			if e := validate(v.Index(i), depth+1); e != nil {
				return e
			}
		}
		return nil
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String || v.Len() > 4096 {
			return errors.New("string map required")
		}
		it := v.MapRange()
		for it.Next() {
			if e := validate(it.Value(), depth+1); e != nil {
				return e
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported value %s", v.Kind())
	}
}
func ReadFrame(r io.Reader) ([]byte, error) {
	var h [4]byte
	if _, e := io.ReadFull(r, h[:]); e != nil {
		return nil, e
	}
	n := binary.BigEndian.Uint32(h[:])
	if n == 0 || n > MaxFrame {
		return nil, errors.New("invalid frame length")
	}
	b := make([]byte, n)
	_, e := io.ReadFull(r, b)
	return b, e
}
func WriteFrame(w io.Writer, b []byte) error {
	if len(b) == 0 || len(b) > MaxFrame {
		return errors.New("invalid frame length")
	}
	var h [4]byte
	binary.BigEndian.PutUint32(h[:], uint32(len(b)))
	if e := writeAll(w, h[:]); e != nil {
		return e
	}
	return writeAll(w, b)
}
func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, e := w.Write(b)
		if e != nil {
			return e
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}
