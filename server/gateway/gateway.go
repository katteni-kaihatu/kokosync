// Package gateway terminates real WebRTC, QUIC and WebSocket connections.
package gateway

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/katteni-kaihatu/kokosync/protocol"
	"github.com/katteni-kaihatu/kokosync/server/core"
	"github.com/pion/webrtc/v4"
	"github.com/quic-go/quic-go"
)

type Server struct {
	Core               *core.Core
	Root, BridgeSecret string
	mu                 sync.Mutex
	pcs                map[*webrtc.PeerConnection]struct{}
}

func New(root, secret string) *Server {
	return &Server{Core: core.New(), Root: root, BridgeSecret: secret, pcs: map[*webrtc.PeerConnection]struct{}{}}
}
func loopback(addr string) bool {
	host, _, e := net.SplitHostPort(addr)
	return e == nil && net.ParseIP(host) != nil && net.ParseIP(host).IsLoopback()
}
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"mode":"local-development","durability":"memory"}`))
	})
	mux.HandleFunc("/ws", s.ws)
	mux.HandleFunc("/bridge", s.ws)
	mux.HandleFunc("/offer", s.offer)
	mux.Handle("/sdk/", http.StripPrefix("/sdk/", http.FileServer(http.Dir(filepath.Join(s.Root, "sdk", "js")))))
	mux.Handle("/vendor/", http.StripPrefix("/vendor/", http.FileServer(http.Dir(filepath.Join(s.Root, "node_modules")))))
	mux.Handle("/", http.FileServer(http.Dir(filepath.Join(s.Root, "examples", "browser"))))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !loopback(r.RemoteAddr) {
			http.Error(w, "local development only", 403)
			return
		}
		// Avoid cross-origin websites connecting to an unauthenticated local service.
		if origin := r.Header.Get("Origin"); origin != "" && origin != "http://"+r.Host && origin != "https://"+r.Host {
			http.Error(w, "same-origin required", 403)
			return
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		mux.ServeHTTP(w, r)
	})
}
func (s *Server) ws(w http.ResponseWriter, r *http.Request) {
	transport := "websocket"
	datagram := false
	if r.URL.Path == "/bridge" {
		if s.BridgeSecret == "" || r.Header.Get("X-Kokosync-Bridge") != s.BridgeSecret {
			http.Error(w, "invalid bridge secret", 403)
			return
		}
		transport = "litenetlib"
		datagram = true
	}
	ws, e := websocket.Accept(w, r, nil)
	if e != nil {
		return
	}
	ws.SetReadLimit(protocol.MaxFrame)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := s.Core.NewPeer(transport, datagram, func(b []byte, _ bool) error {
		writeCtx, done := context.WithTimeout(ctx, 5*time.Second)
		defer done()
		return ws.Write(writeCtx, websocket.MessageBinary, b)
	}, func() { cancel(); _ = ws.CloseNow() })
	defer s.Core.Disconnect(p)
	for {
		kind, b, e := ws.Read(ctx)
		if e != nil {
			return
		}
		if kind != websocket.MessageBinary {
			return
		}
		s.Core.Handle(p, b)
	}
}
func (s *Server) offer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	var offer webrtc.SessionDescription
	if e := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&offer); e != nil {
		http.Error(w, "invalid SDP", 400)
		return
	}
	settings := webrtc.SettingEngine{}
	settings.SetIncludeLoopbackCandidate(true)
	settings.SetIPFilter(func(ip net.IP) bool { return ip.IsLoopback() })
	settings.SetRemoteIPFilter(func(ip net.IP) bool { return ip.IsLoopback() })
	api := webrtc.NewAPI(webrtc.WithSettingEngine(settings))
	pc, e := api.NewPeerConnection(webrtc.Configuration{})
	if e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	s.mu.Lock()
	if len(s.pcs) >= 128 {
		s.mu.Unlock()
		_ = pc.Close()
		http.Error(w, "connection limit", 503)
		return
	}
	s.pcs[pc] = struct{}{}
	s.mu.Unlock()
	var mu sync.RWMutex
	var reliable, realtime *webrtc.DataChannel
	p := s.Core.NewPeer("webrtc", true, func(b []byte, lossy bool) error {
		mu.RLock()
		dc := reliable
		if lossy {
			dc = realtime
		}
		mu.RUnlock()
		if dc == nil || dc.ReadyState() != webrtc.DataChannelStateOpen {
			return errors.New("datachannel not open")
		}
		if lossy && dc.BufferedAmount() > 16000 {
			return errors.New("realtime backpressure")
		}
		deadline := time.Now().Add(5 * time.Second)
		for dc.BufferedAmount() > 65536 {
			if time.Now().After(deadline) {
				return errors.New("datachannel backpressure")
			}
			time.Sleep(2 * time.Millisecond)
		}
		return dc.Send(b)
	}, func() { _ = pc.Close(); s.mu.Lock(); delete(s.pcs, pc); s.mu.Unlock() })
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		mu.Lock()
		switch dc.Label() {
		case "kokosync":
			reliable = dc
		case "kokosync-realtime":
			realtime = dc
		default:
			mu.Unlock()
			_ = dc.Close()
			return
		}
		mu.Unlock()
		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			if msg.IsString || len(msg.Data) > protocol.MaxFrame {
				s.Core.Disconnect(p)
				return
			}
			if dc.Label() == "kokosync-realtime" {
				m, e := protocol.Decode(msg.Data)
				if e != nil || (protocol.String(m, "type") != "latest" && protocol.String(m, "type") != "live") || len(msg.Data) > protocol.MaxRealtimeFrame {
					return
				}
			}
			s.Core.Handle(p, msg.Data)
		})
		dc.OnClose(func() {
			if dc.Label() == "kokosync" {
				s.Core.Disconnect(p)
			}
		})
	})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed || state == webrtc.PeerConnectionStateDisconnected {
			s.Core.Disconnect(p)
		}
	})
	cleanup := true
	defer func() {
		if cleanup {
			s.Core.Disconnect(p)
		}
	}()
	if e = pc.SetRemoteDescription(offer); e != nil {
		http.Error(w, e.Error(), 400)
		return
	}
	answer, e := pc.CreateAnswer(nil)
	if e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if e = pc.SetLocalDescription(answer); e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	select {
	case <-gathered:
	case <-time.After(10 * time.Second):
		http.Error(w, "ICE timeout", 504)
		return
	case <-r.Context().Done():
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(pc.LocalDescription())
	cleanup = false
	// Uncompleted handshakes must not retain the server's connection budget forever.
	time.AfterFunc(20*time.Second, func() {
		if pc.ConnectionState() != webrtc.PeerConnectionStateConnected {
			s.Core.Disconnect(p)
		}
	})
}
func (s *Server) ServeQUIC(ctx context.Context, addr string) error {
	host, _, e := net.SplitHostPort(addr)
	if e != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return errors.New("QUIC must bind a loopback IP")
	}
	cfg, e := DevelopmentTLS()
	if e != nil {
		return e
	}
	listener, e := quic.ListenAddr(addr, cfg, &quic.Config{EnableDatagrams: true, MaxIdleTimeout: 30 * time.Second, KeepAlivePeriod: 10 * time.Second, MaxIncomingStreams: 1, MaxIncomingUniStreams: -1})
	if e != nil {
		return e
	}
	defer listener.Close()
	go func() { <-ctx.Done(); _ = listener.Close() }()
	for {
		conn, e := listener.Accept(ctx)
		if e != nil {
			return e
		}
		if !loopback(conn.RemoteAddr().String()) {
			_ = conn.CloseWithError(1, "local development only")
			continue
		}
		go s.quicConn(ctx, conn)
	}
}
func (s *Server) quicConn(ctx context.Context, conn *quic.Conn) {
	acceptCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	stream, e := conn.AcceptStream(acceptCtx)
	if e != nil {
		_ = conn.CloseWithError(1, "stream required")
		return
	}
	p := s.Core.NewPeer("quic", conn.ConnectionState().SupportsDatagrams.Remote, func(b []byte, lossy bool) error {
		if lossy {
			return conn.SendDatagram(b)
		}
		_ = stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
		return protocol.WriteFrame(stream, b)
	}, func() { _ = conn.CloseWithError(0, "closed") })
	defer s.Core.Disconnect(p)
	go func() {
		for {
			b, e := conn.ReceiveDatagram(ctx)
			if e != nil {
				return
			}
			if len(b) > protocol.MaxRealtimeFrame {
				continue
			}
			m, e := protocol.Decode(b)
			if e != nil {
				continue
			}
			if kind := protocol.String(m, "type"); kind != "latest" && kind != "live" {
				continue
			}
			s.Core.Handle(p, b)
		}
	}()
	for {
		b, e := protocol.ReadFrame(stream)
		if e != nil {
			return
		}
		s.Core.Handle(p, b)
	}
}
func DevelopmentTLS() (*tls.Config, error) {
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		return nil, e
	}
	cert := x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: "kokosync localhost development"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}}
	der, e := x509.CreateCertificate(rand.Reader, &cert, &cert, &key.PublicKey, key)
	if e != nil {
		return nil, e
	}
	pk, e := x509.MarshalECPrivateKey(key)
	if e != nil {
		return nil, e
	}
	pair, e := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: pk}))
	if e != nil {
		return nil, e
	}
	return &tls.Config{Certificates: []tls.Certificate{pair}, NextProtos: []string{"kokosync/1"}, MinVersion: tls.VersionTLS13}, nil
}
func ValidateHTTPAddr(addr string) error {
	host, _, e := net.SplitHostPort(addr)
	if e != nil || net.ParseIP(strings.Trim(host, "[]")) == nil || !net.ParseIP(strings.Trim(host, "[]")).IsLoopback() {
		return errors.New("HTTP must bind a loopback IP")
	}
	return nil
}
