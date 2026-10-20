package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

func TestHostedBoundaryAndProxyOrigin(t *testing.T) {
	s := New("../..", "test-secret")
	if s.Mode() != "local-development" {
		t.Fatal("default mode changed")
	}
	for _, bad := range []struct {
		ip       string
		min, max int
	}{
		{"127.0.0.1", 40000, 40100},
		{"192.168.1.2", 40000, 40100},
		{"::1", 40000, 40100},
		{"45.77.130.146", 0, 40100},
		{"45.77.130.146", 41000, 40000},
		{"45.77.130.146", 40000, 65536},
	} {
		if s.ConfigureHostedWeb(bad.ip, bad.min, bad.max) == nil {
			t.Fatalf("accepted invalid config: %#v", bad)
		}
	}
	if err := s.ConfigureHostedWeb("45.77.130.146", 40000, 40100); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, remote, path, origin, forwarded string
		want                                  int
	}{
		{"trusted TLS proxy", "127.0.0.1:1234", "/health", "https://sync.example.test", "https", 200},
		{"local diagnostic", "127.0.0.1:1234", "/health", "", "", 200},
		{"wrong origin", "127.0.0.1:1234", "/health", "https://evil.example", "https", 403},
		{"wrong scheme", "127.0.0.1:1234", "/health", "http://sync.example.test", "https", 403},
		{"untrusted remote", "203.0.113.7:1234", "/health", "https://sync.example.test", "https", 403},
		{"public bridge", "127.0.0.1:1234", "/bridge", "", "https", 403},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "http://sync.example.test"+tt.path, nil)
			req.RemoteAddr = tt.remote
			req.Header.Set("Origin", tt.origin)
			req.Header.Set("X-Forwarded-Proto", tt.forwarded)
			req.Header.Set("X-Kokosync-Bridge", "test-secret")
			res := httptest.NewRecorder()
			s.Handler().ServeHTTP(res, req)
			if res.Code != tt.want {
				t.Fatalf("status %d, want %d: %s", res.Code, tt.want, res.Body.String())
			}
			if tt.want == 200 && !strings.Contains(res.Body.String(), `"mode":"hosted-web"`) {
				t.Fatal("health did not report hosted mode")
			}
			if tt.want == 200 {
				var health struct {
					Rooms struct {
						Max           int `json:"max"`
						IdleTimeoutMs int `json:"idleTimeoutMs"`
					} `json:"rooms"`
				}
				if err := json.Unmarshal(res.Body.Bytes(), &health); err != nil || health.Rooms.Max != 1024 || health.Rooms.IdleTimeoutMs != 600000 {
					t.Fatalf("health missing room limits: %s (%v)", res.Body.String(), err)
				}
			}
		})
	}
	if err := ValidateHTTPAddr("0.0.0.0:8080"); err == nil {
		t.Fatal("HTTP public bind accepted")
	}
	if err := s.ServeQUIC(context.Background(), "0.0.0.0:8081"); err == nil {
		t.Fatal("QUIC public bind accepted")
	}
}

func TestHostedICEPublishesPublicIPWithinRange(t *testing.T) {
	s := New("../..", "test")
	if err := s.ConfigureHostedWeb("45.77.130.146", 42000, 42020); err != nil {
		t.Fatal(err)
	}
	settings, err := s.rtcSettings()
	if err != nil {
		t.Fatal(err)
	}
	pc, err := webrtc.NewAPI(webrtc.WithSettingEngine(settings)).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	if _, err = pc.CreateDataChannel("kokosync", nil); err != nil {
		t.Fatal(err)
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err = pc.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gathered:
	case <-time.After(5 * time.Second):
		t.Fatal("ICE gathering timed out")
	}
	candidates := 0
	for _, line := range strings.Split(pc.LocalDescription().SDP, "\n") {
		if !strings.HasPrefix(line, "a=candidate:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 8 {
			t.Fatalf("bad ICE candidate: %s", line)
		}
		if fields[4] != "45.77.130.146" {
			t.Fatalf("private address leaked: %s", line)
		}
		port, err := strconv.Atoi(fields[5])
		if err != nil || port < 42000 || port > 42020 {
			t.Fatalf("candidate outside bounded UDP range: %s", line)
		}
		if strings.ToLower(fields[2]) != "udp" {
			t.Fatalf("unexpected ICE transport: %s", line)
		}
		candidates++
	}
	if candidates == 0 {
		t.Fatal("no hosted ICE candidate gathered")
	}
}
