package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/katteni-kaihatu/kokosync/server/core"
	"github.com/katteni-kaihatu/kokosync/server/gateway"
)

func main() {
	httpAddr := flag.String("http", "127.0.0.1:8080", "local HTTP / WebRTC signaling / WebSocket address")
	quicAddr := flag.String("quic", "127.0.0.1:8081", "local QUIC address")
	root := flag.String("root", ".", "repository root for browser assets")
	bridgeSecret := os.Getenv("KOKOSYNC_BRIDGE_SECRET")
	if bridgeSecret == "" {
		bridgeSecret = "local-dev-only"
	}
	secret := flag.String("bridge-secret", bridgeSecret, "local sidecar shared secret (defaults to KOKOSYNC_BRIDGE_SECRET)")
	publicIP := flag.String("public-ip", "", "enable hosted browser WebRTC with this public IPv4 address; requires authenticated TLS proxy")
	rtcUDPMin := flag.Int("rtc-udp-min", 40000, "hosted WebRTC UDP port range minimum")
	rtcUDPMax := flag.Int("rtc-udp-max", 40100, "hosted WebRTC UDP port range maximum")
	limitsPath := flag.String("limits", os.Getenv("KOKOSYNC_LIMITS"), "optional JSON file overriding room count, idle timeout, fragmentation and byte budgets")
	flag.Parse()
	if e := gateway.ValidateHTTPAddr(*httpAddr); e != nil {
		log.Fatal(e)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	s := gateway.New(*root, *secret)
	if *limitsPath != "" {
		f, e := os.Open(*limitsPath)
		if e != nil {
			log.Fatal(e)
		}
		cfg := core.DefaultConfig()
		decoder := json.NewDecoder(f)
		decoder.DisallowUnknownFields()
		e = decoder.Decode(&cfg)
		_ = f.Close()
		if e != nil {
			log.Fatal(e)
		}
		s.Core, e = core.NewWithConfig(cfg)
		if e != nil {
			log.Fatal(e)
		}
	}
	if *publicIP != "" {
		if e := s.ConfigureHostedWeb(*publicIP, *rtcUDPMin, *rtcUDPMax); e != nil {
			log.Fatal(e)
		}
	}
	go s.Core.RunRoomCleanup(ctx)
	httpServer := &http.Server{Addr: *httpAddr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if e := s.ServeQUIC(ctx, *quicAddr); e != nil && ctx.Err() == nil {
			log.Fatal(e)
		}
	}()
	go func() { <-ctx.Done(); _ = httpServer.Close() }()
	log.Printf("kokosync %s: http://%s  loopback QUIC %s (memory durability; no application identity authentication)", s.Mode(), *httpAddr, *quicAddr)
	if *publicIP != "" {
		log.Printf("browser WebRTC public IPv4 %s UDP %d-%d; TLS and access authentication must be enforced by the reverse proxy", *publicIP, *rtcUDPMin, *rtcUDPMax)
	}
	if e := httpServer.ListenAndServe(); e != nil && e != http.ErrServerClosed {
		log.Fatal(e)
	}
}
