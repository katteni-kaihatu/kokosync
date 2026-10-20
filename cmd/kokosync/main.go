package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/katteni-kaihatu/kokosync/server/gateway"
)

func main() {
	httpAddr := flag.String("http", "127.0.0.1:8080", "local HTTP / WebRTC signaling / WebSocket address")
	quicAddr := flag.String("quic", "127.0.0.1:8081", "local QUIC address")
	root := flag.String("root", ".", "repository root for browser assets")
	secret := flag.String("bridge-secret", "local-dev-only", "local sidecar shared secret")
	flag.Parse()
	if e := gateway.ValidateHTTPAddr(*httpAddr); e != nil {
		log.Fatal(e)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	s := gateway.New(*root, *secret)
	httpServer := &http.Server{Addr: *httpAddr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if e := s.ServeQUIC(ctx, *quicAddr); e != nil && ctx.Err() == nil {
			log.Fatal(e)
		}
	}()
	go func() { <-ctx.Done(); _ = httpServer.Close() }()
	log.Printf("kokosync local development: http://%s  QUIC %s (memory durability; no production authentication)", *httpAddr, *quicAddr)
	if e := httpServer.ListenAndServe(); e != nil && e != http.ErrServerClosed {
		log.Fatal(e)
	}
}
