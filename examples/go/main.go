package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	kokosync "github.com/katteni-kaihatu/kokosync/sdk/go"
)

func main() {
	addr := flag.String("address", "127.0.0.1:8081", "QUIC endpoint")
	room := flag.String("room", "mixed", "room")
	peer := flag.String("peer", "go", "peer display name")
	seconds := flag.Int("seconds", 15, "listen duration")
	smoke := flag.Bool("smoke", false, "publish generic data periodically")
	insecure := flag.Bool("insecure-development", false, "allow loopback ephemeral dev certificate")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*seconds)*time.Second)
	defer cancel()
	client, e := kokosync.Connect(ctx, kokosync.Options{Address: *addr, Room: *room, Peer: *peer, State: true, InsecureDevelopment: *insecure})
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	defer client.Close()
	printJSON(client.Info())
	if *smoke {
		_, _ = client.Set(ctx, "hello:"+*peer, map[string]any{"language": "go", "text": "こんにちは"}, nil)
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			var seq uint64
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					seq++
					_, _ = client.Publish(ctx, "hello", map[string]any{"language": "go", "tick": int(seq)})
					_ = client.Latest(ctx, "cursor", seq, map[string]any{"x": int(seq)})
				}
			}
		}()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-client.Done():
			return
		case message := <-client.Messages():
			printJSON(message)
		}
	}
}
func printJSON(v any) {
	b, e := json.Marshal(v)
	if e == nil {
		fmt.Println(string(b))
	}
}
