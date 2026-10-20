package main

import (
	"context"
	"crypto/sha256"
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
	sendTo := flag.String("send-to", "", "send three ordered peer operations and a 1 MiB value to this peer")
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
	if *sendTo != "" {
		go func() {
			for seq := 1; seq <= 3; seq++ {
				receipt, err := client.SendTo(ctx, *sendTo, "demo.peer.operation", map[string]any{"source": *peer, "operation": seq})
				if err != nil {
					fmt.Fprintln(os.Stderr, "peer delivery:", err)
					return
				}
				printJSON(receipt)
			}
			data := make([]byte, 1<<20)
			for i := range data {
				data[i] = byte(i % 251)
			}
			digest := sha256.Sum256(data)
			receipt, err := client.SendTo(ctx, *sendTo, "demo.peer.snapshot", map[string]any{"bytes": data, "size": len(data), "sha256": fmt.Sprintf("%x", digest)})
			if err != nil {
				fmt.Fprintln(os.Stderr, "peer snapshot:", err)
				return
			}
			printJSON(receipt)
		}()
	}
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
	if frame, ok := v.(map[string]any); ok && frame["type"] == "peer_message" {
		copy := make(map[string]any, len(frame))
		for key, value := range frame {
			copy[key] = value
		}
		if value, ok := frame["value"].(map[string]any); ok {
			if bytes, ok := value["bytes"].([]byte); ok {
				digest := sha256.Sum256(bytes)
				copy["value"] = map[string]any{"bytes": len(bytes), "sha256": fmt.Sprintf("%x", digest), "verified": value["sha256"] == fmt.Sprintf("%x", digest)}
			}
		}
		v = copy
	}
	b, e := json.Marshal(v)
	if e == nil {
		fmt.Println(string(b))
	}
}
