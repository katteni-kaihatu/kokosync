# Official LiteNetLib sidecar

```powershell
.tools/dotnet/dotnet.exe run --project server/litenetlib -- --insecure-development
```

The explicit flag enables unauthenticated, unencrypted loopback development only.
The sidecar binds UDP loopback port 8082 and accepts the connection key `kokosync/1`.
`--port` changes the UDP port. `--bridge ws://127.0.0.1:8080/bridge` changes the core
endpoint; only numeric loopback addresses are accepted.
Set `KOKOSYNC_BRIDGE_SECRET` to match the core (default `local-dev-only`).

Each UDP peer gets a separate core WebSocket session. MessagePack bytes are forwarded
unchanged, one frame per message. Ordered traffic uses LiteNetLib ReliableOrdered
channel 0; latest/live use Unreliable and a 1000-byte encoded cap.
There are at most 32 sessions, 16 queued frames per direction, and at most
1 MiB / 256 frames awaiting reliable delivery per peer. Overflow disconnects the
peer. A 5 ms poll loop drives LiteNetLib events. This is a local interoperability
prototype, not a production secure transport.

The bridge uses the official NuGet package `LiteNetLib` version 2.1.4 through the
C# SDK project reference; it does not implement the LiteNetLib packet protocol.
