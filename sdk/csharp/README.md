# KokoSync C# prototype SDK

Requires .NET SDK 10.0.402. Packages are pinned to official LiteNetLib 2.1.4
and MessagePack 3.1.12. Add a project reference to `KokoSync/KokoSync.csproj`.

```csharp
await using var client = await KokoSync.Client.ConnectLiteNetLibAsync(
    "127.0.0.1", 8082, insecureDevelopment: true);
var events = Task.Run(async () => {
    await foreach (var frame in client.Events())
        Console.WriteLine(frame["type"]);
});
await client.JoinAsync("demo", "csharp", state: true);
await client.MessageAsync("chat", "hello");
await client.SetAsync("optional-state", true, expected: 0);
```

Start consuming `Events()` promptly; the consumer queue is bounded by 16 events and
the negotiated `QueueBytes` encoded-byte budget, and disconnects on overflow.
One event reader per client is supported. Events are string-key dictionaries; protocol
counters decode to `ulong` (never convert them to `double`). Application values allow
null, bool, finite safe numbers, string, byte arrays, object arrays, and string-key
dictionaries. Custom CLR objects require explicit conversion. App values have 32-level
nesting. Reliable values support transparent fragmentation after join negotiation;
the default logical envelope limit is 4 MiB, including metadata. Leave space for
topic, key, request ID, and relay metadata when sizing values. Legacy peers retain
the 16 KiB application value and 64 KiB envelope limits.

`ConnectWebSocketAsync` and `ConnectLiteNetLibAsync` accept an optional
`fragmentation: new FragmentationOptions { ... }`. Defaults are `LogicalBytes=4194304`,
`ChunkBytes=12288`, `ReassemblyBytes=8388608`, `InflightBytes=65536`, `Concurrent=4`,
`TimeoutMs=30000`, and `QueueBytes=8388608`. All are positive; `ChunkBytes` is at most
16384. Set `Enabled=false` to disable negotiation. `SelectedFragmentation` exposes
the server-selected limits after join. The receiver is initialized before join,
so large initial snapshots are reassembled and verified before `JoinAsync` completes.

Encoded envelopes and full inbound reservations have byte limits; the outbound
queue also has a 256-envelope limit. These bound protocol storage rather than total
process memory, which also includes decoded objects and transport/runtime overhead.
SHA-256, exact lengths, and contiguous offsets are checked before delivery.
Message/checkpoint/catchup, state, event, and control lanes preserve their own FIFO;
other lanes can progress while a large message waits for credit. Physical transports
still share congestion. A transfer has a fixed deadline from open. Cancelling an
active reliable send, expiry, invalid fragments, and lost transfers close the session
and release queued/reserved data. A cancelled queued send can be removed before it
starts. There is no partial initial state or partial message delivery.

`ConnectWebSocketAsync` is also available. LiteNetLib requires explicit localhost
development opt-in; there is no production encryption or authentication. Ordered
operations use channel 0 ReliableOrdered. Latest/live use Unreliable, at most 1000
encoded bytes (client sends reserve 128 bytes for relay metadata), with receiver
sequence suppression scoped to the publisher's connection epoch. WebSocket latest/live remain
reliable fallback traffic. Live payloads are encoded bytes, with no codec negotiation.

`LatestAsync` and `LiveAsync` keep the same API with optional browser direct routing
in the room. This SDK has no WebRTC adapter: it advertises `directProtocol:"none"`
and `maxDirectPeers:0`, and always submits those packets through the core relay.
`client.Routes` returns immutable `RouteInfo` snapshots for current remote members:
`Peer`, `PublisherEpoch`, `Route`, `Reason`, `RttMs`, `SentDirect`, `ReceivedDirect`,
`SentRelay`, and `ReceivedRelay`. Native routes report `relay`,
`local-adapter-unavailable`, null RTT, and zero direct counts. Counts describe
successful local transport submissions or matching-generation receptions,
including received duplicates suppressed before application delivery; they do not
guarantee unreliable delivery. Broadcast submissions count once for each known
remote member. Raw `SendAsync` latest/live `to` or `exclude` selectors are reflected
in those counts.

Initial routes are available when `JoinAsync` completes. Subsequent membership
changes arrive through `Events()` as `type:"route"` with the same lower-camel-case
fields and `removed:true` for a departure (`false` for an addition). Counter
updates are read from `Routes` and do not emit an extra event per packet. Stale
room/member generations are ignored; departures and close remove routes.
Initial selected peer delivery capabilities are available in
`client.Joined["peers"]`, under each member's optional `peerDelivery` map.
An addition route event includes that same optional `peerDelivery` map, so
applications can inspect capabilities as new peers arrive. Absence means that
peer does not advertise addressed delivery.
Servers without the optional membership metadata retain normal relay behavior
and expose an empty route snapshot. Reliable messages and state always use the
core.

Addressed reliable delivery uses `SendToAsync(peer, topic, value, ct)` and delivers
one complete `type:"peer_message"` event through `Events()`. The C# adapter uses
the core relay and returns a `DeliveryReceipt` with the destination generation,
message ID, topic, sequence, route, and optional receipt RTT. This receipt means
the receiving SDK verified and admitted the value to its bounded event queue.
It does not mean application code processed it or persisted it. A sender leaving
after receipt does not retract the admitted event; its original publisher epoch
remains attached.

```csharp
try
{
    var receipt = await client.SendToAsync("drawing-peer", "brush", new Dictionary<string, object?>
    {
        ["strokeId"] = "application-stable-id",
        ["points"] = new object?[] { 10, 20, 30, 40 }
    }, cancellationToken);
    Console.WriteLine($"{receipt.MessageId}: {receipt.Route}");
}
catch (KokoSync.DeliveryException error)
{
    Console.WriteLine($"{error.Code}: {error.Outcome}");
}
```

Each target generation and topic has an independent stop-and-wait ordering lane.
Values are encoded once as portable MessagePack and verified with SHA-256.
Retries retain the same message ID and sequence; accepted sequences are not
dispatched twice. A queued cancellation has outcome `not-sent`. A failed,
cancelled, or timed-out active send can have outcome `unknown`, poisons that
lane, and makes later sends fail with `LaneFailed`. Other lanes continue.
Do not retry an ambiguous operation as a new ID without application-level event
deduplication. Recipient generations are pinned at invocation and are never
retargeted to a same-name replacement. Rejoining creates fresh lanes.

`DeliveryException.Code` uses the shared SDK error contract:
`UnsupportedCapability`, `PeerUnavailable`, `InvalidArgument`, `InvalidValue`,
`PayloadTooLarge`, `Backpressure`, `Timeout`, `Cancelled`, `Closed`,
`TransportFailed`, `LaneFailed`, or `ProtocolError`.
Peer and topic must be nonempty valid UTF-8 strings within 128 and 256 bytes.
Invalid peer-delivery options also produce `InvalidArgument` with `not-sent`.
An unsupported Value produces `InvalidValue`; a valid Value whose encoded bytes
exceed the negotiated limit produces `PayloadTooLarge`. Queue, receive, and
retained-lane capacity failures use `Backpressure`. Missing or departed peer
generations use `PeerUnavailable`. Wire validation failures are normalized to
`ProtocolError`; legacy remote timeout, transport, and capacity codes are
normalized to the corresponding codes above. Always inspect `Outcome` as well
as `Code`: `unknown` can mean the recipient already admitted the message.

Both connect methods accept `peerDelivery: new PeerDeliveryOptions { ... }`.
Defaults are `MessageBytes=4194304`, `ChunkBytes=12288`, `WindowBytes=65536`,
`QueueBytes=8388608`, `ReceiveBytes=8388608`, `Concurrent=4`, `Queued=128`,
`Lanes=64`, `TimeoutMs=30000`, `AssemblyMs=30000`, and `Retries=5`.
Set `Enabled=false` to disable advertisement; zero retries is supported.
Limits are validated against the protocol ceilings. Queue counts include active
sends; send and receive lanes share the retained lane limit. Control and small
messages have scheduling priority over bulk chunks. Bulk values above 16 KiB
leave one transfer/queue slot and 16 KiB of byte budget for small values; a
configured concurrency of one explicitly serializes transfers. The event queue
also reserves one of its 16 slots and 16 KiB for small deliveries.

`DeliveryStats` provides per-peer-generation, route, and kind snapshots for
`peer_message`, `peer_bulk`, `latest`, and `live`: frames/bytes, messages, retries,
unacknowledged sends, queue reservations, reason, and optional RTT. Counters
describe submissions/receptions, not application execution. Receipt RTT is
unavailable after retransmission because a late receipt may acknowledge an older
attempt. Departed generations are removed from diagnostics. Unsupported peers
fail before transmission. Addressed delivery does not change room message
sequence numbers, retained history, state, checkpoints, or catchup.

Methods include `MessageAsync`, `EventAsync`, `SetAsync`, `DeleteAsync`, `SnapshotAsync`,
`CheckpointAsync`, `CatchupAsync`, `LatestAsync`, `LiveAsync`, `SendBlobAsync`,
`ReceiveBlobAsync`, and `CancelBlobAsync`. `RequestAsync` correlates request IDs
(response timeout of at least 15 seconds, or the configured fragmentation timeout),
and `SendAsync` exposes advanced protocol operations. Use
`Client.Frame` for raw frames. Cancellation of a request does not roll back server work.

Call `ReceiveBlobAsync` for a `blob_open` event, passing a writable stream; it leaves
the stream open. Credit is replenished only after writes. The SDK validates SHA-256,
byte count, contiguous offsets, and flush completion before confirming receipt.
Transfers time out after two minutes and attempt cancellation on failure; partial
sink data must be discarded by the application. No resume is supported.
Blob frames stay on the dedicated streaming path; they do not gain nested envelope
fragmentation. Latest/live also remain small and are never fragmented.

```powershell
.tools/dotnet/dotnet.exe run --project sdk/csharp/KokoSync.Checks
# With the Go core and sidecar running:
.tools/dotnet/dotnet.exe run --project sdk/csharp/KokoSync.Checks -- --integration
# Large initial state, ordered messages, snapshot, checkpoint, and catchup:
.tools/dotnet/dotnet.exe run --project sdk/csharp/KokoSync.Checks -- --fragmentation --ws=ws://127.0.0.1:8080/ws
# Add --udp-port=8082 to exercise LiteNetLib-to-WebSocket fragmentation.
# Native relay routes, membership changes, counters, and UInt64 latest/live:
.tools/dotnet/dotnet.exe run --project sdk/csharp/KokoSync.Checks -- --routing --ws=ws://127.0.0.1:8080/ws --udp-port=8082
# Addressed reliable values and receipts; omit --udp-port for WebSocket-only:
.tools/dotnet/dotnet.exe run --project sdk/csharp/KokoSync.Checks -- --peer-delivery --ws=ws://127.0.0.1:8080/ws --udp-port=8082
```
