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

Start consuming `Events()` promptly; bounded consumer queues disconnect on overflow.
One event reader per client is supported. Events are string-key dictionaries; protocol
counters decode to `ulong` (never convert them to `double`). Application values allow
null, bool, finite safe numbers, string, byte arrays, object arrays, and string-key
dictionaries. Custom CLR objects require explicit conversion. App values have 32-level
nesting and 16 KiB encoded limits.

`ConnectWebSocketAsync` is also available. LiteNetLib requires explicit localhost
development opt-in; there is no production encryption or authentication. Ordered
operations use channel 0 ReliableOrdered. Latest/live use Unreliable, at most 1000
encoded bytes (client sends reserve 128 bytes for relay metadata), with receiver
sequence suppression scoped to the publisher's connection epoch. WebSocket latest/live remain
reliable fallback traffic. Live payloads are encoded bytes, with no codec negotiation.

Methods include `MessageAsync`, `EventAsync`, `SetAsync`, `DeleteAsync`, `SnapshotAsync`,
`CheckpointAsync`, `CatchupAsync`, `LatestAsync`, `LiveAsync`, `SendBlobAsync`,
`ReceiveBlobAsync`, and `CancelBlobAsync`. `RequestAsync` correlates request IDs
(15-second timeout), and `SendAsync` exposes advanced protocol operations. Use
`Client.Frame` for raw frames. Cancellation of a request does not roll back server work.

Call `ReceiveBlobAsync` for a `blob_open` event, passing a writable stream; it leaves
the stream open. Credit is replenished only after writes. The SDK validates SHA-256,
byte count, contiguous offsets, and flush completion before confirming receipt.
Transfers time out after two minutes and attempt cancellation on failure; partial
sink data must be discarded by the application. No resume is supported.

```powershell
.tools/dotnet/dotnet.exe run --project sdk/csharp/KokoSync.Checks
# With the Go core and sidecar running:
.tools/dotnet/dotnet.exe run --project sdk/csharp/KokoSync.Checks -- --integration
```
