using System.Collections.Concurrent;
using System.Security.Cryptography;
using KokoSync;

static void Check(bool condition, string message)
{
    if (!condition) throw new Exception(message);
}
static void Reject(Action operation, string message)
{
    try { operation(); } catch (ArgumentException) { return; }
    throw new Exception(message);
}
var encoded = Wire.Encode(Client.Frame("message", ("seq", ulong.MaxValue), ("value", new object?[] { null, true, -3, 1.25, new byte[] { 1, 2 } })));
var decoded = Wire.Decode(encoded);
Check(decoded["seq"] is ulong counter && counter == ulong.MaxValue, "UInt64 precision was lost.");
Reject(() => Wire.ValidateValue(ulong.MaxValue), "Unsafe app integer accepted.");
Reject(() => Wire.ValidateValue(double.NaN), "NaN accepted.");
Reject(() => Wire.ValidateValue(double.PositiveInfinity), "Infinity accepted.");
Reject(() => Wire.ValidateValue(new byte[16384]), "Oversized app value accepted.");
try
{
    await Client.ConnectLiteNetLibAsync(insecureDevelopment: false);
    throw new Exception("Insecure opt-in gate failed.");
}
catch (ArgumentException) { }
try
{
    await Client.ConnectLiteNetLibAsync("192.0.2.1", insecureDevelopment: true);
    throw new Exception("Loopback gate failed.");
}
catch (ArgumentException) { }
Console.WriteLine("CHECKS_OK codec uint64 safe-values localhost-opt-in");
await FragmentationChecks.RunAsync();
await RoutingChecks.RunAsync();
await PeerDeliveryChecks.RunAsync();
if (args.Contains("--peer-delivery"))
{
    var endpoint = args.FirstOrDefault(arg => arg.StartsWith("--ws="))?[5..] ?? "ws://127.0.0.1:8080/ws";
    var udpArgument = args.FirstOrDefault(arg => arg.StartsWith("--udp-port="))?[11..];
    await PeerDeliveryIntegration.RunAsync(new Uri(endpoint), udpArgument == null ? null : int.Parse(udpArgument));
}
if (args.Contains("--routing"))
{
    var endpoint = args.FirstOrDefault(arg => arg.StartsWith("--ws="))?[5..] ?? "ws://127.0.0.1:8080/ws";
    var udpArgument = args.FirstOrDefault(arg => arg.StartsWith("--udp-port="))?[11..];
    await RoutingChecks.RunIntegrationAsync(new Uri(endpoint), udpArgument == null ? null : int.Parse(udpArgument));
}
if (args.Contains("--fragmentation"))
{
    var endpoint = args.FirstOrDefault(arg => arg.StartsWith("--ws="))?[5..] ?? "ws://127.0.0.1:8080/ws";
    var udpArgument = args.FirstOrDefault(arg => arg.StartsWith("--udp-port="))?[11..];
    await FragmentationIntegration.RunAsync(new Uri(endpoint), udpArgument == null ? null : int.Parse(udpArgument));
}
if (!args.Contains("--integration")) return;

using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(35));
var ct = deadline.Token;
await using var udp = await Client.ConnectLiteNetLibAsync(
    port: int.Parse(args.FirstOrDefault(arg => arg.StartsWith("--udp-port="))?[11..] ?? "8082"),
    insecureDevelopment: true, ct: ct);
await using var ws = await Client.ConnectWebSocketAsync(
    new Uri(args.FirstOrDefault(arg => arg.StartsWith("--ws="))?[5..] ?? "ws://127.0.0.1:8080/ws"), ct);
var room = "csharp-check-" + Guid.NewGuid().ToString("N")[..8];
var message = new TaskCompletionSource<Dictionary<string, object?>>(TaskCreationOptions.RunContinuationsAsynchronously);
var latest = new TaskCompletionSource<Dictionary<string, object?>>(TaskCreationOptions.RunContinuationsAsynchronously);
var receivedBlob = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
var reverseBlob = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
var receivedEmpty = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
var badTransfer = "bad-" + Guid.NewGuid().ToString("N");
var badCredit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
var badRejected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
var badCancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
var wsLoop = Task.Run(async () =>
{
    await foreach (var frame in ws.Events(ct))
    {
        if (frame["type"] is "message") message.TrySetResult(frame);
        if (frame["type"] is "blob_open")
        {
            using var sink = new MemoryStream();
            if ((string)frame["transfer"]! == badTransfer)
            {
                try { await ws.ReceiveBlobAsync(frame, sink, ct); }
                catch (IOException ex) when (ex.Message.Contains("hash")) { badRejected.TrySetResult(); }
                continue;
            }
            await ws.ReceiveBlobAsync(frame, sink, ct);
            if (Client.Number(frame, "size") == 0) receivedEmpty.TrySetResult(sink.ToArray());
            else receivedBlob.TrySetResult(sink.ToArray());
        }
    }
}, ct);
var udpLoop = Task.Run(async () =>
{
    await foreach (var frame in udp.Events(ct))
    {
        if (frame["type"] is "latest") latest.TrySetResult(frame);
        if (frame.GetValueOrDefault("transfer") is string transfer && transfer == badTransfer)
        {
            if (frame["type"] is "blob_credit") badCredit.TrySetResult();
            if (frame["type"] is "blob_cancel") badCancelled.TrySetResult();
        }
        if (frame["type"] is "blob_open")
        {
            using var sink = new MemoryStream();
            await udp.ReceiveBlobAsync(frame, sink, ct);
            reverseBlob.TrySetResult(sink.ToArray());
        }
    }
}, ct);
await ws.JoinAsync(room, "websocket-check", true, ct);
await udp.JoinAsync(room, "udp-check", true, ct);
await udp.MessageAsync("cross-transport", new Dictionary<string, object?> { ["bytes"] = new byte[] { 4, 5 }, ["answer"] = 42 }, ct);
var notification = await message.Task.WaitAsync(ct);
Check((string)notification["peer"]! == "udp-check", "Cross transport message missing.");
var state = await udp.SetAsync("checked", true, 0, ct);
try { await ws.SetAsync("checked", false, 0, ct); throw new Exception("Expected revision conflict."); }
catch (ProtocolException ex) when (ex.Code == "Conflict") { }
var snapshot = await ws.SnapshotAsync(ct);
Check(Client.Number(snapshot, "revision") == Client.Number(state, "revision"), "Snapshot revision mismatch.");
var catchup = await udp.CatchupAsync(ct);
Check(((object?[])catchup["messages"]!).Length >= 1, "Catchup missing ordered message.");
await ws.LatestAsync("position", new object?[] { 10, 20 }, 9007199254740993UL, ct);
Check(Client.Number(await latest.Task.WaitAsync(ct), "seq") == 9007199254740993UL, "Realtime UInt64 precision lost.");
var payload = RandomNumberGenerator.GetBytes(200000);
await udp.SendBlobAsync("websocket-check", new MemoryStream(payload), (ulong)payload.Length, ct);
var forwardResult = await receivedBlob.Task.WaitAsync(ct);
Check(payload.SequenceEqual(forwardResult), "UDP to WS blob mismatch.");
await ws.SendBlobAsync("udp-check", new MemoryStream(payload), (ulong)payload.Length, ct);
var reverseResult = await reverseBlob.Task.WaitAsync(ct);
Check(payload.SequenceEqual(reverseResult), "WS to UDP blob mismatch.");
await udp.SendBlobAsync("websocket-check", new MemoryStream(), 0, ct);
Check((await receivedEmpty.Task.WaitAsync(ct)).Length == 0, "Empty blob failed.");
await udp.RequestAsync(Client.Frame("blob_open", ("transfer", badTransfer), ("to", "websocket-check"), ("size", 3UL)), ct);
await badCredit.Task.WaitAsync(ct);
await udp.SendAsync(Client.Frame("blob_chunk", ("transfer", badTransfer), ("offset", 0UL), ("value", new byte[] { 1, 2, 3 })), ct);
await udp.SendAsync(Client.Frame("blob_end", ("transfer", badTransfer), ("sha256", new string('0', 64))), ct);
await badRejected.Task.WaitAsync(ct);
await badCancelled.Task.WaitAsync(ct);
Console.WriteLine("INTEGRATION_OK message state conflict snapshot catchup realtime uint64 blob-bidirectional-200000 empty-blob hash-rejection cancellation");
deadline.Cancel();
try { await Task.WhenAll(wsLoop, udpLoop); } catch (OperationCanceledException) { }
