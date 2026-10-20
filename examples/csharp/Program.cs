using System.Text.Json;
using System.Security.Cryptography;
using KokoSync;

string Option(string name, string fallback)
{
    var index = Array.IndexOf(args, name);
    return index >= 0 && index + 1 < args.Length ? args[index + 1] : fallback;
}
if (!args.Contains("--insecure-development") && !args.Contains("--websocket"))
{
    Console.Error.WriteLine("LiteNetLib localhost requires --insecure-development.");
    return 2;
}
var smoke = args.Contains("--smoke");
using var stop = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; stop.Cancel(); };
if (smoke) stop.CancelAfter(TimeSpan.FromSeconds(int.Parse(Option("--seconds", "12"))));
await using var client = args.Contains("--websocket")
    ? await Client.ConnectWebSocketAsync(new Uri(Option("--endpoint", "ws://127.0.0.1:8080/ws")), stop.Token)
    : await Client.ConnectLiteNetLibAsync("127.0.0.1", int.Parse(Option("--port", "8082")), true, stop.Token);
var peer = Option("--peer", "csharp-" + Guid.NewGuid().ToString("N")[..6]);
var room = Option("--room", "mixed");
var consume = Task.Run(async () =>
{
    try
    {
        await foreach (var frame in client.Events(stop.Token))
        {
            if (frame.GetValueOrDefault("type") as string == "peer_message" &&
                frame.GetValueOrDefault("value") is Dictionary<string, object?> value &&
                value.GetValueOrDefault("bytes") is byte[] bytes)
            {
                var hash = Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
                var summary = new Dictionary<string, object?>(frame);
                summary["value"] = new { bytes = bytes.Length, sha256 = hash, verified = value.GetValueOrDefault("sha256") as string == hash };
                Console.WriteLine("RECEIVED " + JsonSerializer.Serialize(summary));
            }
            else Console.WriteLine("RECEIVED " + JsonSerializer.Serialize(frame));
        }
    }
    catch (OperationCanceledException) { }
});
var joined = await client.JoinAsync(room, peer, state: true, stop.Token);
Console.WriteLine("JOINED " + JsonSerializer.Serialize(joined));
await client.MessageAsync("hello", new Dictionary<string, object?> { ["from"] = peer, ["text"] = "Hello from C#" }, stop.Token);
await client.SetAsync("greeting:" + peer, "C# optional state", ct: stop.Token);
await client.EventAsync("ready", peer, ct: stop.Token);
await client.LatestAsync("cursor", new object?[] { 20, 30 }, 1UL, stop.Token);
await client.LiveAsync("sample", new byte[] { 1, 2, 3 }, 1UL, 1000, "application/octet-stream", stop.Token);
Console.WriteLine("PUBLISHED");
var sendTo = Option("--send-to", "");
if (sendTo.Length != 0)
{
    for (var sequence = 1; sequence <= 3; sequence++)
        Console.WriteLine("PEER_RECEIPT " + JsonSerializer.Serialize(await client.SendToAsync(sendTo, "demo.peer.operation",
            new Dictionary<string, object?> { ["source"] = peer, ["operation"] = sequence }, stop.Token)));
    var payload = Enumerable.Range(0, 1 << 20).Select(i => (byte)(i % 251)).ToArray();
    Console.WriteLine("PEER_RECEIPT " + JsonSerializer.Serialize(await client.SendToAsync(sendTo, "demo.peer.snapshot",
        new Dictionary<string, object?> { ["bytes"] = payload, ["size"] = payload.Length,
            ["sha256"] = Convert.ToHexString(SHA256.HashData(payload)).ToLowerInvariant() }, stop.Token)));
}
try
{
    if (smoke)
    {
        ulong tick = 1;
        while (!stop.IsCancellationRequested)
        {
            await Task.Delay(1000, stop.Token);
            tick++;
            await client.MessageAsync("hello", new Dictionary<string, object?> { ["language"] = "csharp", ["tick"] = (int)tick }, stop.Token);
            await client.LatestAsync("cursor", new object?[] { (int)tick, 30 }, tick, stop.Token);
        }
    }
    else await Task.Delay(Timeout.Infinite, stop.Token);
}
catch (OperationCanceledException) { }
await consume;
Console.WriteLine(smoke ? "SMOKE_OK" : "CLOSED");
return 0;
