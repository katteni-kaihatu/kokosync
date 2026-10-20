using System.Security.Cryptography;
using System.Threading.Channels;
using KokoSync;

internal static class FragmentationIntegration
{
    private static void Check(bool value, string reason)
    {
        if (!value) throw new Exception(reason);
    }
    internal static async Task RunAsync(Uri endpoint, int? udpPort = null)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        var ct = timeout.Token;
        var options = new FragmentationOptions { ChunkBytes = 2048, InflightBytes = 4096 };
        await using var writer = udpPort.HasValue
            ? await Client.ConnectLiteNetLibAsync(port: udpPort.Value, insecureDevelopment: true, ct: ct, fragmentation: options)
            : await Client.ConnectWebSocketAsync(endpoint, ct, options);
        var room = "fragment-csharp-" + Guid.NewGuid().ToString("N")[..8];
        await writer.JoinAsync(room, "writer", state: true, ct);
        Check(writer.SelectedFragmentation?.ChunkBytes == 2048, "Selected fragmentation limits missing.");
        var writerEvents = Task.Run(async () => { await foreach (var _ in writer.Events(ct)) { } });
        var value = RandomNumberGenerator.GetBytes(100000);
        var state = await writer.SetAsync("large", value, ct: ct);
        await using var reader = await Client.ConnectWebSocketAsync(endpoint, ct, options);
        var messages = Channel.CreateUnbounded<Dictionary<string, object?>>();
        var readerEvents = Task.Run(async () =>
        {
            await foreach (var frame in reader.Events(ct))
                if (frame["type"] is "message") messages.Writer.TryWrite(frame);
        });
        var joined = await reader.JoinAsync(room, "reader", state: true, ct);
        var entries = (object?[])joined["entries"]!;
        Check(entries.Length == 1 && ((byte[])((Dictionary<string, object?>)entries[0]!)["value"]!).SequenceEqual(value),
            "Large initial joined snapshot was incomplete.");
        Check(Client.Number(joined, "revision") == Client.Number(state, "revision"), "Joined revision mismatch.");
        var first = writer.MessageAsync("large", value, ct);
        var second = writer.MessageAsync("after", "next", ct);
        await Task.WhenAll(first, second);
        var firstEvent = await messages.Reader.ReadAsync(ct);
        var secondEvent = await messages.Reader.ReadAsync(ct);
        Check(((byte[])firstEvent["value"]!).SequenceEqual(value) && (string)secondEvent["value"]! == "next",
            "Large message ordering or bytes changed.");
        Check(Client.Number(firstEvent, "seq") < Client.Number(secondEvent, "seq"), "Large message sequence reordered.");
        var catchup = await reader.CatchupAsync(ct);
        Check(((object?[])catchup["messages"]!).Length == 2, "Large retained catchup missing messages.");
        await writer.CheckpointAsync(Client.Number(secondEvent, "seq"), value, ct);
        var checkpointCatchup = await reader.CatchupAsync(ct);
        var checkpoint = (Dictionary<string, object?>)checkpointCatchup["checkpoint"]!;
        Check(((byte[])checkpoint["value"]!).SequenceEqual(value), "Large checkpoint catchup changed bytes.");
        var snapshot = await reader.SnapshotAsync(ct);
        Check(((byte[])((Dictionary<string, object?>)((object?[])snapshot["entries"]!)[0]!)["value"]!).SequenceEqual(value),
            "Large explicit snapshot changed bytes.");
        try { await writer.LatestAsync("too-large", value, 1, ct); throw new Exception("Latest was fragmented."); }
        catch (ArgumentException) { }
        Console.WriteLine($"FRAGMENTATION_INTEGRATION_OK {(udpPort.HasValue ? "litenetlib-websocket" : "websocket")} 100000-bytes initial-joined message-order snapshot checkpoint catchup latest-bound");
        timeout.Cancel();
        try { await Task.WhenAll(writerEvents, readerEvents); } catch (OperationCanceledException) { }
    }
}
