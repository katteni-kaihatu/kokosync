using System.Security.Cryptography;
using System.Threading.Channels;
using KokoSync;

internal static class PeerDeliveryIntegration
{
    private static void Check(bool value, string reason) { if (!value) throw new Exception(reason); }
    internal static async Task RunAsync(Uri endpoint, int? udpPort)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var ct = timeout.Token;
        await using var sender = udpPort.HasValue
            ? await Client.ConnectLiteNetLibAsync(port: udpPort.Value, insecureDevelopment: true, ct: ct)
            : await Client.ConnectWebSocketAsync(endpoint, ct);
        await using var receiver = await Client.ConnectWebSocketAsync(endpoint, ct);
        var room = "delivery-csharp-" + Guid.NewGuid().ToString("N")[..8];
        await sender.JoinAsync(room, "sender", ct: ct);
        var senderReady = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var received = Channel.CreateUnbounded<Dictionary<string, object?>>();
        var echoed = Channel.CreateUnbounded<Dictionary<string, object?>>();
        var senderEvents = Task.Run(async () =>
        {
            await foreach (var frame in sender.Events(ct))
            {
                if (frame["type"] is "route" && frame["peer"] is "receiver" && frame["removed"] is false) senderReady.TrySetResult();
                if (frame["type"] is "peer_message") echoed.Writer.TryWrite(frame);
            }
        });
        var receiverEvents = Task.Run(async () =>
        {
            await foreach (var frame in receiver.Events(ct))
                if (frame["type"] is "peer_message") received.Writer.TryWrite(frame);
        });
        await receiver.JoinAsync(room, "receiver", ct: ct);
        await senderReady.Task.WaitAsync(ct);
        var before = await sender.CatchupAsync(ct);
        var value = RandomNumberGenerator.GetBytes(150000);
        var large = sender.SendToAsync("receiver", "data", value, ct);
        var following = sender.SendToAsync("receiver", "data", "following", ct);
        var small = sender.SendToAsync("receiver", "independent", new Dictionary<string, object?> { ["ok"] = true }, ct);
        var receipts = await Task.WhenAll(large, following, small);
        Check(receipts[0].Seq == 1 && receipts[1].Seq == 2 && receipts[2].Seq == 1 &&
            receipts.All(receipt => receipt.Route == "relay" &&
                (receipt.RttMs is null || double.IsFinite(receipt.RttMs.Value) && receipt.RttMs.Value >= 0)),
            "Receipt lane/route mismatch or invalid RTT.");
        var deliveries = new List<Dictionary<string, object?>>();
        for (var i = 0; i < 3; i++) deliveries.Add(await received.Reader.ReadAsync(ct));
        var sameTopic = deliveries.Where(frame => frame["topic"] is "data").ToArray();
        Check(sameTopic.Length == 2 && Client.Number(sameTopic[0], "seq") == 1 && Client.Number(sameTopic[1], "seq") == 2 &&
            ((byte[])sameTopic[0]["value"]!).SequenceEqual(value), "Large addressed delivery order or bytes changed.");
        await receiver.SendToAsync("sender", "reverse", value, ct);
        Check(((byte[])(await echoed.Reader.ReadAsync(ct))["value"]!).SequenceEqual(value), "Reverse addressed delivery failed.");
        Check(sender.DeliveryStats.Any(stat => stat.Kind == "peer_bulk" && stat.SentMessages == 1 && stat.UnackedMessages == 0),
            "Delivery diagnostics missing or reservation retained.");
        var after = await sender.CatchupAsync(ct);
        Check(Client.Number(after, "seq") == Client.Number(before, "seq") &&
            ((object?[])after["messages"]!).Length == ((object?[])before["messages"]!).Length,
            "Addressed delivery changed room sequence or retained history.");
        timeout.Cancel();
        try { await Task.WhenAll(senderEvents, receiverEvents); } catch (OperationCanceledException) { }
        Console.WriteLine("PEER_DELIVERY_INTEGRATION_OK relay 150000-bytes bidirectional stopwait-order independent-topic receipts diagnostics");
    }
}
