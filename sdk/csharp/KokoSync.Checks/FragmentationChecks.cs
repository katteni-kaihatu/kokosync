using System.Security.Cryptography;
using System.Threading.Channels;
using KokoSync;

internal static class FragmentationChecks
{
    private sealed class Transport : ITransport
    {
        internal readonly Channel<byte[]> Sent = Channel.CreateUnbounded<byte[]>();
        public ValueTask SendAsync(byte[] bytes, bool realtime, CancellationToken ct) => Sent.Writer.WriteAsync(bytes, ct);
        public IAsyncEnumerable<byte[]> ReadAllAsync(CancellationToken ct) => Sent.Reader.ReadAllAsync(ct);
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
    private sealed class ClientTransport : ITransport
    {
        internal readonly Channel<byte[]> Sent = Channel.CreateUnbounded<byte[]>();
        internal readonly Channel<byte[]> Incoming = Channel.CreateUnbounded<byte[]>();
        public ValueTask SendAsync(byte[] bytes, bool realtime, CancellationToken ct) => Sent.Writer.WriteAsync(bytes, ct);
        public IAsyncEnumerable<byte[]> ReadAllAsync(CancellationToken ct) => Incoming.Reader.ReadAllAsync(ct);
        public ValueTask DisposeAsync() { Incoming.Writer.TryComplete(); return ValueTask.CompletedTask; }
    }
    private static void Check(bool value, string reason)
    {
        if (!value) throw new Exception(reason);
    }
    private static async Task Rejected(Func<ValueTask<Dictionary<string, object?>?>> action)
    {
        try { await action(); }
        catch (IOException) { return; }
        throw new Exception("Invalid fragmentation input accepted.");
    }
    internal static async Task RunAsync()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var options = new FragmentationOptions { ChunkBytes = 1024, InflightBytes = 2048 };
        var tx = new Transport();
        var rx = new Transport();
        Exception? failure = null;
        await using var sender = new Fragmentation(tx, options, ex => failure = ex);
        // Receiver is intentionally not negotiated yet: a large joined must work before capabilities arrive.
        await using var receiver = new Fragmentation(rx, options, ex => failure = ex);
        sender.Negotiate(Client.Frame("joined", ("capabilities", new Dictionary<string, object?> { ["fragmentation"] = options.ToWire() })));
        var value = RandomNumberGenerator.GetBytes(90000);
        var first = Client.Frame("message", ("seq", ulong.MaxValue), ("value", value));
        var firstSend = sender.SendAsync(first, Wire.Encode(first, options.LogicalBytes), deadline.Token).AsTask();
        var open = Wire.Decode(await tx.Sent.Reader.ReadAsync(deadline.Token));
        Check(open["type"] is "frag_open", "Large logical envelope was not fragmented.");
        var following = Client.Frame("message", ("seq", 1UL), ("value", "following"));
        var followingSend = sender.SendAsync(following, Wire.Encode(following), deadline.Token).AsTask();
        var state = Client.Frame("set", ("key", "progress"), ("value", true));
        await sender.SendAsync(state, Wire.Encode(state), deadline.Token);
        Check(Wire.Decode(await tx.Sent.Reader.ReadAsync(deadline.Token))["type"] is "set",
            "State could not progress while a bulk message awaited credit.");
        Check(!followingSend.IsCompleted, "Message lane overtook an unfinished large message.");
        var delivered = new List<Dictionary<string, object?>>();
        await receiver.ReceiveAsync(open);
        var reverse = Task.Run(async () =>
        {
            await foreach (var bytes in rx.ReadAllAsync(deadline.Token))
                await sender.ReceiveAsync(Wire.Decode(bytes));
        });
        while (delivered.Count < 2)
        {
            var decoded = await receiver.ReceiveAsync(Wire.Decode(await tx.Sent.Reader.ReadAsync(deadline.Token)));
            if (decoded != null) delivered.Add(decoded);
            await receiver.CompleteReceiveAsync();
        }
        await Task.WhenAll(firstSend, followingSend);
        Check(failure == null, "Fragmentation failed during valid roundtrip.");
        Check(Client.Number(delivered[0], "seq") == ulong.MaxValue &&
            ((byte[])delivered[0]["value"]!).SequenceEqual(value), "Fragmented value or UInt64 changed.");
        Check((string)delivered[1]["value"]! == "following", "Message FIFO changed.");
        deadline.Cancel();
        try { await reverse; } catch (OperationCanceledException) { }

        var invalidTransport = new Transport();
        await using var invalid = new Fragmentation(invalidTransport, options, _ => { });
        var hash = Convert.ToHexStringLower(SHA256.HashData(new byte[] { 1, 2 }));
        await Rejected(() => invalid.ReceiveAsync(Client.Frame("frag_open", ("transfer", "size"), ("size", ulong.MaxValue), ("sha256", hash))));
        await invalid.ReceiveAsync(Client.Frame("frag_open", ("transfer", "offset"), ("size", 2), ("sha256", hash)));
        await Rejected(() => invalid.ReceiveAsync(Client.Frame("frag_open", ("transfer", "offset"), ("size", 2), ("sha256", hash))));
        await Rejected(() => invalid.ReceiveAsync(Client.Frame("frag_chunk", ("transfer", "offset"), ("offset", 1), ("value", new byte[] { 1 }))));
        await invalid.ReceiveAsync(Client.Frame("frag_chunk", ("transfer", "offset"), ("offset", 0), ("value", new byte[] { 2, 1 })));
        await Rejected(() => invalid.ReceiveAsync(Client.Frame("frag_end", ("transfer", "offset"))));
        await Rejected(() => invalid.ReceiveAsync(Client.Frame("frag_done", ("transfer", "missing"))));
        await using var bounded = new Fragmentation(new Transport(),
            options with { Concurrent = 1, ReassemblyBytes = 2, InflightBytes = 1 }, _ => { });
        await bounded.ReceiveAsync(Client.Frame("frag_open", ("transfer", "one"), ("size", 2), ("sha256", hash)));
        await Rejected(() => bounded.ReceiveAsync(Client.Frame("frag_open", ("transfer", "two"), ("size", 2), ("sha256", hash))));
        await Rejected(() => bounded.ReceiveAsync(Client.Frame("frag_chunk", ("transfer", "one"), ("offset", 0), ("value", new byte[] { 1, 2 }))));

        foreach (var kind in new[] { "latest", "live", "frag_open" })
        {
            var nested = Wire.Encode(Client.Frame(kind));
            await invalid.ReceiveAsync(Client.Frame("frag_open", ("transfer", kind), ("size", nested.Length),
                ("sha256", Convert.ToHexStringLower(SHA256.HashData(nested)))));
            await invalid.ReceiveAsync(Client.Frame("frag_chunk", ("transfer", kind), ("offset", 0), ("value", nested)));
            await Rejected(() => invalid.ReceiveAsync(Client.Frame("frag_end", ("transfer", kind))));
        }

        var timedOut = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var expiring = new Fragmentation(new Transport(), options with { TimeoutMs = 20 }, _ => timedOut.TrySetResult());
        await expiring.ReceiveAsync(Client.Frame("frag_open", ("transfer", "expiry"), ("size", 2), ("sha256", hash)));
        await timedOut.Task.WaitAsync(TimeSpan.FromSeconds(2));

        var limitedOptions = options with { QueueBytes = 100000 };
        var limitedTransport = new Transport();
        await using var limited = new Fragmentation(limitedTransport, limitedOptions, _ => { });
        limited.Negotiate(Client.Frame("joined", ("capabilities", new Dictionary<string, object?> { ["fragmentation"] = limitedOptions.ToWire() })));
        using var cancel = new CancellationTokenSource();
        var active = limited.SendAsync(first, Wire.Encode(first, options.LogicalBytes), cancel.Token).AsTask();
        var activeOpen = Wire.Decode(await limitedTransport.Sent.Reader.ReadAsync());
        await Rejected(() => limited.ReceiveAsync(Client.Frame("frag_credit", ("transfer", activeOpen["transfer"]), ("credit", options.InflightBytes + 1))));
        try
        {
            await limited.SendAsync(first, Wire.Encode(first, options.LogicalBytes), CancellationToken.None);
            throw new Exception("Unbounded send queue accepted.");
        }
        catch (IOException) { }
        cancel.Cancel();
        try { await active; } catch (OperationCanceledException) { }
        await CheckEventBudgetAsync();
        Console.WriteLine("FRAGMENTATION_CHECKS_OK 90000-bytes uint64 message-fifo state-progress bounds hash offset duplicate timeout cancellation event-byte-budget");
    }
    private static async Task CheckEventBudgetAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var options = new FragmentationOptions { QueueBytes = 1500 };
        var transport = new ClientTransport();
        await using var client = new Client(transport, options);
        var join = client.JoinAsync("budget", "client", ct: timeout.Token);
        var request = Wire.Decode(await transport.Sent.Reader.ReadAsync(timeout.Token));
        await transport.Incoming.Writer.WriteAsync(Wire.Encode(Client.Frame("joined", ("id", request["id"]),
            ("capabilities", new Dictionary<string, object?> { ["fragmentation"] = options.ToWire() }))));
        await join;
        var value = Wire.Encode(Client.Frame("event", ("value", new byte[1000])));
        await using (var events = client.Events(timeout.Token).GetAsyncEnumerator())
        {
            await transport.Incoming.Writer.WriteAsync(value);
            Check(await events.MoveNextAsync(), "First event missing.");
            await transport.Incoming.Writer.WriteAsync(value);
            Check(await events.MoveNextAsync(), "Consumed event did not release byte budget.");
        }
        var barrier = client.SnapshotAsync(timeout.Token);
        var barrierRequest = Wire.Decode(await transport.Sent.Reader.ReadAsync(timeout.Token));
        await transport.Incoming.Writer.WriteAsync(value);
        await transport.Incoming.Writer.WriteAsync(value);
        await transport.Incoming.Writer.WriteAsync(Wire.Encode(Client.Frame("snapshot", ("id", barrierRequest["id"]))));
        try { await barrier; throw new Exception("Event byte budget did not disconnect a slow consumer."); }
        catch (IOException error) when (error.Message.Contains("Event consumer queue overflow")) { }
        await using var closed = client.Events(timeout.Token).GetAsyncEnumerator();
        try { await closed.MoveNextAsync(); throw new Exception("Overflow retained queued event references."); }
        catch (IOException error) when (error.Message.Contains("Event consumer queue overflow")) { }
    }
}
