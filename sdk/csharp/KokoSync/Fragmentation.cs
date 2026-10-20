using System.Security.Cryptography;
using System.Threading.Channels;

namespace KokoSync;

/// <summary>Positive, bounded reliable-envelope limits advertised at join. Set Enabled=false to disable negotiation.</summary>
public sealed record FragmentationOptions
{
    public bool Enabled { get; init; } = true;
    public int LogicalBytes { get; init; } = 4194304;
    public int ChunkBytes { get; init; } = 12288;
    public int ReassemblyBytes { get; init; } = 8388608;
    public int InflightBytes { get; init; } = 65536;
    public int Concurrent { get; init; } = 4;
    public int TimeoutMs { get; init; } = 30000;
    public int QueueBytes { get; init; } = 8388608;

    internal Dictionary<string, object?> ToWire() => new()
    {
        ["logicalBytes"] = LogicalBytes, ["chunkBytes"] = ChunkBytes,
        ["reassemblyBytes"] = ReassemblyBytes, ["inflightBytes"] = InflightBytes,
        ["concurrent"] = Concurrent, ["timeoutMs"] = TimeoutMs, ["queueBytes"] = QueueBytes
    };
    internal void Validate()
    {
        if (ToWire().Values.Any(value => (int)value! <= 0) || ChunkBytes > 16384)
            throw new ArgumentException("Fragmentation limits must be positive; chunkBytes must not exceed 16384.");
    }
    internal static FragmentationOptions Select(Dictionary<string, object?> map, FragmentationOptions local)
    {
        int Limit(string name, int ceiling)
        {
            if (!map.TryGetValue(name, out var value) || !Fragmentation.TryUnsigned(value, out var number) ||
                number == 0 || number > (ulong)ceiling)
                throw new IOException($"Invalid negotiated fragmentation {name}.");
            return (int)number;
        }
        return new FragmentationOptions
        {
            LogicalBytes = Limit("logicalBytes", local.LogicalBytes),
            ChunkBytes = Limit("chunkBytes", local.ChunkBytes),
            ReassemblyBytes = Limit("reassemblyBytes", local.ReassemblyBytes),
            InflightBytes = Limit("inflightBytes", local.InflightBytes),
            Concurrent = Limit("concurrent", local.Concurrent),
            TimeoutMs = Limit("timeoutMs", local.TimeoutMs),
            QueueBytes = Limit("queueBytes", local.QueueBytes)
        };
    }
}

/// <summary>Owns bounded encoded envelopes. Transport writes are serialized at physical-frame granularity.</summary>
internal sealed class Fragmentation : IAsyncDisposable
{
    private sealed record Pending(byte[] Bytes, bool Fragmentable, CancellationToken Cancellation,
        TaskCompletionSource Completion);
    private sealed class Outbound(int size)
    {
        internal readonly int Size = size;
        internal int Sent;
        internal int Credit;
        internal bool Ended;
        internal readonly SemaphoreSlim Changed = new(0);
        internal readonly TaskCompletionSource Done = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    private sealed class Inbound(int size, string hash, CancellationTokenSource expiry)
    {
        internal readonly byte[] Bytes = new byte[size];
        internal readonly string Hash = hash;
        internal readonly CancellationTokenSource Expiry = expiry;
        internal int Offset;
        internal int Credit;
    }
    private readonly object gate = new();
    private readonly ITransport transport;
    private readonly Action<Exception> onFailure;
    private readonly CancellationTokenSource stop = new();
    private readonly Dictionary<string, Outbound> outgoing = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Inbound> incoming = new(StringComparer.Ordinal);
    private readonly Channel<Pending>[] lanes = Enumerable.Range(0, 6)
        .Select(_ => Channel.CreateUnbounded<Pending>(new UnboundedChannelOptions { SingleReader = true })).ToArray();
    private readonly Task[] workers;
    private volatile FragmentationOptions? selected;
    private readonly FragmentationOptions? advertised;
    private int queuedBytes;
    private int queuedEnvelopes;
    private int reservedBytes;
    private string? pendingCompletion;
    private Exception? failure;

    internal Fragmentation(ITransport transport, FragmentationOptions? options, Action<Exception> onFailure)
    {
        options?.Validate();
        this.transport = transport;
        advertised = options;
        this.onFailure = onFailure;
        workers = lanes.Select(lane => RunLaneAsync(lane.Reader)).ToArray();
    }
    internal int LogicalBytes => selected?.LogicalBytes ?? Wire.MaxFrame;
    internal int ValueBytes => selected?.LogicalBytes ?? 16384;
    internal FragmentationOptions? Selected => selected;
    internal void Negotiate(Dictionary<string, object?> frame)
    {
        if (frame.GetValueOrDefault("capabilities") is Dictionary<string, object?> capabilities &&
            capabilities.TryGetValue("fragmentation", out var capability))
        {
            if (advertised == null) throw new IOException("Unrequested fragmentation capability.");
            if (capability is not Dictionary<string, object?> map) throw new IOException("Invalid fragmentation capability.");
            selected = FragmentationOptions.Select(map, advertised);
        }
    }
    private static int Lane(IReadOnlyDictionary<string, object?> frame) => frame.GetValueOrDefault("type") switch
    {
        "message" or "catchup" or "checkpoint" => 1,
        "set" or "delete" or "state" or "snapshot" => 2,
        "event" => 3,
        "peer_chunk" when frame.GetValueOrDefault("bulk") is true => 5,
        "peer_chunk" => 4,
        "ack" when frame.ContainsKey("seq") => 1,
        "ack" when frame.ContainsKey("revision") => 2,
        _ => 0
    };
    internal ValueTask SendAsync(IReadOnlyDictionary<string, object?> frame, byte[] bytes, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var type = frame.GetValueOrDefault("type") as string;
        var pending = new Pending(bytes, type != "join" && !PeerDelivery.IsFrame(type) && !(type?.StartsWith("blob_", StringComparison.Ordinal) ?? false), ct,
            new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
        lock (gate)
        {
            if (failure != null) throw new IOException("Session closed.", failure);
            var queueLimit = selected?.QueueBytes ?? advertised?.QueueBytes ?? 8388608;
            if (bytes.Length > queueLimit - queuedBytes || queuedEnvelopes >= 256)
                throw new IOException("Reliable envelope send queue is full.");
            queuedBytes += bytes.Length;
            queuedEnvelopes++;
            if (!lanes[Lane(frame)].Writer.TryWrite(pending))
            {
                queuedBytes -= bytes.Length;
                queuedEnvelopes--;
                throw new IOException("Session closed.");
            }
        }
        return new ValueTask(pending.Completion.Task);
    }
    private async Task RunLaneAsync(ChannelReader<Pending> lane)
    {
        await foreach (var pending in lane.ReadAllAsync())
        {
            var started = false;
            try
            {
                pending.Cancellation.ThrowIfCancellationRequested();
                stop.Token.ThrowIfCancellationRequested();
                using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(stop.Token, pending.Cancellation);
                var options = selected;
                started = true;
                if (options != null && pending.Fragmentable && pending.Bytes.Length > options.ChunkBytes)
                    await SendFragmentedAsync(pending.Bytes, options, cancellation.Token);
                else
                    await transport.SendAsync(pending.Bytes, false, cancellation.Token);
                pending.Completion.TrySetResult();
            }
            catch (OperationCanceledException) when (pending.Cancellation.IsCancellationRequested && !stop.IsCancellationRequested)
            {
                pending.Completion.TrySetCanceled(pending.Cancellation);
                if (started) Fail(new IOException("An active reliable send was cancelled."));
            }
            catch (Exception ex) { pending.Completion.TrySetException(ex); Fail(ex); }
            finally
            {
                lock (gate) { queuedBytes -= pending.Bytes.Length; queuedEnvelopes--; }
            }
        }
    }
    private async Task SendFragmentedAsync(byte[] bytes, FragmentationOptions options, CancellationToken ct)
    {
        var id = Guid.NewGuid().ToString("N");
        var state = new Outbound(bytes.Length);
        var opened = false;
        using var expiry = CancellationTokenSource.CreateLinkedTokenSource(ct);
        try
        {
            // At most four semantic lanes can be active; lower negotiated concurrency waits without blocking control replies.
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                lock (gate)
                {
                    if (outgoing.Count < options.Concurrent) { outgoing.Add(id, state); break; }
                }
                await Task.Delay(5, ct);
            }
            expiry.CancelAfter(options.TimeoutMs);
            opened = true;
            await ControlAsync("frag_open", expiry.Token, ("transfer", id), ("size", bytes.Length),
                ("sha256", Convert.ToHexStringLower(SHA256.HashData(bytes))));
            while (state.Sent < bytes.Length)
            {
                int offset, count;
                lock (gate)
                {
                    offset = state.Sent;
                    count = Math.Min(options.ChunkBytes, Math.Min(state.Credit, bytes.Length - offset));
                    state.Credit -= count;
                    state.Sent += count;
                }
                if (count == 0) { await state.Changed.WaitAsync(expiry.Token); continue; }
                await ControlAsync("frag_chunk", expiry.Token, ("transfer", id), ("offset", offset),
                    ("value", bytes.AsSpan(offset, count).ToArray()));
                await Task.Yield();
            }
            lock (gate) state.Ended = true;
            await ControlAsync("frag_end", expiry.Token, ("transfer", id));
            await state.Done.Task.WaitAsync(expiry.Token);
        }
        catch (Exception ex)
        {
            if (opened)
            {
                try
                {
                    using var cancel = new CancellationTokenSource(250);
                    await ControlAsync("frag_cancel", cancel.Token, ("transfer", id), ("reason", "Transfer failed."));
                }
                catch { }
                Fail(ex);
            }
            throw;
        }
        finally { lock (gate) outgoing.Remove(id); }
    }
    private ValueTask ControlAsync(string type, CancellationToken ct, params (string Key, object? Value)[] fields)
        => transport.SendAsync(Wire.Encode(Client.Frame(type, fields)), false, ct);

    internal async ValueTask<Dictionary<string, object?>?> ReceiveAsync(Dictionary<string, object?> frame)
    {
        var type = frame.GetValueOrDefault("type") as string;
        if (type == null || !type.StartsWith("frag_", StringComparison.Ordinal)) return frame;
        var options = selected ?? advertised ?? throw new IOException("Fragmentation was not enabled.");
        if (!TryUnsigned(frame.GetValueOrDefault("v"), out var version) || version != 1 ||
            frame.GetValueOrDefault("transfer") is not string id || id.Length is 0 or > 128)
            throw new IOException("Invalid fragment metadata.");
        switch (type)
        {
            case "frag_open":
            {
                var size = Unsigned(frame, "size");
                if (size == 0 || size > (ulong)options.LogicalBytes ||
                    frame.GetValueOrDefault("sha256") is not string hash || hash.Length != 64 ||
                    hash.Any(c => c is not (>= '0' and <= '9') and not (>= 'a' and <= 'f')))
                    throw new IOException("Invalid fragment size or hash.");
                Inbound state;
                lock (gate)
                {
                    if (incoming.ContainsKey(id) || incoming.Count >= options.Concurrent || size > (ulong)(options.ReassemblyBytes - reservedBytes))
                        throw new IOException("Fragment reservation limit or duplicate transfer.");
                    state = new Inbound((int)size, hash, CancellationTokenSource.CreateLinkedTokenSource(stop.Token));
                    incoming.Add(id, state);
                    reservedBytes += (int)size;
                    state.Credit = Math.Min(options.InflightBytes, (int)size);
                }
                _ = ExpireAsync(id, state, options.TimeoutMs);
                await ControlAsync("frag_credit", stop.Token, ("transfer", id), ("credit", state.Credit));
                break;
            }
            case "frag_chunk":
            {
                int credit;
                lock (gate)
                {
                    if (!incoming.TryGetValue(id, out var state) ||
                        frame.GetValueOrDefault("value") is not byte[] value || value.Length == 0 ||
                        value.Length > options.ChunkBytes || value.Length > state.Credit ||
                        Unsigned(frame, "offset") != (ulong)state.Offset || value.Length > state.Bytes.Length - state.Offset)
                        throw new IOException("Invalid fragment chunk or offset.");
                    value.CopyTo(state.Bytes, state.Offset);
                    state.Offset += value.Length;
                    state.Credit -= value.Length;
                    credit = value.Length;
                    state.Credit += credit;
                }
                if (credit > 0) await ControlAsync("frag_credit", stop.Token, ("transfer", id), ("credit", credit));
                break;
            }
            case "frag_end":
            {
                Inbound state;
                lock (gate)
                {
                    if (!incoming.TryGetValue(id, out state!) || state.Offset != state.Bytes.Length)
                        throw new IOException("Incomplete fragment transfer.");
                    incoming.Remove(id);
                    reservedBytes -= state.Bytes.Length;
                    state.Expiry.Cancel();
                }
                state.Expiry.Dispose();
                if (Convert.ToHexStringLower(SHA256.HashData(state.Bytes)) != state.Hash)
                    throw new IOException("Fragment hash mismatch.");
                var decoded = Wire.Decode(state.Bytes, options.LogicalBytes);
                if (decoded.GetValueOrDefault("type") is not string logicalType ||
                    logicalType.StartsWith("frag_", StringComparison.Ordinal) || PeerDelivery.IsFrame(logicalType) || logicalType is "latest" or "live" ||
                    !TryUnsigned(decoded.GetValueOrDefault("v"), out var logicalVersion) || logicalVersion != 1)
                    throw new IOException("Invalid fragmented logical envelope.");
                pendingCompletion = id;
                return decoded;
            }
            case "frag_credit":
            {
                var credit = Unsigned(frame, "credit");
                lock (gate)
                {
                    if (!outgoing.TryGetValue(id, out var state)) break; // A completed transfer's credit may arrive late.
                    if (credit == 0 || credit > (ulong)(options.InflightBytes - state.Credit))
                        throw new IOException("Invalid fragment credit.");
                    state.Credit += (int)credit;
                    state.Changed.Release();
                }
                break;
            }
            case "frag_done":
                lock (gate)
                {
                    if (!outgoing.TryGetValue(id, out var state) || !state.Ended)
                        throw new IOException("Unexpected fragment completion.");
                    state.Done.TrySetResult();
                }
                break;
            case "frag_cancel": throw new IOException("Remote cancelled a reliable envelope.");
            default: throw new IOException("Unknown fragmentation operation.");
        }
        return null;
    }
    internal async ValueTask CompleteReceiveAsync()
    {
        if (pendingCompletion is not { } id) return;
        pendingCompletion = null;
        await ControlAsync("frag_done", stop.Token, ("transfer", id));
    }
    private async Task ExpireAsync(string id, Inbound state, int timeoutMs)
    {
        try
        {
            await Task.Delay(timeoutMs, state.Expiry.Token);
            lock (gate)
            {
                if (!incoming.TryGetValue(id, out var current) || current != state) return;
            }
            try
            {
                using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(stop.Token);
                cancellation.CancelAfter(250);
                await ControlAsync("frag_cancel", cancellation.Token, ("transfer", id), ("reason", "Transfer expired."));
            }
            finally { Fail(new IOException("Reliable envelope reassembly timed out.")); }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) { Fail(ex); }
    }
    internal void Fail(Exception error)
    {
        lock (gate)
        {
            if (failure != null) return;
            failure = error;
            foreach (var lane in lanes) lane.Writer.TryComplete();
            foreach (var state in incoming.Values) { state.Expiry.Cancel(); state.Expiry.Dispose(); }
            incoming.Clear();
            reservedBytes = 0;
        }
        stop.Cancel();
        onFailure(error);
    }
    internal static bool TryUnsigned(object? value, out ulong result)
    {
        switch (value)
        {
            case byte n: result = n; return true;
            case ushort n: result = n; return true;
            case uint n: result = n; return true;
            case ulong n: result = n; return true;
            case sbyte n when n >= 0: result = (ulong)n; return true;
            case short n when n >= 0: result = (ulong)n; return true;
            case int n when n >= 0: result = (ulong)n; return true;
            case long n when n >= 0: result = (ulong)n; return true;
            default: result = 0; return false;
        }
    }
    private static ulong Unsigned(Dictionary<string, object?> frame, string key)
        => TryUnsigned(frame.GetValueOrDefault(key), out var result) ? result : throw new IOException($"Invalid fragment {key}.");
    public async ValueTask DisposeAsync()
    {
        Fail(new IOException("Session closed."));
        await Task.WhenAll(workers);
        stop.Dispose();
    }
}
