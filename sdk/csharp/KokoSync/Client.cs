using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Threading.Channels;

namespace KokoSync;

public sealed class ProtocolException(string code, string message) : Exception($"{code}: {message}")
{
    public string Code { get; } = code;
}

/// <summary>Prototype session. Consume Events promptly; slow consumers disconnect.</summary>
public sealed class Client : IAsyncDisposable
{
    private readonly ITransport transport;
    private readonly CancellationTokenSource stop = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource<Dictionary<string, object?>>> requests = new();
    private readonly ConcurrentDictionary<string, Channel<Dictionary<string, object?>>> blobs = new();
    private readonly ConcurrentDictionary<string, bool> receivingBlobs = new();
    private readonly Channel<Dictionary<string, object?>> events = Channel.CreateBounded<Dictionary<string, object?>>(16);
    private readonly Task receive;
    private readonly Dictionary<string, ulong> realtimeSequences = new();
    public Dictionary<string, object?>? Joined { get; private set; }
    private Client(ITransport transport) { this.transport = transport; receive = ReceiveAsync(); }

    public static async Task<Client> ConnectLiteNetLibAsync(string host = "127.0.0.1", int port = 8082,
        bool insecureDevelopment = false, CancellationToken ct = default)
    {
        var transport = new LiteTransport(host, port, insecureDevelopment);
        try { await transport.WaitConnectedAsync(ct); return new Client(transport); }
        catch { await transport.DisposeAsync(); throw; }
    }

    public static async Task<Client> ConnectWebSocketAsync(Uri endpoint, CancellationToken ct = default)
    {
        var socket = new ClientWebSocket();
        try { await socket.ConnectAsync(endpoint, ct); return new Client(new SocketTransport(socket)); }
        catch { socket.Dispose(); throw; }
    }

    public IAsyncEnumerable<Dictionary<string, object?>> Events(CancellationToken ct = default) => events.Reader.ReadAllAsync(ct);

    public async Task<Dictionary<string, object?>> JoinAsync(string room, string peer, bool state = false, CancellationToken ct = default)
    {
        Joined = await RequestAsync(Frame("join", ("room", room), ("peer", peer), ("state", state)), ct);
        return Joined;
    }
    public Task<Dictionary<string, object?>> MessageAsync(string topic, object? value, CancellationToken ct = default)
        => ValueRequest("message", topic, value, ct);
    public Task<Dictionary<string, object?>> EventAsync(string topic, object? value, uint? ttlMs = null, CancellationToken ct = default)
    {
        Wire.ValidateValue(value);
        var frame = Frame("event", ("topic", topic), ("value", value));
        if (ttlMs.HasValue) frame["ttlMs"] = ttlMs.Value;
        return RequestAsync(frame, ct);
    }
    public Task<Dictionary<string, object?>> SetAsync(string key, object? value, ulong? expected = null, CancellationToken ct = default)
    {
        Wire.ValidateValue(value);
        var frame = Frame("set", ("key", key), ("value", value));
        if (expected.HasValue) frame["expected"] = expected.Value;
        return RequestAsync(frame, ct);
    }
    public Task<Dictionary<string, object?>> DeleteAsync(string key, ulong? expected = null, CancellationToken ct = default)
    {
        var frame = Frame("delete", ("key", key));
        if (expected.HasValue) frame["expected"] = expected.Value;
        return RequestAsync(frame, ct);
    }
    public Task<Dictionary<string, object?>> SnapshotAsync(CancellationToken ct = default) => RequestAsync(Frame("snapshot"), ct);
    public Task<Dictionary<string, object?>> CatchupAsync(CancellationToken ct = default) => RequestAsync(Frame("catchup"), ct);
    public Task<Dictionary<string, object?>> CheckpointAsync(ulong seq, object? value, CancellationToken ct = default)
    {
        Wire.ValidateValue(value);
        return RequestAsync(Frame("checkpoint", ("seq", seq), ("value", value)), ct);
    }
    public ValueTask LatestAsync(string key, object? value, ulong seq, CancellationToken ct = default)
    {
        Wire.ValidateValue(value);
        return SendAsync(Frame("latest", ("key", key), ("value", value), ("seq", seq)), ct);
    }
    public ValueTask LiveAsync(string stream, byte[] value, ulong seq, uint ttlMs, string? codec = null, CancellationToken ct = default)
    {
        var frame = Frame("live", ("stream", stream), ("value", value), ("seq", seq), ("ttlMs", ttlMs));
        if (codec != null) frame["codec"] = codec;
        return SendAsync(frame, ct);
    }
    private Task<Dictionary<string, object?>> ValueRequest(string type, string topic, object? value, CancellationToken ct)
    {
        Wire.ValidateValue(value);
        return RequestAsync(Frame(type, ("topic", topic), ("value", value)), ct);
    }

    /// <summary>Advanced operations. Values and frame size are checked; callers own protocol semantics.</summary>
    public ValueTask SendAsync(IReadOnlyDictionary<string, object?> frame, CancellationToken ct = default)
    {
        if (frame.TryGetValue("value", out var value)) Wire.ValidateValue(value);
        var bytes = Wire.Encode(frame);
        var realtime = frame.TryGetValue("type", out var type) && type is "latest" or "live";
        if (realtime && bytes.Length > Wire.MaxRealtimeFrame - 128) throw new ArgumentException("Realtime frame exceeds 872 bytes (128 bytes reserved for relay metadata).");
        return transport.SendAsync(bytes, realtime, ct);
    }

    public async Task<Dictionary<string, object?>> RequestAsync(Dictionary<string, object?> frame, CancellationToken ct = default)
    {
        var id = frame.TryGetValue("id", out var existing) ? (string)existing! : Guid.NewGuid().ToString("N");
        frame["id"] = id;
        var waiter = new TaskCompletionSource<Dictionary<string, object?>>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!requests.TryAdd(id, waiter)) throw new ArgumentException("Request id already in flight.");
        try { await SendAsync(frame, ct); return await waiter.Task.WaitAsync(TimeSpan.FromSeconds(15), ct); }
        finally { requests.TryRemove(id, out _); }
    }

    /// <summary>Credit-controlled transfer; source must provide exactly size bytes. Completes after receiver hash/sink confirmation.</summary>
    public async Task SendBlobAsync(string to, Stream source, ulong size, CancellationToken ct = default)
    {
        if (size > 64UL * 1024 * 1024) throw new ArgumentException("Blob exceeds 64 MiB.");
        var transfer = Guid.NewGuid().ToString("N");
        var channel = Channel.CreateBounded<Dictionary<string, object?>>(16);
        blobs[transfer] = channel;
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct, stop.Token);
        deadline.CancelAfter(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        try
        {
            await RequestAsync(Frame("blob_open", ("transfer", transfer), ("to", to), ("size", size)), token);
            using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            ulong offset = 0, credit = 0;
            while (offset < size)
            {
                while (credit == 0)
                {
                    var next = await channel.Reader.ReadAsync(token);
                    if (next["type"] is "blob_cancel") throw new IOException("Remote cancelled blob.");
                    if (next["type"] is "blob_credit") credit += Number(next, "credit");
                }
                var buffer = new byte[(int)Math.Min(12288UL, Math.Min(credit, size - offset))];
                await source.ReadExactlyAsync(buffer, token);
                hash.AppendData(buffer);
                await SendAsync(Frame("blob_chunk", ("transfer", transfer), ("offset", offset), ("value", buffer)), token);
                offset += (ulong)buffer.Length;
                credit -= (ulong)buffer.Length;
            }
            await SendAsync(Frame("blob_end", ("transfer", transfer), ("sha256", Convert.ToHexStringLower(hash.GetHashAndReset()))), token);
            while (true)
            {
                var next = await channel.Reader.ReadAsync(token);
                if (next["type"] is "blob_done") break;
                if (next["type"] is "blob_cancel") throw new IOException("Remote cancelled blob.");
            }
        }
        catch { await TryCancelBlobAsync(transfer); throw; }
        finally { blobs.TryRemove(transfer, out _); }
    }

    /// <summary>Call on a blob_open event. Writes the sink before replenishing credit; leaves sink open.</summary>
    public async Task ReceiveBlobAsync(Dictionary<string, object?> offer, Stream sink, CancellationToken ct = default)
    {
        var transfer = (string)offer["transfer"]!;
        var size = Number(offer, "size");
        if (size > 64UL * 1024 * 1024) throw new ArgumentException("Blob exceeds 64 MiB.");
        if (!blobs.TryGetValue(transfer, out var channel)) throw new IOException("Blob offer is no longer active.");
        if (!receivingBlobs.TryAdd(transfer, true)) throw new ArgumentException("Transfer already active.");
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct, stop.Token);
        deadline.CancelAfter(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        try
        {
            using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            ulong offset = 0;
            if (size != 0)
                await SendAsync(Frame("blob_credit", ("transfer", transfer), ("credit", Math.Min(65536UL, size))), token);
            while (true)
            {
                var next = await channel.Reader.ReadAsync(token);
                if (next["type"] is "blob_cancel") throw new IOException("Remote cancelled blob.");
                if (next["type"] is "blob_chunk")
                {
                    var buffer = (byte[])next["value"]!;
                    if (buffer.Length > 12288 || Number(next, "offset") != offset || offset + (ulong)buffer.Length > size)
                        throw new IOException("Invalid blob chunk offset or size.");
                    await sink.WriteAsync(buffer, token);
                    hash.AppendData(buffer);
                    offset += (ulong)buffer.Length;
                    await SendAsync(Frame("blob_credit", ("transfer", transfer), ("credit", (ulong)buffer.Length)), token);
                }
                else if (next["type"] is "blob_end")
                {
                    var actual = Convert.ToHexStringLower(hash.GetHashAndReset());
                    if (offset != size || actual != (string)next["sha256"]!) throw new IOException("Blob hash or length mismatch.");
                    await sink.FlushAsync(token);
                    await SendAsync(Frame("blob_done", ("transfer", transfer)), token);
                    return;
                }
            }
        }
        catch { await TryCancelBlobAsync(transfer); throw; }
        finally { blobs.TryRemove(transfer, out _); receivingBlobs.TryRemove(transfer, out _); }
    }
    public async ValueTask CancelBlobAsync(string transfer, CancellationToken ct = default)
    {
        try { await SendAsync(Frame("blob_cancel", ("transfer", transfer)), ct); }
        finally
        {
            if (blobs.TryRemove(transfer, out var channel))
                channel.Writer.TryComplete(new IOException("Blob cancelled locally."));
        }
    }
    private async Task TryCancelBlobAsync(string transfer)
    {
        try { using var timeout = new CancellationTokenSource(1000); await CancelBlobAsync(transfer, timeout.Token); } catch { }
    }
    private async Task ReceiveAsync()
    {
        Exception error = new IOException("Session closed.");
        try
        {
            await foreach (var bytes in transport.ReadAllAsync(stop.Token))
            {
                var frame = Wire.Decode(bytes);
                var type = frame["type"] as string;
                // Register before exposing the offer: an empty transfer can end before
                // application code begins accepting it, and cancellation can arrive early.
                if (type == "blob_open" && frame.GetValueOrDefault("transfer") is string offeredTransfer)
                {
                    if (blobs.Count >= 4 || !blobs.TryAdd(offeredTransfer, Channel.CreateBounded<Dictionary<string, object?>>(16)))
                        throw new IOException("Too many or duplicate blob offers.");
                    if (!events.Writer.TryWrite(frame)) throw new IOException("Event consumer queue overflow.");
                    continue;
                }
                if (type == "blob_cancel" && frame.GetValueOrDefault("transfer") is string cancelledTransfer &&
                    blobs.TryRemove(cancelledTransfer, out var cancelledChannel))
                {
                    cancelledChannel.Writer.TryComplete(new IOException("Remote cancelled blob."));
                    continue;
                }
                if (type is "latest" or "live")
                {
                    var streamKey = $"{type}\0{frame.GetValueOrDefault("peer")}\0{frame.GetValueOrDefault("publisherEpoch")}\0{frame.GetValueOrDefault(type == "latest" ? "key" : "stream")}";
                    var sequence = Number(frame, "seq");
                    if (realtimeSequences.TryGetValue(streamKey, out var previous) && sequence <= previous) continue;
                    if (realtimeSequences.Count >= 1024 && !realtimeSequences.ContainsKey(streamKey))
                        throw new IOException("Realtime stream limit exceeded.");
                    realtimeSequences[streamKey] = sequence;
                }
                if (frame.TryGetValue("id", out var id) && id is string requestId &&
                    type is "joined" or "ack" or "error" or "snapshot" or "catchup" or "pong" or "stats" &&
                    requests.TryGetValue(requestId, out var waiter))
                {
                    if (type == "error") waiter.TrySetException(new ProtocolException((string)frame["code"]!, (string)frame["message"]!));
                    else waiter.TrySetResult(frame);
                    continue;
                }
                if (frame.TryGetValue("transfer", out var transfer) && transfer is string key &&
                    blobs.TryGetValue(key, out var channel))
                {
                    if (!channel.Writer.TryWrite(frame)) throw new IOException("Blob consumer queue overflow.");
                }
                else if (!events.Writer.TryWrite(frame)) throw new IOException("Event consumer queue overflow.");
            }
        }
        catch (Exception ex) { error = ex; }
        finally
        {
            events.Writer.TryComplete(error);
            foreach (var waiter in requests.Values) waiter.TrySetException(error);
            foreach (var channel in blobs.Values) channel.Writer.TryComplete(error);
            stop.Cancel();
            await transport.DisposeAsync();
        }
    }
    public static Dictionary<string, object?> Frame(string type, params (string Key, object? Value)[] fields)
    {
        var result = new Dictionary<string, object?> { ["v"] = 1, ["type"] = type };
        foreach (var (key, value) in fields) result[key] = value;
        return result;
    }
    public static ulong Number(IReadOnlyDictionary<string, object?> frame, string key) => Convert.ToUInt64(frame[key]);
    public async ValueTask DisposeAsync()
    {
        stop.Cancel();
        await transport.DisposeAsync();
        await receive;
        stop.Dispose();
    }
}
