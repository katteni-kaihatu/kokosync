namespace KokoSync;

public sealed record PeerDeliveryOptions
{
    public bool Enabled { get; init; } = true;
    public int MessageBytes { get; init; } = 4194304;
    public int ChunkBytes { get; init; } = 12288;
    public int WindowBytes { get; init; } = 65536;
    public int QueueBytes { get; init; } = 8388608;
    public int ReceiveBytes { get; init; } = 8388608;
    public int Concurrent { get; init; } = 4;
    public int Queued { get; init; } = 128;
    public int Lanes { get; init; } = 64;
    public int TimeoutMs { get; init; } = 30000;
    public int AssemblyMs { get; init; } = 30000;
    public int Retries { get; init; } = 5;
    internal Dictionary<string, object?> ToWire() => new()
    {
        ["version"] = 1, ["messageBytes"] = MessageBytes, ["chunkBytes"] = ChunkBytes,
        ["windowBytes"] = WindowBytes, ["queueBytes"] = QueueBytes, ["receiveBytes"] = ReceiveBytes,
        ["concurrent"] = Concurrent, ["queued"] = Queued, ["lanes"] = Lanes,
        ["timeoutMs"] = TimeoutMs, ["assemblyMs"] = AssemblyMs, ["retries"] = Retries
    };
    internal void Validate()
    {
        try { Parse(ToWire()); }
        catch (ArgumentException ex) { throw new DeliveryException("InvalidArgument", "not-sent", ex.Message); }
    }
    internal static PeerDeliveryOptions Parse(Dictionary<string, object?> map)
    {
        int Read(string key, int ceiling, int minimum = 1)
        {
            if (!Fragmentation.TryUnsigned(map.GetValueOrDefault(key), out var n) || n < (ulong)minimum || n > (ulong)ceiling)
                throw new ArgumentException($"Invalid peer delivery limit: {key}.");
            return (int)n;
        }
        Read("version", 1);
        return new PeerDeliveryOptions
        {
            MessageBytes = Read("messageBytes", 64 * 1024 * 1024), ChunkBytes = Read("chunkBytes", 16384),
            WindowBytes = Read("windowBytes", 256 * 1024), QueueBytes = Read("queueBytes", 256 * 1024 * 1024),
            ReceiveBytes = Read("receiveBytes", 256 * 1024 * 1024), Concurrent = Read("concurrent", 32),
            Queued = Read("queued", 4096), Lanes = Read("lanes", 1024), TimeoutMs = Read("timeoutMs", 300000),
            AssemblyMs = Read("assemblyMs", 300000), Retries = Read("retries", 20, 0)
        };
    }
    internal PeerDeliveryOptions Min(PeerDeliveryOptions other) => this with
    {
        MessageBytes = Math.Min(MessageBytes, other.MessageBytes), ChunkBytes = Math.Min(ChunkBytes, other.ChunkBytes),
        WindowBytes = Math.Min(WindowBytes, other.WindowBytes), TimeoutMs = Math.Min(TimeoutMs, other.TimeoutMs),
        Retries = Math.Min(Retries, other.Retries)
    };
}

public sealed record DeliveryReceipt(string Peer, string PublisherEpoch, string Topic, string MessageId,
    ulong Seq, string Route, double? RttMs);

public sealed class DeliveryException(string code, string outcome, string message) : IOException($"{code}: {message}")
{
    public string Code { get; } = code;
    public string Outcome { get; } = outcome;
}

public sealed record DeliveryStat(string Peer, string PublisherEpoch, string Route, string Kind,
    ulong SentFrames, ulong ReceivedFrames, ulong? SentBytes, ulong? ReceivedBytes,
    ulong SentMessages, ulong ReceivedMessages, ulong Retries, int UnackedMessages,
    int QueuedMessages, long QueuedBytes, string Reason, double? RttMs);
