using System.Net;
using System.Net.WebSockets;
using System.Threading.Channels;
using LiteNetLib;

namespace KokoSync;

internal interface ITransport : IAsyncDisposable
{
    ValueTask SendAsync(byte[] bytes, bool realtime, CancellationToken ct);
    IAsyncEnumerable<byte[]> ReadAllAsync(CancellationToken ct);
}

internal sealed class SocketTransport(ClientWebSocket socket) : ITransport
{
    private readonly SemaphoreSlim sendLock = new(1);
    public async ValueTask SendAsync(byte[] bytes, bool realtime, CancellationToken ct)
    {
        await sendLock.WaitAsync(ct);
        try { await socket.SendAsync(bytes, WebSocketMessageType.Binary, true, ct); }
        finally { sendLock.Release(); }
    }
    public async IAsyncEnumerable<byte[]> ReadAllAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var bytes = new byte[Wire.MaxFrame];
            var size = 0;
            ValueWebSocketReceiveResult result;
            do
            {
                if (size == bytes.Length) throw new IOException("Oversized WebSocket frame.");
                result = await socket.ReceiveAsync(bytes.AsMemory(size), ct);
                if (result.MessageType == WebSocketMessageType.Close) yield break;
                if (result.MessageType != WebSocketMessageType.Binary) throw new IOException("Expected binary WebSocket.");
                size += result.Count;
            } while (!result.EndOfMessage);
            yield return bytes.AsSpan(0, size).ToArray();
        }
    }
    public ValueTask DisposeAsync() { socket.Abort(); socket.Dispose(); return ValueTask.CompletedTask; }
}

internal sealed class LiteTransport : ITransport
{
    private readonly NetManager manager;
    private readonly CancellationTokenSource stop = new();
    private readonly Channel<byte[]> incoming = Channel.CreateBounded<byte[]>(new BoundedChannelOptions(16) { FullMode = BoundedChannelFullMode.Wait });
    private readonly Channel<(byte[], bool)> outgoing = Channel.CreateBounded<(byte[], bool)>(new BoundedChannelOptions(16) { FullMode = BoundedChannelFullMode.Wait });
    private readonly TaskCompletionSource<NetPeer> connected = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Task poll;
    private NetPeer? peer;
    private int disposed;
    private int pendingBytes;
    private int pendingFrames;

    public LiteTransport(string host, int port, bool insecureDevelopment)
    {
        if (!insecureDevelopment || !IPAddress.TryParse(host, out var ip) || !IPAddress.IsLoopback(ip))
            throw new ArgumentException("LiteNetLib requires insecureDevelopment:true and a numeric loopback address.");
        var listener = new EventBasedNetListener();
        manager = new NetManager(listener) { AutoRecycle = true, DisconnectTimeout = 5000, MaxConnectAttempts = 5 };
        listener.PeerConnectedEvent += p => { peer = p; connected.TrySetResult(p); };
        listener.DeliveryEvent += (_, data) => { pendingBytes -= (int)data; pendingFrames--; };
        listener.PeerDisconnectedEvent += (_, info) =>
        {
            var error = new IOException($"LiteNetLib disconnected: {info.Reason}");
            connected.TrySetException(error);
            incoming.Writer.TryComplete(error);
            stop.Cancel();
        };
        listener.NetworkReceiveEvent += (_, reader, _, method) =>
        {
            if (reader.AvailableBytes > Wire.MaxFrame ||
                (method == DeliveryMethod.Unreliable && reader.AvailableBytes > Wire.MaxRealtimeFrame))
            { stop.Cancel(); return; }
            if (!incoming.Writer.TryWrite(reader.GetRemainingBytes())) stop.Cancel();
        };
        if (!manager.Start(IPAddress.Loopback, IPAddress.IPv6Loopback, 0)) throw new IOException("Could not bind UDP socket.");
        manager.Connect(host, port, "kokosync/1");
        poll = Task.Run(async () =>
        {
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    manager.PollEvents();
                    if (peer != null)
                        while (outgoing.Reader.TryRead(out var frame))
                        {
                            if (frame.Item2) peer.Send(frame.Item1, 0, DeliveryMethod.Unreliable);
                            else
                            {
                                if (pendingBytes + frame.Item1.Length > 1048576 || pendingFrames >= 256)
                                    throw new IOException("Reliable UDP delivery queue overflow.");
                                pendingBytes += frame.Item1.Length;
                                pendingFrames++;
                                peer.SendWithDeliveryEvent(frame.Item1, 0, DeliveryMethod.ReliableOrdered, frame.Item1.Length);
                            }
                        }
                    await Task.Delay(5, stop.Token);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { connected.TrySetException(ex); incoming.Writer.TryComplete(ex); }
            finally { manager.Stop(); incoming.Writer.TryComplete(); outgoing.Writer.TryComplete(); }
        });
    }
    public async Task WaitConnectedAsync(CancellationToken ct) => await connected.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
    public ValueTask SendAsync(byte[] bytes, bool realtime, CancellationToken ct) => outgoing.Writer.WriteAsync((bytes, realtime), ct);
    public IAsyncEnumerable<byte[]> ReadAllAsync(CancellationToken ct) => incoming.Reader.ReadAllAsync(ct);
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0) return;
        stop.Cancel(); await poll; stop.Dispose();
    }
}
