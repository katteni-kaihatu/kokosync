using System.Collections.Concurrent;
using System.Net;
using System.Net.WebSockets;
using System.Threading.Channels;
using KokoSync;
using LiteNetLib;

if (!args.Contains("--insecure-development"))
{
    Console.Error.WriteLine("Localhost prototype only. Pass --insecure-development explicitly.");
    return 2;
}
string Option(string name, string fallback)
{
    var index = Array.IndexOf(args, name);
    return index >= 0 && index + 1 < args.Length ? args[index + 1] : fallback;
}
var port = int.Parse(Option("--port", "8082"));
var bridge = new Uri(Option("--bridge", "ws://127.0.0.1:8080/bridge"));
if (bridge.Scheme != "ws" || !IPAddress.TryParse(bridge.Host, out var address) || !IPAddress.IsLoopback(address))
    throw new ArgumentException("Bridge must be ws:// with a numeric loopback address.");
var secret = Environment.GetEnvironmentVariable("KOKOSYNC_BRIDGE_SECRET") ?? "local-dev-only";
using var stop = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; stop.Cancel(); };
var sessions = new ConcurrentDictionary<int, BridgeSession>();
var listener = new EventBasedNetListener();
var manager = new NetManager(listener) { AutoRecycle = true, DisconnectTimeout = 5000 };
listener.ConnectionRequestEvent += request =>
{
    if (IPAddress.IsLoopback(request.RemoteEndPoint.Address) && sessions.Count < 32) request.AcceptIfKey("kokosync/1");
    else request.Reject();
};
listener.PeerConnectedEvent += peer =>
{
    var session = new BridgeSession(peer, bridge, secret, stop.Token);
    if (!sessions.TryAdd(peer.Id, session)) session.Stop();
};
listener.PeerDisconnectedEvent += (peer, _) => { if (sessions.TryRemove(peer.Id, out var session)) session.Stop(); };
listener.DeliveryEvent += (peer, data) =>
{
    if (sessions.TryGetValue(peer.Id, out var session)) session.Delivered((int)data);
};
listener.NetworkReceiveEvent += (peer, reader, _, method) =>
{
    if (!sessions.TryGetValue(peer.Id, out var session)) return;
    if (reader.AvailableBytes > Wire.MaxFrame ||
        (method == DeliveryMethod.Unreliable && reader.AvailableBytes > Wire.MaxRealtimeFrame) ||
        !session.ToCore.Writer.TryWrite(reader.GetRemainingBytes())) session.Stop();
};
if (!manager.Start(IPAddress.Loopback, IPAddress.IPv6Loopback, port)) throw new IOException("Cannot bind LiteNetLib UDP port.");
Console.WriteLine($"LITENETLIB_READY udp=127.0.0.1:{port} bridge={bridge} package=2.1.4");
try
{
    while (!stop.IsCancellationRequested)
    {
        manager.PollEvents();
        foreach (var session in sessions.Values)
        {
            if (session.IsStopped) { session.Peer.Disconnect(); continue; }
            while (session.ToPeer.Reader.TryRead(out var bytes))
            {
                var frame = Wire.Decode(bytes);
                var realtime = frame.GetValueOrDefault("type") is "latest" or "live";
                if (realtime && bytes.Length > Wire.MaxRealtimeFrame) continue;
                if (realtime) session.Peer.Send(bytes, 0, DeliveryMethod.Unreliable);
                else if (session.TrackDelivery(bytes.Length))
                    session.Peer.SendWithDeliveryEvent(bytes, 0, DeliveryMethod.ReliableOrdered, bytes.Length);
                else { session.Stop(); break; }
            }
        }
        await Task.Delay(5, stop.Token);
    }
}
catch (OperationCanceledException) { }
finally
{
    foreach (var session in sessions.Values) session.Stop();
    manager.Stop();
}
return 0;

sealed class BridgeSession
{
    public NetPeer Peer { get; }
    // 16 * maximum frame = 1 MiB per direction, including frames waiting for WS setup.
    public Channel<byte[]> ToCore { get; } = Channel.CreateBounded<byte[]>(16);
    public Channel<byte[]> ToPeer { get; } = Channel.CreateBounded<byte[]>(16);
    private readonly CancellationTokenSource stop;
    private readonly ClientWebSocket socket = new();
    private int pendingBytes;
    private int pendingFrames;
    public bool IsStopped => stop.IsCancellationRequested;
    public bool TrackDelivery(int size)
    {
        if (pendingBytes + size > 1048576 || pendingFrames >= 256) return false;
        pendingBytes += size;
        pendingFrames++;
        return true;
    }
    public void Delivered(int size) { pendingBytes -= size; pendingFrames--; }
    public BridgeSession(NetPeer peer, Uri bridge, string secret, CancellationToken ct)
    {
        Peer = peer;
        stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        socket.Options.SetRequestHeader("X-Kokosync-Bridge", secret);
        _ = RunAsync(bridge);
    }
    public void Stop() { stop.Cancel(); socket.Abort(); ToCore.Writer.TryComplete(); ToPeer.Writer.TryComplete(); }
    private async Task RunAsync(Uri bridge)
    {
        try
        {
            using var connectTimeout = CancellationTokenSource.CreateLinkedTokenSource(stop.Token);
            connectTimeout.CancelAfter(TimeSpan.FromSeconds(5));
            await socket.ConnectAsync(bridge, connectTimeout.Token);
            var send = SendAsync();
            var receive = ReceiveAsync();
            await Task.WhenAny(send, receive);
            Stop();
            await Task.WhenAll(send, receive);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            if (!stop.IsCancellationRequested) Console.Error.WriteLine($"Bridge peer {Peer.Id}: {ex.Message}");
        }
        finally { Stop(); socket.Dispose(); }
    }
    private async Task SendAsync()
    {
        await foreach (var bytes in ToCore.Reader.ReadAllAsync(stop.Token))
            await socket.SendAsync(bytes, WebSocketMessageType.Binary, true, stop.Token);
    }
    private async Task ReceiveAsync()
    {
        while (!stop.IsCancellationRequested)
        {
            var buffer = new byte[Wire.MaxFrame];
            var offset = 0;
            ValueWebSocketReceiveResult result;
            do
            {
                if (offset == buffer.Length) throw new IOException("Oversized bridge frame.");
                result = await socket.ReceiveAsync(buffer.AsMemory(offset), stop.Token);
                if (result.MessageType == WebSocketMessageType.Close) return;
                if (result.MessageType != WebSocketMessageType.Binary) throw new IOException("Expected binary bridge frame.");
                offset += result.Count;
            } while (!result.EndOfMessage);
            if (!ToPeer.Writer.TryWrite(buffer.AsSpan(0, offset).ToArray())) throw new IOException("Slow UDP consumer.");
        }
    }
}
