using System.Threading.Channels;
using KokoSync;

internal static class RoutingChecks
{
    private sealed class Transport : ITransport
    {
        internal readonly Channel<byte[]> Sent = Channel.CreateUnbounded<byte[]>();
        internal readonly Channel<byte[]> Incoming = Channel.CreateUnbounded<byte[]>();
        public ValueTask SendAsync(byte[] bytes, bool realtime, CancellationToken ct) => Sent.Writer.WriteAsync(bytes, ct);
        public IAsyncEnumerable<byte[]> ReadAllAsync(CancellationToken ct) => Incoming.Reader.ReadAllAsync(ct);
        public ValueTask DisposeAsync() { Incoming.Writer.TryComplete(); return ValueTask.CompletedTask; }
        internal ValueTask Receive(Dictionary<string, object?> frame) => Incoming.Writer.WriteAsync(Wire.Encode(frame));
    }
    private static void Check(bool condition, string reason)
    {
        if (!condition) throw new Exception(reason);
    }
    private static Dictionary<string, object?> Member(string peer, string generation) => new()
    {
        ["peer"] = peer, ["publisherEpoch"] = generation, ["directProtocol"] = "webrtc", ["maxDirectPeers"] = 2
    };
    private static Dictionary<string, object?> Membership(string type, string peer, string generation, string epoch = "room")
    {
        var frame = Client.Frame(type, ("epoch", epoch));
        foreach (var pair in Member(peer, generation)) frame[pair.Key] = pair.Value;
        return frame;
    }
    private static Dictionary<string, object?> Latest(string peer, string generation, ulong seq) =>
        Client.Frame("latest", ("peer", peer), ("publisherEpoch", generation), ("key", "position"), ("seq", seq), ("value", 1));

    internal static async Task RunAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var transport = new Transport();
        var client = new Client(transport, null);
        try
        {
            var joining = client.JoinAsync("routing", "local", ct: timeout.Token);
            var request = Wire.Decode(await transport.Sent.Reader.ReadAsync(timeout.Token));
            var advertised = (Dictionary<string, object?>)request["routing"]!;
            Check(advertised["directProtocol"] is "none" && Client.Number(advertised, "maxDirectPeers") == 0,
                "C# advertised an unavailable direct adapter.");
            await transport.Receive(Client.Frame("joined", ("id", request["id"]), ("epoch", "room"),
                ("peer", "local"), ("publisherEpoch", "local-generation"), ("peers", new object?[] { Member("peer", "one") })));
            await joining;
            var initial = client.Routes.Single();
            Check(initial.Route == "relay" && initial.Reason == "local-adapter-unavailable" && initial.RttMs == null &&
                initial.SentDirect == 0 && initial.ReceivedDirect == 0, "Native route diagnostic mismatch.");
            await client.LatestAsync("position", 1, ulong.MaxValue, timeout.Token);
            var latest = Wire.Decode(await transport.Sent.Reader.ReadAsync(timeout.Token));
            Check(latest["type"] is "latest" && Client.Number(latest, "seq") == ulong.MaxValue &&
                client.Routes.Single().SentRelay == 1 && initial.SentRelay == 0, "Relay submission count or immutable snapshot failed.");

            await using var events = client.Events(timeout.Token).GetAsyncEnumerator();
            await transport.Receive(Latest("peer", "one", ulong.MaxValue));
            Check(await events.MoveNextAsync() && events.Current["type"] is "latest" &&
                Client.Number(events.Current, "seq") == ulong.MaxValue, "Latest API or UInt64 changed.");
            await transport.Receive(Latest("peer", "one", ulong.MaxValue)); // Count reception, suppress duplicate delivery.
            var peerJoined = Membership("peer_joined", "second", "other");
            peerJoined["peerDelivery"] = new PeerDeliveryOptions { MessageBytes = 123456 }.ToWire();
            await transport.Receive(peerJoined);
            Check(await events.MoveNextAsync() && events.Current["type"] is "route" && events.Current["peer"] is "second",
                "Membership route event or duplicate suppression failed.");
            Check(events.Current.GetValueOrDefault("peerDelivery") is Dictionary<string, object?> selected &&
                Client.Number(selected, "messageBytes") == 123456 && Client.Number(selected, "version") == 1,
                "Membership route event omitted selected peer delivery capabilities.");
            Check(client.Routes.Single(route => route.Peer == "peer").ReceivedRelay == 2, "Relay reception count missing.");
            await client.SendAsync(Client.Frame("live", ("stream", "audio"), ("seq", 1UL), ("ttlMs", 100),
                ("value", new byte[] { 1 }), ("exclude", new object?[] { "peer" })), timeout.Token);
            await transport.Sent.Reader.ReadAsync(timeout.Token);
            Check(client.Routes.Single(route => route.Peer == "peer").SentRelay == 1 &&
                client.Routes.Single(route => route.Peer == "second").SentRelay == 1, "Directed relay counters counted excluded peer.");

            await transport.Receive(Membership("peer_left", "peer", "one", "stale-room"));
            await transport.Receive(Membership("peer_left", "peer", "stale-generation"));
            await transport.Receive(Membership("peer_joined", "peer", "two"));
            await transport.Receive(Membership("peer_left", "peer", "one"));
            Check(await events.MoveNextAsync() && events.Current["removed"] is true &&
                events.Current["publisherEpoch"] is "one" && client.Routes.Count == 1, "Stale membership replaced current generation.");
            await transport.Receive(Membership("peer_joined", "peer", "one"));
            await transport.Receive(Membership("peer_joined", "peer", "two"));
            Check(await events.MoveNextAsync() && events.Current["publisherEpoch"] is "two",
                "Departed generation was resurrected.");
            await transport.Receive(Latest("peer", "one", 1));
            await transport.Receive(Latest("peer", "two", 1));
            Check(await events.MoveNextAsync() && events.Current["type"] is "latest" && events.Current["publisherEpoch"] is "two" &&
                client.Routes.Single(route => route.Peer == "peer").ReceivedRelay == 1, "Realtime stale generation filtering failed.");
        }
        finally { await client.DisposeAsync(); }
        Check(client.Routes.Count == 0, "Closed session retained routes.");

        var legacy = new RelayRoutes();
        legacy.Join(Client.Frame("joined", ("epoch", "legacy")));
        Check(legacy.Snapshot().Count == 0 && legacy.Received(Latest("unknown", "legacy", 1)),
            "Missing optional metadata broke older server compatibility.");
        legacy.Join(Client.Frame("joined", ("epoch", "new-room"), ("peer", "self"), ("publisherEpoch", "self-new"),
            ("peers", new object?[] { Member("new", "generation") })));
        Check(legacy.Update(Membership("peer_joined", "stale", "generation")) == null &&
            legacy.Snapshot().Single().Peer == "new", "Room epoch transition retained stale members.");
        Console.WriteLine("ROUTING_CHECKS_OK native-relay advertisement membership immutable-snapshot counters selectors stale-epochs duplicate-suppression close legacy");
    }

    internal static async Task RunIntegrationAsync(Uri endpoint, int? udpPort)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var ct = timeout.Token;
        await using var writer = udpPort.HasValue
            ? await Client.ConnectLiteNetLibAsync(port: udpPort.Value, insecureDevelopment: true, ct: ct)
            : await Client.ConnectWebSocketAsync(endpoint, ct);
        var room = "routes-csharp-" + Guid.NewGuid().ToString("N")[..8];
        await writer.JoinAsync(room, "writer", ct: ct);
        var memberAdded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var memberRemoved = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var liveReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writerEvents = Task.Run(async () =>
        {
            await foreach (var frame in writer.Events(ct))
            {
                if (frame["type"] is "route" && frame["peer"] is "reader")
                {
                    if (frame["removed"] is true) memberRemoved.TrySetResult();
                    else memberAdded.TrySetResult();
                }
                if (frame["type"] is "live" && frame["peer"] is "reader") liveReceived.TrySetResult();
            }
        });
        await using (var reader = await Client.ConnectWebSocketAsync(endpoint, ct))
        {
            await reader.JoinAsync(room, "reader", ct: ct);
            Check(reader.Routes.Single().Peer == "writer" && reader.Routes.Single().Route == "relay",
                "Initial live server membership route missing.");
            await memberAdded.Task.WaitAsync(ct);
            await reader.LiveAsync("audio", new byte[] { 1, 2 }, ulong.MaxValue, 500, ct: ct);
            await liveReceived.Task.WaitAsync(ct);
            Check(reader.Routes.Single().SentRelay == 1 && writer.Routes.Single().ReceivedRelay == 1,
                "Live relay counters missing.");
            await writer.LatestAsync("position", 42, ulong.MaxValue, ct);
            await using var events = reader.Events(ct).GetAsyncEnumerator();
            while (await events.MoveNextAsync())
                if (events.Current["type"] is "latest" && events.Current["peer"] is "writer") break;
            Check(Client.Number(events.Current, "seq") == ulong.MaxValue &&
                reader.Routes.Single().ReceivedRelay == 1 && writer.Routes.Single().SentRelay == 1,
                "Latest route counters or UInt64 changed across transports.");
        }
        await memberRemoved.Task.WaitAsync(ct);
        Check(writer.Routes.Count == 0, "Departed live peer retained route.");
        timeout.Cancel();
        try { await writerEvents; } catch (OperationCanceledException) { }
        Console.WriteLine("ROUTING_INTEGRATION_OK initial-membership join-event native-relay live latest uint64 counters departure");
    }
}
