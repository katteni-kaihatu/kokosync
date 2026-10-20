using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Threading.Channels;
using KokoSync;

internal static class PeerDeliveryChecks
{
    private sealed class Transport : ITransport
    {
        internal readonly Channel<byte[]> Frames = Channel.CreateUnbounded<byte[]>();
        internal bool FailWrites;
        public ValueTask SendAsync(byte[] bytes, bool realtime, CancellationToken ct)
            => FailWrites ? ValueTask.FromException(new IOException("Test transport failed.")) : Frames.Writer.WriteAsync(bytes, ct);
        public IAsyncEnumerable<byte[]> ReadAllAsync(CancellationToken ct) => Frames.Reader.ReadAllAsync(ct);
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
    private sealed class ClientTransport : ITransport
    {
        internal readonly Channel<byte[]> Incoming = Channel.CreateUnbounded<byte[]>();
        internal readonly Channel<byte[]> Outgoing = Channel.CreateUnbounded<byte[]>();
        public ValueTask SendAsync(byte[] bytes, bool realtime, CancellationToken ct) => Outgoing.Writer.WriteAsync(bytes, ct);
        public IAsyncEnumerable<byte[]> ReadAllAsync(CancellationToken ct) => Incoming.Reader.ReadAllAsync(ct);
        public ValueTask DisposeAsync() { Incoming.Writer.TryComplete(); return ValueTask.CompletedTask; }
    }
    private sealed class Pair : IAsyncDisposable
    {
        internal readonly PeerDelivery A;
        internal readonly PeerDelivery B;
        internal readonly ConcurrentQueue<Dictionary<string, object?>> Received = new();
        internal readonly ConcurrentQueue<Dictionary<string, object?>> Responses = new();
        internal readonly ConcurrentQueue<Dictionary<string, object?>> Sent = new();
        internal int DropDone;
        internal int RetryComplete;
        internal bool DropBulk, DuplicateChunks, ReorderChunk, RejectAdmission;
        private int reordered;
        private readonly CancellationTokenSource stop = new();
        private readonly Task forward, reverse;
        internal Pair(PeerDeliveryOptions? limits = null)
        {
            limits ??= new PeerDeliveryOptions();
            var a = new Transport();
            var b = new Transport();
            A = new PeerDelivery(a, limits, (_, _) => true);
            B = new PeerDelivery(b, limits, (frame, _) =>
            {
                if (RejectAdmission) return false;
                Received.Enqueue(frame);
                return true;
            });
            A.Join(Joined("a", "ag", "b", "bg", limits));
            B.Join(Joined("b", "bg", "a", "ag", limits));
            forward = Task.Run(async () =>
            {
                await foreach (var bytes in a.ReadAllAsync(stop.Token))
                {
                    var frame = Wire.Decode(bytes);
                    Sent.Enqueue(frame);
                    if (DropBulk && frame["type"] is "peer_chunk" && frame["bulk"] is true) continue;
                    if (ReorderChunk && frame["type"] is "peer_chunk" && Interlocked.Exchange(ref reordered, 1) == 0)
                    {
                        var future = new Dictionary<string, object?>(frame) { ["offset"] = (ulong)((byte[])frame["value"]!).Length };
                        await B.ReceiveAsync(future, bytes.Length);
                    }
                    await B.ReceiveAsync(frame, bytes.Length);
                    if (DuplicateChunks && frame["type"] is "peer_chunk") await B.ReceiveAsync(frame, bytes.Length);
                }
            });
            reverse = Task.Run(async () =>
            {
                await foreach (var bytes in b.ReadAllAsync(stop.Token))
                {
                    var frame = Wire.Decode(bytes);
                    Responses.Enqueue(frame);
                    if (frame["type"] is "peer_done" && DropDone > 0) { Interlocked.Decrement(ref DropDone); continue; }
                    if (frame["type"] is "peer_done" && RetryComplete > 0)
                    {
                        Interlocked.Decrement(ref RetryComplete);
                        var size = Sent.First(sent => sent["type"] is "peer_open" && Equals(sent["messageId"], frame["messageId"]))["size"];
                        await A.ReceiveAsync(new Dictionary<string, object?>(frame)
                        { ["type"] = "peer_credit", ["offset"] = size, ["limit"] = size, ["retry"] = true }, bytes.Length);
                        continue;
                    }
                    await A.ReceiveAsync(frame, bytes.Length);
                }
            });
        }
        public async ValueTask DisposeAsync()
        {
            await A.DisposeAsync();
            await B.DisposeAsync();
            stop.Cancel();
            try { await Task.WhenAll(forward, reverse); } catch (OperationCanceledException) { }
            stop.Dispose();
        }
    }
    internal static Dictionary<string, object?> Joined(string self, string generation, string peer, string peerGeneration,
        PeerDeliveryOptions limits) => Client.Frame("joined", ("epoch", "room"), ("peer", self), ("publisherEpoch", generation),
        ("capabilities", new Dictionary<string, object?> { ["peerDelivery"] = limits.ToWire() }),
        ("peers", new object?[] { new Dictionary<string, object?>
        { ["peer"] = peer, ["publisherEpoch"] = peerGeneration, ["peerDelivery"] = limits.ToWire() } }));
    private static Dictionary<string, object?> Frame(string type, string topic, string id, ulong seq, params (string, object?)[] extra)
    {
        var frame = Client.Frame(type, ("epoch", "room"), ("peer", "a"), ("publisherEpoch", "ag"),
            ("to", "b"), ("targetEpoch", "bg"), ("topic", topic), ("messageId", id), ("seq", seq));
        foreach (var (key, value) in extra) frame[key] = value;
        return frame;
    }
    private static void Check(bool value, string reason) { if (!value) throw new Exception(reason); }
    private static async Task<DeliveryException> Failure(Task<DeliveryReceipt> task)
    {
        try { await task; }
        catch (DeliveryException error) { return error; }
        throw new Exception("Expected delivery error.");
    }
    private static async Task Until(Func<bool> condition, CancellationToken ct)
    {
        while (!condition()) await Task.Delay(5, ct);
    }
    internal static async Task RunAsync()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var ct = deadline.Token;
        var payload = RandomNumberGenerator.GetBytes(100000);
        await using (var pair = new Pair { DropDone = 1 })
        {
            var receipt = await pair.A.SendToAsync("b", "faults", payload, ct);
            Check(receipt.Seq == 1 && receipt.Route == "relay" && receipt.RttMs == null, "Retransmitted receipt reported an ambiguous RTT.");
            Check(pair.Received.Count == 1 && ((byte[])pair.Received.Single()["value"]!).SequenceEqual(payload),
                "Duplicate/reordered chunks or dropped receipt duplicated/corrupted delivery.");
            Check(pair.A.Snapshot().Single(stat => stat.Kind == "peer_bulk").Retries >= 1, "Lost receipt did not retry.");
            var second = await pair.A.SendToAsync("b", "faults", "after", ct);
            Check(second.Seq == 2 && pair.Received.Count == 2, "Stop-wait sequence did not advance once.");
            Check(pair.A.Snapshot().All(stat => stat.UnackedMessages == 0 && stat.QueuedMessages == 0 && stat.QueuedBytes == 0),
                "Completed send retained reservations.");
        }
        await using (var pair = new Pair { DuplicateChunks = true, ReorderChunk = true })
        {
            await pair.A.SendToAsync("b", "reordering", payload, ct);
            Check(pair.Received.Count == 1 && ((byte[])pair.Received.Single()["value"]!).SequenceEqual(payload),
                "Duplicated or reordered fragments corrupted application delivery.");
        }
        await using (var pair = new Pair { RetryComplete = 1 })
        {
            var receipt = await pair.A.SendToAsync("b", "retry-credit", "value", ct);
            Check(receipt.RttMs == null && pair.A.Snapshot().Single().RttMs == null &&
                pair.Sent.Count(frame => frame["type"] is "peer_open") == 1 &&
                pair.Sent.Count(frame => frame["type"] is "peer_complete") == 2 && pair.Received.Count == 1,
                "Retry credit completion retransmission reported an ambiguous RTT or duplicated delivery.");
        }
        {
            var transport = new Transport();
            var options = new PeerDeliveryOptions();
            await using var sender = new PeerDelivery(transport, options, (_, _) => true);
            sender.Join(Joined("a", "ag", "b", "bg", options));
            var sending = sender.SendToAsync("b", "stale-credit", "value", ct);
            var open = Wire.Decode(await transport.Frames.Reader.ReadAsync(ct));
            var response = new Dictionary<string, object?>(open)
            {
                ["type"] = "peer_credit", ["peer"] = "b", ["publisherEpoch"] = "bg",
                ["to"] = "a", ["targetEpoch"] = "ag", ["offset"] = 0, ["limit"] = open["size"]
            };
            await sender.ReceiveAsync(response, 100);
            Check(Wire.Decode(await transport.Frames.Reader.ReadAsync(ct))["type"] is "peer_chunk",
                "Sender omitted the initial chunk.");
            response["offset"] = open["size"];
            await sender.ReceiveAsync(response, 100);
            Check(Wire.Decode(await transport.Frames.Reader.ReadAsync(ct))["type"] is "peer_complete",
                "Sender omitted initial completion.");
            response["offset"] = 0;
            response["retry"] = true;
            await sender.ReceiveAsync(response, 100);
            using (var observation = CancellationTokenSource.CreateLinkedTokenSource(ct))
            {
                observation.CancelAfter(100);
                try
                {
                    await transport.Frames.Reader.ReadAsync(observation.Token);
                    throw new Exception("Stale retry credit rewound acknowledged progress or reset completion.");
                }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested) { }
            }
            response["type"] = "peer_done";
            await sender.ReceiveAsync(response, 100);
            Check((await sending).RttMs != null && sender.Snapshot().Single().Retries == 0,
                "Ignored stale retry credit changed the receipt or retry accounting.");
        }
        await using (var pair = new Pair { DropBulk = true })
        {
            using var activeCancellation = new CancellationTokenSource();
            using var queuedCancellation = new CancellationTokenSource();
            var first = pair.A.SendToAsync("b", "cancel", payload, activeCancellation.Token);
            await Until(() => pair.Sent.Any(frame => frame["type"] is "peer_open"), ct);
            var queued = pair.A.SendToAsync("b", "cancel", "never", queuedCancellation.Token);
            queuedCancellation.Cancel();
            Check((await Failure(queued)).Outcome == "not-sent", "Queued cancellation was ambiguous.");
            activeCancellation.Cancel();
            Check((await Failure(first)).Outcome == "unknown", "Active cancellation claimed not-sent.");
            Check((await Failure(pair.A.SendToAsync("b", "cancel", "later", ct))).Code == "LaneFailed",
                "Cancelled active lane silently skipped a sequence.");
            var other = await pair.A.SendToAsync("b", "other", "works", ct);
            Check(other.Seq == 1 && pair.Received.Count == 1, "A failed lane blocked another topic.");
        }
        await using (var pair = new Pair { DropBulk = true })
        {
            var first = pair.A.SendToAsync("b", "pinned", payload, ct);
            await Until(() => pair.Sent.Any(frame => frame["type"] is "peer_open"), ct);
            var queued = pair.A.SendToAsync("b", "pinned", "old", ct);
            pair.A.MemberChanged(Client.Frame("peer_left", ("peer", "b"), ("publisherEpoch", "bg")), true);
            var activeError = await Failure(first);
            var queuedError = await Failure(queued);
            Check(activeError.Outcome == "unknown" && queuedError.Outcome == "not-sent" &&
                activeError.Code == "PeerUnavailable" && queuedError.Code == "PeerUnavailable",
                "Departure retargeted pending sends or lost outcome semantics.");
            var options = new PeerDeliveryOptions();
            pair.A.MemberChanged(Client.Frame("peer_joined", ("peer", "b"), ("publisherEpoch", "replacement"),
                ("peerDelivery", options.ToWire())), false);
            pair.B.Join(Joined("b", "replacement", "a", "ag", options));
            pair.DropBulk = false;
            var receipt = await pair.A.SendToAsync("b", "pinned", "new", ct);
            Check(receipt.PublisherEpoch == "replacement" && receipt.Seq == 1 && pair.Received.Count == 1 &&
                pair.A.Snapshot().All(stat => stat.PublisherEpoch == "replacement"),
                "Same-name replacement reused an old lane or diagnostics.");
        }
        await using (var pair = new Pair { DropBulk = true })
        {
            using var bulkCancellation = new CancellationTokenSource();
            var bulk = Enumerable.Range(0, 3).Select(i => pair.A.SendToAsync("b", "bulk-" + i, payload, bulkCancellation.Token)).ToArray();
            await Until(() => pair.Sent.Count(frame => frame["type"] is "peer_open") >= 3, ct);
            var small = await pair.A.SendToAsync("b", "small", 42, ct);
            Check(small.Seq == 1 && pair.Received.Count == 1, "Saturated bulk transfers blocked reserved small-message capacity.");
            bulkCancellation.Cancel();
            foreach (var task in bulk) await Failure(task);
        }
        await using (var pair = new Pair { RejectAdmission = true })
        {
            var failure = await Failure(pair.A.SendToAsync("b", "full", "value", ct));
            Check(failure.Code == "Backpressure" && failure.Outcome == "unknown" && pair.Received.IsEmpty,
                "Receipt preceded receiver application admission.");
        }
        await using (var pair = new Pair(new PeerDeliveryOptions { Lanes = 1 }))
        {
            await pair.A.SendToAsync("b", "retained", 1, ct);
            Check((await Failure(pair.A.SendToAsync("b", "new-lane", 2, ct))).Code == "Backpressure",
                "A live high-water lane was evicted to admit another topic.");
        }
        await using (var pair = new Pair(new PeerDeliveryOptions { AssemblyMs = 30 }))
        {
            var hash = Convert.ToHexStringLower(SHA256.HashData(new byte[] { 1 }));
            await pair.B.ReceiveAsync(Frame("peer_open", "expiry", "expired", 1, ("size", 1), ("sha256", hash)), 100);
            await Until(() => pair.Responses.Any(frame => frame["type"] is "peer_cancel" && frame["code"] is "Timeout"), ct);
            await pair.B.ReceiveAsync(Frame("peer_open", "expiry", "expired", 1, ("size", 1), ("sha256", hash)), 100);
            await Until(() => pair.Responses.Any(frame => frame["type"] is "peer_cancel" && frame["code"] is "LaneFailed"), ct);
            Check(pair.Received.IsEmpty, "Expired assembly was dispatched.");
        }
        await using (var pair = new Pair(new PeerDeliveryOptions { Retries = 0 }) { DropDone = 100 })
        {
            var failure = await Failure(pair.A.SendToAsync("b", "no-retries", 1, ct));
            Check(failure.Code == "Timeout" && failure.Outcome == "unknown" && pair.Received.Count == 1 &&
                pair.Sent.Count(frame => frame["type"] is "peer_open") == 1, "Zero retries changed ambiguity or sent an extra open.");
        }
        foreach (var kind in new[] { "peer_credit", "peer_done" })
        {
            await using var pair = new Pair { DropBulk = true };
            var sending = pair.A.SendToAsync("b", "invalid-response", payload, ct);
            await Until(() => pair.Sent.Any(frame => frame["type"] is "peer_open"), ct);
            var open = pair.Sent.First(frame => frame["type"] is "peer_open");
            var response = new Dictionary<string, object?>(open)
            {
                ["type"] = kind, ["peer"] = "b", ["publisherEpoch"] = "bg", ["to"] = "a", ["targetEpoch"] = "ag",
                ["offset"] = payload.Length + 5, ["limit"] = payload.Length + 5
            };
            await pair.A.ReceiveAsync(response, 100);
            var failure = await Failure(sending);
            Check(failure.Code == "ProtocolError",
                "Impossible receiver progress or premature receipt was accepted.");
        }
        await using (var pair = new Pair())
        {
            var hash = Convert.ToHexStringLower(SHA256.HashData(new byte[] { 1 }));
            await pair.B.ReceiveAsync(Frame("peer_cancel", "cancel-first", "before", 1, ("code", "Cancelled")), 100);
            await pair.B.ReceiveAsync(Frame("peer_open", "cancel-first", "before", 1, ("size", 1), ("sha256", hash)), 100);
            await Until(() => pair.Responses.Any(frame => frame["code"] as string == "LaneFailed"), ct);
            var first = await pair.A.SendToAsync("b", "accepted-cancel", 1, ct);
            await pair.B.ReceiveAsync(Frame("peer_cancel", "accepted-cancel", first.MessageId, 1, ("code", "Cancelled")), 100);
            Check((await pair.A.SendToAsync("b", "accepted-cancel", 2, ct)).Seq == 2,
                "Cancellation retracted an already accepted sequence.");
            await pair.B.ReceiveAsync(Frame("peer_open", "hash", "bad", 1, ("size", 1), ("sha256", hash)), 100);
            await pair.B.ReceiveAsync(Frame("peer_chunk", "hash", "bad", 1, ("offset", 0), ("value", new byte[] { 2 }), ("bulk", false)), 100);
            await pair.B.ReceiveAsync(Frame("peer_complete", "hash", "bad", 1), 100);
            await Until(() => pair.Responses.Any(frame => frame.GetValueOrDefault("code") as string == "HashMismatch"), ct);
            Check(pair.Received.Count == 2, "Invalid hash reached application admission.");
        }
        var limits = new PeerDeliveryOptions();
        await using (var unsupported = new PeerDelivery(new Transport(), limits, (_, _) => true))
        {
            var joined = Joined("a", "ag", "b", "bg", limits);
            ((Dictionary<string, object?>)((object?[])joined["peers"]!)[0]!).Remove("peerDelivery");
            unsupported.Join(joined);
            var failure = await Failure(unsupported.SendToAsync("b", "legacy", 1, ct));
            Check(failure.Code == "UnsupportedCapability" && failure.Outcome == "not-sent", "Legacy destination was not rejected before transmission.");
        }
        await CheckApplicationQueueAsync(ct);
        await CheckErrorContractAsync(ct);
        Console.WriteLine("PEER_DELIVERY_CHECKS_OK large-value lost-receipt duplicates reorder sequence cancellation poison admission retained-lanes expiry bulk-small-progress appqueue-reserve admitted-before-departure legacy");
    }
    private static async Task CheckErrorContractAsync(CancellationToken ct)
    {
        var options = new PeerDeliveryOptions { MessageBytes = 16 };
        var transport = new Transport();
        await using (var sender = new PeerDelivery(transport, options, (_, _) => true))
        {
            sender.Join(Joined("a", "ag", "b", "bg", options));
            foreach (var (peer, topic, value, expected) in new (string, string, object?, string)[]
            {
                ("", "topic", null, "InvalidArgument"),
                ("b", "", null, "InvalidArgument"),
                ("b", "\ud800", null, "InvalidArgument"),
                (new string('x', 129), "topic", null, "InvalidArgument"),
                ("b", new string('x', 257), null, "InvalidArgument"),
                ("missing", "topic", null, "PeerUnavailable"),
                ("b", "topic", double.NaN, "InvalidValue"),
                ("b", "topic", DateTime.UtcNow, "InvalidValue"),
                ("b", "topic", new byte[16], "PayloadTooLarge")
            })
            {
                var error = await Failure(sender.SendToAsync(peer, topic, value, ct));
                Check(error.Code == expected && error.Outcome == "not-sent", $"Incorrect preflight error: {expected}.");
            }
            Check(!transport.Frames.Reader.TryRead(out _), "Preflight failure transmitted a frame.");
            using var cancelled = new CancellationTokenSource();
            cancelled.Cancel();
            var cancellation = await Failure(sender.SendToAsync("b", "topic", null, cancelled.Token));
            Check(cancellation.Code == "Cancelled" && cancellation.Outcome == "not-sent", "Incorrect cancelled error.");
            sender.Close();
            var closed = await Failure(sender.SendToAsync("b", "topic", null, ct));
            Check(closed.Code == "Closed" && closed.Outcome == "not-sent", "Incorrect closed error.");
        }
        try
        {
            new PeerDeliveryOptions { Queued = 0 }.Validate();
            throw new Exception("Invalid options were accepted.");
        }
        catch (DeliveryException error)
        {
            Check(error.Code == "InvalidArgument" && error.Outcome == "not-sent", "Incorrect options error.");
        }
        foreach (var (wire, expected) in new (string, string)[]
        {
            ("InvalidMessage", "ProtocolError"), ("InvalidPayload", "ProtocolError"),
            ("HashMismatch", "ProtocolError"), ("DuplicateMismatch", "ProtocolError"),
            ("UnknownTransfer", "ProtocolError"), ("StaleGeneration", "PeerUnavailable"),
            ("DeadlineExceeded", "Timeout"), ("TransportClosed", "Closed"),
            ("Capacity", "Backpressure"), ("Backpressure", "Backpressure"),
            ("Unsupported", "UnsupportedCapability"), ("PeerMissing", "PeerUnavailable"), ("PeerLeft", "PeerUnavailable"),
            ("PeerNotFound", "PeerUnavailable"), ("TransportError", "TransportFailed"),
            ("UnrecognizedRemoteCode", "ProtocolError")
        })
        {
            var frames = new Transport();
            await using var sender = new PeerDelivery(frames, options, (_, _) => true);
            sender.Join(Joined("a", "ag", "b", "bg", options));
            var sending = sender.SendToAsync("b", "wire-error", null, ct);
            var open = Wire.Decode(await frames.Frames.Reader.ReadAsync(ct));
            await sender.ReceiveAsync(new Dictionary<string, object?>(open)
            {
                ["type"] = "peer_relay_error", ["peer"] = "b", ["publisherEpoch"] = "bg",
                ["to"] = "a", ["targetEpoch"] = "ag", ["code"] = wire
            }, 100);
            var error = await Failure(sending);
            Check(error.Code == expected && error.Outcome == "unknown", $"Incorrect normalized wire error: {wire}.");
        }
        await using (var sender = new PeerDelivery(new Transport { FailWrites = true }, options, (_, _) => true))
        {
            sender.Join(Joined("a", "ag", "b", "bg", options));
            var error = await Failure(sender.SendToAsync("b", "broken-transport", null, ct));
            Check(error.Code == "TransportFailed" && error.Outcome == "unknown", "Incorrect transport failure error.");
        }
    }
    private static async Task CheckApplicationQueueAsync(CancellationToken ct)
    {
        using var pumpCancellation = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var limits = new PeerDeliveryOptions();
        var receiverTransport = new ClientTransport();
        await using var receiver = new Client(receiverTransport, null, limits);
        var join = receiver.JoinAsync("room", "b", ct: ct);
        var request = Wire.Decode(await receiverTransport.Outgoing.Reader.ReadAsync(ct));
        var joined = Joined("b", "bg", "a", "ag", limits);
        joined["id"] = request["id"];
        await receiverTransport.Incoming.Writer.WriteAsync(Wire.Encode(joined), ct);
        await join;
        var senderTransport = new Transport();
        await using var sender = new PeerDelivery(senderTransport, limits, (_, _) => true);
        sender.Join(Joined("a", "ag", "b", "bg", limits));
        var forward = Task.Run(async () =>
        {
            await foreach (var bytes in senderTransport.ReadAllAsync(pumpCancellation.Token))
                await receiverTransport.Incoming.Writer.WriteAsync(bytes, pumpCancellation.Token);
        });
        var reverse = Task.Run(async () =>
        {
            await foreach (var bytes in receiverTransport.Outgoing.Reader.ReadAllAsync(pumpCancellation.Token))
                await sender.ReceiveAsync(Wire.Decode(bytes), bytes.Length);
        });
        try
        {
            var bulk = new byte[20000];
            for (var i = 0; i < 15; i++) await sender.SendToAsync("b", "bulk-events", bulk, ct);
            Check((await Failure(sender.SendToAsync("b", "bulk-events", bulk, ct))).Code == "Backpressure",
                "Completed bulk messages consumed the reserved small application slot.");
            await sender.SendToAsync("b", "small-event", 42, ct);
            await using var events = receiver.Events(ct).GetAsyncEnumerator();
            for (var i = 0; i < 16; i++)
            {
                Check(await events.MoveNextAsync(), "Admitted application event disappeared.");
                Check(events.Current["type"] is "peer_message", "Unexpected queue event.");
            }
            Check(events.Current["topic"] is "small-event", "Reserved small message was not admitted.");
            await sender.SendToAsync("b", "max-value", new byte[limits.MessageBytes - 5], ct);
            Check(await events.MoveNextAsync() && ((byte[])events.Current["value"]!).Length == limits.MessageBytes - 5,
                "Peer-message metadata incorrectly consumed the Value limit.");
            await sender.SendToAsync("b", "stale", bulk, ct);
            sender.Close();
            await receiverTransport.Incoming.Writer.WriteAsync(Wire.Encode(Client.Frame("peer_left",
                ("epoch", "room"), ("peer", "a"), ("publisherEpoch", "ag"))), ct);
            await Until(() => receiver.Routes.Count == 0, ct);
            Check(await events.MoveNextAsync() && events.Current["type"] is "peer_message" &&
                events.Current["publisherEpoch"] is "ag" && events.Current["topic"] is "stale",
                "A receipt-admitted message was lost when its sender departed.");
            Check(await events.MoveNextAsync() && events.Current["type"] is "route" && events.Current["removed"] is true,
                "Departure notification was lost.");
        }
        finally
        {
            pumpCancellation.Cancel();
            try { await Task.WhenAll(forward, reverse); } catch (OperationCanceledException) { }
        }
    }
}
