using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace KokoSync;

internal sealed class PeerDelivery : IAsyncDisposable
{
    private sealed record Member(string Epoch, PeerDeliveryOptions? Limits);
    private sealed class SendLane((string Peer, string Epoch, string Topic) key)
    {
        internal readonly (string Peer, string Epoch, string Topic) Key = key;
        internal readonly LinkedList<Pending> Queue = new();
        internal ulong Sequence;
        internal string LastId = "", LastKind = "peer_message";
        internal bool Running;
        internal bool Failed;
    }
    private sealed class Pending(SendLane lane, byte[] bytes, PeerDeliveryOptions limits, CancellationTokenSource cancellation,
        CancellationToken caller, string room, string source, string sourceEpoch)
    {
        internal readonly SendLane Lane = lane;
        internal readonly byte[] Bytes = bytes;
        internal readonly string Hash = Convert.ToHexStringLower(SHA256.HashData(bytes));
        internal readonly string Id = Guid.NewGuid().ToString("N");
        internal readonly PeerDeliveryOptions Limits = limits;
        internal readonly CancellationTokenSource Cancellation = cancellation;
        internal readonly CancellationToken Token = cancellation.Token;
        internal readonly CancellationToken Caller = caller;
        internal readonly string Room = room, Source = source, SourceEpoch = sourceEpoch;
        internal readonly TaskCompletionSource<DeliveryReceipt> Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly SemaphoreSlim Changed = new(0, 1);
        internal CancellationTokenRegistration Registration;
        internal LinkedListNode<Pending>? Node;
        internal ulong Sequence;
        internal bool Started, Released, Done, CompleteSent, CompleteEver, Retried;
        internal int Sent, MaxSent, Ack, Limit, Progress;
        internal long Opened;
        internal double? ReceiptRtt;
        internal DeliveryException? Error;
        internal string Kind => Bytes.Length > 16384 ? "peer_bulk" : "peer_message";
        internal void Signal() { if (Changed.CurrentCount == 0) try { Changed.Release(); } catch (SemaphoreFullException) { } }
    }
    private sealed class ReceiveLane
    {
        internal ulong HighWater;
        internal string Id = "", Hash = "";
        internal int Size;
        internal bool Failed;
        internal Incoming? Active;
    }
    private sealed class Incoming(Dictionary<string, object?> open, byte[] bytes, string hash, PeerDeliveryOptions limits)
    {
        internal readonly Dictionary<string, object?> Open = open;
        internal readonly byte[] Bytes = bytes;
        internal readonly string Hash = hash;
        internal readonly PeerDeliveryOptions Limits = limits;
        internal readonly CancellationTokenSource Expiry = new();
        internal int Offset;
        internal int Limit = Math.Min(bytes.Length, limits.WindowBytes);
        internal string Kind => Bytes.Length > 16384 ? "peer_bulk" : "peer_message";
    }
    private readonly object gate = new();
    private readonly PeerDeliveryOptions options;
    private readonly PeerScheduler scheduler;
    private readonly Func<Dictionary<string, object?>, bool, bool> admit;
    private readonly CancellationTokenSource stop = new();
    private readonly CancellationToken lifetime;
    private readonly SemaphoreSlim totalSlots;
    private readonly SemaphoreSlim bulkSlots;
    private readonly Dictionary<string, Member> members = new(StringComparer.Ordinal);
    private readonly Dictionary<(string, string, string), SendLane> sendLanes = new();
    private readonly Dictionary<(string, string, string), ReceiveLane> receiveLanes = new();
    private readonly Dictionary<string, Pending> active = new(StringComparer.Ordinal);
    private readonly Dictionary<(string, string, string), DeliveryStat> stats = new();
    private readonly HashSet<Task> workers = new();
    private PeerDeliveryOptions? core;
    private string epoch = "", localPeer = "", localGeneration = "";
    private int queued, bulkQueued, receiving, bulkReceiving;
    private long queuedBytes, bulkQueuedBytes, receivedBytes, bulkReceivedBytes;
    private bool closed;

    internal PeerDelivery(ITransport transport, PeerDeliveryOptions options, Func<Dictionary<string, object?>, bool, bool> admit)
    {
        options.Validate();
        this.options = options;
        lifetime = stop.Token;
        this.admit = admit;
        scheduler = new PeerScheduler(transport);
        totalSlots = new(options.Concurrent);
        bulkSlots = new(Math.Max(1, options.Concurrent - 1));
    }
    internal static bool IsFrame(string? kind) => kind is "peer_open" or "peer_credit" or "peer_chunk" or
        "peer_complete" or "peer_done" or "peer_cancel" or "peer_relay_error";
    internal IReadOnlyList<DeliveryStat> Snapshot()
    {
        lock (gate) return stats.Values.ToArray();
    }
    internal void Join(Dictionary<string, object?> joined)
    {
        lock (gate)
        {
            if (localGeneration.Length != 0 && (localGeneration != joined.GetValueOrDefault("publisherEpoch") as string ||
                epoch != joined.GetValueOrDefault("epoch") as string))
            {
                foreach (var lane in sendLanes.Values.ToArray())
                    foreach (var pending in lane.Queue.ToArray())
                    {
                        pending.Error = Error("GenerationChanged", pending.Started, "Session generation changed.");
                        pending.Cancellation.Cancel();
                    }
                foreach (var lane in receiveLanes.Values) ReleaseIncoming(lane);
                sendLanes.Clear(); receiveLanes.Clear(); stats.Clear();
            }
            epoch = joined.GetValueOrDefault("epoch") as string ?? "";
            localPeer = joined.GetValueOrDefault("peer") as string ?? "";
            localGeneration = joined.GetValueOrDefault("publisherEpoch") as string ?? "";
            core = options.Enabled && joined.GetValueOrDefault("capabilities") is Dictionary<string, object?> capabilities &&
                capabilities.GetValueOrDefault("peerDelivery") is Dictionary<string, object?> map ? PeerDeliveryOptions.Parse(map) : null;
            members.Clear();
            if (joined.GetValueOrDefault("peers") is object?[] roster)
                foreach (var member in roster.OfType<Dictionary<string, object?>>()) AddMember(member);
        }
    }
    private void AddMember(Dictionary<string, object?> frame)
    {
        if (frame.GetValueOrDefault("peer") is not string peer || frame.GetValueOrDefault("publisherEpoch") is not string generation) return;
        members[peer] = new Member(generation, frame.GetValueOrDefault("peerDelivery") is Dictionary<string, object?> map
            ? PeerDeliveryOptions.Parse(map) : null);
    }
    internal void MemberChanged(Dictionary<string, object?> frame, bool removed)
    {
        lock (gate)
        {
            var peer = (string)frame["peer"]!;
            var generation = (string)frame["publisherEpoch"]!;
            if (!removed) { AddMember(frame); return; }
            if (!members.TryGetValue(peer, out var member) || member.Epoch != generation) return;
            members.Remove(peer);
            foreach (var lane in sendLanes.Values.Where(lane => lane.Key.Peer == peer && lane.Key.Epoch == generation).ToArray())
            {
                lane.Failed = true;
                foreach (var pending in lane.Queue.ToArray())
                {
                    pending.Error = Error("TargetChanged", pending.Started, "The recipient generation departed.");
                    pending.Cancellation.Cancel();
                }
                sendLanes.Remove(lane.Key);
            }
            foreach (var pair in receiveLanes.Where(pair => pair.Key.Item1 == peer && pair.Key.Item2 == generation).ToArray())
            {
                ReleaseIncoming(pair.Value);
                receiveLanes.Remove(pair.Key);
            }
            foreach (var key in stats.Keys.Where(key => key.Item1 == peer && key.Item2 == generation).ToArray()) stats.Remove(key);
        }
    }
    internal async Task<DeliveryReceipt> SendToAsync(string peer, string topic, object? value, CancellationToken ct)
    {
        var invoked = Stopwatch.GetTimestamp();
        Pending pending;
        Member member;
        PeerDeliveryOptions limits;
        if (!ValidIdentity(peer, 128) || !ValidIdentity(topic, 256))
            throw Error("InvalidArgument", false, "Peer and topic must be nonempty valid UTF-8 within their byte limits.");
        if (ct.IsCancellationRequested) throw Error("Cancelled", false, "Send cancelled before admission.");
        lock (gate)
        {
            if (closed) throw Error("Closed", false, "Session closed.");
            if (core == null || !options.Enabled) throw Error("UnsupportedCapability", false, "Peer delivery is not supported by this session.");
            if (!members.TryGetValue(peer, out member!)) throw Error("PeerNotFound", false, "Recipient is not a current member.");
            if (member.Limits == null) throw Error("UnsupportedCapability", false, "Recipient does not support peer delivery.");
            limits = options.Min(core).Min(member.Limits);
        }
        byte[] bytes;
        try { bytes = Wire.EncodeValue(value, limits.MessageBytes); }
        catch (Wire.ValueTooLargeException ex) { throw Error("PayloadTooLarge", false, ex.Message); }
        catch (ArgumentException ex) { throw Error("InvalidValue", false, ex.Message); }
        lock (gate)
        {
            if (!members.TryGetValue(peer, out var current) || current.Epoch != member.Epoch)
                throw Error("TargetChanged", false, "Recipient generation changed before admission.");
            if (closed) throw Error("Closed", false, "Session closed.");
            if (ct.IsCancellationRequested) throw Error("Cancelled", false, "Send cancelled before admission.");
            var bulk = bytes.Length > 16384;
            if (queued >= options.Queued || bytes.Length > options.QueueBytes - queuedBytes ||
                bulk && (bulkQueued >= Math.Max(1, options.Queued - 1) || bytes.Length > options.QueueBytes - 16384L - bulkQueuedBytes))
                throw Error("Capacity", false, "Outgoing delivery budget is full.");
            var remaining = limits.TimeoutMs - Stopwatch.GetElapsedTime(invoked).TotalMilliseconds;
            if (remaining <= 0) throw Error("Timeout", false, "Send deadline elapsed before admission.");
            var key = (peer, member.Epoch, topic);
            if (!sendLanes.TryGetValue(key, out var lane))
            {
                if (sendLanes.Count + receiveLanes.Count >= options.Lanes) throw Error("Capacity", false, "Retained ordering lane limit reached.");
                sendLanes.Add(key, lane = new SendLane(key));
            }
            if (lane.Failed) throw Error("LaneFailed", false, "This ordering lane requires a fresh session generation.");
            var cancellation = CancellationTokenSource.CreateLinkedTokenSource(stop.Token, ct);
            cancellation.CancelAfter(TimeSpan.FromMilliseconds(remaining));
            pending = new Pending(lane, bytes, limits, cancellation, ct, epoch, localPeer, localGeneration);
            pending.Node = lane.Queue.AddLast(pending);
            queued++; queuedBytes += bytes.Length;
            if (bulk) { bulkQueued++; bulkQueuedBytes += bytes.Length; }
            UpdateStat(peer, member.Epoch, pending.Kind, stat => stat with
            { QueuedMessages = stat.QueuedMessages + 1, QueuedBytes = stat.QueuedBytes + bytes.Length });
            pending.Registration = pending.Token.Register(() => CancelQueued(pending));
            if (!lane.Running)
            {
                lane.Running = true;
                var worker = RunLaneAsync(lane);
                workers.Add(worker);
                _ = worker.ContinueWith(done => { lock (gate) workers.Remove(done); },
                    CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            }
        }
        try { return await pending.Completion.Task; }
        finally { pending.Registration.Dispose(); pending.Cancellation.Dispose(); }
    }
    private void CancelQueued(Pending pending)
    {
        lock (gate)
        {
            if (pending.Started || pending.Released) return;
            ReleasePending(pending);
            pending.Completion.TrySetException(pending.Error ?? Error(CancellationCode(pending), false, "Queued send cancelled or expired."));
        }
    }
    private async Task RunLaneAsync(SendLane lane)
    {
        while (true)
        {
            Pending pending;
            lock (gate)
            {
                if (lane.Queue.First == null) { lane.Running = false; return; }
                pending = lane.Queue.First.Value;
            }
            var bulkSlot = false;
            var totalSlot = false;
            DeliveryReceipt? receipt = null;
            DeliveryException? error = null;
            try
            {
                if (pending.Bytes.Length > 16384) { await bulkSlots.WaitAsync(pending.Token); bulkSlot = true; }
                await totalSlots.WaitAsync(pending.Token); totalSlot = true;
                lock (gate)
                {
                    pending.Token.ThrowIfCancellationRequested();
                    if (pending.Released) continue;
                    if (lane.Failed) throw Error("LaneFailed", false, "An earlier send failed.");
                    if (lane.Sequence == ulong.MaxValue) throw Error("LaneFailed", false, "Sequence exhausted.");
                    pending.Sequence = ++lane.Sequence;
                    lane.LastId = pending.Id; lane.LastKind = pending.Kind;
                    pending.Started = true;
                    active.Add(pending.Id, pending);
                    UpdateStat(lane.Key.Peer, lane.Key.Epoch, pending.Kind, stat => stat with
                    { UnackedMessages = stat.UnackedMessages + 1, SentMessages = stat.SentMessages + 1 });
                }
                receipt = await TransmitAsync(pending);
            }
            catch (Exception ex)
            {
                error = pending.Error ?? ex as DeliveryException ?? Error(ex is OperationCanceledException ? CancellationCode(pending) : "TransportError",
                    pending.Started, ex.Message);
                if (pending.Started)
                {
                    lock (gate) lane.Failed = true;
                    try
                    {
                        using var cancellation = new CancellationTokenSource(250);
                        await SendFrameAsync(Frame(pending, "peer_cancel", ("code", error.Code), ("message", error.Message)), pending.Kind, cancellation.Token);
                    }
                    catch { }
                }
            }
            finally
            {
                lock (gate)
                {
                    active.Remove(pending.Id);
                    if (pending.Started) UpdateStat(lane.Key.Peer, lane.Key.Epoch, pending.Kind,
                        stat => stat with { UnackedMessages = Math.Max(0, stat.UnackedMessages - 1) });
                    ReleasePending(pending);
                    if (lane.Failed)
                        foreach (var next in lane.Queue.ToArray())
                        {
                            ReleasePending(next);
                            next.Completion.TrySetException(Error("LaneFailed", false, "An earlier delivery in this lane failed."));
                        }
                }
                if (totalSlot) totalSlots.Release();
                if (bulkSlot) bulkSlots.Release();
                if (receipt != null) pending.Completion.TrySetResult(receipt);
                else if (error != null) pending.Completion.TrySetException(error);
            }
        }
    }
    private void ReleasePending(Pending pending)
    {
        if (pending.Released) return;
        pending.Released = true;
        if (pending.Node?.List != null) pending.Lane.Queue.Remove(pending.Node);
        if (pending.Lane.Sequence == 0 && !pending.Lane.Failed && pending.Lane.Queue.Count == 0 &&
            sendLanes.TryGetValue(pending.Lane.Key, out var current) && ReferenceEquals(current, pending.Lane))
            sendLanes.Remove(pending.Lane.Key);
        queued--; queuedBytes -= pending.Bytes.Length;
        if (pending.Bytes.Length > 16384) { bulkQueued--; bulkQueuedBytes -= pending.Bytes.Length; }
        UpdateStat(pending.Lane.Key.Peer, pending.Lane.Key.Epoch, pending.Kind, stat => stat with
        { QueuedMessages = Math.Max(0, stat.QueuedMessages - 1), QueuedBytes = Math.Max(0, stat.QueuedBytes - pending.Bytes.Length) });
    }
    private async Task<DeliveryReceipt> TransmitAsync(Pending pending)
    {
        var rounds = 0;
        var retryDelay = 500;
        var observedProgress = 0;
        var retryAt = Environment.TickCount64 + retryDelay;
        async Task Open()
        {
            lock (gate)
            {
                pending.Sent = pending.Ack;
                pending.Limit = pending.Ack;
                pending.CompleteSent = false;
                pending.Opened = Stopwatch.GetTimestamp();
            }
            await SendFrameAsync(Frame(pending, "peer_open", ("size", pending.Bytes.Length), ("sha256", pending.Hash)), pending.Kind, pending.Token);
        }
        await Open();
        while (true)
        {
            int offset, count;
            bool complete;
            lock (gate)
            {
                if (pending.Error != null) throw pending.Error;
                if (pending.Done)
                {
                    double? rtt = pending.Retried ? null : pending.ReceiptRtt ?? Stopwatch.GetElapsedTime(pending.Opened).TotalMilliseconds;
                    UpdateStat(pending.Lane.Key.Peer, pending.Lane.Key.Epoch, pending.Kind, stat => stat with { RttMs = rtt });
                    return new(pending.Lane.Key.Peer, pending.Lane.Key.Epoch, pending.Lane.Key.Topic,
                        pending.Id, pending.Sequence, "relay", rtt);
                }
                pending.Token.ThrowIfCancellationRequested();
                if (pending.Progress != observedProgress)
                { observedProgress = pending.Progress; retryDelay = 500; retryAt = Environment.TickCount64 + retryDelay; }
                offset = pending.Sent;
                count = Math.Min(pending.Limits.ChunkBytes, pending.Limit - offset);
                if (count > 0)
                {
                    if (offset < pending.MaxSent) pending.Retried = true;
                    pending.Sent += count;
                    pending.MaxSent = Math.Max(pending.MaxSent, pending.Sent);
                }
                complete = count == 0 && pending.Sent == pending.Bytes.Length && !pending.CompleteSent;
                if (complete)
                {
                    if (pending.CompleteEver) pending.Retried = true;
                    pending.CompleteSent = true;
                    pending.CompleteEver = true;
                }
            }
            if (count > 0)
            {
                await SendFrameAsync(Frame(pending, "peer_chunk", ("offset", offset),
                    ("value", pending.Bytes.AsSpan(offset, count).ToArray()), ("bulk", pending.Bytes.Length > 16384)), pending.Kind, pending.Token);
                continue;
            }
            if (complete)
            {
                await SendFrameAsync(Frame(pending, "peer_complete"), pending.Kind, pending.Token);
                continue;
            }
            if (await pending.Changed.WaitAsync((int)Math.Max(1, retryAt - Environment.TickCount64), pending.Token)) continue;
            if (rounds++ >= pending.Limits.Retries) throw Error("Timeout", true, "End-to-end delivery receipt was not received.");
            lock (gate)
            {
                pending.Retried = true;
                UpdateStat(pending.Lane.Key.Peer, pending.Lane.Key.Epoch, pending.Kind, stat => stat with { Retries = stat.Retries + 1 });
            }
            await Open();
            retryDelay = Math.Min(2000, retryDelay * 2);
            retryAt = Environment.TickCount64 + retryDelay;
        }
    }
    private Dictionary<string, object?> Frame(Pending pending, string type, params (string, object?)[] extra)
    {
        var frame = Client.Frame(type, ("to", pending.Lane.Key.Peer), ("targetEpoch", pending.Lane.Key.Epoch),
            ("epoch", pending.Room), ("peer", pending.Source), ("publisherEpoch", pending.SourceEpoch),
            ("topic", pending.Lane.Key.Topic), ("messageId", pending.Id), ("seq", pending.Sequence));
        foreach (var (key, value) in extra) frame[key] = value;
        return frame;
    }
    private Dictionary<string, object?> Reply(Dictionary<string, object?> source, string type, params (string, object?)[] extra)
    {
        var frame = Client.Frame(type, ("to", source["peer"]), ("targetEpoch", source["publisherEpoch"]),
            ("epoch", epoch), ("peer", localPeer), ("publisherEpoch", localGeneration),
            ("topic", source["topic"]), ("messageId", source["messageId"]), ("seq", source["seq"]));
        foreach (var (key, value) in extra) frame[key] = value;
        return frame;
    }
    private async Task SendFrameAsync(Dictionary<string, object?> frame, string kind, CancellationToken ct)
    {
        var bytes = Wire.Encode(frame);
        var priority = frame["type"] is "peer_chunk" ? (kind == "peer_bulk" ? 2 : 1) : 0;
        await scheduler.SendAsync(bytes, priority, ct);
        lock (gate) UpdateStat((string)frame["to"]!, (string)frame["targetEpoch"]!, kind, stat => stat with
        { SentFrames = stat.SentFrames + 1, SentBytes = stat.SentBytes + (ulong)bytes.Length });
    }
    internal async ValueTask<bool> ReceiveAsync(Dictionary<string, object?> frame, int wireBytes)
    {
        var type = frame.GetValueOrDefault("type") as string;
        if (!IsFrame(type)) return false;
        Dictionary<string, object?>? response = null;
        var kind = "peer_message";
        lock (gate)
        {
            if (closed || core == null || !Unsigned(frame, "v", out var version) || version != 1 ||
                !ValidString(frame, "peer", 128) || !ValidString(frame, "topic", 256) ||
                !ValidString(frame, "messageId", 128) || !Unsigned(frame, "seq", out var seq) || seq == 0 ||
                frame.GetValueOrDefault("epoch") as string != epoch || frame.GetValueOrDefault("to") as string != localPeer ||
                frame.GetValueOrDefault("targetEpoch") as string != localGeneration) return true;
            var peer = (string)frame["peer"]!;
            var generation = frame.GetValueOrDefault("publisherEpoch") as string;
            var id = (string)frame["messageId"]!;
            if (active.TryGetValue(id, out var pending) && pending.Lane.Key.Peer == peer &&
                pending.Lane.Key.Epoch == generation && pending.Lane.Key.Topic == (string)frame["topic"]! && pending.Sequence == seq &&
                type is "peer_credit" or "peer_done" or "peer_cancel" or "peer_relay_error")
            {
                kind = pending.Kind;
                CountReceive(peer, generation!, kind, wireBytes);
                if (pending.Done) return true;
                if (type == "peer_done" && !pending.CompleteEver)
                    pending.Error = Error("InvalidReceipt", true, "Receipt arrived before message completion was submitted.");
                else if (type == "peer_done")
                { pending.Done = true; pending.ReceiptRtt = Stopwatch.GetElapsedTime(pending.Opened).TotalMilliseconds; }
                else if (type is "peer_cancel" or "peer_relay_error")
                    pending.Error = Error(frame.GetValueOrDefault("code") as string ?? "RemoteError", true,
                        frame.GetValueOrDefault("message") as string ?? "Remote endpoint rejected delivery.");
                else if (!Unsigned(frame, "offset", out var offset) || !Unsigned(frame, "limit", out var limit) ||
                    offset > (ulong)pending.MaxSent || limit < offset || limit > (ulong)pending.Bytes.Length ||
                    limit - offset > (ulong)pending.Limits.WindowBytes)
                    pending.Error = Error("InvalidCredit", true, "Receiver sent invalid cumulative credit.");
                else if ((int)offset >= pending.Ack)
                {
                    if ((int)offset > pending.Ack) pending.Progress++;
                    pending.Ack = (int)offset;
                    pending.Limit = Math.Max(pending.Limit, (int)limit);
                    if (frame.GetValueOrDefault("retry") is true)
                    { pending.Sent = Math.Max(pending.Ack, (int)offset); pending.CompleteSent = false; }
                    else pending.Sent = Math.Max(pending.Sent, pending.Ack);
                }
                else return true;
                pending.Signal();
                return true;
            }
            if (!members.TryGetValue(peer, out var member) || member.Epoch != generation || member.Limits == null) return true;
            var key = (peer, generation!, (string)frame["topic"]!);
            if (sendLanes.TryGetValue(key, out var completedSend) && completedSend.LastId == id &&
                completedSend.Sequence == seq && type is "peer_credit" or "peer_done" or "peer_cancel" or "peer_relay_error")
            {
                CountReceive(peer, generation!, completedSend.LastKind, wireBytes);
                return true;
            }
            if (type is "peer_done" or "peer_credit" or "peer_relay_error") return true;
            receiveLanes.TryGetValue(key, out var lane);
            kind = lane?.Active?.Kind ?? (lane?.Size > 16384 || type == "peer_chunk" && frame.GetValueOrDefault("bulk") is true ||
                type == "peer_open" && Unsigned(frame, "size", out var announcedSize) && announcedSize > 16384 ? "peer_bulk" : "peer_message");
            CountReceive(peer, generation!, kind, wireBytes);
            var limits = options.Min(core).Min(member.Limits);
            response = ReceiveLocked(frame, type!, seq, key, lane, limits);
        }
        if (response != null)
        {
            try { await SendFrameAsync(response, kind, stop.Token); }
            catch (Exception) when (stop.IsCancellationRequested) { }
        }
        return true;
    }
    private Dictionary<string, object?>? ReceiveLocked(Dictionary<string, object?> frame, string type, ulong seq,
        (string, string, string) key, ReceiveLane? lane, PeerDeliveryOptions limits)
    {
        var id = (string)frame["messageId"]!;
        if (lane != null && seq <= lane.HighWater)
        {
            if (seq == lane.HighWater && (id != lane.Id || type == "peer_open" &&
                (!Unsigned(frame, "size", out var oldSize) || oldSize != (ulong)lane.Size || frame.GetValueOrDefault("sha256") as string != lane.Hash)))
                return Reply(frame, "peer_cancel", ("code", "MetadataMismatch"));
            return Reply(frame, "peer_done");
        }
        if (lane?.Failed == true) return Reply(frame, "peer_cancel", ("code", "LaneFailed"));
        if (type == "peer_cancel" && lane?.Active == null)
        {
            if (lane == null)
            {
                if (sendLanes.Count + receiveLanes.Count >= options.Lanes) return Reply(frame, "peer_cancel", ("code", "Capacity"));
                receiveLanes.Add(key, lane = new ReceiveLane());
            }
            lane.Failed = true;
            return null;
        }
        if (type != "peer_open" && lane?.Active == null) return Reply(frame, "peer_cancel", ("code", "UnknownTransfer"));
        if (lane == null)
        {
            if (sendLanes.Count + receiveLanes.Count >= options.Lanes) return Reply(frame, "peer_cancel", ("code", "Capacity"));
            receiveLanes.Add(key, lane = new ReceiveLane());
        }
        Dictionary<string, object?> Reject(string code)
        {
            ReleaseIncoming(lane);
            lane.Failed = true;
            return Reply(frame, "peer_cancel", ("code", code));
        }
        if (seq != lane.HighWater + 1) return Reject("InvalidSequence");
        if (type == "peer_cancel") { ReleaseIncoming(lane); lane.Failed = true; return null; }
        if (type == "peer_open")
        {
            if (!Unsigned(frame, "size", out var size) || size == 0 || size > (ulong)limits.MessageBytes ||
                frame.GetValueOrDefault("sha256") is not string hash || hash.Length != 64 ||
                hash.Any(ch => ch is not (>= '0' and <= '9') and not (>= 'a' and <= 'f'))) return Reject("InvalidOpen");
            if (lane.Active != null)
            {
                if ((string)lane.Active.Open["messageId"]! != id || lane.Active.Bytes.Length != (int)size || lane.Active.Hash != hash)
                    return Reject("MetadataMismatch");
                return Credit(lane.Active);
            }
            var bulk = size > 16384;
            if (receiving >= options.Concurrent || size > (ulong)Math.Max(0, options.ReceiveBytes - receivedBytes) ||
                bulk && (bulkReceiving >= Math.Max(1, options.Concurrent - 1) ||
                    size > (ulong)Math.Max(0, options.ReceiveBytes - 16384L - bulkReceivedBytes)))
                return Reject("Capacity");
            var incoming = new Incoming(frame, new byte[(int)size], hash, limits);
            lane.Active = incoming;
            receiving++; receivedBytes += (long)size;
            if (bulk) { bulkReceiving++; bulkReceivedBytes += (long)size; }
            _ = ExpireAsync(lane, incoming);
            return Credit(incoming);
        }
        var transfer = lane.Active!;
        if ((string)transfer.Open["messageId"]! != id) return Reject("MetadataMismatch");
        if (type == "peer_chunk")
        {
            if (!Unsigned(frame, "offset", out var offset) || offset > (ulong)transfer.Bytes.Length ||
                frame.GetValueOrDefault("value") is not byte[] bytes || bytes.Length == 0 || bytes.Length > transfer.Limits.ChunkBytes ||
                frame.GetValueOrDefault("bulk") is not bool bulk || bulk != (transfer.Bytes.Length > 16384) ||
                (ulong)bytes.Length > (ulong)transfer.Bytes.Length - offset) return Reject("InvalidChunk");
            if (offset > (ulong)transfer.Offset) return Credit(transfer, true);
            if (offset < (ulong)transfer.Offset)
            {
                if (offset + (ulong)bytes.Length <= (ulong)transfer.Offset &&
                    transfer.Bytes.AsSpan((int)offset, bytes.Length).SequenceEqual(bytes)) return Credit(transfer);
                return Credit(transfer, true);
            }
            if (bytes.Length > transfer.Limit - transfer.Offset) return Reject("CreditExceeded");
            bytes.CopyTo(transfer.Bytes, transfer.Offset);
            transfer.Offset += bytes.Length;
            transfer.Limit = (int)Math.Min(transfer.Bytes.Length, (long)transfer.Offset + transfer.Limits.WindowBytes);
            return Credit(transfer);
        }
        if (type != "peer_complete") return Reject("InvalidType");
        if (transfer.Offset != transfer.Bytes.Length) return Credit(transfer, true);
        if (Convert.ToHexStringLower(SHA256.HashData(transfer.Bytes)) != transfer.Hash) return Reject("HashMismatch");
        object? value;
        try { value = Wire.DecodeValue(transfer.Bytes); }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        { return Reject("InvalidValue"); }
        var message = Client.Frame("peer_message", ("peer", frame["peer"]), ("publisherEpoch", frame["publisherEpoch"]),
            ("targetEpoch", localGeneration), ("epoch", epoch), ("topic", frame["topic"]),
            ("messageId", id), ("seq", seq), ("value", value));
        if (!admit(message, transfer.Bytes.Length > 16384)) return Reject("Capacity");
        lane.HighWater = seq; lane.Id = id; lane.Hash = transfer.Hash; lane.Size = transfer.Bytes.Length;
        UpdateStat(key.Item1, key.Item2, transfer.Kind, stat => stat with { ReceivedMessages = stat.ReceivedMessages + 1 });
        ReleaseIncoming(lane);
        return Reply(frame, "peer_done");
    }
    private Dictionary<string, object?> Credit(Incoming transfer, bool retry = false)
        => Reply(transfer.Open, "peer_credit", ("offset", transfer.Offset), ("limit", transfer.Limit), ("retry", retry));
    private void ReleaseIncoming(ReceiveLane lane)
    {
        if (lane.Active is not { } incoming) return;
        lane.Active = null;
        incoming.Expiry.Cancel();
        receiving--; receivedBytes -= incoming.Bytes.Length;
        if (incoming.Bytes.Length > 16384) { bulkReceiving--; bulkReceivedBytes -= incoming.Bytes.Length; }
    }
    private async Task ExpireAsync(ReceiveLane lane, Incoming incoming)
    {
        try
        {
            await Task.Delay(options.AssemblyMs, incoming.Expiry.Token);
            Dictionary<string, object?> cancel;
            lock (gate)
            {
                if (lane.Active != incoming) return;
                ReleaseIncoming(lane); lane.Failed = true;
                cancel = Reply(incoming.Open, "peer_cancel", ("code", "Timeout"));
            }
            await SendFrameAsync(cancel, incoming.Kind, lifetime);
        }
        catch (OperationCanceledException) { }
        catch (IOException) { }
        finally { incoming.Expiry.Dispose(); }
    }
    private static readonly UTF8Encoding IdentityEncoding = new(false, true);
    private static bool ValidIdentity(string? text, int max)
    {
        if (string.IsNullOrEmpty(text)) return false;
        try { return IdentityEncoding.GetByteCount(text) <= max; }
        catch (EncoderFallbackException) { return false; }
    }
    private static bool ValidString(Dictionary<string, object?> frame, string key, int max)
        => frame.GetValueOrDefault(key) is string text && ValidIdentity(text, max);
    private static bool Unsigned(Dictionary<string, object?> frame, string key, out ulong value)
        => Fragmentation.TryUnsigned(frame.GetValueOrDefault(key), out value);
    private static DeliveryException Error(string code, bool started, string message)
        => new(code switch
        {
            "Unsupported" => "UnsupportedCapability",
            "PeerMissing" or "PeerLeft" or "PeerNotFound" or "TargetChanged" or "GenerationChanged" or "StaleGeneration" => "PeerUnavailable",
            "Capacity" => "Backpressure",
            "DeadlineExceeded" => "Timeout",
            "TransportClosed" => "Closed",
            "TransportError" => "TransportFailed",
            "UnsupportedCapability" or "PeerUnavailable" or "InvalidArgument" or "InvalidValue" or "PayloadTooLarge" or
                "Backpressure" or "Timeout" or "Cancelled" or "Closed" or "TransportFailed" or "LaneFailed" or "ProtocolError" => code,
            _ => "ProtocolError"
        }, started ? "unknown" : "not-sent", message);
    private string CancellationCode(Pending pending) => closed ? "Closed" : pending.Caller.IsCancellationRequested ? "Cancelled" : "Timeout";
    private void UpdateStat(string peer, string generation, string kind, Func<DeliveryStat, DeliveryStat> update)
    {
        if (closed || !members.TryGetValue(peer, out var member) || member.Epoch != generation) return;
        var key = (peer, generation, kind);
        if (!stats.TryGetValue(key, out var stat))
            stat = new(peer, generation, "relay", kind, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "local-adapter-unavailable", null);
        stats[key] = update(stat);
    }
    private void CountReceive(string peer, string generation, string kind, int bytes)
        => UpdateStat(peer, generation, kind, stat => stat with { ReceivedFrames = stat.ReceivedFrames + 1, ReceivedBytes = stat.ReceivedBytes + (ulong)bytes });
    internal void RealtimeSent(RouteInfo[] recipients, string kind, int bytes)
    {
        lock (gate)
            foreach (var recipient in recipients)
                UpdateStat(recipient.Peer, recipient.PublisherEpoch, kind, stat => stat with
                { SentFrames = stat.SentFrames + 1, SentMessages = stat.SentMessages + 1, SentBytes = stat.SentBytes + (ulong)bytes });
    }
    internal void RealtimeReceived(Dictionary<string, object?> frame, int bytes)
    {
        if (frame.GetValueOrDefault("peer") is not string peer || frame.GetValueOrDefault("publisherEpoch") is not string generation) return;
        lock (gate) UpdateStat(peer, generation, (string)frame["type"]!, stat => stat with
        { ReceivedFrames = stat.ReceivedFrames + 1, ReceivedMessages = stat.ReceivedMessages + 1, ReceivedBytes = stat.ReceivedBytes + (ulong)bytes });
    }
    internal void Close()
    {
        lock (gate)
        {
            if (closed) return;
            closed = true;
            stop.Cancel();
            foreach (var lane in receiveLanes.Values) ReleaseIncoming(lane);
            members.Clear(); stats.Clear(); receiveLanes.Clear();
        }
    }
    public async ValueTask DisposeAsync()
    {
        Close();
        await scheduler.DisposeAsync();
        Task[] pendingWorkers;
        lock (gate) pendingWorkers = workers.ToArray();
        await Task.WhenAll(pendingWorkers);
        lock (gate) sendLanes.Clear();
        stop.Dispose();
    }
}
