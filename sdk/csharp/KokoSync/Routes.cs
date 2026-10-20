namespace KokoSync;

/// <summary>A snapshot of one peer's latest/live transport route and submission/reception counters.</summary>
public sealed record RouteInfo(string Peer, string PublisherEpoch, string Route, string Reason,
    double? RttMs, ulong SentDirect, ulong ReceivedDirect, ulong SentRelay, ulong ReceivedRelay)
{
    internal Dictionary<string, object?> Event(bool removed) => Client.Frame("route",
        ("peer", Peer), ("publisherEpoch", PublisherEpoch), ("route", Route), ("reason", Reason),
        ("rttMs", RttMs), ("sentDirect", SentDirect), ("receivedDirect", ReceivedDirect),
        ("sentRelay", SentRelay), ("receivedRelay", ReceivedRelay), ("removed", removed));
}

internal sealed class RelayRoutes
{
    private readonly object gate = new();
    private readonly Dictionary<string, RouteInfo> peers = new(StringComparer.Ordinal);
    private readonly HashSet<(string Peer, string Epoch)> retired = new();
    private readonly Queue<(string Peer, string Epoch)> retirementOrder = new();
    private string? epoch;
    private string? localPeer;
    private string? localPublisherEpoch;
    private bool membership;
    internal IReadOnlyList<RouteInfo> Snapshot()
    {
        lock (gate) return peers.Values.OrderBy(route => route.Peer, StringComparer.Ordinal).ToArray();
    }
    internal void Join(Dictionary<string, object?> joined)
    {
        lock (gate)
        {
            Clear();
            epoch = joined.GetValueOrDefault("epoch") as string;
            localPeer = joined.GetValueOrDefault("peer") as string;
            localPublisherEpoch = joined.GetValueOrDefault("publisherEpoch") as string;
            membership = epoch != null && joined.GetValueOrDefault("peers") is object?[];
            if (!membership) return; // Older servers do not expose an authoritative roster.
            foreach (var value in (object?[])joined["peers"]!)
                if (value is Dictionary<string, object?> peer) Add(peer);
        }
    }
    internal (RouteInfo Route, bool Removed)? Update(Dictionary<string, object?> frame)
    {
        lock (gate)
        {
            if (!membership || frame.GetValueOrDefault("epoch") as string != epoch ||
                frame.GetValueOrDefault("peer") is not string peer ||
                frame.GetValueOrDefault("publisherEpoch") is not string generation || peer == localPeer)
                return null;
            if (frame.GetValueOrDefault("type") is "peer_joined")
            {
                // The core requires the matching departure before reusing an occupied peer ID.
                if (peers.ContainsKey(peer) || retired.Contains((peer, generation))) return null;
                var added = Add(frame);
                return added == null ? null : (added, false);
            }
            if (!peers.TryGetValue(peer, out var route) || route.PublisherEpoch != generation) return null;
            peers.Remove(peer);
            if (retired.Add((peer, generation))) retirementOrder.Enqueue((peer, generation));
            if (retirementOrder.Count > 256) retired.Remove(retirementOrder.Dequeue());
            return (route, true);
        }
    }
    private RouteInfo? Add(Dictionary<string, object?> member)
    {
        if (member.GetValueOrDefault("peer") is not string peer || peer.Length == 0 || peer == localPeer ||
            member.GetValueOrDefault("publisherEpoch") is not string generation || generation.Length == 0 ||
            peers.ContainsKey(peer)) return null;
        if (peers.Count >= 31) throw new IOException("Membership roster exceeds the room peer limit.");
        var route = new RouteInfo(peer, generation, "relay", "local-adapter-unavailable", null, 0, 0, 0, 0);
        peers.Add(peer, route);
        return route;
    }
    internal RouteInfo[] Recipients(IReadOnlyDictionary<string, object?> frame)
    {
        lock (gate)
        {
            var hasTo = frame.TryGetValue("to", out var to);
            var hasExclude = frame.TryGetValue("exclude", out var exclude);
            if (hasTo && hasExclude) return [];
            var value = hasTo ? to : exclude;
            HashSet<string>? selector = null;
            if (hasTo || hasExclude)
            {
                if (value is not object?[] values || values.Length > 32 || values.Any(item => item is not string)) return [];
                selector = new HashSet<string>(values.Cast<string>(), StringComparer.Ordinal);
                if (selector.Count != values.Length || selector.Any(peer => peer != localPeer && !peers.ContainsKey(peer))) return [];
            }
            return peers.Values.Where(route => selector == null ||
                (hasTo ? selector.Contains(route.Peer) : !selector.Contains(route.Peer))).ToArray();
        }
    }
    internal void Sent(RouteInfo[] recipients)
    {
        lock (gate)
            foreach (var recipient in recipients)
                if (peers.TryGetValue(recipient.Peer, out var current) && current.PublisherEpoch == recipient.PublisherEpoch)
                    peers[recipient.Peer] = current with { SentRelay = Increment(current.SentRelay) };
    }
    internal bool Received(Dictionary<string, object?> frame)
    {
        lock (gate)
        {
            if (!membership) return true;
            if (frame.TryGetValue("epoch", out var roomEpoch) && roomEpoch as string != epoch) return false;
            if (frame.GetValueOrDefault("peer") is not string peer ||
                frame.GetValueOrDefault("publisherEpoch") is not string generation) return false;
            if (peer == localPeer) return generation == localPublisherEpoch;
            if (!peers.TryGetValue(peer, out var current) || current.PublisherEpoch != generation) return false;
            peers[peer] = current with { ReceivedRelay = Increment(current.ReceivedRelay) };
            return true;
        }
    }
    private static ulong Increment(ulong value) => value == ulong.MaxValue ? value : value + 1;
    internal void Clear()
    {
        lock (gate)
        {
            peers.Clear();
            retired.Clear();
            retirementOrder.Clear();
            epoch = localPeer = localPublisherEpoch = null;
            membership = false;
        }
    }
}
