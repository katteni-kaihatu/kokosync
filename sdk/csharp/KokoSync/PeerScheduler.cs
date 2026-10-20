namespace KokoSync;

/// <summary>One physical frame per turn, with control and small-value priority over bulk chunks.</summary>
internal sealed class PeerScheduler : IAsyncDisposable
{
    private sealed record Packet(byte[] Bytes, CancellationToken Token, TaskCompletionSource Done);
    private readonly object gate = new();
    private readonly Queue<Packet>[] queues = [new(), new(), new()];
    private readonly SemaphoreSlim available = new(0);
    private readonly CancellationTokenSource stop = new();
    private readonly ITransport transport;
    private readonly Task worker;
    private int count;
    internal PeerScheduler(ITransport transport)
    {
        this.transport = transport;
        worker = RunAsync();
    }
    internal Task SendAsync(byte[] bytes, int priority, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var packet = new Packet(bytes, ct, new(TaskCreationOptions.RunContinuationsAsynchronously));
        lock (gate)
        {
            if (stop.IsCancellationRequested) throw new IOException("Peer delivery scheduler is closed.");
            if (count >= 128) throw new IOException("Peer delivery frame queue is full.");
            queues[priority].Enqueue(packet);
            count++;
            available.Release();
        }
        return packet.Done.Task;
    }
    private async Task RunAsync()
    {
        try
        {
            while (true)
            {
                await available.WaitAsync(stop.Token);
                Packet packet;
                lock (gate)
                {
                    packet = queues.First(queue => queue.Count != 0).Dequeue();
                    count--;
                }
                try
                {
                    using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(stop.Token, packet.Token);
                    await transport.SendAsync(packet.Bytes, false, cancellation.Token);
                    packet.Done.TrySetResult();
                }
                catch (Exception ex) { packet.Done.TrySetException(ex); }
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            lock (gate)
                foreach (var queue in queues)
                    while (queue.TryDequeue(out var packet)) packet.Done.TrySetException(new IOException("Session closed."));
        }
    }
    public async ValueTask DisposeAsync()
    {
        stop.Cancel();
        await worker;
        stop.Dispose();
    }
}
