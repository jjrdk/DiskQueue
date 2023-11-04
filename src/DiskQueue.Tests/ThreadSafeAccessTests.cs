namespace DiskQueue.Tests;

using System;
using System.Threading;
using System.Threading.Tasks;
using AsyncDiskQueue;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

public class ThreadSafeAccessTests
{
    [Fact]
    public async Task Can_enqueue_and_dequeue_on_separate_threads()
    {
        int t1S, t2S;
        t1S = t2S = 0;
        const int target = 100;
        var rnd = new Random(DateTimeOffset.Now.Millisecond);

        var subject = await PersistentQueue.Create("queue_a", Substitute.For<ILogger<PersistentQueue>>());
        var t1 = Task.Run(
            async () =>
            {
                var data = new byte[] { 1, 2, 3, 4 };
                for (var i = 0; i < target; i++)
                {
                    using var session = subject.OpenSession();
                    await session.Enqueue(data);
                    Interlocked.Increment(ref t1S);
                    await session.Flush();
                }
            });
        var t2 = Task.Run(
            async () =>
            {
                for (var i = 0; i < target; i++)
                {
                    using var session = subject.OpenSession();
                    await session.Dequeue(CancellationToken.None);
                    Interlocked.Increment(ref t2S);
                    await session.Flush();
                }
            });

        await t1;
        await t2;
        Assert.Equal(target, t1S);
        Assert.Equal(target, t2S);
    }

    [Fact]
    public async Task Can_sequence_queues_on_separate_threads()
    {
        int t1S, t2S;
        t1S = t2S = 0;
        const int target = 100;

        var t1 = Task.Run(
            async () =>
            {
                for (var i = 0; i < target; i++)
                {
                    await using var subject = await PersistentQueue.Create("queue_b", Substitute.For<ILogger<PersistentQueue>>(), TimeSpan.FromSeconds(10))
                        ;
                    using var session = subject.OpenSession();
                    await session.Enqueue(new byte[] { 1, 2, 3, 4 });
                    Interlocked.Increment(ref t1S);
                    await session.Flush();
                }
            });
        var t2 = Task.Run(
            async () =>
            {
                for (var i = 0; i < target; i++)
                {
                    using var source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                    var subject = await PersistentQueue
                        .Create("queue_b", Substitute.For<ILogger<PersistentQueue>>(), cancellationToken: source.Token)
                        ;
                    using var session = subject.OpenSession();
                    await session.Dequeue(CancellationToken.None);
                    Interlocked.Increment(ref t2S);
                    await session.Flush(source.Token);
                    await subject.DisposeAsync();
                }
            });

        await t1;
        await t2;

        Assert.True(t1S == target);
        Assert.True(t2S == target);
    }
}
