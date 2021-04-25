namespace DiskQueue.Tests
{
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

            var subject = await PersistentQueue.Create("queue_a", Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false);
            var t1 = Task.Run(
                async () =>
                {
                    for (var i = 0; i < target; i++)
                    {
                        using var session = subject.OpenSession();
                        await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                        Interlocked.Increment(ref t1S);
                        Thread.Sleep(rnd.Next(0, 100));
                        await session.Flush().ConfigureAwait(false);
                    }
                });
            var t2 = Task.Run(
                async () =>
                {
                    for (var i = 0; i < target; i++)
                    {
                        using var session = subject.OpenSession();
                        await session.Dequeue(CancellationToken.None).ConfigureAwait(false);
                        Interlocked.Increment(ref t2S);
                        Thread.Sleep(rnd.Next(0, 100));
                        await session.Flush().ConfigureAwait(false);
                    }
                });

            //t1.Start();
            //t2.Start();

            await t1.ConfigureAwait(false);
            await t2.ConfigureAwait(false);
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
                        await using var subject = await PersistentQueue.Create("queue_b", Substitute.For<ILogger<IPersistentQueue>>(), TimeSpan.FromSeconds(10))
                            .ConfigureAwait(false);
                        using var session = subject.OpenSession();
                        await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                        Interlocked.Increment(ref t1S);
                        await session.Flush().ConfigureAwait(false);
                    }
                });
            var t2 = Task.Run(
                async () =>
                {
                    for (var i = 0; i < target; i++)
                    {
                        using var source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                        var subject = await PersistentQueue
                            .Create("queue_b", Substitute.For<ILogger<IPersistentQueue>>(), cancellationToken: source.Token)
                            .ConfigureAwait(false);
                        using var session = subject.OpenSession();
                        await session.Dequeue(CancellationToken.None).ConfigureAwait(false);
                        Interlocked.Increment(ref t2S);
                        await session.Flush(source.Token).ConfigureAwait(false);
                        await subject.DisposeAsync().ConfigureAwait(false);
                    }
                });

            await t1.ConfigureAwait(false);
            await t2.ConfigureAwait(false);

            Assert.True(t1S == target);
            Assert.True(t2S == target);
        }
    }
}
