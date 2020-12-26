using System;
using System.Threading;
using NUnit.Framework;
// ReSharper disable PossibleNullReferenceException

namespace DiskQueue.Tests
{
    using System.Threading.Tasks;
    using AsyncDiskQueue;
    using Microsoft.Extensions.Logging;
    using NSubstitute;

    [TestFixture]
    public class ThreadSafeAccessTests
    {
        [Test]
        public async Task Can_enqueue_and_dequeue_on_separate_threads()
        {
            int t1S, t2S;
            t1S = t2S = 0;
            const int target = 100;
            var rnd = new Random();

            var subject = await DiskQueue.Create("queue_a", Substitute.For<ILoggerFactory>()).ConfigureAwait(false);
            var t1 = Task.Run(
                async () =>
                {
                    for (var i = 0; i < target; i++)
                    {
                        using var session = subject.OpenSession();
                        Console.Write("(");
                        await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                        Interlocked.Increment(ref t1S);
                        Thread.Sleep(rnd.Next(0, 100));
                        await session.Flush().ConfigureAwait(false);
                        Console.Write(")");
                    }
                });
            var t2 = Task.Run(
                async () =>
                {
                    for (var i = 0; i < target; i++)
                    {
                        using var session = subject.OpenSession();
                        Console.Write("<");
                        await session.Dequeue(CancellationToken.None).ConfigureAwait(false);
                        Interlocked.Increment(ref t2S);
                        Thread.Sleep(rnd.Next(0, 100));
                        await session.Flush().ConfigureAwait(false);
                        Console.Write(">");
                    }
                });

            //t1.Start();
            //t2.Start();

            await t1.ConfigureAwait(false);
            await t2.ConfigureAwait(false);
            Assert.That(t1S, Is.EqualTo(target));
            Assert.That(t2S, Is.EqualTo(target));
        }

        [Test]
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
                        await using var subject = await DiskQueue.Create("queue_b", Substitute.For<ILoggerFactory>(), TimeSpan.FromSeconds(10))
                            .ConfigureAwait(false);
                        using var session = subject.OpenSession();
                        Console.Write("(");
                        await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                        Interlocked.Increment(ref t1S);
                        await session.Flush().ConfigureAwait(false);
                        Console.Write(")");
                    }
                });
            var t2 = Task.Run(
                async () =>
                {
                    for (var i = 0; i < target; i++)
                    {
                        using var source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                       await using var subject = await DiskQueue
                            .Create("queue_b", Substitute.For<ILoggerFactory>(), cancellationToken: source.Token)
                            .ConfigureAwait(false);
                        using var session = subject.OpenSession();
                        Console.Write("<");
                        await session.Dequeue(CancellationToken.None).ConfigureAwait(false);
                        Interlocked.Increment(ref t2S);
                        await session.Flush().ConfigureAwait(false);
                        Console.Write(">");
                    }
                });

            await t1.ConfigureAwait(false);
            await t2.ConfigureAwait(false);

            Assert.That(t1S, Is.EqualTo(target));
            Assert.That(t2S, Is.EqualTo(target));
        }
    }
}
