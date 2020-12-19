using System;
using System.Threading;
using NUnit.Framework;
// ReSharper disable PossibleNullReferenceException

namespace DiskQueue.Tests
{
    using System.Threading.Tasks;

    [TestFixture]
    public class ThreadSafeAccessTests
    {
        [Test]
        public async Task Can_enqueue_and_dequeue_on_separate_threads()
        {
            int t1s, t2s;
            t1s = t2s = 0;
            const int target = 100;
            var rnd = new Random();


            IPersistentQueue _subject = new PersistentQueue("queue_a");
            var t1 = Task.Run(
                async () =>
                {
                    for (int i = 0; i < target; i++)
                    {
                        using var session = _subject.OpenSession();
                        Console.Write("(");
                        await session.Enqueue(new byte[] {1, 2, 3, 4}).ConfigureAwait(false);
                        Interlocked.Increment(ref t1s);
                        Thread.Sleep(rnd.Next(0, 100));
                        await session.Flush().ConfigureAwait(false);
                        Console.Write(")");
                    }
                });
            var t2 = Task.Run(
                async () =>
                {
                    for (int i = 0; i < target; i++)
                    {
                        using var session = _subject.OpenSession();
                        Console.Write("<");
                        session.Dequeue();
                        Interlocked.Increment(ref t2s);
                        Thread.Sleep(rnd.Next(0, 100));
                        await session.Flush().ConfigureAwait(false);
                        Console.Write(">");
                    }
                });

            //t1.Start();
            //t2.Start();

            await t1;
            await t2;
            Assert.That(t1s, Is.EqualTo(target));
            Assert.That(t2s, Is.EqualTo(target));
        }

        [Test]
        public async Task Can_sequence_queues_on_separate_threads()
        {
            int t1s, t2s;
            t1s = t2s = 0;
            const int target = 100;

            var t1 = Task.Run(
                async () =>
                {
                    for (int i = 0; i < target; i++)
                    {
                        using var subject = PersistentQueue.WaitFor("queue_b", TimeSpan.FromSeconds(10));
                        using (var session = subject.OpenSession())
                        {
                            Console.Write("(");
                            await session.Enqueue(new byte[] {1, 2, 3, 4}).ConfigureAwait(false);
                            Interlocked.Increment(ref t1s);
                            await session.Flush().ConfigureAwait(false);
                            Console.Write(")");
                        }

                        Thread.Sleep(0);
                    }
                });
            var t2 = Task.Run(
                async () =>
                {
                    for (int i = 0; i < target; i++)
                    {
                        using var subject = PersistentQueue.WaitFor("queue_b", TimeSpan.FromSeconds(10));
                        using (var session = subject.OpenSession())
                        {
                            Console.Write("<");
                            session.Dequeue();
                            Interlocked.Increment(ref t2s);
                            await session.Flush().ConfigureAwait(false);
                            Console.Write(">");
                        }

                        Thread.Sleep(0);
                    }
                });

            await t1;
            await t2;

            Assert.That(t1s, Is.EqualTo(target));
            Assert.That(t2s, Is.EqualTo(target));
        }
    }
}
