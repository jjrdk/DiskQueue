using System;
using System.Collections.Generic;
using System.Threading;
using NUnit.Framework;
// ReSharper disable PossibleNullReferenceException

namespace DiskQueue.Tests
{
    using System.Threading.Tasks;

    [TestFixture]
    public class MultipleProcessAccessTests
    {
        [Test,
        Description("Multiple PersistentQueue instances are " +
                    "pretty much the same as multiple processes to " +
                    "the DiskQueue library")]
        public void Can_access_from_multiple_queues_if_used_carefully()
        {
            var received = new List<byte[]>();
            int numberOfItems = 10;

            var waitHandle = new ManualResetEvent(false);
            var t1 = new Thread(async () =>
            {
                for (int i = 0; i < numberOfItems; i++)
                {
                    await AddToQueue(new byte[] { 1, 2, 3 }).ConfigureAwait(false);
                }

                waitHandle.Set();
            });
            var t2 = new Thread(async () =>
            {
                while (received.Count < numberOfItems)
                {
                    var data = await ReadQueue().ConfigureAwait(false);
                    if (data != null) received.Add(data);
                }

                waitHandle.Set();
            });

            t1.Start();
            waitHandle.WaitOne();
            waitHandle.Reset();

            t2.Start();

            var ok = waitHandle.WaitOne();

            if (!ok)
            {
                t2.Abort();
                Assert.Fail("Did not receive all data in time");
            }
            Assert.That(received.Count, Is.EqualTo(numberOfItems), "received items");
        }

        async Task AddToQueue(byte[] data)
        {
            Thread.Sleep(150);
            using var queue = PersistentQueue.WaitFor(SharedStorage, TimeSpan.FromSeconds(30));
            using var session = queue.OpenSession();
            await session.Enqueue(data).ConfigureAwait(false);
            await session.Flush().ConfigureAwait(false);
        }

        async Task<byte[]> ReadQueue()
        {
            Thread.Sleep(150);
            using var queue = PersistentQueue.WaitFor(SharedStorage, TimeSpan.FromSeconds(30));
            using var session = queue.OpenSession();
            var data = session.Dequeue();
            await session.Flush().ConfigureAwait(false);
            return data;
        }

        private string SharedStorage => "./MultipleAccess";
    }
}