namespace DiskQueue.Tests
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Threading;
    using System.Threading.Tasks;
    using AsyncDiskQueue;
    using Microsoft.Extensions.Logging;
    using NSubstitute;
    using Xunit;

    public class MultipleProcessAccessTests
    {
        [Fact,
         Description(
             "Multiple PersistentQueue instances are "
             + "pretty much the same as multiple processes to "
             + "the DiskQueue library")]
        public void Can_access_from_multiple_queues_if_used_carefully()
        {
            var received = new List<byte[]>();
            var numberOfItems = 10;

            var waitHandle = new ManualResetEvent(false);
            _ = Task.Run(
                async () =>
                {
                    for (var i = 0; i < numberOfItems; i++)
                    {
                        await AddToQueue(new byte[] {1, 2, 3}).ConfigureAwait(false);
                    }

                    waitHandle.Set();
                });

            waitHandle.WaitOne();
            waitHandle.Reset();

            _ = Task.Run(
                async () =>
                {
                    while (received.Count < numberOfItems)
                    {
                        var data = await ReadQueue().ConfigureAwait(false);
                        if (data != null) received.Add(data);
                    }

                    waitHandle.Set();
                });

            var ok = waitHandle.WaitOne();

            Assert.True(ok, "Did not receive all data in time");
            Assert.Equal(numberOfItems, received.Count);
        }

        static async Task AddToQueue(byte[] data)
        {
            await Task.Delay(150).ConfigureAwait(false);
            await using var queue = await PersistentQueue.Create(
                    SharedStorage,
                    Substitute.For<ILoggerFactory>(),
                    TimeSpan.FromSeconds(30))
                .ConfigureAwait(false);
            using var session = queue.OpenSession();
            await session.Enqueue(data).ConfigureAwait(false);
            await session.Flush().ConfigureAwait(false);
        }

        static async Task<byte[]> ReadQueue()
        {
            await Task.Delay(150).ConfigureAwait(false);
            await using var queue = await PersistentQueue.Create(
                    SharedStorage,
                    Substitute.For<ILoggerFactory>(),
                    TimeSpan.FromSeconds(30))
                .ConfigureAwait(false);
            using var session = queue.OpenSession();
            var data = await session.Dequeue(CancellationToken.None).ConfigureAwait(false);
            await session.Flush().ConfigureAwait(false);
            return data;
        }

        private static string SharedStorage
        {
            get { return "./MultipleAccess"; }
        }
    }
}
