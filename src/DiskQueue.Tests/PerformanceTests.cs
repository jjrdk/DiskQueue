using NUnit.Framework;
using System;
using System.Collections.Generic;
// ReSharper disable PossibleNullReferenceException
// ReSharper disable AssignNullToNotNullAttribute

namespace DiskQueue.Tests
{
    using System.Threading.Tasks;
    using Implementation;

    [TestFixture, Explicit]
    public class PerformanceTests : PersistentQueueTestsBase
    {
        [Test,
         Description(
             "With a mid-range SSD, this is some 20x slower than with a single flush (depends on disk speed)")]
        public async Task Enqueue_million_items_with_100_flushes()
        {
            await using var queue = await PersistentQueue.Create(Path).ConfigureAwait(false);
            for (var i = 0; i < 100; i++)
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 10000; j++)
                {
                    await session.Enqueue(Guid.NewGuid().ToByteArray()).ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Enqueue_million_items_with_single_flush()
        {
            await using var queue = await PersistentQueue.Create(Path).ConfigureAwait(false);
            using var session = queue.OpenSession();
            for (var i = 0; i < LargeCount; i++)
            {
                await session.Enqueue(Guid.NewGuid().ToByteArray()).ConfigureAwait(false);
            }

            await session.Flush().ConfigureAwait(false);
        }

        [Test]
        public async Task Enqueue_and_dequeue_million_items_same_queue()
        {
            await using var queue = await PersistentQueue.Create(Path).ConfigureAwait(false);
            using (var session = queue.OpenSession())
            {
                for (var i = 0; i < LargeCount; i++)
                {
                    await session.Enqueue(Guid.NewGuid().ToByteArray()).ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }

            using (var session = queue.OpenSession())
            {
                for (var i = 0; i < LargeCount; i++)
                {
                    _ = new Guid(await session.Dequeue().ConfigureAwait(false));
                }

                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Enqueue_and_dequeue_million_items_restart_queue()
        {
            await using (var queue = await PersistentQueue.Create(Path).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var i = 0; i < LargeCount; i++)
                {
                    await session.Enqueue(Guid.NewGuid().ToByteArray()).ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }

            await using (var queue = await PersistentQueue.Create(Path).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var i = 0; i < LargeCount; i++)
                {
                    _ = new Guid(await session.Dequeue().ConfigureAwait(false));
                }

                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Enqueue_and_dequeue_large_items_with_restart_queue()
        {
            var random = new Random();
            var itemsSizes = new List<int>();
            await using (var queue = await PersistentQueue.Create(Path).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var i = 0; i < SmallCount; i++)
                {
                    var data = new byte[random.Next(1024 * 512, 1024 * 1024)];
                    itemsSizes.Add(data.Length);
                    await session.Enqueue(data).ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }

            await using (var queue = await PersistentQueue.Create(Path).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var i = 0; i < SmallCount; i++)
                {
                    Assert.AreEqual(itemsSizes[i], (await session.Dequeue().ConfigureAwait(false)).Length);
                }

                await session.Flush().ConfigureAwait(false);
            }
        }

        private const int LargeCount = 1000000;
        private const int SmallCount = 500;

    }
}
