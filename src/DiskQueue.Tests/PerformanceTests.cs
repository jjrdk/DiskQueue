using NUnit.Framework;
using System;
using System.Collections.Generic;
// ReSharper disable PossibleNullReferenceException
// ReSharper disable AssignNullToNotNullAttribute

namespace DiskQueue.Tests
{
    using System.Threading.Tasks;

    [TestFixture, Explicit]
    public class PerformanceTests : PersistentQueueTestsBase
    {
        [Test,
         Description(
             "With a mid-range SSD, this is some 20x slower " + "than with a single flush (depends on disk speed)")]
        public async Task Enqueue_million_items_with_100_flushes()
        {
            using var queue = new PersistentQueue(Path);
            for (int i = 0; i < 100; i++)
            {
                using var session = queue.OpenSession();
                for (int j = 0; j < 10000; j++)
                {
                    await session.Enqueue(Guid.NewGuid().ToByteArray()).ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Enqueue_million_items_with_single_flush()
        {
            using var queue = new PersistentQueue(Path);
            using var session = queue.OpenSession();
            for (int i = 0; i < largeCount; i++)
            {
                await session.Enqueue(Guid.NewGuid().ToByteArray()).ConfigureAwait(false);
            }

            await session.Flush().ConfigureAwait(false);
        }

        [Test]
        public async Task Enqueue_and_dequeue_million_items_same_queue()
        {
            using var queue = new PersistentQueue(Path);
            using (var session = queue.OpenSession())
            {
                for (int i = 0; i < largeCount; i++)
                {
                    await session.Enqueue(Guid.NewGuid().ToByteArray()).ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }

            using (var session = queue.OpenSession())
            {
                for (int i = 0; i < largeCount; i++)
                {
                    _ = new Guid(session.Dequeue());
                }

                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Enqueue_and_dequeue_million_items_restart_queue()
        {
            using (var queue = new PersistentQueue(Path))
            {
                using var session = queue.OpenSession();
                for (int i = 0; i < largeCount; i++)
                {
                    await session.Enqueue(Guid.NewGuid().ToByteArray()).ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }

            using (var queue = new PersistentQueue(Path))
            {
                using var session = queue.OpenSession();
                for (int i = 0; i < largeCount; i++)
                {
                    _ = new Guid(session.Dequeue());
                }

                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Enqueue_and_dequeue_large_items_with_restart_queue()
        {
            var random = new Random();
            var itemsSizes = new List<int>();
            using (var queue = new PersistentQueue(Path))
            {
                using var session = queue.OpenSession();
                for (int i = 0; i < smallCount; i++)
                {
                    var data = new byte[random.Next(1024 * 512, 1024 * 1024)];
                    itemsSizes.Add(data.Length);
                    await session.Enqueue(data).ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }

            using (var queue = new PersistentQueue(Path))
            {
                using var session = queue.OpenSession();
                for (int i = 0; i < smallCount; i++)
                {
                    Assert.AreEqual(itemsSizes[i], session.Dequeue().Length);
                }

                await session.Flush().ConfigureAwait(false);
            }
        }

        private const int largeCount = 1000000;
        private const int smallCount = 500;

    }
}
