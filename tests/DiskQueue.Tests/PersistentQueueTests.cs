using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
// ReSharper disable PossibleNullReferenceException

namespace DiskQueue.Tests
{
    using System.Threading;
    using System.Threading.Tasks;
    using AsyncDiskQueue;
    using AsyncDiskQueue.Implementation;
    using Microsoft.Extensions.Logging;
    using NSubstitute;

    [TestFixture]
    public class AsyncEnumerableTests : PersistentQueueTestsBase
    {
        [Test]
        public async Task WhenEnumeratingOverQueueThenReturnsItems()
        {
            const int count = 5;
            var items = new List<Guid>();
            using var tokenSource = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>(), cancellationToken: tokenSource.Token).ConfigureAwait(false))
            {
                using (var fillSession = queue.OpenSession(g => g.ToByteArray(), b => new Guid(b)))
                {
                    for (var i = 0; i < count; i++)
                    {
                        await fillSession.Enqueue(Guid.NewGuid(), tokenSource.Token).ConfigureAwait(false);
                    }

                    await fillSession.Flush(tokenSource.Token).ConfigureAwait(false);
                }

                using var session = queue.OpenSession(g => g.ToByteArray(), b => new Guid(b));
                try
                {
                    await foreach (var g in session.ToAsyncEnumerable(tokenSource.Token))
                    {
                        items.Add(g);
                    }
                }
                catch (TaskCanceledException) { }
            }

            Assert.AreEqual(count, items.Count);
        }

        [Test]
        public async Task WhenEnumeratingOverEmptyQueueThenReturnsNoItems()
        {
            var items = new List<Guid>();
            using var tokenSource = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>(), cancellationToken: tokenSource.Token).ConfigureAwait(false))
            {
                using var session = queue.OpenSession(g => g.ToByteArray(), b => new Guid(b));
                try
                {
                    await foreach (var g in session.ToAsyncEnumerable(tokenSource.Token))
                    {
                        items.Add(g);
                    }
                }
                catch (TaskCanceledException) { }
            }

            CollectionAssert.IsEmpty(items);
        }
    }

    [TestFixture]
    public class PersistentQueueTests : PersistentQueueTestsBase
    {
        [Test]
        public void Only_single_instance_of_queue_can_exists_at_any_one_time()
        {
            var invalidOperationException = Assert.ThrowsAsync<InvalidOperationException>(
               async () =>
                {
                    await using (await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false))
                    {
                        // ReSharper disable once ObjectCreationAsStatement
                        _ = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false);
                    }
                });

            Assert.That(
                invalidOperationException.Message,
                Is.EqualTo("Another instance of the queue is already in action, or directory does not exists"));
        }

        [Test]
        public async Task If_a_non_running_process_has_a_lock_then_can_start_an_instance()
        {
            Directory.CreateDirectory(Path);
            var lockFilePath = System.IO.Path.Combine(Path, "lock");
            await File.WriteAllTextAsync(lockFilePath, "78924759045").ConfigureAwait(false);

            await using (await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false))
            {
                Assert.Pass();
            }
        }

        [Test]
        public async Task Can_create_new_queue()
        {
            await using var q = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false);
        }

        [Test]
        public async Task Corrupt_index_file_should_throw()
        {
            var buffer = new List<byte>();
            buffer.AddRange(Guid.NewGuid().ToByteArray());
            buffer.AddRange(Guid.NewGuid().ToByteArray());
            buffer.AddRange(Guid.NewGuid().ToByteArray());

            Directory.CreateDirectory(Path);
            await File.WriteAllBytesAsync(System.IO.Path.Combine(Path, "transaction.log"), buffer.ToArray()).ConfigureAwait(false);

            var invalidOperationException = Assert.ThrowsAsync<UnableToSetupException>(
            async () =>
                {
                    // ReSharper disable once ObjectCreationAsStatement
                    _ = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>(), TimeSpan.FromSeconds(10)).ConfigureAwait(false);
                });

            Assert.That(
                invalidOperationException.Message,
                Is.EqualTo(
                    "Unexpected data in transaction log. Expected to get transaction separator but got unknown data. Tx #1"));
        }

        [Test]
        public async Task Dequeing_from_empty_queue_will_return_null()
        {
            await using var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false);
            using var session = queue.OpenSession();
            Assert.IsNull(await session.Dequeue());
        }

        [Test]
        public async Task Can_enqueue_data_in_queue()
        {
            await using var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false);
            using var session = queue.OpenSession();
            await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
            await session.Flush().ConfigureAwait(false);
        }

        [Test]
        public async Task Can_dequeue_data_from_queue()
        {
            await using var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false);
            using var session = queue.OpenSession();
            await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
            await session.Flush().ConfigureAwait(false);
            CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, await session.Dequeue());
        }

        [Test]
        public async Task Can_dequeue_data_from_queue_twice_when_read_not_flushed()
        {
            await using var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false);
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            using (var session = queue.OpenSession())
            {
                CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, await session.Dequeue());
            }

            using (var session = queue.OpenSession())
            {
                CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, await session.Dequeue());
                await session.Flush().ConfigureAwait(false);
            }

            using (var session = queue.OpenSession())
            {
                Assert.IsNull(await session.Dequeue());
            }
        }

        [Test]
        public async Task Can_enqueue_and_dequeue_data_after_restarting_queue()
        {
            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false))
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false))
            using (var session = queue.OpenSession())
            {
                CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, await session.Dequeue());
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task After_dequeue_from_queue_item_no_longer_on_queue()
        {
            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false))
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false))
            using (var session = queue.OpenSession())
            {
                CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, await session.Dequeue());
                Assert.IsNull(await session.Dequeue());
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task After_dequeue_from_queue_item_no_longer_on_queue_with_queues_restarts()
        {
            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false))
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false))
            using (var session = queue.OpenSession())
            {
                CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, await session.Dequeue());
                await session.Flush().ConfigureAwait(false);
            }

            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false))
            using (var session = queue.OpenSession())
            {
                Assert.IsNull(await session.Dequeue());
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Not_flushing_the_session_will_revert_dequeued_items()
        {
            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false))
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false))
            using (var session = queue.OpenSession())
            {
                CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, await session.Dequeue());
                //Explicitly omitted: session.Flush();
            }

            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false))
            using (var session = queue.OpenSession())
            {
                CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, await session.Dequeue());
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Not_flushing_the_session_will_revert_dequeued_items_two_sessions_same_queue()
        {
            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false))
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false))
            using (var session2 = queue.OpenSession())
            {
                using (var session1 = queue.OpenSession())
                {
                    CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, await session1.Dequeue());
                    //Explicitly omitted: session.Flush();
                }

                CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, await session2.Dequeue());
                await session2.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Two_sessions_off_the_same_queue_cannot_get_same_item()
        {
            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false))
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false))
            using (var session2 = queue.OpenSession())
            using (var session1 = queue.OpenSession())
            {
                CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, await session1.Dequeue());
                Assert.IsNull(await session2.Dequeue());
            }
        }

        [Test]
        public async Task Items_are_reverted_in_their_original_order()
        {
            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false))
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1 }).ConfigureAwait(false);
                await session.Enqueue(new byte[] { 2 }).ConfigureAwait(false);
                await session.Enqueue(new byte[] { 3 }).ConfigureAwait(false);
                await session.Enqueue(new byte[] { 4 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            for (var i = 0; i < 4; i++)
            {
                await using var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false);
                using var session = queue.OpenSession();
                CollectionAssert.AreEqual(new byte[] { 1 }, await session.Dequeue(), "Incorrect order on turn " + (i + 1));
                CollectionAssert.AreEqual(new byte[] { 2 }, await session.Dequeue(), "Incorrect order on turn " + (i + 1));
                CollectionAssert.AreEqual(new byte[] { 3 }, await session.Dequeue(), "Incorrect order on turn " + (i + 1));
                // Dispose without `session.Flush();`
            }
        }
    }
}
