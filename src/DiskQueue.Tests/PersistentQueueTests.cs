using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
// ReSharper disable PossibleNullReferenceException

namespace DiskQueue.Tests
{
    using System.Threading.Tasks;

    [TestFixture]
    public class PersistentQueueTests : PersistentQueueTestsBase
    {
        [Test]
        public void Only_single_instance_of_queue_can_exists_at_any_one_time()
        {
            var invalidOperationException = Assert.Throws<InvalidOperationException>(
                () =>
                {
                    using (new PersistentQueue(Path))
                    {
                        // ReSharper disable once ObjectCreationAsStatement
                        new PersistentQueue(Path);
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

            using (new PersistentQueue(Path))
            {
                Assert.Pass();
            }
        }

        [Test]
        public void Can_create_new_queue()
        {
            new PersistentQueue(Path).Dispose();
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

            var invalidOperationException = Assert.Throws<InvalidOperationException>(
                () =>
                {
                    // ReSharper disable once ObjectCreationAsStatement
                    new PersistentQueue(Path);
                });

            Assert.That(
                invalidOperationException.Message,
                Is.EqualTo(
                    "Unexpected data in transaction log. Expected to get transaction separator but got unknown data. Tx #1"));
        }

        [Test]
        public void Dequeing_from_empty_queue_will_return_null()
        {
            using var queue = new PersistentQueue(Path);
            using var session = queue.OpenSession();
            Assert.IsNull(session.Dequeue());
        }

        [Test]
        public async Task Can_enqueue_data_in_queue()
        {
            using var queue = new PersistentQueue(Path);
            using var session = queue.OpenSession();
            await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
            await session.Flush().ConfigureAwait(false);
        }

        [Test]
        public async Task Can_dequeue_data_from_queue()
        {
            using var queue = new PersistentQueue(Path);
            using var session = queue.OpenSession();
            await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
            await session.Flush().ConfigureAwait(false);
            CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, session.Dequeue());
        }

        [Test]
        public async Task Can_enqueue_and_dequeue_data_after_restarting_queue()
        {
            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, session.Dequeue());
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task After_dequeue_from_queue_item_no_longer_on_queue()
        {
            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, session.Dequeue());
                Assert.IsNull(session.Dequeue());
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task After_dequeue_from_queue_item_no_longer_on_queue_with_queues_restarts()
        {
            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, session.Dequeue());
                await session.Flush().ConfigureAwait(false);
            }

            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                Assert.IsNull(session.Dequeue());
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Not_flushing_the_session_will_revert_dequeued_items()
        {
            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, session.Dequeue());
                //Explicitly omitted: session.Flush();
            }

            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, session.Dequeue());
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Not_flushing_the_session_will_revert_dequeued_items_two_sessions_same_queue()
        {
            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            using (var queue = new PersistentQueue(Path))
            using (var session2 = queue.OpenSession())
            {
                using (var session1 = queue.OpenSession())
                {
                    CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, session1.Dequeue());
                    //Explicitly omitted: session.Flush();
                }

                CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, session2.Dequeue());
                await session2.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Two_sessions_off_the_same_queue_cannot_get_same_item()
        {
            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            using (var queue = new PersistentQueue(Path))
            using (var session2 = queue.OpenSession())
            using (var session1 = queue.OpenSession())
            {
                CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, session1.Dequeue());
                Assert.IsNull(session2.Dequeue());
            }
        }

        [Test]
        public async Task Items_are_reverted_in_their_original_order()
        {
            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1 }).ConfigureAwait(false);
                await session.Enqueue(new byte[] { 2 }).ConfigureAwait(false);
                await session.Enqueue(new byte[] { 3 }).ConfigureAwait(false);
                await session.Enqueue(new byte[] { 4 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            for (int i = 0; i < 4; i++)
            {
                using var queue = new PersistentQueue(Path);
                using var session = queue.OpenSession();
                CollectionAssert.AreEqual(new byte[] { 1 }, session.Dequeue(), "Incorrect order on turn " + (i + 1));
                CollectionAssert.AreEqual(new byte[] { 2 }, session.Dequeue(), "Incorrect order on turn " + (i + 1));
                CollectionAssert.AreEqual(new byte[] { 3 }, session.Dequeue(), "Incorrect order on turn " + (i + 1));
                // Dispose without `session.Flush();`
            }
        }
    }
}
