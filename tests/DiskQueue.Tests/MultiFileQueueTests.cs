using NUnit.Framework;
using System.IO;

namespace DiskQueue.Tests
{
    using System.Threading.Tasks;
    using AsyncDiskQueue;
    using Microsoft.Extensions.Logging;
    using NSubstitute;

    [TestFixture]
    public class MultiFileQueueTests : PersistentQueueTestsBase
    {
        [Test]
        public async Task Entering_more_than_count_of_items_will_work()
        {
            await using var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false);
            for (byte i = 0; i < 11; i++)
            {
                using var session = queue.OpenSession();
                await session.Enqueue(new[] { i }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            Assert.AreEqual(11, ((IDiskQueueStore)queue).EstimatedCountOfItemsInQueue);
        }

        [Test]
        public async Task When_creating_more_items_than_allowed_in_first_file_will_create_additional_file()
        {
            await using var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false);
            for (byte i = 0; i < 11; i++)
            {
                using var session = queue.OpenSession();
                await session.Enqueue(new[] { i }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            Assert.AreEqual(1, ((IDiskQueueStore)queue).CurrentFileNumber);
        }

        [Test]
        public async Task Can_resume_writing_to_second_file_when_restart_queue()
        {
            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false))
            {
                for (byte i = 0; i < 11; i++)
                {
                    using var session = queue.OpenSession();
                    await session.Enqueue(new[] { i }).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }

                Assert.AreEqual(1, ((IDiskQueueStore)queue).CurrentFileNumber);
            }

            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false))
            {
                for (byte i = 0; i < 2; i++)
                {
                    using var session = queue.OpenSession();
                    await session.Enqueue(new[] { i }).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }

                Assert.AreEqual(1, ((IDiskQueueStore)queue).CurrentFileNumber);
            }
        }

        [Test]
        public async Task Can_dequeue_from_all_files()
        {
            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false))
            {
                for (byte i = 0; i < 12; i++)
                {
                    using var session = queue.OpenSession();
                    await session.Enqueue(new[] { i }).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }

                Assert.AreEqual(1, ((IDiskQueueStore)queue).CurrentFileNumber);
            }

            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false))
            {
                for (byte i = 0; i < 12; i++)
                {
                    using var session = queue.OpenSession();
                    Assert.AreEqual(i, (await session.Dequeue().ConfigureAwait(false))[0]);
                    await session.Flush().ConfigureAwait(false);
                }
            }
        }

        [Test]
        public async Task Can_dequeue_from_all_files_after_restart()
        {
            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false))
            {
                for (byte i = 0; i < 12; i++)
                {
                    using var session = queue.OpenSession();
                    await session.Enqueue(new[] { i }).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }

                Assert.AreEqual(1, ((IDiskQueueStore)queue).CurrentFileNumber);
            }

            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false))
            {
                for (byte i = 0; i < 3; i++)
                {
                    using var session = queue.OpenSession();
                    await session.Enqueue(new[] { i }).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }

                Assert.AreEqual(1, ((IDiskQueueStore)queue).CurrentFileNumber);
            }


            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (byte i = 0; i < 12; i++)
                {
                    Assert.AreEqual(i, (await session.Dequeue().ConfigureAwait(false))[0]);
                    await session.Flush().ConfigureAwait(false);
                }

                for (byte i = 0; i < 3; i++)
                {
                    Assert.AreEqual(i, (await session.Dequeue().ConfigureAwait(false))[0]);
                    await session.Flush().ConfigureAwait(false);
                }
            }
        }

        [Test]
        public async Task After_reading_all_items_from_file_that_is_not_the_active_file_should_delete_file()
        {
            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10, persistent: true).ConfigureAwait(false))
            {
                for (byte i = 0; i < 12; i++)
                {
                    using var session = queue.OpenSession();
                    await session.Enqueue(new[] { i }).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }

                Assert.AreEqual(1, ((IDiskQueueStore)queue).CurrentFileNumber);
            }

            await using (var queue = await DiskQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false))
            {
                for (byte i = 0; i < 12; i++)
                {
                    using var session = queue.OpenSession();
                    Assert.AreEqual(i, (await session.Dequeue().ConfigureAwait(false))[0]);
                    await session.Flush().ConfigureAwait(false);
                }
            }

            Assert.IsFalse(File.Exists(System.IO.Path.Combine(Path, "data.0")));
        }
    }
}
