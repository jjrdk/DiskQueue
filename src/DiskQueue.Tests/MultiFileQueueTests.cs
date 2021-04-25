namespace DiskQueue.Tests
{
    using System.IO;
    using System.Threading.Tasks;
    using AsyncDiskQueue;
    using Microsoft.Extensions.Logging;
    using NSubstitute;
    using Xunit;

    public class MultiFileQueueTests : PersistentQueueTestsBase
    {
        [Fact]
        public async Task Entering_more_than_count_of_items_will_work()
        {
            await using var queue = await PersistentQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false);
            for (byte i = 0; i < 11; i++)
            {
                using var session = queue.OpenSession();
                await session.Enqueue(new[] { i }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            Assert.Equal(11, ((IPersistentQueueStore)queue).EstimatedCountOfItemsInQueue);
        }

        [Fact]
        public async Task When_creating_more_items_than_allowed_in_first_file_will_create_additional_file()
        {
            await using var queue = await PersistentQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false);
            for (byte i = 0; i < 11; i++)
            {
                using var session = queue.OpenSession();
                await session.Enqueue(new[] { i }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            Assert.Equal(1, ((IPersistentQueueStore)queue).CurrentFileNumber);
        }

        [Fact]
        public async Task Can_resume_writing_to_second_file_when_restart_queue()
        {
            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false))
            {
                for (byte i = 0; i < 11; i++)
                {
                    using var session = queue.OpenSession();
                    await session.Enqueue(new[] { i }).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }

                Assert.Equal(1, ((IPersistentQueueStore)queue).CurrentFileNumber);
            }

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false))
            {
                for (byte i = 0; i < 2; i++)
                {
                    using var session = queue.OpenSession();
                    await session.Enqueue(new[] { i }).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }

                Assert.Equal(1, ((IPersistentQueueStore)queue).CurrentFileNumber);
            }
        }

        [Fact]
        public async Task Can_dequeue_from_all_files()
        {
            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false))
            {
                for (byte i = 0; i < 12; i++)
                {
                    using var session = queue.OpenSession();
                    await session.Enqueue(new[] { i }).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }

                Assert.Equal(1, ((IPersistentQueueStore)queue).CurrentFileNumber);
            }

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false))
            {
                for (byte i = 0; i < 12; i++)
                {
                    using var session = queue.OpenSession();
                    Assert.Equal(i, (await session.Dequeue().ConfigureAwait(false))[0]);
                    await session.Flush().ConfigureAwait(false);
                }
            }
        }

        [Fact]
        public async Task Can_dequeue_from_all_files_after_restart()
        {
            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false))
            {
                for (byte i = 0; i < 12; i++)
                {
                    using var session = queue.OpenSession();
                    await session.Enqueue(new[] { i }).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }

                Assert.Equal(1, ((IPersistentQueueStore)queue).CurrentFileNumber);
            }

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false))
            {
                for (byte i = 0; i < 3; i++)
                {
                    using var session = queue.OpenSession();
                    await session.Enqueue(new[] { i }).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }

                Assert.Equal(1, ((IPersistentQueueStore)queue).CurrentFileNumber);
            }


            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (byte i = 0; i < 12; i++)
                {
                    Assert.Equal(i, (await session.Dequeue().ConfigureAwait(false))[0]);
                    await session.Flush().ConfigureAwait(false);
                }

                for (byte i = 0; i < 3; i++)
                {
                    Assert.Equal(i, (await session.Dequeue().ConfigureAwait(false))[0]);
                    await session.Flush().ConfigureAwait(false);
                }
            }
        }

        [Fact]
        public async Task After_reading_all_items_from_file_that_is_not_the_active_file_should_delete_file()
        {
            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false))
            {
                for (byte i = 0; i < 12; i++)
                {
                    using var session = queue.OpenSession();
                    await session.Enqueue(new[] { i }).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }

                Assert.Equal(1, ((IPersistentQueueStore)queue).CurrentFileNumber);
            }

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILoggerFactory>(), maxFileSize: 10).ConfigureAwait(false))
            {
                for (byte i = 0; i < 12; i++)
                {
                    using var session = queue.OpenSession();
                    Assert.Equal(i, (await session.Dequeue().ConfigureAwait(false))[0]);
                    await session.Flush().ConfigureAwait(false);
                }
            }

            Assert.False(File.Exists(System.IO.Path.Combine(Path, "data.0")));
        }
    }
}
