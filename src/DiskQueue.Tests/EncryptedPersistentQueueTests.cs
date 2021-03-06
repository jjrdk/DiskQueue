namespace DiskQueue.Tests
{
    using System;
    using System.Security.Cryptography;
    using System.Threading.Tasks;
    using AsyncDiskQueue;
    using Microsoft.Extensions.Logging;
    using NSubstitute;
    using Xunit;

    public class EncryptedPersistentQueueTests : PersistentQueueTestsBase
    {
        [Fact]
        public async Task CanReadBackFromEncryptedQueue()
        {
            var data = Guid.NewGuid().ToByteArray();
            using var algo = CreateAlgo();
            await using var queue = await PersistentQueue.Create(
                    Path,
                    Substitute.For<ILogger<IPersistentQueue>>(),
                    paranoidFlushing: false,
                    symmetricAlgorithm: algo)
                .ConfigureAwait(false);
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(data).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            using (var session = queue.OpenSession())
            {
                var result = await session.Dequeue().ConfigureAwait(false);
                Assert.Equal<byte>(data, result);
            }
        }

        private static SymmetricAlgorithm CreateAlgo()
        {
            var algo = Aes.Create();
            algo.GenerateIV();
            algo.GenerateKey();
            return algo;
        }

        [Fact]
        public async Task Dequeing_from_empty_queue_will_return_null()
        {
            using var algo = CreateAlgo();
            await using var queue = await PersistentQueue
                .Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), symmetricAlgorithm: algo)
                .ConfigureAwait(false);
            using var session = queue.OpenSession();
            Assert.Null(await session.Dequeue().ConfigureAwait(false));
        }

        [Fact]
        public async Task Can_enqueue_and_dequeue_data_after_restarting_queue()
        {
            using var algo = CreateAlgo();
            await using (var queue = await PersistentQueue
                .Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), symmetricAlgorithm: algo)
                .ConfigureAwait(false))
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            await using (var queue = await PersistentQueue
                .Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), symmetricAlgorithm: algo)
                .ConfigureAwait(false))
            using (var session = queue.OpenSession())
            {
                Assert.Equal<byte>(new byte[] { 1, 2, 3, 4 }, await session.Dequeue().ConfigureAwait(false));
                await session.Flush().ConfigureAwait(false);
            }
        }
    }
}
