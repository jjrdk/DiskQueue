namespace DiskQueue.Tests;

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
                Substitute.For<ILogger<PersistentQueue>>(),
                paranoidFlushing: false,
                symmetricAlgorithm: algo);
        using (var session = queue.OpenSession())
        {
            await session.Enqueue(data);
            await session.Flush();
        }

        using (var session = queue.OpenSession())
        {
            var result = await session.Dequeue();
            Assert.Equal(data, result);
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
    public async Task Dequeuing_from_empty_queue_will_return_null()
    {
        using var algo = CreateAlgo();
        await using var queue = await PersistentQueue
            .Create(Path, Substitute.For<ILogger<PersistentQueue>>(), symmetricAlgorithm: algo);
        using var session = queue.OpenSession();
        Assert.True((await session.Dequeue()).IsEmpty);
    }

    [Fact]
    public async Task Can_enqueue_and_dequeue_data_after_restarting_queue()
    {
        using var algo = CreateAlgo();
        await using (var queue = await PersistentQueue
            .Create(Path, Substitute.For<ILogger<PersistentQueue>>(), symmetricAlgorithm: algo)
)
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3, 4 });
                await session.Flush();
            }

        await using (var queue = await PersistentQueue
            .Create(Path, Substitute.For<ILogger<PersistentQueue>>(), symmetricAlgorithm: algo)
)
            using (var session = queue.OpenSession())
            {
                Assert.Equal(new byte[] { 1, 2, 3, 4 }, await session.Dequeue());
                await session.Flush();
            }
    }
}
