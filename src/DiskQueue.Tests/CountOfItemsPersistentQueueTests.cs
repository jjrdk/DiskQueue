namespace DiskQueue.Tests;

using System.Threading.Tasks;
using AsyncDiskQueue;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

public class CountOfItemsPersistentQueueTests : PersistentQueueTestsBase
{
    [Fact]
    public async Task Can_get_count_from_queue()
    {
        await using var queue =
            await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>());
        Assert.Equal(0, ((IPersistentQueueStore) queue).EstimatedCountOfItemsInQueue);
    }

    [Fact]
    public async Task Can_enter_items_and_get_count_of_items()
    {
        await using var queue =
            await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>());
        for (byte i = 0; i < 5; i++)
        {
            using var session = queue.OpenSession();
            await session.Enqueue(new[] {i});
            await session.Flush();
        }

        Assert.Equal(5, ((IPersistentQueueStore) queue).EstimatedCountOfItemsInQueue);
    }


    [Fact]
    public async Task Can_get_count_of_items_after_queue_restart()
    {
        await using (var queue =
            await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>()))
        {
            for (byte i = 0; i < 5; i++)
            {
                using var session = queue.OpenSession();
                await session.Enqueue(new[] {i});
                await session.Flush();
            }
        }

        await using (var queue =
            await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>()))
        {
            Assert.Equal(5, ((IPersistentQueueStore) queue).EstimatedCountOfItemsInQueue);
        }
    }
}
