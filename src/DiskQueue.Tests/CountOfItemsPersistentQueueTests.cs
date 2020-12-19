using NUnit.Framework;
// ReSharper disable PossibleNullReferenceException

namespace DiskQueue.Tests
{
    using System.Threading.Tasks;
    using Implementation;

    [TestFixture]
    public class CountOfItemsPersistentQueueTests : PersistentQueueTestsBase
    {
        [Test]
        public async Task Can_get_count_from_queue()
        {
            await using var queue = await PersistentQueue.Create(Path).ConfigureAwait(false);
            Assert.AreEqual(0, queue.EstimatedCountOfItemsInQueue);
        }

        [Test]
        public async Task Can_enter_items_and_get_count_of_items()
        {
            await using var queue = await PersistentQueue.Create(Path).ConfigureAwait(false);
            for (byte i = 0; i < 5; i++)
            {
                using var session = queue.OpenSession();
                await session.Enqueue(new[] {i}).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            Assert.AreEqual(5, queue.EstimatedCountOfItemsInQueue);
        }


        [Test]
        public async Task Can_get_count_of_items_after_queue_restart()
        {
            await using (var queue = await PersistentQueue.Create(Path).ConfigureAwait(false))
            {
                for (byte i = 0; i < 5; i++)
                {
                    using var session = queue.OpenSession();
                    await session.Enqueue(new[] {i}).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }
            }

            await using (var queue = await PersistentQueue.Create(Path).ConfigureAwait(false))
            {
                Assert.AreEqual(5, queue.EstimatedCountOfItemsInQueue);
            }
        }
    }
}