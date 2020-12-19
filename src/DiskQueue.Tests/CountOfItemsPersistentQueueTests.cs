using NUnit.Framework;
// ReSharper disable PossibleNullReferenceException

namespace DiskQueue.Tests
{
    using System.Threading.Tasks;

    [TestFixture]
    public class CountOfItemsPersistentQueueTests : PersistentQueueTestsBase
    {
        [Test]
        public void Can_get_count_from_queue()
        {
            using var queue = new PersistentQueue(Path);
            Assert.AreEqual(0, queue.EstimatedCountOfItemsInQueue);
        }

        [Test]
        public async Task Can_enter_items_and_get_count_of_items()
        {
            using var queue = new PersistentQueue(Path);
            for (byte i = 0; i < 5; i++)
            {
                using var session = queue.OpenSession();
                await session.Enqueue(new[] { i }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }
            Assert.AreEqual(5, queue.EstimatedCountOfItemsInQueue);
        }


        [Test]
        public async Task Can_get_count_of_items_after_queue_restart()
        {
            using (var queue = new PersistentQueue(Path))
            {
                for (byte i = 0; i < 5; i++)
                {
                    using var session = queue.OpenSession();
                    await session.Enqueue(new[] { i }).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }
            }

            using (var queue = new PersistentQueue(Path))
            {
                Assert.AreEqual(5, queue.EstimatedCountOfItemsInQueue);
            }
        }
    }
}