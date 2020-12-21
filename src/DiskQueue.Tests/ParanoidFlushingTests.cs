using NUnit.Framework;
// ReSharper disable PossibleNullReferenceException

namespace DiskQueue.Tests
{
    using System.Threading.Tasks;
    using AsyncDiskQueue;

    [TestFixture]
    public class ParanoidFlushingTests
    {
        readonly byte[] _one = { 1, 2, 3, 4 };
        readonly byte[] _two = { 5, 6, 7, 8 };

        [Test]
        public async Task Paranoid_flushing_still_respects_session_rollback()
        {
            await using var queue = await PersistentQueue.Create("./queue", paranoidFlushing: true).ConfigureAwait(false);

            // Flush only `_one`
            using (var s1 = queue.OpenSession())
            {
                await s1.Enqueue(_one).ConfigureAwait(false);
                await s1.Flush().ConfigureAwait(false);
                await s1.Enqueue(_two).ConfigureAwait(false);
            }

            // Read without flushing
            using (var s2 = queue.OpenSession())
            {
                Assert.That(await s2.Dequeue().ConfigureAwait(false), Is.EquivalentTo(_one), "Unexpected item at head of queue");
                Assert.That(await s2.Dequeue().ConfigureAwait(false), Is.Null, "Too many items on queue");
            }

            // Read again WITH flushing
            using (var s3 = queue.OpenSession())
            {
                Assert.That(await s3.Dequeue().ConfigureAwait(false), Is.EquivalentTo(_one), "Queue was unexpectedly empty?");
                Assert.That(await s3.Dequeue().ConfigureAwait(false), Is.Null, "Too many items on queue");
                await s3.Flush().ConfigureAwait(false);
            }

            // Read empty queue to be sure
            using var s4 = queue.OpenSession();
            Assert.That(await s4.Dequeue().ConfigureAwait(false), Is.Null, "Queue was not empty after flush");
            await s4.Flush().ConfigureAwait(false);
        }
    }
}
