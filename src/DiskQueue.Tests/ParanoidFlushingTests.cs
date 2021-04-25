namespace DiskQueue.Tests
{
    using System.Threading.Tasks;
    using AsyncDiskQueue;
    using Microsoft.Extensions.Logging;
    using NSubstitute;
    using Xunit;

    public class ParanoidFlushingTests
    {
        readonly byte[] _one = {1, 2, 3, 4};
        readonly byte[] _two = {5, 6, 7, 8};

        [Fact]
        public async Task Paranoid_flushing_still_respects_session_rollback()
        {
            await using var queue = await PersistentQueue
                .Create("./queue", Substitute.For<ILogger<IPersistentQueue>>(), paranoidFlushing: true)
                .ConfigureAwait(false);

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
                Assert.Equal(_one, await s2.Dequeue().ConfigureAwait(false));
                Assert.Null(await s2.Dequeue().ConfigureAwait(false));
            }

            // Read again WITH flushing
            using (var s3 = queue.OpenSession())
            {
                Assert.Equal(_one, await s3.Dequeue().ConfigureAwait(false));
                Assert.Null(await s3.Dequeue().ConfigureAwait(false));
                await s3.Flush().ConfigureAwait(false);
            }

            // Read empty queue to be sure
            using var s4 = queue.OpenSession();
            Assert.Null(await s4.Dequeue().ConfigureAwait(false));
            await s4.Flush().ConfigureAwait(false);
        }
    }
}
