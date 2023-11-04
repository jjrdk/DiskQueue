using System;
using System.IO;

namespace DiskQueue.Tests;

using System.Threading.Tasks;
using AsyncDiskQueue;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

public class ParanoidFlushingTests
{
    private readonly byte[] one = { 1, 2, 3, 4 };
    private readonly byte[] two = { 5, 6, 7, 8 };

    [Fact]
    public async Task Paranoid_flushing_still_respects_session_rollback()
    {
        var path = $"./queue_{Guid.NewGuid():N}";
        await using (var queue = await PersistentQueue
            .Create(path, Substitute.For<ILogger<PersistentQueue>>(), paranoidFlushing: true))
        {
            // Flush only `_one`
            using (var s1 = queue.OpenSession())
            {
                await s1.Enqueue(one);
                await s1.Flush();
                await s1.Enqueue(two);
            }

            // Read without flushing
            using (var s2 = queue.OpenSession())
            {
                Assert.Equal(one, await s2.Dequeue());
                Assert.True((await s2.Dequeue()).IsEmpty);
            }

            // Read again WITH flushing
            using (var s3 = queue.OpenSession())
            {
                Assert.Equal(one, await s3.Dequeue());
                Assert.True((await s3.Dequeue()).IsEmpty);
                await s3.Flush();
            }

            // Read empty queue to be sure
            using var s4 = queue.OpenSession();
            Assert.True((await s4.Dequeue()).IsEmpty);
            await s4.Flush();
        }

        Directory.Delete(path, true);
    }
}
