namespace DiskQueue.Tests;

using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using AsyncDiskQueue;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

public class MultipleProcessAccessTests
{
    [Fact,
     Description(
         "Multiple PersistentQueue instances are "
       + "pretty much the same as multiple processes to "
       + "the DiskQueue library")]
    public void Can_access_from_multiple_queues_if_used_carefully()
    {
        var received = 0;
        var numberOfItems = 10;

        var waitHandle = new ManualResetEvent(false);
        _ = Task.Run(
            async () =>
            {
                for (var i = 0; i < numberOfItems; i++)
                {
                    await AddToQueue(new byte[] { 1, 2, 3 });
                }

                waitHandle.Set();
            });

        waitHandle.WaitOne();
        waitHandle.Reset();

        _ = Task.Run(
            async () =>
            {
                while (received < numberOfItems)
                {
                    var data = await ReadQueue();
                    if (!data.IsEmpty)
                    {
                        Interlocked.Increment(ref received);
                    }
                }

                waitHandle.Set();
            });

        var ok = waitHandle.WaitOne();

        Assert.True(ok, "Did not receive all data in time");
        Assert.Equal(numberOfItems, received);
    }

    private static async Task AddToQueue(byte[] data)
    {
//        await Task.Delay(150);
        await using var queue = await PersistentQueue.Create(
                SharedStorage,
                Substitute.For<ILogger<PersistentQueue>>(),
                TimeSpan.FromSeconds(30))
            ;
        using var session = queue.OpenSession();
        await session.Enqueue(data);
        await session.Flush();
    }

    private static async Task<ReadOnlyMemory<byte>> ReadQueue()
    {
//        await Task.Delay(150);
        await using var queue = await PersistentQueue.Create(
                SharedStorage,
                Substitute.For<ILogger<PersistentQueue>>(),
                TimeSpan.FromSeconds(30))
            ;
        using var session = queue.OpenSession();
        var data = await session.Dequeue(CancellationToken.None);
        await session.Flush();
        return data;
    }

    private static string SharedStorage
    {
        get { return "./MultipleAccess"; }
    }
}
