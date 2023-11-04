namespace DiskQueue.Tests;

using System;
using System.Threading;
using System.Threading.Tasks;
using AsyncDiskQueue;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

public class LongTermDequeueTests : IDisposable
{
    private readonly IPersistentQueue _q;
    private readonly CancellationTokenSource _source;

    public LongTermDequeueTests()
    {
        _source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        _q = PersistentQueue.Create("./queue", Substitute.For<ILogger<PersistentQueue>>(), cancellationToken: _source.Token)
            .GetAwaiter()
            .GetResult();
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        _q.DisposeAsync().AsTask().Wait();
        _source.Dispose();
    }

    [Fact]
    public async Task Can_enqueue_during_a_long_dequeue()
    {
        var s1 = _q.OpenSession();

        using (var s2 = _q.OpenSession())
        {
            await s2.Enqueue(new byte[] {1, 2, 3, 4});
            await s2.Flush();
        }

        var x = await s1.Dequeue(CancellationToken.None);
        await s1.Flush();
        s1.Dispose();

        Assert.Equal(new byte[] {1, 2, 3, 4}, x);
    }

}
