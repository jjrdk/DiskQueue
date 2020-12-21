namespace DiskQueue.Reactive.Tests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using AsyncDiskQueue;
    using AsyncDiskQueue.Reactive;
    using Microsoft.Extensions.Logging;
    using NSubstitute;
    using Xunit;

    public class PersistenEnumerableTests : QueueObservableTestBase
    {
        [Fact]
        public async Task WhenEnumeratingOverQueueThenGetsItems()
        {
            const int total = 100;
            var rnd = new Random(DateTime.UtcNow.Millisecond);
            var content = new byte[5 * 1024 * 1024];
            rnd.NextBytes(content);
            await using var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), 2 * 1024 * 1024, paranoidFlushing: false).ConfigureAwait(false);
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(content).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            var count = 0;
            using (var session = queue.OpenSession())
            {
                await foreach (var item in session.ToAsyncEnumerable(b => b))
                {
                    if (Interlocked.Increment(ref count) == total)
                    {
                        break;
                    };
                    await session.Enqueue(item).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }
            }

            Assert.Equal(total, count);
        }
    }

    public class PersistentObservableTests : QueueObservableTestBase
    {
        [Fact]
        public async Task SimpleObserverTest()
        {
            await using var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false);
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }
            var observable = new PersistentBuffer(queue, 3);
            var waitHandle = new ManualResetEventSlim(false);
            var observer = new TestSubscriber(waitHandle);
            ((IObservable<byte[]>)observable).Subscribe(observer);

            observable.Start();

            var success = waitHandle.Wait(TimeSpan.FromSeconds(3));
            await observable.DisposeAsync().ConfigureAwait(false);

            Assert.True(success);
            Assert.NotNull(observer.LastMessage);
        }

        [Fact]
        public async Task WhenObserverCrashesMoreThanRetryThenObservesError()
        {
            await using var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false);
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }
            var observable = new PersistentBuffer(queue, 1);
            var waitHandle = new ManualResetEventSlim(false);
            var observer = new CrashSubscriber(waitHandle);
            ((IObservable<byte[]>)observable).Subscribe(observer);

            observable.Start();

            waitHandle.Wait(TimeSpan.FromSeconds(3));

            Assert.NotNull(observer.LastError);
            await observable.DisposeAsync().ConfigureAwait(false);
        }

        [Fact]
        public async Task WhenObserverCrashesLessThanRetryThenCompletes()
        {
            await using var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false);
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }
            var observable = new PersistentBuffer(queue, 10);
            var waitHandle = new ManualResetEventSlim(false);
            var observer = new CrashSubscriber(waitHandle);
            ((IObservable<byte[]>)observable).Subscribe(observer);

            observable.Start();

            waitHandle.Wait(TimeSpan.FromSeconds(30));
            await observable.Stop().ConfigureAwait(false);

            Assert.True(observer.Completed);
            await observable.DisposeAsync().ConfigureAwait(false);
        }

        [Fact]
        public async Task WhenObservingEmptyQueueThenCompletes()
        {
            await using var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false);

            var observable = new PersistentBuffer(queue, 10);
            var waitHandle = new ManualResetEventSlim(false);
            var observer = new TestSubscriber(waitHandle);
            ((IObservable<byte[]>)observable).Subscribe(observer);

            observable.Start();

            await Task.Delay(TimeSpan.FromSeconds(2)).ConfigureAwait(false);
            await observable.Stop().ConfigureAwait(false);

            waitHandle.Wait(TimeSpan.FromSeconds(30));

            Assert.True(observer.Completed);
            await observable.DisposeAsync().ConfigureAwait(false);
        }
    }
}