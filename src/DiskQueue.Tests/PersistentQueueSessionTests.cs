namespace DiskQueue.Tests
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using AsyncDiskQueue;
    using AsyncDiskQueue.Implementation;
    using Microsoft.Extensions.Logging;
    using NSubstitute;
    using NSubstitute.Core;
    using Xunit;

    public class PersistentQueueSessionTests : PersistentQueueTestsBase
    {
        [Fact]
        public async Task Errors_raised_during_pending_write_will_be_thrown_on_flush()
        {
            var limitedSizeStream = new MemoryStream(new byte[4]);
            var queueStub = PersistentQueueWithMemoryStream(limitedSizeStream);

            var pendingWriteException = await Assert.ThrowsAsync<AggregateException>(
                async () =>
                {
                    using var session = new PersistentQueueSession(
                        queueStub,
                        limitedSizeStream,
                        1024 * 1024,
                        null,
                        Substitute.For<ILogger<IPersistentQueueSession>>());
                    await session.Enqueue(new byte[64 * 1024 * 1024 + 1]).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }).ConfigureAwait(false);

            Assert.Equal("Memory stream is not expandable.",
                pendingWriteException.InnerExceptions[0].Message);
        }

        [Fact]
        public async Task Errors_raised_during_flush_write_will_be_thrown_as_is()
        {
            var limitedSizeStream = new MemoryStream(new byte[4]);
            var queueStub = PersistentQueueWithMemoryStream(limitedSizeStream);

            var notSupportedException = await Assert.ThrowsAsync<AggregateException>(
                async () =>
                {
                    using var session = new PersistentQueueSession(
                        queueStub,
                        limitedSizeStream,
                        1024 * 1024,
                        null,
                        Substitute.For<ILogger<IPersistentQueueSession>>());
                    await session.Enqueue(new byte[64]).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }).ConfigureAwait(false);

            Assert.Equal(@"Memory stream is not expandable.", notSupportedException.InnerExceptions[0].Message);
        }

        [Fact]
        public async Task If_data_stream_is_truncated_will_raise_error()
        {
            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false))
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            await using (var fs = new FileStream(System.IO.Path.Combine(Path, "data.0"), FileMode.Open))
            {
                fs.SetLength(2); //corrupt the file
            }

            var invalidOperationException = await Assert.ThrowsAsync<InvalidOperationException>(
             async () =>
                {
                    await using var queue = await PersistentQueue.Create(Path, Substitute.For<ILoggerFactory>()).ConfigureAwait(false);
                    using var session = queue.OpenSession();
                    await session.Dequeue().ConfigureAwait(false);
                }).ConfigureAwait(false);

            Assert.Equal("End of file reached while trying to read queue item",
                invalidOperationException.Message);
        }

        private static IPersistentQueueStore PersistentQueueWithMemoryStream(MemoryStream limitedSizeStream)
        {
            var queueStub = Substitute.For<IPersistentQueueStore>();

            queueStub.WhenForAnyArgs(async x => await x.AcquireWriter(null, null, null).ConfigureAwait(false))
                .Do(c => CallActionArgument(c, limitedSizeStream).Wait());
            return queueStub;
        }

        private static Task<long> CallActionArgument(CallInfo c, MemoryStream ms)
        {
            var func = (Func<Stream, Task<long>>)c.Args()[1];
            return func(ms);
        }
    }
}
