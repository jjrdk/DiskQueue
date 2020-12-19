using DiskQueue.Implementation;
using NSubstitute;
using NSubstitute.Core;
using NUnit.Framework;
using System;
using System.IO;
// ReSharper disable PossibleNullReferenceException

namespace DiskQueue.Tests
{
    using System.Threading.Tasks;

    [TestFixture]
    public class PersistentQueueSessionTests : PersistentQueueTestsBase
    {
        [Test]
        public void Errors_raised_during_pending_write_will_be_thrown_on_flush()
        {
            var limitedSizeStream = new MemoryStream(new byte[4]);
            var queueStub = PersistentQueueWithMemoryStream(limitedSizeStream);

            var pendingWriteException = Assert.ThrowsAsync<AggregateException>(
                async () =>
                {
                    using var session = new PersistentQueueSession(queueStub, limitedSizeStream, 1024 * 1024);
                    await session.Enqueue(new byte[64 * 1024 * 1024 + 1]).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                });

            Assert.That(
                pendingWriteException.InnerExceptions[0].Message,
                Is.EqualTo("Memory stream is not expandable."));
        }

        [Test]
        public void Errors_raised_during_flush_write_will_be_thrown_as_is()
        {
            var limitedSizeStream = new MemoryStream(new byte[4]);
            var queueStub = PersistentQueueWithMemoryStream(limitedSizeStream);

            var notSupportedException = Assert.ThrowsAsync<AggregateException>(
                async () =>
                {
                    using var session = new PersistentQueueSession(queueStub, limitedSizeStream, 1024 * 1024);
                    await session.Enqueue(new byte[64]).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                });

            Assert.That(notSupportedException.InnerExceptions[0].Message, Is.EqualTo(@"Memory stream is not expandable."));
        }

        [Test]
        public async Task If_data_stream_is_truncated_will_raise_error()
        {
          await  using (var queue = await PersistentQueue.Create(Path).ConfigureAwait(false))
            using (var session = queue.OpenSession())
            {
                await session.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            await using (var fs = new FileStream(System.IO.Path.Combine(Path, "data.0"), FileMode.Open))
            {
                fs.SetLength(2); //corrupt the file
            }

            var invalidOperationException = Assert.ThrowsAsync<InvalidOperationException>(
             async () =>
                {
                    await using var queue = await PersistentQueue.Create(Path).ConfigureAwait(false);
                    using var session = queue.OpenSession();
                    await session.Dequeue().ConfigureAwait(false);
                });

            Assert.That(
                invalidOperationException.Message,
                Is.EqualTo("End of file reached while trying to read queue item"));
        }

        static IPersistentQueue PersistentQueueWithMemoryStream(MemoryStream limitedSizeStream)
        {
            var queueStub = Substitute.For<IPersistentQueue>();

            queueStub.WhenForAnyArgs(async x => await x.AcquireWriter(null, null, null).ConfigureAwait(false))
                .Do(c => CallActionArgument(c, limitedSizeStream).Wait());
            return queueStub;
        }

        static Task<long> CallActionArgument(CallInfo c, MemoryStream ms)
        {
            var func = (Func<Stream, Task<long>>)c.Args()[1];
            return func(ms);
        }
    }
}
