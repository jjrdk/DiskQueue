using System;
using System.Linq;
using NUnit.Framework;

namespace DiskQueue.Tests
{
    using System.Threading;
    using System.Threading.Tasks;
    using AsyncDiskQueue;
    using Microsoft.Extensions.Logging;
    using NSubstitute;

    [TestFixture]
    public class LongTermDequeueTests
    {
        private IPersistentQueue _q;
        private CancellationTokenSource _source;

        [SetUp]
        public async Task Setup()
        {
            _source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            _q = await PersistentQueue.Create("./queue", Substitute.For<ILogger<IPersistentQueue>>(), cancellationToken: _source.Token).ConfigureAwait(false);
        }

        [TearDown]
        public async Task Teardown()
        {
            await _q.DisposeAsync().ConfigureAwait(false);
            _source.Dispose();
        }

        [Test]
        public async Task Can_enqueue_during_a_long_dequeue()
        {
            var s1 = _q.OpenSession();

            using (var s2 = _q.OpenSession())
            {
                await s2.Enqueue(new byte[] { 1, 2, 3, 4 }).ConfigureAwait(false);
                await s2.Flush().ConfigureAwait(false);
            }

            var x = await s1.Dequeue(CancellationToken.None).ConfigureAwait(false);
            await s1.Flush().ConfigureAwait(false);
            s1.Dispose();

            Assert.That(x.SequenceEqual(new byte[] { 1, 2, 3, 4 }));
        }

    }
}