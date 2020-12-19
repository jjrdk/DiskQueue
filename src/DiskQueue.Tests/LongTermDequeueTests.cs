using System;
using System.Linq;
using NUnit.Framework;

namespace DiskQueue.Tests
{
    using System.Threading.Tasks;

    [TestFixture]
    public class LongTermDequeueTests
    {
        IPersistentQueue _q;

        [SetUp]
        public void Setup()
        {
            _q = PersistentQueue.WaitFor("./queue", TimeSpan.FromSeconds(10));
        }

        [TearDown]
        public void Teardown()
        {
            _q.Dispose();
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

            var x = s1.Dequeue();
            await s1.Flush().ConfigureAwait(false);
            s1.Dispose();

            Assert.That(x.SequenceEqual(new byte[] { 1, 2, 3, 4 }));
        }

    }
}