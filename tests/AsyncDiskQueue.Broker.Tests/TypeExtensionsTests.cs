namespace AsyncDiskQueue.Broker.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using NSubstitute;
    using Xunit;

    public class TypeExtensionsTests
    {
        [Fact]
        public void CanGetInheritanceHierarchy()
        {
            var baseTypes = typeof(DeepInheritance).GetInheritanceChain().ToArray();

            Assert.Equal(4, baseTypes.Length);
        }

        private class DeepInheritance : AggregateException { }
    }

    public class MessageBrokerTests : MessageBrokerTestBase
    {
        [Fact]
        public async Task WhenPublishingMessageThenIsReceivedBySubscriber()
        {
            var waitHandle = new ManualResetEvent(false);
            Task Handle(TestItem data)
            {
                waitHandle.Set();
                return Task.CompletedTask;
            }

            var message = new TestItem { Value = "test" };
            await using var broker = new MessageBrokerImpl(new DirectoryInfo(Path), Substitute.For<ILoggerFactory>());
            using var subscription = broker.Subscribe<TestItem>(Handle);

            await broker.Publish("tester", message).ConfigureAwait(false);

            var handled = waitHandle.WaitOne(TimeSpan.FromSeconds(20));

            Assert.True(handled);
        }
    }

    public class TestItem
    {
        public string Value { get; set; }
    }
}
