namespace AsyncDiskQueue.Broker.Tests
{
    using System;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using NSubstitute;
    using Xunit;

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
            await using var subscription = broker.Subscribe<TestItem>(Handle);

            await broker.Publish("tester", message).ConfigureAwait(false);

            var handled = waitHandle.WaitOne(TimeSpan.FromSeconds(20));

            Assert.True(handled);
        }

        [Fact]
        public async Task WhenPublishingMessageThenIsReceivedByInterfaceSubscriber()
        {
            var waitHandle = new ManualResetEvent(false);

            Task Handle(ITestItem data)
            {
                waitHandle.Set();
                return Task.CompletedTask;
            }

            var message = new TestItem { Value = "test" };
            await using var broker = new MessageBrokerImpl(new DirectoryInfo(Path), Substitute.For<ILoggerFactory>());
            await using var subscription = broker.Subscribe<ITestItem>(Handle);

            await broker.Publish("tester", message).ConfigureAwait(false);

            var handled = waitHandle.WaitOne(TimeSpan.FromSeconds(20));

            Assert.True(handled);
        }

        [Fact]
        public async Task WhenPublishingInterfaceMessageThenIsNotReceivedByImplementationSubscriber()
        {
            var waitHandle = new ManualResetEvent(false);

            Task Handle(TestItem data)
            {
                waitHandle.Set();
                return Task.CompletedTask;
            }

            var message = new TestItem { Value = "test" };
            await using var broker = new MessageBrokerImpl(new DirectoryInfo(Path), Substitute.For<ILoggerFactory>());
            await using var subscription = broker.Subscribe<TestItem>(Handle);

            await broker.Publish<ITestItem>("tester", message).ConfigureAwait(false);

            var handled = waitHandle.WaitOne(TimeSpan.FromSeconds(3));

            Assert.False(handled);
        }

        [Fact]
        public async Task WhenSubscriptionIsCancelledThenPublishedMessageIsNotHandled()
        {
            var waitHandle = new ManualResetEvent(false);

            Task Handle(TestItem data)
            {
                waitHandle.Set();
                return Task.CompletedTask;
            }

            var message = new TestItem { Value = "test" };
            await using var broker = new MessageBrokerImpl(new DirectoryInfo(Path), Substitute.For<ILoggerFactory>());
            await using (broker.Subscribe<TestItem>(Handle))
            {
            }

            await broker.Publish("tester", message).ConfigureAwait(false);

            var handled = waitHandle.WaitOne(TimeSpan.FromSeconds(3));

            Assert.False(handled);
        }
    }
}
