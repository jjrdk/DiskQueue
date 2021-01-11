namespace AsyncDiskQueue.Broker.Tests
{
    using System;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;
    using NSubstitute;
    using Xunit;

    public class MessageBrokerTests : MessageBrokerTestBase
    {
        [Fact]
        public async Task CanInitializePermanentSubscriptions()
        {
            _ = await MessageBroker.Create(new DirectoryInfo(Path), new NullLoggerFactory())
                .ConfigureAwait(false);
        }

        [Fact]
        public async Task WhenPublishingMessageThenIsReceivedBySubscriber()
        {
            var waitHandle = new ManualResetEvent(false);

            Task Handle(TestItem data, CancellationToken cancellationToken)
            {
                waitHandle.Set();
                return Task.CompletedTask;
            }

            var message = new TestItem { Value = "test" };
            await using IMessageBroker broker = await MessageBroker
                .Create(new DirectoryInfo(Path), Substitute.For<ILoggerFactory>())
                .ConfigureAwait(false);
            await using var subscription = await broker.Subscribe(
                    new SubscriptionRequest(
                        Guid.NewGuid().ToString("N"),
                        new DelegateReceiver<TestItem>(Handle)))
                .ConfigureAwait(false);
            var (bytes, topics) = Message.Create("tester", message);
            await broker.Publish(bytes, topics).ConfigureAwait(false);

            var handled = waitHandle.WaitOne(TimeSpan.FromSeconds(20));

            Assert.True(handled);
        }

        [Fact]
        public async Task WhenPublishingMessageThenIsReceivedByInterfaceSubscriber()
        {
            var waitHandle = new ManualResetEvent(false);

            Task Handle(ITestItem data, CancellationToken cancellationToken)
            {
                waitHandle.Set();
                return Task.CompletedTask;
            }

            var message = new TestItem { Value = "test" };
            await using IMessageBroker broker = await MessageBroker
                .Create(new DirectoryInfo(Path), Substitute.For<ILoggerFactory>())
                .ConfigureAwait(false);
            await using var subscription = await broker.Subscribe(
                    new SubscriptionRequest(
                        Guid.NewGuid().ToString("N"),
                        new DelegateReceiver<ITestItem>(Handle)))
                .ConfigureAwait(false);

            var (bytes, topics) = Message.Create("tester", message);
            await broker.Publish(bytes, topics).ConfigureAwait(false);
            var handled = waitHandle.WaitOne(TimeSpan.FromSeconds(20));

            Assert.True(handled);
        }

        [Fact]
        public async Task WhenPublishingInterfaceMessageThenIsNotReceivedByImplementationSubscriber()
        {
            var waitHandle = new ManualResetEvent(false);

            Task Handle(TestItem data, CancellationToken cancellationToken)
            {
                waitHandle.Set();
                return Task.CompletedTask;
            }

            ITestItem message = new TestItem { Value = "test" };
            await using IMessageBroker broker = await MessageBroker
                .Create(new DirectoryInfo(Path), Substitute.For<ILoggerFactory>())
                .ConfigureAwait(false);
            await using var subscription = await broker.Subscribe(
                    new SubscriptionRequest(
                        Guid.NewGuid().ToString("N"),
                        new DelegateReceiver<TestItem>(Handle)))
                .ConfigureAwait(false);

            var (bytes, topics) = Message.Create("tester", message);
            await broker.Publish(bytes, topics).ConfigureAwait(false);

            var handled = waitHandle.WaitOne(TimeSpan.FromSeconds(2));

            Assert.False(handled);
        }

        [Fact]
        public async Task WhenSubscriptionIsCancelledThenPublishedMessageIsNotHandled()
        {
            var waitHandle = new ManualResetEvent(false);

            Task Handle(TestItem data, CancellationToken cancellationToken)
            {
                waitHandle.Set();
                return Task.CompletedTask;
            }

            var message = new TestItem { Value = "test" };
            await using IMessageBroker broker = await MessageBroker
                .Create(new DirectoryInfo(Path), Substitute.For<ILoggerFactory>())
                .ConfigureAwait(false);
            await using (await broker.Subscribe(
                    new SubscriptionRequest(
                        Guid.NewGuid().ToString("N"),
                        new DelegateReceiver<TestItem>(Handle)))
                .ConfigureAwait(false))
            {
            }

            var (bytes, topics) = Message.Create("tester", message);
            await broker.Publish(bytes, topics).ConfigureAwait(false);

            var handled = waitHandle.WaitOne(TimeSpan.FromSeconds(3));

            Assert.False(handled);
        }
    }
}
