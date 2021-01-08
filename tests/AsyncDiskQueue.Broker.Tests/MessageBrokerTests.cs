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
            var broker = MessageBrokerImpl.Create(new DirectoryInfo(Path), new NullLoggerFactory());
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

            var message = new TestItem {Value = "test"};
            await using var broker = await MessageBrokerImpl
                .Create(new DirectoryInfo(Path), Substitute.For<ILoggerFactory>())
                .ConfigureAwait(false);
            await using var subscription = await broker.Subscribe(
                    new SubscriptionRequest(
                        Guid.NewGuid().ToString("N"),
                        false,
                        new DelegateReceiver<TestItem>(Handle)))
                .ConfigureAwait(false);
            await broker.Publish("tester", message).ConfigureAwait(false);

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

            var message = new TestItem {Value = "test"};
            await using var broker = await MessageBrokerImpl
                .Create(new DirectoryInfo(Path), Substitute.For<ILoggerFactory>())
                .ConfigureAwait(false);
            await using var subscription = await broker.Subscribe(
                    new SubscriptionRequest(
                        Guid.NewGuid().ToString("N"),
                        false,
                        new DelegateReceiver<ITestItem>(Handle)))
                .ConfigureAwait(false);

            await broker.Publish("tester", message).ConfigureAwait(false);
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

            var message = new TestItem {Value = "test"};
            await using var broker = await MessageBrokerImpl
                .Create(new DirectoryInfo(Path), Substitute.For<ILoggerFactory>())
                .ConfigureAwait(false);
            await using var subscription = await broker.Subscribe(
                    new SubscriptionRequest(
                        Guid.NewGuid().ToString("N"),
                        false,
                        new DelegateReceiver<TestItem>(Handle)))
                .ConfigureAwait(false);

            await broker.Publish<ITestItem>("tester", message).ConfigureAwait(false);

            var handled = waitHandle.WaitOne(TimeSpan.FromSeconds(3));

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

            var message = new TestItem {Value = "test"};
            await using var broker = await MessageBrokerImpl
                .Create(new DirectoryInfo(Path), Substitute.For<ILoggerFactory>())
                .ConfigureAwait(false);
            await using (await broker.Subscribe(
                    new SubscriptionRequest(
                        Guid.NewGuid().ToString("N"),
                        false,
                        new DelegateReceiver<TestItem>(Handle)))
                .ConfigureAwait(false))
            {
            }

            await broker.Publish("tester", message).ConfigureAwait(false);

            var handled = waitHandle.WaitOne(TimeSpan.FromSeconds(3));

            Assert.False(handled);
        }
    }
}
