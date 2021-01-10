namespace AsyncDiskQueue.Broker.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;
    using NSubstitute;
    using Serializers;
    using Xunit;

    public class TcpHostTests : MessageBrokerTestBase
    {
        [Fact]
        public async void WhenReceivingMessageFromTcpSocketThenSendsToBroker()
        {
            var waitHandle = new ManualResetEventSlim(false);
            var broker = Substitute.For<IMessageBroker>();
            broker.When(b => b.Publish(Arg.Any<MessagePayload>())).Do(c => waitHandle.Set());
            await using var connector = new TcpHost(
                new IPEndPoint(IPAddress.IPv6Loopback, 7001),
                Substitute.For<ILogger<TcpHost>>(),
                broker,
                Serializers.Serializer.Deserialize);
            await using var client = new TcpNetworkClient(
                new IPEndPoint(IPAddress.IPv6Loopback, 7001),
                new SubscriptionRequest("test", new DelegateReceiver<TestItem>((_, __) => Task.CompletedTask)),
                new NullLogger<TcpNetworkClient>(),
                Serializers.Serializer.Serialize);
            //await Task.Delay(TimeSpan.FromMilliseconds(250)).ConfigureAwait(false);
            var payload = new MessagePayload(
                "here",
                new TestItem {Value = "test"},
                typeof(TestItem).GetInheritanceNames().ToArray(),
                new Dictionary<string, object>(),
                TimeSpan.Zero,
                DateTimeOffset.UtcNow,
                Guid.NewGuid().ToString("N"));

            await client.Send(payload, CancellationToken.None).ConfigureAwait(false);

            var success = waitHandle.Wait(TimeSpan.FromSeconds(5));

            Assert.True(success);
        }

        [Fact]
        public async void WhenReceivingMessageFromTcpSocketThenSendsToOtherClients()
        {
            var waitHandle = new ManualResetEventSlim(false);
            var broker = await MessageBroker.Create(
                    new DirectoryInfo(Path),
                    new NullLoggerFactory(),
                    Serializer.Serialize,
                    Serializer.Deserialize)
                .ConfigureAwait(false);

            await using var connector = new TcpHost(
                new IPEndPoint(IPAddress.IPv6Loopback, 7001),
                Substitute.For<ILogger<TcpHost>>(),
                broker,
                Serializer.Deserialize);
            await using var subscriber = new TcpNetworkClient(
                new IPEndPoint(IPAddress.IPv6Loopback, 7001),
                new SubscriptionRequest(
                    "subscriber",
                    new DelegateReceiver<TestItem>(
                        (_, __) =>
                        {
                            waitHandle.Set();
                            return Task.CompletedTask;
                        })),
                new NullLogger<TcpNetworkClient>(),
                Serializer.Serialize);
            await using var publisher = new TcpNetworkClient(
                new IPEndPoint(IPAddress.IPv6Loopback, 7001),
                new SubscriptionRequest("test"),
                new NullLogger<TcpNetworkClient>(),
                Serializer.Serialize);

            await Task.Delay(TimeSpan.FromMilliseconds(500)).ConfigureAwait(false);

            var payload = new MessagePayload(
                "here",
                new TestItem {Value = "test"},
                typeof(TestItem).GetInheritanceNames().ToArray(),
                new Dictionary<string, object>(),
                TimeSpan.Zero,
                DateTimeOffset.UtcNow,
                Guid.NewGuid().ToString("N"));

            await publisher.Send(payload, CancellationToken.None).ConfigureAwait(false);

            var success = waitHandle.Wait(Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(5));

            Assert.True(success);
        }

        [Fact]
        public async void WhenReceivingMessageFromBrokerThenSendsToTcpSocket()
        {
            var waitHandle = new ManualResetEventSlim(false);
            IMessageBroker broker = await MessageBroker.Create(
                    new DirectoryInfo(Path),
                    new NullLoggerFactory(),
                    Serializer.Serialize,
                    Serializer.Deserialize)
                .ConfigureAwait(false);
            await using var connector = new TcpHost(
                new IPEndPoint(IPAddress.IPv6Loopback, 7001),
                Substitute.For<ILogger<TcpHost>>(),
                broker,
                Serializer.Deserialize);
            await using var client = new TcpNetworkClient(
                new IPEndPoint(IPAddress.IPv6Loopback, 7001),
                new SubscriptionRequest(
                    "test",
                    new DelegateReceiver<TestItem>(
                        (t, c) =>
                        {
                            waitHandle.Set();
                            return Task.CompletedTask;
                        })),
                new NullLogger<TcpNetworkClient>(),
                Serializer.Serialize);
            await Task.Delay(250).ConfigureAwait(false);

            await broker.Publish(Message.Create("test", new TestItem {Value = "test"})).ConfigureAwait(false);

            var success = waitHandle.Wait(TimeSpan.FromMilliseconds(50000));

            Assert.True(success);
        }
    }
}
