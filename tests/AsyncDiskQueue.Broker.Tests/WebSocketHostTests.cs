namespace AsyncDiskQueue.Broker.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Abstractions;
    using Clients;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;
    using NSubstitute;
    using Serializers.Newtonsoft;
    using Xunit;

    public class WebSocketHostTests : MessageBrokerTestBase
    {
        [Fact]
        public async void WhenReceivingMessageFromTcpSocketThenSendsToOtherClients()
        {
            var waitHandle = new ManualResetEventSlim(false);
            var broker = await MessageBroker.Create(
                    new DirectoryInfo(Path),
                    new NullLoggerFactory())
                .ConfigureAwait(false);

            await using var connector = new WebSocketHost(
                Substitute.For<ILogger<WebSocketHost>>(),
                broker);
            await using var subscriber = new WebSocketClient(
                new Uri("ws://localhost:7000"),
                new SubscriptionRequest(
                    "subscriber",
                    new DelegateReceiver<TestItem>(
                        (_, _) =>
                        {
                            waitHandle.Set();
                            return Task.CompletedTask;
                        })),
                new NullLogger<WebSocketClient>(),
                Serializer.Serialize);
            await using var publisher = new WebSocketClient(
                new Uri("ws://localhost:7000"),
                new SubscriptionRequest("test"),
                new NullLogger<WebSocketClient>(),
                Serializer.Serialize);

            await Task.Delay(TimeSpan.FromMilliseconds(500)).ConfigureAwait(false);

            var payload = new MessagePayload(
                "here",
                new TestItem { Value = "test" },
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
        public async void WhenReceivingMessageFromWebSocketThenSendsToBroker()
        {
            var waitHandle = new ManualResetEventSlim(false);
            var broker = Substitute.For<IMessageBroker>();
            broker.When(b => b.Publish(Arg.Any<Memory<byte>>(), Arg.Any<string[]>())).Do(_ => waitHandle.Set());
            await using var connector = new WebSocketHost(
                Substitute.For<ILogger<WebSocketHost>>(),
                broker);
            await using var client = new WebSocketClient(
                new Uri("ws://localhost:7000"),
                new SubscriptionRequest("test"),
                new NullLogger<WebSocketClient>(),
                Serializer.Serialize,
                50);
            var payload = new MessagePayload(
                "here",
                new TestItem { Value = "test" },
                typeof(TestItem).GetInheritanceNames().ToArray(),
                new Dictionary<string, object>(),
                TimeSpan.Zero,
                DateTimeOffset.UtcNow,
                Guid.NewGuid().ToString("N"));

            await client.Send(payload, CancellationToken.None).ConfigureAwait(false);

            var success = waitHandle.Wait(TimeSpan.FromSeconds(30));

            Assert.True(success);
        }

        [Fact]
        public async void WhenReceivingMessageFromBrokerThenSendsToWebSocket()
        {
            var waitHandle = new ManualResetEventSlim(false);
            IMessageBroker broker = await MessageBroker.Create(
                    new DirectoryInfo(Path),
                    new NullLoggerFactory())
                .ConfigureAwait(false);
            await using var connector = new WebSocketHost(
                Substitute.For<ILogger<WebSocketHost>>(),
                broker);
            await using var client = new WebSocketClient(
                new Uri("ws://localhost:7000"),
                new SubscriptionRequest(
                    "test",
                    new DelegateReceiver<TestItem>(
                        (_, _) =>
                        {
                            waitHandle.Set();
                            return Task.CompletedTask;
                        })),
                new NullLogger<WebSocketClient>(),
                Serializer.Serialize);
            await Task.Delay(250).ConfigureAwait(false);

            var (bytes, topics) = Message.Create("test", new TestItem { Value = "test" });
            await broker.Publish(bytes, topics).ConfigureAwait(false);

            var success = waitHandle.Wait(TimeSpan.FromMilliseconds(50000));

            Assert.True(success);
        }
    }
}
