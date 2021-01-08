namespace AsyncDiskQueue.Broker.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net.WebSockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;
    using NSubstitute;
    using Xunit;

    public class WebSocketHostTests : MessageBrokerTestBase
    {
        [Fact]
        public async void WhenReceivingMessageFromWebSocketThenSendsToBroker()
        {
            var waitHandle = new ManualResetEventSlim(false);
            var broker = Substitute.For<IMessageBroker>();
            broker.When(b => b.Publish(Arg.Any<MessagePayload>())).Do(c => waitHandle.Set());
            await using var connector = new WebSocketHost<TestItem>(
                Substitute.For<ILogger<WebSocketHost<TestItem>>>(),
                broker);
            await using var client = new WebSocketClient(
                new Uri("ws://localhost:7000"),
                new SubscriptionRequest("test"),
                new NullLogger<WebSocketClient>());
            var payload = new MessagePayload(
                "here",
                new TestItem { Value = "test" },
                typeof(TestItem).GetInheritanceChain().Select(x => x.AssemblyQualifiedName).ToArray(),
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
            var broker = await MessageBroker.Create(new DirectoryInfo(Path), new NullLoggerFactory())
                .ConfigureAwait(false);
            await using var connector = new WebSocketHost<TestItem>(
                Substitute.For<ILogger<WebSocketHost<TestItem>>>(),
                broker);
            await using var client = new WebSocketClient(
                new Uri("ws://localhost:7000"),
                new SubscriptionRequest(
                    "test",
                    new DelegateReceiver<TestItem>(
                        (t, c) =>
                        {
                            waitHandle.Set();
                            return Task.CompletedTask;
                        })),
                new NullLogger<WebSocketClient>());
            await Task.Delay(250).ConfigureAwait(false);

            await broker.Publish(Message.Create("test", new TestItem { Value = "test" })).ConfigureAwait(false);

            var success = waitHandle.Wait(TimeSpan.FromMilliseconds(50000));

            Assert.True(success);
        }
    }
}
