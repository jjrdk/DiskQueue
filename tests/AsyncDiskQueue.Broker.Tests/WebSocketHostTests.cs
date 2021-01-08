namespace AsyncDiskQueue.Broker.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net.WebSockets;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;
    using Newtonsoft.Json;
    using NSubstitute;
    using Xunit;

    public class WebSocketHostTests : MessageBrokerTestBase
    {
        [Fact]
        public async void WhenReceivingMessageFromWebSocketThenSendsToBroker()
        {
            var waitHandle = new ManualResetEventSlim(false);
            var broker = Substitute.For<IMessageBroker>();
            broker.When(b => b.Publish(Arg.Any<string>(), Arg.Any<TestItem>())).Do(c => waitHandle.Set());
            await using var connector = new WebSocketHost<TestItem>(
                Substitute.For<ILogger<WebSocketHost<TestItem>>>(),
                broker);
            using var client = new ClientWebSocket();
            await client.ConnectAsync(new Uri("ws://localhost:7000/mq/" + typeof(TestItem).Hash()), CancellationToken.None).ConfigureAwait(false);
            var payload = new MessagePayload(
                "here",
                new TestItem { Value = "test" },
                typeof(TestItem).GetInheritanceChain().Select(x => x.AssemblyQualifiedName).ToArray(),
                new Dictionary<string, object>(),
                TimeSpan.Zero,
                DateTimeOffset.UtcNow,
                Guid.NewGuid().ToString("N"));
            var json = Serializer.Serialize(payload);

            await client.SendAsync(json, WebSocketMessageType.Text, true, CancellationToken.None).ConfigureAwait(false);

            var success = waitHandle.Wait(TimeSpan.FromSeconds(30));

            Assert.True(success);
        }

        [Fact]
        public async void WhenReceivingMessageFromBrokerThenSendsToWebSocket()
        {
            var broker = await MessageBrokerImpl.Create(new DirectoryInfo(Path), new NullLoggerFactory())
                .ConfigureAwait(false);
            await using var connector = new WebSocketHost<TestItem>(
                Substitute.For<ILogger<WebSocketHost<TestItem>>>(),
                broker);
            using var client = new ClientWebSocket();
            await client.ConnectAsync(new Uri("ws://localhost:7000/mq/" + typeof(TestItem).Hash()), CancellationToken.None).ConfigureAwait(false);
            await Task.Delay(250).ConfigureAwait(false);
            var buffer = new byte[1024];
            var receive = client.ReceiveAsync(buffer, CancellationToken.None).ConfigureAwait(false);

            await broker.Publish("test", new TestItem { Value = "test" }).ConfigureAwait(false);

            var result = await receive;
            var value = Encoding.UTF8.GetString(buffer[..result.Count]);
            var json = JsonConvert.DeserializeObject<MessagePayload>(value);

            Assert.NotNull(json.Payload);
        }
    }
}
