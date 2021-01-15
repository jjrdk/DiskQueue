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
    using Microsoft.Extensions.Logging.Abstractions;
    using NSubstitute;
    using Serializers.Newtonsoft;
    using Xunit;
    using Xunit.Abstractions;

    [Trait("os", "unix")]
    public class UnixSocketHostTests : MessageBrokerTestBase
    {
        private readonly string _endPoint =
            $"/tmp/{System.IO.Path.GetFileNameWithoutExtension(System.IO.Path.GetRandomFileName())}.sock";

        private readonly ITestOutputHelper _outputHelper;

        public UnixSocketHostTests(ITestOutputHelper outputHelper)
        {
            outputHelper.WriteLine("Starting unix socket test");
            if (File.Exists(_endPoint))
            {
                File.Delete(_endPoint);
            }

            _outputHelper = outputHelper;
        }

        [Fact]
        public async void WhenReceivingMessageFromUnixSocketThenSendsToBroker()
        {
            var waitHandle = new ManualResetEventSlim(false);
            var broker = Substitute.For<IMessageBroker>();
            broker.When(b => b.Publish(Arg.Any<Memory<byte>>(), Arg.Any<string[]>())).Do(_ => waitHandle.Set());
            await using var connector = new UnixSocketHost(
                _endPoint,
                _outputHelper.BuildLoggerFor<UnixSocketHost>(),
                broker);
            await Task.Delay(250).ConfigureAwait(false);
            await using var client = new UnixSocketClient(
                _endPoint,
                new SubscriptionRequest("test", new DelegateReceiver<TestItem>((_, _) => Task.CompletedTask)),
                _outputHelper.BuildLoggerFor<UnixSocketClient>(),
                Serializer.Serialize);
            await Task.Delay(TimeSpan.FromMilliseconds(250)).ConfigureAwait(false);
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
        public async void WhenReceivingMessageFromUnixSocketThenSendsToOtherClients()
        {
            var waitHandle = new ManualResetEventSlim(false);
            var broker = await MessageBroker.Create(new DirectoryInfo(Path), new NullLoggerFactory())
                .ConfigureAwait(false);

            await using var connector = new UnixSocketHost(
                _endPoint,
                _outputHelper.BuildLoggerFor<UnixSocketHost>(),
                broker);

            await Task.Delay(250).ConfigureAwait(false);

            await using var subscriber = new UnixSocketClient(
                _endPoint,
                new SubscriptionRequest(
                    "subscriber",
                    new DelegateReceiver<TestItem>(
                        (_, _) =>
                        {
                            waitHandle.Set();
                            return Task.CompletedTask;
                        })),
                _outputHelper.BuildLoggerFor<UnixSocketClient>(),
                Serializer.Serialize);

            await Task.Delay(250).ConfigureAwait(false);

            await using var publisher = new UnixSocketClient(
                _endPoint,
                new SubscriptionRequest("test"),
                _outputHelper.BuildLoggerFor<UnixSocketClient>(),
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
        public async void WhenReceivingMessageFromBrokerThenSendsToUnixSocket()
        {
            var waitHandle = new ManualResetEventSlim(false);
            IMessageBroker broker = await MessageBroker.Create(new DirectoryInfo(Path), new NullLoggerFactory())
                .ConfigureAwait(false);
            await using var connector = new UnixSocketHost(
                _endPoint,
                _outputHelper.BuildLoggerFor<UnixSocketHost>(),
                broker);

            await Task.Delay(250).ConfigureAwait(false);

            await using var client = new UnixSocketClient(
                _endPoint,
                new SubscriptionRequest(
                    "test",
                    new DelegateReceiver<TestItem>(
                        (_, _) =>
                        {
                            waitHandle.Set();
                            return Task.CompletedTask;
                        })),
                _outputHelper.BuildLoggerFor<UnixSocketClient>(),
                Serializer.Serialize);

            await Task.Delay(250).ConfigureAwait(false);

            var (bytes, topics) = Message.Create("test", new TestItem {Value = "test"});
            await broker.Publish(bytes, topics).ConfigureAwait(false);

            var success = waitHandle.Wait(TimeSpan.FromMilliseconds(50000));

            Assert.True(success);
        }

        /// <inheritdoc />
        public override void Dispose()
        {
            base.Dispose();
            _outputHelper.WriteLine("Deleting " + _endPoint);
            System.IO.File.Delete(_endPoint);
            GC.SuppressFinalize(this);
        }
    }
}
