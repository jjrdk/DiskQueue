namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Buffers;
    using System.IO;
    using System.Net;
    using System.Net.Sockets;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;

    internal class TcpHost : IAsyncDisposable
    {
        private readonly CancellationTokenSource _tokenSource = new();
        private readonly TcpListener _listener;
        private readonly ILogger<TcpHost> _logger;
        private readonly IMessageBroker _broker;
        private readonly Task _worker;

        public TcpHost(
            IPEndPoint endPoint,
            ILogger<TcpHost> logger,
            IMessageBroker broker)
        {
            _listener = new(endPoint) { ExclusiveAddressUse = true };
            _listener.Server.LingerState = new LingerOption(true, 3);
            _logger = logger;
            _broker = broker;
            _listener.Start();
            _worker = DoWork();
        }

        private async Task DoWork()
        {
            _listener.Start();
            while (!_tokenSource.IsCancellationRequested)
            {
                try
                {
                    var context = await _listener.AcceptTcpClientAsync().ConfigureAwait(false);
                    ProcessRequest(context, _tokenSource.Token);
                }
                catch (SocketException)
                {
                }
            }
        }

        private async void ProcessRequest(TcpClient socket, CancellationToken cancellationToken)
        {
            await using var stream = socket.GetStream();

            var (topic, subscription) = await PerformHandshake(socket, stream, cancellationToken).ConfigureAwait(false);

            try
            {
                while (socket.Connected && !cancellationToken.IsCancellationRequested)
                {
                    var msgBuffer = ArrayPool<byte>.Shared.Rent(4);
                    _ = await stream.ReadAsync(msgBuffer.AsMemory(0, 4), cancellationToken).ConfigureAwait(false);
                    ArrayPool<byte>.Shared.Return(msgBuffer);
                    var msgSize = BitConverter.ToInt32(msgBuffer, 0);

                    var buffer = ArrayPool<byte>.Shared.Rent(msgSize);
                    var receiveResult = await stream.ReadAsync(buffer.AsMemory(0, msgSize), cancellationToken)
                        .ConfigureAwait(false);
                    if (receiveResult != msgSize)
                    {
                        var message = $"Expected {msgSize} bytes, but received {receiveResult}.";
                        _logger.LogError(message);
                        throw new InvalidDataException(message);
                    }

                    await _broker.Publish(buffer.AsMemory(0, msgSize), topic.Split('^')).ConfigureAwait(false);
                    ArrayPool<byte>.Shared.Return(buffer);

                    stream.WriteByte(0);
                    await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error receiving content");
            }
            finally
            {
                await subscription.DisposeAsync().ConfigureAwait(false);
            }
        }

        private async Task<(string topic, IAsyncDisposable subscription)> PerformHandshake(TcpClient socket, Stream stream, CancellationToken cancellationToken)
        {
            var endpoint = await ReadString(stream, cancellationToken).ConfigureAwait(false);
            var topic = await ReadString(stream, cancellationToken).ConfigureAwait(false);
            var subscription = topic == "send"
                ? new NullDisposable()
                : await _broker.Subscribe(new SubscriptionRequest(endpoint, new TcpSubscriber(topic, socket)))
                    .ConfigureAwait(false);

            stream.WriteByte(0);
            await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            return (topic, subscription);
        }

        private static async Task<string> ReadString(Stream stream, CancellationToken cancellationToken)
        {
            var b = ArrayPool<byte>.Shared.Rent(4);
            var read = await stream.ReadAsync(b.AsMemory(0, 4), cancellationToken).ConfigureAwait(false);
            if (read != 4)
            {
                throw new InvalidDataException();
            }

            var length = BitConverter.ToInt32(b);
            var endPointBuffer = ArrayPool<byte>.Shared.Rent(length);
            read = await stream.ReadAsync(endPointBuffer.AsMemory(0, length), cancellationToken).ConfigureAwait(false);
            if (read != length)
            {
                throw new InvalidDataException();
            }

            ArrayPool<byte>.Shared.Return(b);
            ArrayPool<byte>.Shared.Return(endPointBuffer);
            return Encoding.UTF8.GetString(endPointBuffer.AsSpan(0, length));
        }

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            _listener.Stop();
            _tokenSource.Cancel();
            await _worker.ConfigureAwait(false);
            _worker.Dispose();
            _tokenSource.Dispose();
        }
    }
}