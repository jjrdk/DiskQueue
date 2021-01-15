namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Buffers;
    using System.IO;
    using System.Net.Sockets;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;

    internal class UnixSocketHost : IAsyncDisposable
    {
        private readonly CancellationTokenSource _tokenSource = new();
        private readonly Socket _listener;
        private readonly ILogger<UnixSocketHost> _logger;
        private readonly IMessageBroker _broker;
        private readonly Task _worker;

        public UnixSocketHost(string endPoint, ILogger<UnixSocketHost> logger, IMessageBroker broker)
        {
            _listener = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            _listener.Bind(new UnixDomainSocketEndPoint(endPoint));
            _logger = logger;
            _broker = broker;
            _worker = DoWork();
        }

        private async Task DoWork()
        {
            _listener.Listen(10);
            _logger.LogInformation("Listening started");

            while (!_tokenSource.IsCancellationRequested)
            {
                try
                {
                    var context = await _listener.AcceptAsync().ConfigureAwait(false);
                    ProcessRequest(context, _tokenSource.Token);
                }
                catch (SocketException)
                {
                }
            }
        }

        private async void ProcessRequest(Socket socket, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Initiating handshake for {socket.RemoteEndPoint}");

            var (topic, subscription) = await PerformHandshake(socket, cancellationToken).ConfigureAwait(false);

            _logger.LogInformation($"Completed handshake for {string.Join(", ", topic.Split('^'))}");
            try
            {
                while (socket.Connected && !cancellationToken.IsCancellationRequested)
                {
                    var msgBuffer = ArrayPool<byte>.Shared.Rent(4);
                    _ = await socket.ReceiveAsync(msgBuffer.AsMemory(0, 4), SocketFlags.None, cancellationToken)
                        .ConfigureAwait(false);
                    ArrayPool<byte>.Shared.Return(msgBuffer);
                    var msgSize = BitConverter.ToInt32(msgBuffer, 0);

                    var buffer = ArrayPool<byte>.Shared.Rent(msgSize);
                    var receiveResult = await socket.ReceiveAsync(
                            buffer.AsMemory(0, msgSize),
                            SocketFlags.None,
                            cancellationToken)
                        .ConfigureAwait(false);
                    if (receiveResult != msgSize)
                    {
                        var message = $"Expected {msgSize} bytes, but received {receiveResult}.";
                        _logger.LogError(message);
                        throw new InvalidDataException(message);
                    }

                    await _broker.Publish(buffer.AsMemory(0, msgSize), topic.Split('^')).ConfigureAwait(false);
                    ArrayPool<byte>.Shared.Return(buffer);

                    await socket.SendAsync(new byte[] { 0 }, SocketFlags.None, cancellationToken).ConfigureAwait(false);
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
                _logger.LogInformation("Closing socket");
            }
        }

        private async Task<(string topic, IAsyncDisposable subscription)> PerformHandshake(
            Socket socket,
            CancellationToken cancellationToken)
        {
            var endpoint = await ReadString(socket, cancellationToken).ConfigureAwait(false);
            var topic = await ReadString(socket, cancellationToken).ConfigureAwait(false);
            var subscription = topic == "send"
                ? new NullDisposable()
                : await _broker.Subscribe(new SubscriptionRequest(endpoint, new SocketSubscriber(topic, socket)))
                    .ConfigureAwait(false);

            await socket.SendAsync(new ReadOnlyMemory<byte>(new byte[] { 0 }), SocketFlags.None, cancellationToken)
                .ConfigureAwait(false);
            return (topic, subscription);
        }

        private static async Task<string> ReadString(Socket stream, CancellationToken cancellationToken)
        {
            var b = ArrayPool<byte>.Shared.Rent(4);
            var read = await stream.ReceiveAsync(b.AsMemory(0, 4), SocketFlags.None, cancellationToken)
                .ConfigureAwait(false);
            if (read != 4)
            {
                throw new InvalidDataException();
            }

            var length = BitConverter.ToInt32(b);
            var endPointBuffer = ArrayPool<byte>.Shared.Rent(length);
            read = await stream.ReceiveAsync(endPointBuffer.AsMemory(0, length), SocketFlags.None, cancellationToken)
                .ConfigureAwait(false);
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
            _tokenSource.Cancel();
            await _worker.ConfigureAwait(false);
            _worker.Dispose();
            _tokenSource.Dispose();
            _listener.Close();
            _listener.Dispose();
            _logger.LogInformation("Closed host socket");
        }
    }
}
