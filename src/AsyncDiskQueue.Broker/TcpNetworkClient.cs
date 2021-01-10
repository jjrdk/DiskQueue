namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;

    public class TcpNetworkClient : IAsyncDisposable
    {
        private readonly CancellationTokenSource _tokenSource = new();
        private readonly IPEndPoint _endPoint;
        private readonly ILogger<TcpNetworkClient> _logger;
        private readonly Func<MessagePayload, byte[]> _serializer;
        private readonly Dictionary<string, TcpClient> _clients;
        private readonly Task[] _listeners;

        public TcpNetworkClient(
            IPEndPoint endPoint,
            ISubscriptionRequest subscriptionRequest,
            ILogger<TcpNetworkClient> logger,
            Func<MessagePayload, byte[]> serializer)
        {
            _endPoint = endPoint;
            _logger = logger;
            _serializer = serializer;
            var tuples = subscriptionRequest.MessageReceivers.Select(
                    x =>
                    {
                        var socket = KeyValuePair.Create(x.Topic, new TcpClient());
                        return (socket, Listen(subscriptionRequest.SubscriberInfo.EndPoint, x.Topic, socket.Value, x.OnNext, _tokenSource.Token));
                    })
                .ToArray();
            _clients = new Dictionary<string, TcpClient>(tuples.Select(x => x.socket));
            _listeners = tuples.Select(x => x.Item2).ToArray();
        }

        private async Task Listen(
            string endpoint,
            string topic,
            TcpClient client,
            Func<byte[], CancellationToken, Task> handler,
            CancellationToken cancellationToken)
        {
            await client.ConnectAsync(_endPoint.Address, _endPoint.Port, _tokenSource.Token).ConfigureAwait(false);
            await using var networkStream = client.GetStream();
            var readStream = networkStream;
            await PerformHandshake(endpoint, topic, networkStream, cancellationToken).ConfigureAwait(false);

            try
            {
                while (client.Connected && !cancellationToken.IsCancellationRequested)
                {
                    var msgBuffer = ArrayPool<byte>.Shared.Rent(4);
                    _ = await readStream.ReadAsync(msgBuffer.AsMemory(0, 4), cancellationToken).ConfigureAwait(false);
                    ArrayPool<byte>.Shared.Return(msgBuffer);
                    var msgSize = BitConverter.ToInt32(msgBuffer, 0);
                    var buffer = ArrayPool<byte>.Shared.Rent(msgSize);

                    var memory = buffer.AsMemory(0, msgSize);
                    _ = await readStream.ReadAsync(memory, cancellationToken).ConfigureAwait(false);

                    await handler(memory.ToArray(), cancellationToken).ConfigureAwait(false);
                    ArrayPool<byte>.Shared.Return(buffer);
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
                client.Close();
                client.Dispose();
            }
        }

        private async Task PerformHandshake(
            string endpoint,
            string topic,
            Stream stream,
            CancellationToken cancellationToken)
        {
            var endpointBytes = Encoding.UTF8.GetBytes(endpoint);
            var topicBytes = Encoding.UTF8.GetBytes(topic);
            await stream.WriteAsync(BitConverter.GetBytes(endpointBytes.Length), cancellationToken)
                .ConfigureAwait(false);
            await stream.WriteAsync(endpointBytes, cancellationToken).ConfigureAwait(false);
            await stream.WriteAsync(BitConverter.GetBytes(topicBytes.Length), cancellationToken).ConfigureAwait(false);
            await stream.WriteAsync(topicBytes, cancellationToken).ConfigureAwait(false);
            await stream.FlushAsync(cancellationToken).ConfigureAwait(false);

            var result = stream.ReadByte();
            if (result != 0)
            {
                throw new Exception("Initialization failed");
            }
        }

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            _tokenSource.Cancel();
            await Task.WhenAll(
                    _listeners.Select(
                        async x =>
                        {
                            await x.ConfigureAwait(false);
                            x.Dispose();
                        }))
                .ConfigureAwait(false);

            foreach (var socket in _clients.Values)
            {
                try
                {
                    socket.Close();
                    socket.Dispose();
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }

        public async Task Send(MessagePayload payload, CancellationToken cancellationToken)
        {
            var tasks = payload.Topics.Distinct().Select(
                    async topic =>
                    {
                        try
                        {
                            var client = new TcpClient { NoDelay = true, LingerState = new LingerOption(true, 5) };
                            await client.ConnectAsync(_endPoint.Address, _endPoint.Port, cancellationToken)
                                .ConfigureAwait(false);

                            await using var networkStream = client.GetStream();
                            await PerformHandshake("send", "send", networkStream, cancellationToken)
                                .ConfigureAwait(false);

                            var bytes = _serializer(payload with { Topics = new[] { topic } });
                            await networkStream.WriteAsync(BitConverter.GetBytes(bytes.Length), cancellationToken)
                                .ConfigureAwait(false);
                            await networkStream.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
                            await networkStream.FlushAsync(cancellationToken).ConfigureAwait(false);

                            var result = networkStream.ReadByte();
                            if (result != 0)
                            {
                                throw new Exception();
                            }
                            client.Close();
                            client.Dispose();
                        }
                        catch (OperationCanceledException)
                        {
                        }
                        catch (Exception e)
                        {
                            _logger.LogError(e, "Error while sending message");
                        }
                    });
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
    }
}