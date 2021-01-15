namespace AsyncDiskQueue.Broker.Clients
{
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net.Sockets;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Abstractions;
    using Microsoft.Extensions.Logging;

    public class UnixSocketClient : IAsyncDisposable
    {
        private readonly CancellationTokenSource _tokenSource = new();
        private readonly string _endPoint;
        private readonly ILogger<UnixSocketClient> _logger;
        private readonly Func<MessagePayload, byte[]> _serializer;
        private readonly Dictionary<string, Socket> _clients;
        private readonly Task[] _listeners;

        public UnixSocketClient(
            string endPoint,
            ISubscriptionRequest subscriptionRequest,
            ILogger<UnixSocketClient> logger,
            Func<MessagePayload, byte[]> serializer)
        {
            _endPoint = endPoint;
            _logger = logger;
            _serializer = serializer;
            var tuples = subscriptionRequest.MessageReceivers.Select(
                    x =>
                    {
                        var pair = KeyValuePair.Create(
                            x.Topic,
                            new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified));
                        return (pair,
                            Listen(
                                subscriptionRequest.SubscriberInfo.EndPoint,
                                x.Topic,
                                pair.Value,
                                x.OnNext,
                                _tokenSource.Token));
                    })
                .ToArray();
            _clients = new Dictionary<string, Socket>(tuples.Select(x => x.pair));
            _listeners = tuples.Select(x => x.Item2).ToArray();
        }

        private async Task Listen(
            string endpoint,
            string topic,
            Socket client,
            Func<byte[], CancellationToken, Task> handler,
            CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Connecting to {endpoint}");
            await client.ConnectAsync(new UnixDomainSocketEndPoint(endpoint), _tokenSource.Token).ConfigureAwait(false);
            await PerformHandshake(endpoint, topic, client, cancellationToken).ConfigureAwait(false);

            _logger.LogInformation("Handshake completed");
            try
            {
                while (client.Connected && !cancellationToken.IsCancellationRequested)
                {
                    var msgBuffer = ArrayPool<byte>.Shared.Rent(4);
                    _ = await client.ReceiveAsync(msgBuffer.AsMemory(0, 4), SocketFlags.None, cancellationToken).ConfigureAwait(false);
                    ArrayPool<byte>.Shared.Return(msgBuffer);
                    var msgSize = BitConverter.ToInt32(msgBuffer, 0);
                    var buffer = ArrayPool<byte>.Shared.Rent(msgSize);

                    var memory = buffer.AsMemory(0, msgSize);
                    _ = await client.ReceiveAsync(memory, SocketFlags.None, cancellationToken).ConfigureAwait(false);

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
                _logger.LogInformation("Closed client socket");
            }
        }

        private static async Task PerformHandshake(
            string endpoint,
            string topic,
            Socket stream,
            CancellationToken cancellationToken)
        {
            var endpointBytes = Encoding.UTF8.GetBytes(endpoint);
            var topicBytes = Encoding.UTF8.GetBytes(topic);
            await stream.SendAsync(BitConverter.GetBytes(endpointBytes.Length), SocketFlags.None, cancellationToken).ConfigureAwait(false);
            await stream.SendAsync(endpointBytes, SocketFlags.None, cancellationToken).ConfigureAwait(false);
            await stream.SendAsync(BitConverter.GetBytes(topicBytes.Length), SocketFlags.None, cancellationToken).ConfigureAwait(false);
            await stream.SendAsync(topicBytes, SocketFlags.None, cancellationToken).ConfigureAwait(false);

            var result = await stream.ReceiveAsync(new byte[] { 0 }, SocketFlags.None, cancellationToken)
                .ConfigureAwait(false);
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
                        _logger.LogInformation("Creating send socket for " + _endPoint);
                        using var client = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                        await client.ConnectAsync(new UnixDomainSocketEndPoint(_endPoint), cancellationToken)
                            .ConfigureAwait(false);

                        await PerformHandshake("send", topic, client, cancellationToken)
                            .ConfigureAwait(false);

                        var bytes = _serializer(payload with { Topics = new[] { topic } });
                        await client.SendAsync(BitConverter.GetBytes(bytes.Length), SocketFlags.None, cancellationToken)
                            .ConfigureAwait(false);
                        await client.SendAsync(bytes, SocketFlags.None, cancellationToken).ConfigureAwait(false);

                        var result = await client.ReceiveAsync(new byte[] { 0 }, SocketFlags.None, cancellationToken).ConfigureAwait(false);
                        client.Close();
                        if (result != 0)
                        {
                            throw new InvalidDataException();
                        }
                        _logger.LogInformation("Data sent to " + _endPoint);
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