namespace AsyncDiskQueue.Broker.Clients
{
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net.WebSockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Abstractions;
    using Microsoft.Extensions.Logging;

    public class WebSocketClient : IAsyncDisposable
    {
        private readonly Uri _host;
        private readonly ILogger<WebSocketClient> _logger;
        private readonly Func<MessagePayload, byte[]> _serializer;
        private readonly int _bufferSize;
        private readonly CancellationTokenSource _tokenSource = new();
        private readonly Dictionary<string, ClientWebSocket> _webSockets;
        private readonly Task[] _listeners;

        public WebSocketClient(
            Uri host,
            ISubscriptionRequest subscriptionRequest,
            ILogger<WebSocketClient> logger,
            Func<MessagePayload, byte[]> serializer,
            int bufferSize = 4 * 1024)
        {
            _host = host;
            _logger = logger;
            _serializer = serializer;
            _bufferSize = bufferSize;
            var tuples = subscriptionRequest.MessageReceivers.Select(
                    x =>
                    {
                        var topicUri = GetTopicUri(host, subscriptionRequest.SubscriberInfo.EndPoint, x.Topic);
                        var socket = KeyValuePair.Create<string, ClientWebSocket>(x.Topic, new ClientWebSocket());
                        return (socket, Listen(topicUri, socket.Value, x.OnNext, _tokenSource.Token));
                    })
                .ToArray<(KeyValuePair<string, ClientWebSocket> socket, Task)>();
            _webSockets = new Dictionary<string, ClientWebSocket>(tuples.Select(x => x.socket));
            _listeners = tuples.Select(x => x.Item2).ToArray();
        }

        private static Uri GetTopicUri(Uri host, string endPoint, string topic)
        {
            var builder = new UriBuilder(host);
            var s = $"{endPoint}/{topic}";
            builder.Path += builder.Path.EndsWith('/') ? s : $"/{s}";
            return builder.Uri;
        }

        public async Task Send(MessagePayload message, CancellationToken cancellationToken = default)
        {
            var tasks = Enumerable.Select<string, Task>(
                message.Topics,
                async t =>
                {
                    var existing = _webSockets.TryGetValue(t, out var socket);
                    if (!existing)
                    {
                        socket = new ClientWebSocket();
                        await socket.ConnectAsync(GetTopicUri(_host, "send", t), cancellationToken)
                            .ConfigureAwait(false);
                    }

                    var buffer = _serializer(message with {Topics = new[] {t}});

                    for (var i = 0; i < buffer.Length; i += _bufferSize)
                    {
                        var count = Math.Min(buffer.Length - i, _bufferSize);
                        var segment = new ArraySegment<byte>(buffer, i, count);

                        await socket.SendAsync(
                                segment,
                                WebSocketMessageType.Binary,
                                i + count == buffer.Length,
                                cancellationToken)
                            .ConfigureAwait(false);
                    }

                    if (!existing)
                    {
                        await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, null, cancellationToken)
                            .ConfigureAwait(false);
                    }
                });
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        private async Task Listen(
            Uri host,
            ClientWebSocket webSocket,
            Func<byte[], CancellationToken, Task> handler,
            CancellationToken cancellationToken)
        {
            await webSocket.ConnectAsync(host, _tokenSource.Token).ConfigureAwait(false);

            byte[] buffer = null;
            try
            {
                buffer = ArrayPool<byte>.Shared.Rent(1024 * 64);
                var msg = new List<byte>();
                while (webSocket.State == WebSocketState.Open)
                {
                    var receiveResult = await webSocket.ReceiveAsync(buffer, cancellationToken).ConfigureAwait(false);

                    switch (receiveResult.MessageType)
                    {
                        case WebSocketMessageType.Close:
                            await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "", cancellationToken)
                                .ConfigureAwait(false);
                            break;
                        case WebSocketMessageType.Text:
                            _logger.LogError("Received text bytes");
                            break;
                        case WebSocketMessageType.Binary:
                            msg.AddRange(buffer[0..receiveResult.Count]);
                            if (receiveResult.EndOfMessage)
                            {
                                await handler(msg.ToArray(), cancellationToken).ConfigureAwait(false);
                                msg.Clear();
                            }

                            break;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogDebug("Task cancelled");
            }
            catch (WebSocketException ws)
            {
                _logger.LogError(ws, ws.Message);
            }
            catch (Exception e)
            {
                // Just log any exceptions to the console. Pretty much any exception that occurs when calling `SendAsync`/`ReceiveAsync`/`CloseAsync` is unrecoverable in that it will abort the connection and leave the `WebSocket` instance in an unusable state.
                _logger.LogError(e, "Error receiving content");
            }
            finally
            {
                // Clean up by disposing the WebSocket once it is closed/aborted.
                if (webSocket.State == WebSocketState.Open
                    || webSocket.State == WebSocketState.CloseReceived
                    || webSocket.State == WebSocketState.CloseSent)
                {
                    await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, null, cancellationToken)
                        .ConfigureAwait(false);
                }

                webSocket.Dispose();
                if (buffer != null)
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
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
            await Task.WhenAll(
                    _webSockets.Values.Select(
                        async socket =>
                        {
                            try
                            {
                                await socket.CloseAsync(
                                        WebSocketCloseStatus.NormalClosure,
                                        null,
                                        CancellationToken.None)
                                    .ConfigureAwait(false);
                                socket.Dispose();
                            }
                            catch (ObjectDisposedException)
                            {
                            }
                        }))
                .ConfigureAwait(false);
        }
    }
}
