﻿namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using System.Net;
    using System.Net.WebSockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;

    internal class WebSocketHost : IAsyncDisposable
    {
        private readonly ILogger<WebSocketHost> _logger;
        private readonly IMessageBroker _broker;
        private readonly int _bufferSize;
        private readonly HttpListener _listener = new();
        private readonly CancellationTokenSource _tokenSource = new();
        private readonly Task _worker;

        public WebSocketHost(IEnumerable<string> prefixes, ILogger<WebSocketHost> logger, IMessageBroker broker, int bufferSize = 1024 * 64)
        {
            foreach (var prefix in prefixes)
            {
                _listener.Prefixes.Add(prefix);
            }
            _logger = logger;
            _broker = broker;
            _bufferSize = bufferSize;
            _listener.Start();
            _worker = DoWork();
        }

        private async Task DoWork()
        {
            while (!_tokenSource.IsCancellationRequested)
            {
                try
                {
                    var context = await _listener.GetContextAsync().ConfigureAwait(false);
                    if (context.Request.IsWebSocketRequest)
                    {
                        ProcessRequest(context, _tokenSource.Token);
                    }
                    else
                    {
                        context.Response.StatusCode = (int)HttpStatusCode.Forbidden;
                    }
                }
                catch (ObjectDisposedException)
                {
                }
                catch (HttpListenerException)
                {
                }
            }
        }

        private async void ProcessRequest(HttpListenerContext listenerContext, CancellationToken cancellationToken)
        {
            WebSocketContext webSocketContext;
            try
            {
                webSocketContext = await listenerContext.AcceptWebSocketAsync(null, TimeSpan.MaxValue);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error connecting websocket");
                listenerContext.Response.StatusCode = 500;
                listenerContext.Response.Close();
                return;
            }

            var webSocket = webSocketContext.WebSocket;
            const char separator = '/';
            var absolutePath = listenerContext.Request.Url!.AbsolutePath.Trim(separator);
            var endpoint = absolutePath[..absolutePath.IndexOf(separator)].Trim(separator);
            var topic = absolutePath[absolutePath.IndexOf(separator)..].Trim(separator);
            var subscription = await _broker.Subscribe(
                    new SubscriptionRequest(endpoint, new WebSocketSubscriber(topic, webSocket)))
                .ConfigureAwait(false);
            byte[] buffer = null;
            try
            {
                buffer = ArrayPool<byte>.Shared.Rent(_bufferSize);
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
                            break;
                        case WebSocketMessageType.Binary:
                            msg.AddRange(buffer[..receiveResult.Count]);
                            if (receiveResult.EndOfMessage)
                            {
                                await _broker.Publish(msg.ToArray(), topic).ConfigureAwait(false);
                                msg.Clear();
                            }

                            break;
                    }
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
                // Clean up by disposing the WebSocket once it is closed/aborted.
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
            _listener.Stop();
            _tokenSource.Cancel();
            await _worker.ConfigureAwait(false);
            _worker.Dispose();
            _tokenSource.Dispose();
        }
    }
}
