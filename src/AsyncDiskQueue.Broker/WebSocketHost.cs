namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using System.Net;
    using System.Net.WebSockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;

    internal class WebSocketHost<T> : IAsyncDisposable where T : class
    {
        private readonly ILogger<WebSocketHost<T>> _logger;
        private readonly IMessageBroker _broker;
        private readonly HttpListener _listener = new();
        private readonly CancellationTokenSource _tokenSource = new();
        private readonly Task _worker;

        public WebSocketHost(ILogger<WebSocketHost<T>> logger, IMessageBroker broker)
        {
            _listener.Prefixes.Add("http://localhost:7000/");
            _logger = logger;
            _broker = broker;
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
                catch (ObjectDisposedException) { }
                catch (HttpListenerException) { }
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
            var absolutePath = listenerContext.Request.Url!.AbsolutePath.Trim('/');
            var endpoint = absolutePath[..absolutePath.IndexOf('/')].Trim('/');
            var topic = absolutePath[absolutePath.IndexOf('/')..].Trim('/');
            var subscription = await _broker.Subscribe(
                    new SubscriptionRequest(
                        endpoint,
                        false,
                        new WebSocketSubscriber(topic, webSocket)))
                .ConfigureAwait(false);
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
                            await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "", cancellationToken).ConfigureAwait(false);
                            break;
                        case WebSocketMessageType.Text:
                            {
                                msg.AddRange(buffer[0..receiveResult.Count]);
                                if (receiveResult.EndOfMessage)
                                {
                                    var json = Serializer.Deserialize(msg.ToArray());
                                    await _broker.Publish(
                                            listenerContext.Request.RemoteEndPoint.ToString(),
                                            (T)json.Payload)
                                        .ConfigureAwait(false);
                                    msg.Clear();
                                }

                                break;
                            }
                        case WebSocketMessageType.Binary:
                            // await webSocket.SendAsync(new ArraySegment<byte>(buffer, 0, receiveResult.Count), WebSocketMessageType.Binary, receiveResult.EndOfMessage, CancellationToken.None).ConfigureAwait(false);
                            break;
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception e)
            {
                // Just log any exceptions to the console. Pretty much any exception that occurs when calling `SendAsync`/`ReceiveAsync`/`CloseAsync` is unrecoverable in that it will abort the connection and leave the `WebSocket` instance in an unusable state.
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