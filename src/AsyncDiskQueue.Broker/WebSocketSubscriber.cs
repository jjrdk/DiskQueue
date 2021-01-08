namespace AsyncDiskQueue.Broker
{
    using System.Net.WebSockets;
    using System.Threading;
    using System.Threading.Tasks;

    internal class WebSocketSubscriber : IMessageReceiver
    {
        private readonly WebSocket _webSocket;

        public WebSocketSubscriber(string topic, WebSocket webSocket)
        {
            _webSocket = webSocket;
            Topic = topic;
        }

        /// <inheritdoc />
        public string Topic { get; }

        /// <inheritdoc />
        public bool Persistent => false;

        /// <inheritdoc />
        public Task OnNext(byte[] msg, CancellationToken cancellationToken)
        {
            return _webSocket.SendAsync(msg, WebSocketMessageType.Text, true, cancellationToken);
        }
    }
}