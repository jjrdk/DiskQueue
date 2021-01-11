namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Abstractions;

    internal class TcpSubscriber : IMessageReceiver
    {
        private readonly TcpClient _tcpClient;

        public TcpSubscriber(string topic, TcpClient tcpClient)
        {
            _tcpClient = tcpClient;
            Topic = topic;
        }

        /// <inheritdoc />
        public string Topic { get; }

        /// <inheritdoc />
        public bool Persistent => false;

        /// <inheritdoc />
        public async Task OnNext(byte[] msg, CancellationToken cancellationToken)
        {
            var networkStream = _tcpClient.GetStream();
            await networkStream.WriteAsync(BitConverter.GetBytes(msg.Length), cancellationToken).ConfigureAwait(false);
            await networkStream.WriteAsync(msg, cancellationToken).ConfigureAwait(false);
        }
    }
}