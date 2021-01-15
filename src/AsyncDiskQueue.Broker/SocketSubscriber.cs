namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Abstractions;

    internal class SocketSubscriber : IMessageReceiver
    {
        private readonly Socket _tcpClient;

        public SocketSubscriber(string topic, Socket tcpClient)
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
            await _tcpClient.SendAsync(BitConverter.GetBytes(msg.Length), SocketFlags.None, cancellationToken).ConfigureAwait(false);
            await _tcpClient.SendAsync(msg, SocketFlags.None, cancellationToken).ConfigureAwait(false);
        }
    }
}