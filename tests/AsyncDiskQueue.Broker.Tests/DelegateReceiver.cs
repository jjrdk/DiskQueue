namespace AsyncDiskQueue.Broker.Tests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Serializers;

    public class DelegateReceiver<T> : IMessageReceiver
    {
        private readonly Func<T, CancellationToken, Task> _handler;

        public DelegateReceiver(Func<T, CancellationToken, Task> handler)
        {
            Topic = typeof(T).Hash();
            _handler = handler;
        }

        /// <inheritdoc />
        public string Topic { get; }

        /// <inheritdoc />
        public bool Persistent => false;

        /// <inheritdoc />
        public Task OnNext(byte[] msg, CancellationToken cancellationToken)
        {
            var t = Serializer.Deserialize(msg);
            return _handler((T)t.Payload, cancellationToken);
        }
    }
}