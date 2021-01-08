namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public interface ISubscriptionSource : ISubscription
    {
        IAsyncDisposable Connect(IMessageReceiver receiver);
    }

    public interface IMessageReceiver
    {
        string Topic { get; }

        bool Persistent { get; }

        Task OnNext(byte[] msg, CancellationToken cancellationToken);
    }

    public interface ISubscription : IMessageReceiver, IAsyncDisposable
    {
        string EndPoint { get; }
    }

    public interface ISubscriptionRequest
    {
        SubscriberInfo SubscriberInfo { get; }

        IMessageReceiver[] MessageReceivers { get; }
    }

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
