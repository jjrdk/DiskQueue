namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    internal class SubscriptionWorker : ISubscription
    {
        private readonly IMessageReceiver _handler;
        private Func<ISubscription, Task> _onUnsubscribe;

        public SubscriptionWorker(
            string endPoint,
            IMessageReceiver handler,
            Func<ISubscription, Task> onUnsubscribe)
        {
            EndPoint = endPoint;
            Topic = handler.Topic;
            _handler = handler;
            _onUnsubscribe = onUnsubscribe;
        }

        public string EndPoint { get; }

        public string Topic { get; }

        /// <inheritdoc />
        public bool Persistent => false;

        /// <inheritdoc />
        public Task OnNext(byte[] payload, CancellationToken cancellationToken)
        {
            return _handler.OnNext(payload, cancellationToken);
        }

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            await _onUnsubscribe(this).ConfigureAwait(false);
            _onUnsubscribe = null;
        }
    }
}
