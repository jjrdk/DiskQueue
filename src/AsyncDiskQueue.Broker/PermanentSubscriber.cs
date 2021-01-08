namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    internal class PermanentSubscriber : ISubscriptionSource
    {
        private readonly IDiskQueue _queue;
        private IMessageReceiver _subscription;
        private CancellationTokenSource _tokenSource;
        private Task _worker;

        public PermanentSubscriber(
            string endPoint,
            string topic,
            IDiskQueue queue)
        {
            EndPoint = endPoint;
            Topic = topic;
            _queue = queue;
        }

        /// <inheritdoc />
        public string EndPoint { get; }

        /// <inheritdoc />
        public string Topic { get; }

        /// <inheritdoc />
        public bool Persistent => _subscription?.Persistent ?? true;

        /// <inheritdoc />
        public async Task OnNext(byte[] msg, CancellationToken cancellationToken)
        {
            using var session = _queue.OpenSession();
            await session.Enqueue(msg, cancellationToken).ConfigureAwait(false);
            await session.Flush(cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public ValueTask DisposeAsync()
        {
            return Disconnect();
        }

        private async ValueTask Disconnect()
        {
            _tokenSource.Cancel();
            await _worker.ConfigureAwait(false);
            _worker.Dispose();
            _tokenSource.Dispose();
            _worker = null;
            _tokenSource = null;
        }

        private async Task Worker(CancellationToken cancellationToken)
        {
            using var session = _queue.OpenSession();
            await foreach (var item in session.ToAsyncEnumerable(cancellationToken).ConfigureAwait(false))
            {
                await _subscription.OnNext(item, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        public IAsyncDisposable Connect(IMessageReceiver subscriptionRequest)
        {
            _subscription = subscriptionRequest ?? throw new ArgumentNullException(nameof(subscriptionRequest));
            _tokenSource = new CancellationTokenSource();
            _worker = Worker(_tokenSource.Token);

            return new SubscriptionWorker(EndPoint, subscriptionRequest, s => Disconnect().AsTask());
        }
    }
}