namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    internal class Subscription<T> : ISubscription<T>, IAsyncDisposable
    {
        private readonly ICollection<ISubscription> _subscriptions;
        private Func<Task> _onUnsubscribe;

        public Subscription(Func<T, Task> handler, ICollection<ISubscription> subscriptions, Func<Task> onUnsubscribe)
        {
            Handler = handler;
            _subscriptions = subscriptions;
            _onUnsubscribe = onUnsubscribe;
        }

        /// <inheritdoc />
        public bool Persistent { get; } = false;

        public Func<T, Task> Handler { get; }

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            _subscriptions.Remove(this);
            await _onUnsubscribe().ConfigureAwait(false);
            _onUnsubscribe = null;
        }
    }
}