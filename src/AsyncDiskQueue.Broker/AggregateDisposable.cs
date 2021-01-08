namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    internal class NullDisposable : IAsyncDisposable
    {
        /// <inheritdoc />
        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    internal class AggregateDisposable : IAsyncDisposable
    {
        private readonly IEnumerable<IAsyncDisposable> _innerDisposables;

        public AggregateDisposable(IEnumerable<IAsyncDisposable> innerDisposables)
        {
            _innerDisposables = innerDisposables.ToArray();
        }

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            await Task.WhenAll(_innerDisposables.Select(x => x.DisposeAsync().AsTask())).ConfigureAwait(false);
        }
    }
}