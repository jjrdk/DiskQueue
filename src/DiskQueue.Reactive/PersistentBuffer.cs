namespace AsyncDiskQueue.Reactive
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Polly;

    public class PersistentBuffer : IObservable<byte[]>, IObserver<byte[]>, IAsyncDisposable
    {
        private readonly IDiskQueue _queue;
        private readonly int _retryCount;
        private CancellationTokenSource _tokenSource;
        private readonly List<IObserver<byte[]>> _subscribers = new List<IObserver<byte[]>>();
        private Task _work;

        public PersistentBuffer(IDiskQueue queue, int retryCount)
        {
            _queue = queue;
            _retryCount = retryCount;
        }

        public void Start()
        {
            lock (_queue)
            {
                if (_tokenSource != null)
                {
                    return;
                }

                _tokenSource = new CancellationTokenSource();
                _work = ProcessMessages();
            }
        }

        public async Task Stop()
        {
            if (_tokenSource == null)
            {
                return;
            }

            _tokenSource.Cancel();
            await _work.ConfigureAwait(false);
            _tokenSource = null;
        }

        /// <inheritdoc />
        IDisposable IObservable<byte[]>.Subscribe(IObserver<byte[]> observer)
        {
            return new Subscription<byte[]>(observer, _subscribers);
        }

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            await Stop().ConfigureAwait(false);
            _tokenSource?.Dispose();
        }

        /// <inheritdoc />
        void IObserver<byte[]>.OnCompleted()
        {
            foreach (var subscriber in _subscribers)
            {
                subscriber.OnCompleted();
            }

            Stop().Wait();
        }

        /// <inheritdoc />
        void IObserver<byte[]>.OnError(Exception error)
        {
            foreach (var subscriber in _subscribers)
            {
                subscriber.OnError(error);
            }
        }

        /// <inheritdoc />
        void IObserver<byte[]>.OnNext(byte[] value)
        {
            var task = Task.Run(
                async () =>
                {
                    using var session = _queue.OpenSession();
                    await session.Enqueue(value).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                });
            task.Wait();
        }

        private async Task ProcessMessages()
        {
            try
            {
                await Policy.Handle<SubscriberException>()
                    .WaitAndRetryAsync(_retryCount, i => TimeSpan.FromMilliseconds(i * 100))
                    .ExecuteAsync(
                        async () => { await ProcessQueueContent().ConfigureAwait(false); })
                    .ConfigureAwait(false);

                foreach (var subscriber in _subscribers)
                {
                    subscriber.OnCompleted();
                }
            }
            catch (Exception e)
            {
                foreach (var subscriber in _subscribers)
                {
                    subscriber.OnError(e);
                }
            }
        }

        private async Task ProcessQueueContent()
        {
            try
            {
                using var session = _queue.OpenSession();
                while (!_tokenSource.IsCancellationRequested)
                {
                    var s = session;
                    var content = await Policy.HandleResult<byte[]>(b => b == null)
                        .WaitAndRetryForeverAsync(i => TimeSpan.FromMilliseconds(i * 100))
                        .ExecuteAsync(async () => await s.Dequeue(_tokenSource.Token).ConfigureAwait(false))
                        .ConfigureAwait(false);

                    NotifySubscribers(content);

                    await session.Flush().ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        private void NotifySubscribers(byte[] content)
        {
            foreach (var subscriber in _subscribers)
            {
                try
                {
                    subscriber.OnNext(content);
                }
                catch
                {
                    throw new SubscriberException();
                }
            }
        }
    }

    public class Subscription<T> : IDisposable
    {
        private readonly IObserver<T> _observer;
        private readonly ICollection<IObserver<T>> _observers;

        public Subscription(IObserver<T> observer, ICollection<IObserver<T>> observers)
        {
            _observer = observer;
            _observers = observers;
            observers.Add(observer);
        }

        void IDisposable.Dispose()
        {
            _observers.Remove(_observer);
            GC.SuppressFinalize(this);
        }
    }
}
