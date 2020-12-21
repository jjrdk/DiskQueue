namespace DiskQueue.Reactive
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Polly;

    public class PersistentBuffer : IObservable<byte[]>, IObserver<byte[]>, IAsyncDisposable
    {
        private readonly IPersistentQueue queue;
        private readonly int retryCount;
        private CancellationTokenSource tokenSource;
        private readonly List<IObserver<byte[]>> subscribers = new List<IObserver<byte[]>>();
        private Task work;

        public PersistentBuffer(IPersistentQueue queue, int retryCount)
        {
            this.queue = queue;
            this.retryCount = retryCount;
        }

        public void Start()
        {
            lock (queue)
            {
                if (tokenSource != null)
                {
                    return;
                }

                tokenSource = new CancellationTokenSource();
                work = ProcessMessages();
            }
        }

        public async Task Stop()
        {
            if (tokenSource == null)
            {
                return;
            }

            tokenSource.Cancel();
            await work.ConfigureAwait(false);
            tokenSource = null;
        }

        /// <inheritdoc />
        IDisposable IObservable<byte[]>.Subscribe(IObserver<byte[]> observer)
        {
            return new Subscription<byte[]>(observer, subscribers);
        }

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            await Stop().ConfigureAwait(false);
            tokenSource?.Dispose();
        }

        /// <inheritdoc />
        void IObserver<byte[]>.OnCompleted()
        {
            foreach (var subscriber in subscribers)
            {
                subscriber.OnCompleted();
            }

            Stop().Wait();
        }

        /// <inheritdoc />
        void IObserver<byte[]>.OnError(Exception error)
        {
            foreach (var subscriber in subscribers)
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
                    using var session = queue.OpenSession();
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
                    .WaitAndRetryAsync(retryCount, i => TimeSpan.FromMilliseconds(i * 100))
                    .ExecuteAsync(
                        async () => { await ProcessQueueContent().ConfigureAwait(false); })
                    .ConfigureAwait(false);

                foreach (var subscriber in subscribers)
                {
                    subscriber.OnCompleted();
                }
            }
            catch (Exception e)
            {
                foreach (var subscriber in subscribers)
                {
                    subscriber.OnError(e);
                }
            }
        }

        private async Task ProcessQueueContent()
        {
            try
            {
                using var session = queue.OpenSession();
                while (!tokenSource.IsCancellationRequested)
                {
                    var s = session;
                    var content = await Policy.HandleResult<byte[]>(b => b == null)
                        .WaitAndRetryForeverAsync(i => TimeSpan.FromMilliseconds(i * 100))
                        .ExecuteAsync(async () => await s.Dequeue(tokenSource.Token).ConfigureAwait(false))
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
            foreach (var subscriber in subscribers)
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
        private readonly IObserver<T> observer;
        private readonly ICollection<IObserver<T>> observers;

        public Subscription(IObserver<T> observer, ICollection<IObserver<T>> observers)
        {
            this.observer = observer;
            this.observers = observers;
            observers.Add(observer);
        }

        void IDisposable.Dispose()
        {
            observers.Remove(observer);
            GC.SuppressFinalize(this);
        }
    }
}
