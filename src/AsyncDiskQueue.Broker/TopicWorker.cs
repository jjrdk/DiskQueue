namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Abstractions;

    internal class TopicWorker : IAsyncDisposable
    {
        private readonly CancellationTokenSource _tokenSource = new();

        private Task Work { get; set; }

        public static TopicWorker Create(IDiskQueue queue, IEnumerable<IMessageReceiver> subscribers)
        {
            var worker = new TopicWorker();

            async Task DoWork()
            {
                using var diskQueueSession = queue.OpenSession();
                await foreach (var item in diskQueueSession.ToAsyncEnumerable(cancellationToken: worker._tokenSource.Token))
                {
                    // ReSharper disable once PossibleMultipleEnumeration
                    var s = subscribers.ToArray();
                    var tasks = s.Select(s => s.OnNext(item, CancellationToken.None));
                    await Task.WhenAll(tasks).ConfigureAwait(false);

                    if (worker._tokenSource.Token.IsCancellationRequested)
                    {
                        break;
                    }
                }
            }

            worker.Work = DoWork();

            return worker;
        }

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            _tokenSource.Cancel();
            await Work.ConfigureAwait(false);
            _tokenSource.Dispose();
            Work.Dispose();
        }
    }
}