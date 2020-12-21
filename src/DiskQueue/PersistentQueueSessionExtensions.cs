namespace DiskQueue
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;

    public static class PersistentQueueSessionExtensions
    {
        public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(
            this IPersistentQueueSession<T> session,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var count = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                var data = await session.Dequeue(cancellationToken).ConfigureAwait(false);
                //var g = default(T);
                if (ReferenceEquals(null, data) || data.Equals(default(T)))
                {
                    count = Math.Min(10, count + 1);
                    await Task.Delay(count * 100, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    count = 0;
                    yield return data;
                }
            }
        }

        public static async IAsyncEnumerable<byte[]> ToAsyncEnumerable(
            this IPersistentQueueSession session,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var count = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                var data = await session.Dequeue(cancellationToken).ConfigureAwait(false);
                if (data == null)
                {
                    count = Math.Min(10, count + 1);
                    await Task.Delay(count * 100, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    count = 0;
                    yield return data;
                }
            }
        }
    }
}
