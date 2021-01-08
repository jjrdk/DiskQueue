namespace AsyncDiskQueue
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Defines the extension methods of a persistent queue.
    /// </summary>
    public static class PersistentQueueSessionExtensions
    {
        /// <summary>
        /// Creates an <see cref="IAsyncEnumerable{T}"/> from the <see cref="IDiskQueueSession"/>.
        /// </summary>
        /// <typeparam name="T">The <see cref="Type"/> of item held in the queue.</typeparam>
        /// <param name="session">The <see cref="IDiskQueueSession"/> to convert.</param>
        /// <param name="cancellationToken">The <see cref="CancellationToken"/> for the async operation.</param>
        /// <returns>An <see cref="IAsyncEnumerable{T}"/> returning non-null queue values.</returns>
        public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(
            this IDiskQueueSession<T> session,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var count = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                var data = await session.Dequeue(cancellationToken).ConfigureAwait(false);
                if (data is null || data.Equals(default(T)))
                {
                    count = Math.Min(10, count + 1);
                    try
                    {
                        await Task.Delay(count * 100, cancellationToken).ConfigureAwait(false);
                    }
                    catch (TaskCanceledException)
                    {
                        break;
                    }
                }
                else
                {
                    count = 0;
                    yield return data;
                }

                await session.Flush(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Creates an <see cref="IAsyncEnumerable{T}"/> from the <see cref="IDiskQueueSession"/>.
        /// </summary>
        /// <param name="session">The <see cref="IDiskQueueSession"/> to convert.</param>
        /// <param name="cancellationToken">The <see cref="CancellationToken"/> for the async operation.</param>
        /// <returns>An <see cref="IAsyncEnumerable{T}"/> returning non-null queue values.</returns>
        public static async IAsyncEnumerable<byte[]> ToAsyncEnumerable(
            this IDiskQueueSession session,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var count = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                var data = await session.Dequeue(cancellationToken).ConfigureAwait(false);
                if (data == null)
                {
                    try
                    {
                        count = Math.Min(10, count + 1);
                        await Task.Delay(count * 100, cancellationToken).ConfigureAwait(false);
                    }
                    catch (TaskCanceledException)
                    {
                        yield break;
                    }
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
