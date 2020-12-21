namespace DiskQueue.Reactive
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using Polly;

    public static class PersistentAsyncEnumerable
    {
        public static async IAsyncEnumerable<byte[]> ToAsyncEnumerable(
            this IPersistentQueueSession session,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var content = await Policy.HandleResult<byte[]>(b => b == null)
                    .WaitAndRetryForeverAsync(i => TimeSpan.FromMilliseconds(Math.Min(i * 100, 1000)))
                    .ExecuteAsync(
                        async (_, c) => await session.Dequeue(c).ConfigureAwait(false),
                        new Context(),
                        cancellationToken)
                    .ConfigureAwait(false);
                yield return content;
            }
        }

        public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(
            this IPersistentQueueSession session,
            Func<byte[], T> deserializer,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (var item in session.ToAsyncEnumerable(cancellationToken))
            {
                yield return deserializer(item);
            }
        }
    }
}