using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace AsyncDiskQueue;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Defines the extension methods of a persistent queue.
/// </summary>
public static class PersistentQueueExtensions
{
    /// <summary>
    /// Opens a typed session.
    /// </summary>
    /// <typeparam name="T">The <see cref="Type"/> of item held in the queue.</typeparam>
    /// <param name="queue">The <see cref="IPersistentQueue"/> to open the session from.</param>
    /// <param name="serializer">The serializer function.</param>
    /// <param name="deserializer">The deserializer function.</param>
    /// <returns></returns>
    public static IPersistentQueueSession<T> OpenSession<T>(
        this IPersistentQueue queue,
        Func<T, byte[]> serializer,
        Func<ReadOnlyMemory<byte>, T> deserializer)
    {
        return new TypedPersistentQueueSession<T>(queue.OpenSession(), serializer, deserializer);
    }

    private class TypedPersistentQueueSession<T> : IPersistentQueueSession<T>
    {
        private readonly IPersistentQueueSession rawSession;
        private readonly Func<T, byte[]> serializer;
        private readonly Func<ReadOnlyMemory<byte>, T> deserializer;

        public TypedPersistentQueueSession(
            IPersistentQueueSession rawSession,
            Func<T, byte[]> serializer,
            Func<ReadOnlyMemory<byte>, T> deserializer)
        {
            this.rawSession = rawSession;
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            rawSession.Dispose();
            GC.SuppressFinalize(this);
        }

        /// <inheritdoc />
        public Task Enqueue(T data, CancellationToken cancellationToken = default)
        {
            var bytes = serializer(data);
            return bytes == null ? Task.CompletedTask : rawSession.Enqueue(bytes, cancellationToken);
        }

        /// <inheritdoc />
        public async Task<T> Dequeue(CancellationToken cancellationToken = default)
        {
            var bytes = await rawSession.Dequeue(cancellationToken).ConfigureAwait(false);
            return bytes.IsEmpty ? default : deserializer(bytes);
        }

        /// <inheritdoc />
        public Task Flush(CancellationToken cancellationToken = default)
        {
            return rawSession.Flush(cancellationToken);
        }
    }

    /// <summary>
    /// Converts the <see cref="IPersistentQueueSession"/> to an <see cref="IAsyncEnumerable{T}"/>.
    /// </summary>
    /// <param name="session">The queue to convert.</param>
    /// <param name="deserializer">The item deserializer.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> for the async operation.</param>
    /// <typeparam name="T">The item <see cref="Type"/>.</typeparam>
    /// <returns>The queue as an <see cref="IAsyncEnumerable{T}"/></returns>
    public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(
        this IPersistentQueueSession session,
        Func<ReadOnlyMemory<byte>, T> deserializer,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in session.ToAsyncEnumerable(cancellationToken))
        {
            yield return deserializer(item);
        }
    }
}
