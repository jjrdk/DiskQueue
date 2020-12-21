namespace AsyncDiskQueue
{
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
            Func<byte[], T> deserializer)
        {
            return new TypedPersistentQueueSession<T>(queue.OpenSession(), serializer, deserializer);
        }

        private class TypedPersistentQueueSession<T> : IPersistentQueueSession<T>
        {
            private readonly IPersistentQueueSession rawSession;
            private readonly Func<T, byte[]> serializer;
            private readonly Func<byte[], T> deserializer;

            public TypedPersistentQueueSession(IPersistentQueueSession rawSession, Func<T, byte[]> serializer, Func<byte[], T> deserializer)
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
                return bytes == null ? default : deserializer(bytes);
            }

            /// <inheritdoc />
            public Task Flush(CancellationToken cancellationToken = default)
            {
                return rawSession.Flush(cancellationToken);
            }
        }
    }
}