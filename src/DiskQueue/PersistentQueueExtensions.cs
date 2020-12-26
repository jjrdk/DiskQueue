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
        /// <param name="queue">The <see cref="IDiskQueue"/> to open the session from.</param>
        /// <param name="serializer">The serializer function.</param>
        /// <param name="deserializer">The deserializer function.</param>
        /// <returns></returns>
        public static IDiskQueueSession<T> OpenSession<T>(
            this IDiskQueue queue,
            Func<T, byte[]> serializer,
            Func<byte[], T> deserializer)
        {
            return new TypedDiskQueueSession<T>(queue.OpenSession(), serializer, deserializer);
        }

        private class TypedDiskQueueSession<T> : IDiskQueueSession<T>
        {
            private readonly IDiskQueueSession _rawSession;
            private readonly Func<T, byte[]> _serializer;
            private readonly Func<byte[], T> _deserializer;

            public TypedDiskQueueSession(IDiskQueueSession rawSession, Func<T, byte[]> serializer, Func<byte[], T> deserializer)
            {
                _rawSession = rawSession;
                _serializer = serializer;
                _deserializer = deserializer;
            }

            /// <inheritdoc />
            public void Dispose()
            {
                _rawSession.Dispose();
                GC.SuppressFinalize(this);
            }

            /// <inheritdoc />
            public Task Enqueue(T data, CancellationToken cancellationToken = default)
            {
                var bytes = _serializer(data);
                return bytes == null ? Task.CompletedTask : _rawSession.Enqueue(bytes, cancellationToken);
            }

            /// <inheritdoc />
            public async Task<T> Dequeue(CancellationToken cancellationToken = default)
            {
                var bytes = await _rawSession.Dequeue(cancellationToken).ConfigureAwait(false);
                return bytes == null ? default : _deserializer(bytes);
            }

            /// <inheritdoc />
            public Task Flush(CancellationToken cancellationToken = default)
            {
                return _rawSession.Flush(cancellationToken);
            }
        }
    }
}