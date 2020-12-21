namespace AsyncDiskQueue.Implementation
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Security.Cryptography;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Default persistent queue session.
    /// <para>You should use <see cref="IPersistentQueue.OpenSession"/> to get a session.</para>
    /// <example>using (var q = PersistentQueue.WaitFor("myQueue")) using (var session = q.OpenSession()) { ... }</example>
    /// </summary>
    internal sealed class PersistentQueueSession : IPersistentQueueSession
    {
        private readonly SymmetricAlgorithm symmetricAlgorithm;
        private readonly List<Operation> operations = new List<Operation>();
        private Stream currentStream;
        private readonly int writeBufferSize;
        private readonly IPersistentQueueStore queue;
        private readonly List<Stream> streamsToDisposeOnFlush = new List<Stream>();
        private static readonly object CtorLock = new object();
        private volatile bool disposed;
        private readonly List<ReadOnlyMemory<byte>> buffer = new List<ReadOnlyMemory<byte>>();
        private int bufferSize;
        private const int MinSizeThatMakeAsyncWritePractical = 64 * 1024;

        /// <summary>
        /// Create a default persistent queue session.
        /// <para>You should use <see cref="IPersistentQueue.OpenSession"/> to get a session.</para>
        /// <example>using (var q = PersistentQueue.WaitFor("myQueue")) using (var session = q.OpenSession()) { ... }</example>
        /// </summary>
        public PersistentQueueSession(
            IPersistentQueueStore queue,
            Stream currentStream,
            int writeBufferSize,
            SymmetricAlgorithm symmetricAlgorithm)
        {
            this.symmetricAlgorithm = symmetricAlgorithm;
            lock (CtorLock)
            {
                this.queue = queue;
                this.currentStream = currentStream;
                if (writeBufferSize < MinSizeThatMakeAsyncWritePractical)
                {
                    writeBufferSize = MinSizeThatMakeAsyncWritePractical;
                }

                this.writeBufferSize = writeBufferSize;
                disposed = false;
            }
        }

        /// <summary>
        /// Queue data for a later decode. Data is written on `Flush()`
        /// </summary>
        public async Task Enqueue(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (symmetricAlgorithm != null)
            {
                await EnqueueEncrypted(data, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                buffer.Add(data);
                bufferSize += data.Length;
            }

            if (bufferSize > writeBufferSize)
            {
                await queue.AcquireWriter(currentStream, AsyncWriteToStream, OnReplaceStream)
                    .ConfigureAwait(false);
            }
        }

        private async Task EnqueueEncrypted(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            await using var memoryStream = new MemoryStream();
            await using (var cs = new CryptoStream(memoryStream, symmetricAlgorithm.CreateEncryptor(), CryptoStreamMode.Write, true))
            {
                await cs.WriteAsync(data, cancellationToken).ConfigureAwait(false);
                await cs.FlushAsync(cancellationToken).ConfigureAwait(false);
                await cs.FlushFinalBlockAsync(cancellationToken).ConfigureAwait(false);
                cs.Close();
            }

            var toAdd = memoryStream.ToArray();
            buffer.Add(toAdd);
            bufferSize += toAdd.Length;
        }

        private async Task<long> AsyncWriteToStream(Stream stream)
        {
            var data = ConcatenateBufferAndAddIndividualOperations((int)stream.Position);
            var positionAfterWrite = stream.Position + data.Length;
            await stream.WriteAsync(data).ConfigureAwait(false);

            return positionAfterWrite;
        }

        private byte[] ConcatenateBufferAndAddIndividualOperations(int start)
        {
            var data = new byte[bufferSize];
            var index = 0;
            foreach (var bytes in buffer)
            {
                operations.Add(new Operation(OperationType.Enqueue, queue.CurrentFileNumber, start, bytes.Length));
                Buffer.BlockCopy(bytes.ToArray(), 0, data, index, bytes.Length);
                bytes.CopyTo(data.AsMemory(index, bytes.Length));
                start += bytes.Length;
                index += bytes.Length;
            }

            bufferSize = 0;
            buffer.Clear();
            return data;
        }

        private void OnReplaceStream(Stream newStream)
        {
            streamsToDisposeOnFlush.Add(currentStream);
            currentStream = newStream;
        }

        /// <summary>
        /// Try to pull data from the queue. Data is removed from the queue on `Flush()`
        /// </summary>
        /// <param name="cancellationToken"></param>
        public async Task<byte[]> Dequeue(CancellationToken cancellationToken = default)
        {
            var entry = await queue.Dequeue(cancellationToken).ConfigureAwait(false);
            if (entry == null)
            {
                return null;
            }

            operations.Add(new Operation(OperationType.Dequeue, entry.FileNumber, entry.Start, entry.Length));
            if (symmetricAlgorithm != null)
            {
                await using var outputStream = new MemoryStream();
                await using var dataStream = new MemoryStream(entry.Data);
                await using var cryptoStream = new CryptoStream(
                    dataStream,
                    symmetricAlgorithm.CreateDecryptor(),
                    CryptoStreamMode.Read);
                await cryptoStream.CopyToAsync(outputStream, cancellationToken).ConfigureAwait(false);
                await cryptoStream.FlushAsync(cancellationToken).ConfigureAwait(false);
                return outputStream.ToArray();
            }

            return entry.Data;
        }

        /// <summary>
        /// Commit actions taken in this session since last flush.
        /// If the session is disposed with no flush, actions are not persisted
        /// to the queue (Enqueues are not written, dequeues are left on the queue)
        /// </summary>
        /// <param name="cancellationToken"></param>
        public async Task Flush(CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            try
            {
                await queue.AcquireWriter(currentStream, AsyncWriteToStream, OnReplaceStream)
                    .ConfigureAwait(false);
            }
            finally
            {
                foreach (var stream in streamsToDisposeOnFlush)
                {
                    await stream.FlushAsync(CancellationToken.None).ConfigureAwait(false);
                    await stream.DisposeAsync().ConfigureAwait(false);
                }

                streamsToDisposeOnFlush.Clear();
            }

            await currentStream.FlushAsync(cancellationToken).ConfigureAwait(false);
            await queue.CommitTransaction(operations).ConfigureAwait(false);
            operations.Clear();
        }

        /// <summary>
        /// Close session, restoring any non-flushed operations
        /// </summary>
        public void Dispose()
        {
            lock (CtorLock)
            {
                if (disposed)
                {
                    return;
                }

                disposed = true;
                queue.Reinstate(operations);
                operations.Clear();
                foreach (var stream in streamsToDisposeOnFlush)
                {
                    stream.Dispose();
                }

                currentStream.Dispose();
                GC.SuppressFinalize(this);
            }
        }

        /// <summary>
        /// Dispose queue on destructor. This is a safety-valve. You should ensure you
        /// dispose of sessions normally.
        /// </summary>
        ~PersistentQueueSession()
        {
            if (disposed) return;
            Dispose();
        }
    }
}
