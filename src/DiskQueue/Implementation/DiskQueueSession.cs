namespace AsyncDiskQueue.Implementation
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Security.Cryptography;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;


    /// <summary>
    /// <para>You should use <see cref="IDiskQueue.OpenSession"/> to get a session.</para>e.OpenSession"/> to get a session.</para>
    /// <example>using (var q = PersistentQueue.WaitFor("myQueue")) using (var session = q.OpenSession()) { ... }</example>
    /// </summary>
    internal sealed class DiskQueueSession : IDiskQueueSession
    {
        private readonly SymmetricAlgorithm _symmetricAlgorithm;
        private readonly ILogger<IDiskQueueSession> _logger;
        private readonly List<Operation> _operations = new List<Operation>();
        private Stream _currentStream;
        private readonly int _writeBufferSize;
        private IDiskQueueStore _queue;
        private readonly List<Stream> _streamsToDisposeOnFlush = new List<Stream>();
        private static readonly object CtorLock = new object();
        private volatile bool _disposed;
        private readonly List<ReadOnlyMemory<byte>> _buffer = new List<ReadOnlyMemory<byte>>();
        private int _bufferSize;
        private const int MinSizeThatMakeAsyncWritePractical = 64 * 1024;

        /// <para>You should use <see cref="IDiskQueue.OpenSession"/> to get a session.</para>ssion.
        /// <para>You should use <see cref="IDiskQueue.OpenSession"/> to get a session.</para>
        /// <example>using (var q = PersistentQueue.WaitFor("myQueue")) using (var session = q.OpenSession()) { ... }</example>
        /// </summary>
        public DiskQueueSession(
            IDiskQueueStore queue,
            Stream currentStream,
            int writeBufferSize,
            SymmetricAlgorithm symmetricAlgorithm,
            ILogger<IDiskQueueSession> logger)
        {
            _symmetricAlgorithm = symmetricAlgorithm;
            _logger = logger;
            lock (CtorLock)
            {
                _queue = queue;
                _currentStream = currentStream;
                if (writeBufferSize < MinSizeThatMakeAsyncWritePractical)
                {
                    writeBufferSize = MinSizeThatMakeAsyncWritePractical;
                }

                _writeBufferSize = writeBufferSize;
                _disposed = false;
            }
        }

        /// <summary>
        /// Queue data for a later decode. Data is written on `Flush()`
        /// </summary>
        public async Task Enqueue(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _logger.LogDebug("Enqueue item");
            if (_symmetricAlgorithm != null)
            {
                await EnqueueEncrypted(data, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                _buffer.Add(data);
                _bufferSize += data.Length;
            }

            if (_bufferSize > _writeBufferSize)
            {
                await _queue.AcquireWriter(_currentStream, AsyncWriteToStream, OnReplaceStream)
                    .ConfigureAwait(false);
            }
        }

        private async Task EnqueueEncrypted(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            _logger.LogDebug("Encrypting content");
            await using var memoryStream = new MemoryStream();
            await using (var cs = new CryptoStream(memoryStream, _symmetricAlgorithm.CreateEncryptor(), CryptoStreamMode.Write, true))
            {
                await cs.WriteAsync(data, cancellationToken).ConfigureAwait(false);
                await cs.FlushAsync(cancellationToken).ConfigureAwait(false);
                await cs.FlushFinalBlockAsync(cancellationToken).ConfigureAwait(false);
                cs.Close();
            }

            var toAdd = memoryStream.ToArray();
            _buffer.Add(toAdd);
            _bufferSize += toAdd.Length;
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
            var data = new byte[_bufferSize];
            var index = 0;
            foreach (var bytes in _buffer)
            {
                _operations.Add(new Operation(OperationType.Enqueue, _queue.CurrentFileNumber, start, bytes.Length));
                Buffer.BlockCopy(bytes.ToArray(), 0, data, index, bytes.Length);
                bytes.CopyTo(data.AsMemory(index, bytes.Length));
                start += bytes.Length;
                index += bytes.Length;
            }

            _bufferSize = 0;
            _buffer.Clear();
            return data;
        }

        private void OnReplaceStream(Stream newStream)
        {
            _streamsToDisposeOnFlush.Add(_currentStream);
            _currentStream = newStream;
        }

        /// <summary>
        /// Try to pull data from the queue. Data is removed from the queue on `Flush()`
        /// </summary>
        /// <param name="cancellationToken"></param>
        public async Task<byte[]> Dequeue(CancellationToken cancellationToken = default)
        {
            _logger.LogDebug("Dequeue item");
            var entry = await _queue.Dequeue(cancellationToken).ConfigureAwait(false);
            if (entry == null)
            {
                return null;
            }

            _operations.Add(new Operation(OperationType.Dequeue, entry.FileNumber, entry.Start, entry.Length));
            if (_symmetricAlgorithm != null)
            {
                await using var outputStream = new MemoryStream();
                await using var dataStream = new MemoryStream(entry.Data);
                await using var cryptoStream = new CryptoStream(
                    dataStream,
                    _symmetricAlgorithm.CreateDecryptor(),
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
                await _queue.AcquireWriter(_currentStream, AsyncWriteToStream, OnReplaceStream)
                    .ConfigureAwait(false);
            }
            finally
            {
                foreach (var stream in _streamsToDisposeOnFlush)
                {
                    await stream.FlushAsync(CancellationToken.None).ConfigureAwait(false);
                    await stream.DisposeAsync().ConfigureAwait(false);
                }

                _streamsToDisposeOnFlush.Clear();
            }

            await _currentStream.FlushAsync(cancellationToken).ConfigureAwait(false);
            await _queue.CommitTransaction(_operations).ConfigureAwait(false);
            _operations.Clear();
        }

        /// <summary>
        /// Close session, restoring any non-flushed operations
        /// </summary>
        public void Dispose()
        {
            lock (CtorLock)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;
                _queue.Reinstate(_operations);
                _queue = null;
                _operations.Clear();
                foreach (var stream in _streamsToDisposeOnFlush)
                {
                    stream.Dispose();
                }
                _streamsToDisposeOnFlush.Clear();

                _currentStream.Dispose();
                _currentStream = null;
                GC.SuppressFinalize(this);
            }
        }

        /// <summary>
        /// Dispose queue on destructor. This is a safety-valve. You should ensure you
        /// dispose of sessions normally.
        /// </summary>
        ~DiskQueueSession()
        {
            if (_disposed) return;
            Dispose();
        }
    }
}
