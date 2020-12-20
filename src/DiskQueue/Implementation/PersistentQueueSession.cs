using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace DiskQueue.Implementation
{
    using System.Security.Cryptography;
    using System.Threading.Tasks;

    /// <summary>
	/// Default persistent queue session.
	/// <para>You should use <see cref="IPersistentQueue.OpenSession"/> to get a session.</para>
	/// <example>using (var q = PersistentQueue.WaitFor("myQueue")) using (var session = q.OpenSession()) { ... }</example>
	/// </summary>
	internal sealed class PersistentQueueSession : IPersistentQueueSession
    {
        private readonly List<Operation> operations = new List<Operation>();
        private Stream currentStream;
        private readonly int writeBufferSize;
        private readonly IPersistentQueue queue;
        private readonly List<Stream> streamsToDisposeOnFlush = new List<Stream>();
        private static readonly object _ctorLock = new object();
        volatile bool disposed;

        private readonly List<byte[]> buffer = new List<byte[]>();
        private int bufferSize;

        private const int MinSizeThatMakeAsyncWritePractical = 64 * 1024;

        /// <summary>
        /// Create a default persistent queue session.
        /// <para>You should use <see cref="IPersistentQueue.OpenSession"/> to get a session.</para>
        /// <example>using (var q = PersistentQueue.WaitFor("myQueue")) using (var session = q.OpenSession()) { ... }</example>
        /// </summary>
        public PersistentQueueSession(IPersistentQueue queue, Stream currentStream, int writeBufferSize)
        {
            lock (_ctorLock)
            {
                this.queue = queue;
                this.currentStream = currentStream;
                if (writeBufferSize < MinSizeThatMakeAsyncWritePractical)
                    writeBufferSize = MinSizeThatMakeAsyncWritePractical;
                this.writeBufferSize = writeBufferSize;
                disposed = false;
            }
        }

        /// <summary>
        /// Queue data for a later decode. Data is written on `Flush()`
        /// </summary>
        public async Task Enqueue(byte[] data, CancellationToken cancellationToken = default)
        {
            buffer.Add(data);
            bufferSize += data.Length;
            if (bufferSize > writeBufferSize)
            {
                await AsyncFlushBuffer(cancellationToken).ConfigureAwait(false);
            }
        }

        private Task AsyncFlushBuffer(CancellationToken cancellationToken)
        {
            return queue.AcquireWriter(currentStream, AsyncWriteToStream, OnReplaceStream, cancellationToken);
        }

        //private void SyncFlushBuffer()
        //{
        //	queue.AcquireWriter(currentStream, stream =>
        //	{
        //		byte[] data = ConcatenateBufferAndAddIndividualOperations(stream);
        //		stream.Write(data, 0, data.Length);
        //		return stream.Position;
        //	}, OnReplaceStream);
        //}

        private async Task<long> AsyncWriteToStream(Stream stream)
        {
            byte[] data = ConcatenateBufferAndAddIndividualOperations(stream);
            long positionAfterWrite = stream.Position + data.Length;
            await stream.WriteAsync(data, 0, data.Length).ConfigureAwait(false);

            return positionAfterWrite;
        }

        private byte[] ConcatenateBufferAndAddIndividualOperations(Stream stream)
        {
            var data = new byte[bufferSize];
            var start = (int)stream.Position;
            var index = 0;
            foreach (var bytes in buffer)
            {
                operations.Add(new Operation(
                    OperationType.Enqueue,
                    queue.CurrentFileNumber,
                    start,
                    bytes.Length
                ));
                Buffer.BlockCopy(bytes, 0, data, index, bytes.Length);
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
            try
            {
                await AsyncFlushBuffer(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                foreach (var stream in streamsToDisposeOnFlush)
                {
                    await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                    await stream.DisposeAsync().ConfigureAwait(false);
                }
                streamsToDisposeOnFlush.Clear();
            }
            await currentStream.FlushAsync(cancellationToken).ConfigureAwait(false);
            await queue.CommitTransaction(operations, cancellationToken).ConfigureAwait(false);
            operations.Clear();
        }

        //private void WaitForPendingWrites()
        //{
        //    while (pendingWritesHandles.Count != 0)
        //    {
        //        var handles = pendingWritesHandles.Take(64).ToArray();
        //        foreach (var handle in handles)
        //        {
        //            pendingWritesHandles.Remove(handle);
        //        }
        //        WaitHandle.WaitAll(handles);
        //        foreach (var handle in handles)
        //        {
        //            handle.Close();
        //        }
        //        AssertNoPendingWritesFailures();
        //    }
        //}

        //private void AssertNoPendingWritesFailures()
        //{
        //    lock (pendingWritesFailures)
        //    {
        //        if (pendingWritesFailures.Count == 0)
        //            return;

        //        var array = pendingWritesFailures.ToArray();
        //        pendingWritesFailures.Clear();
        //        throw new PendingWriteException(array);
        //    }
        //}

        /// <summary>
        /// Close session, restoring any non-flushed operations
        /// </summary>
        public void Dispose()
        {
            lock (_ctorLock)
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
        /// Dispose queue on destructor. This is a safty-valve. You should ensure you
        /// dispose of sessions normally.
        /// </summary>
        ~PersistentQueueSession()
        {
            if (disposed) return;
            Dispose();
        }
    }
}