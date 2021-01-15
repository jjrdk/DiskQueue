// Copyright (c) 2005 - 2008 Ayende Rahien (ayende@ayende.com)
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//     this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//     * Neither the name of Ayende Rahien nor the names of its
//     contributors may be used to endorse or promote products derived from this
//     software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
// THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

namespace AsyncDiskQueue
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Security.Cryptography;
    using System.Threading;
    using System.Threading.Tasks;
    using Implementation;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Implements the persistent queue.
    /// </summary>
    public class DiskQueue : IDiskQueue, IDiskQueueStore
    {
        private readonly SemaphoreSlim _entriesSemaphore = new SemaphoreSlim(1);
        private readonly SemaphoreSlim _writerSemaphore = new SemaphoreSlim(1);
        private readonly SemaphoreSlim _transactionLogSemaphore = new SemaphoreSlim(1);
        private readonly HashSet<Entry> _checkedOutEntries = new HashSet<Entry>();
        private readonly Dictionary<int, int> _countOfItemsPerFile = new Dictionary<int, int>();
        private readonly LinkedList<Entry> _entries = new LinkedList<Entry>();
        private readonly string _path;
        private readonly bool _throwOnConflict;
        private static readonly SemaphoreSlim ConfigSemaphore = new SemaphoreSlim(1);
        private static readonly object ConfigLock = new object();
        private volatile bool _disposed;
        private FileStream _fileLock;
        private readonly SymmetricAlgorithm _symmetricAlgorithm;
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger<IDiskQueue> _logger;
        private readonly bool _persistent;
        private readonly bool _trimTransactionLogOnDispose;
        private readonly int _suggestedReadBuffer;
        private readonly int _suggestedWriteBuffer;
        private readonly long _suggestedMaxTransactionLogSize;
        private readonly int _maxFileSize;
        private readonly bool _paranoidFlushing;
        private int _currentFileNumber;
        private long _currentFilePosition;

        private DiskQueue(
            string path,
            bool persistent,
            int maxFileSize,
            bool throwOnConflict,
            int maxTransactionLogSize,
            bool trimTransactionLogOnDispose,
            bool paranoidFlushing,
            int suggestedReadBuffer,
            int suggestedWriteBuffer,
            SymmetricAlgorithm symmetricAlgorithm,
            ILoggerFactory loggerFactory)
        {
            if (path == null)
            {
                throw new ArgumentNullException(nameof(path));
            }

            _persistent = persistent;
            _trimTransactionLogOnDispose = trimTransactionLogOnDispose;
            _symmetricAlgorithm = symmetricAlgorithm;
            _loggerFactory = loggerFactory;
            _logger = loggerFactory.CreateLogger<IDiskQueue>();
            lock (ConfigLock)
            {
                _disposed = true;
                _trimTransactionLogOnDispose = trimTransactionLogOnDispose;
                _paranoidFlushing = paranoidFlushing;
                _suggestedMaxTransactionLogSize = maxTransactionLogSize;
                _suggestedReadBuffer = suggestedReadBuffer;
                _suggestedWriteBuffer = suggestedWriteBuffer;
                _throwOnConflict = throwOnConflict;

                _maxFileSize = maxFileSize;
                try
                {
                    _path = Path.GetFullPath(path);
                    if (!Directory.Exists(_path))
                    {
                        CreateDirectory(_path);
                    }

                    LockQueue();
                }
                catch (UnauthorizedAccessException ex)
                {
                    _logger.LogError(ex, "Unauthorized access");
                    throw new UnauthorizedAccessException(
                        $"Directory \"{path}\" does not exist or is missing write permissions");
                }
                catch (IOException e)
                {
                    _logger.LogError(e, "IO exception");
                    throw new InvalidOperationException("Another instance of the queue is already in action, or directory does not exists", e);
                }
            }
        }

        /// <summary>
        /// Creates a new instance of a persistent queue.
        /// </summary>
        /// <param name="path">The storage path for queue files.</param>
        /// <param name="loggerFactory">The logger.</param>
        /// <param name="maxWait">The max wait time to create the queue.</param>
        /// <param name="persistent">Persistent queues are not deleted from disk when disposed.</param>
        /// <param name="maxFileSize">The max size for data files.</param>
        /// <param name="throwOnConflict">Throw on file conflicts.</param>
        /// <param name="suggestedMaxTransactionLogSize">The suggested transaction log size.</param>
        /// <param name="trimTransactionLogOnDispose">Trim transaction log on dispose.</param>
        /// <param name="paranoidFlushing">Flush buffers paranoid.</param>
        /// <param name="suggestedReadBuffer">Suggested size of read buffer.</param>
        /// <param name="suggestedWriteBuffer">Suggested size of write buffer.</param>
        /// <param name="symmetricAlgorithm">The symmetric algorithm for queue content encryption.</param>
        /// <returns>An <see cref="IDiskQueue"/> as an async operation.</returns>
        public static Task<IDiskQueue> Create(
            string path,
            ILoggerFactory loggerFactory,
            TimeSpan maxWait,
            bool persistent = true,
            int maxFileSize = Constants._32Megabytes,
            bool throwOnConflict = true,
            int suggestedMaxTransactionLogSize = Constants._32Megabytes,
            bool trimTransactionLogOnDispose = true,
            bool paranoidFlushing = true,
            int suggestedReadBuffer = Constants._1Megabyte,
            int suggestedWriteBuffer = Constants._1Megabyte,
            SymmetricAlgorithm symmetricAlgorithm = null)
        {
            using var source = new CancellationTokenSource(maxWait);
            return Create(
                path,
                loggerFactory,
                persistent,
                maxFileSize: maxFileSize,
                throwOnConflict: throwOnConflict,
                suggestedMaxTransactionLogSize: suggestedMaxTransactionLogSize,
                trimTransactionLogOnDispose: trimTransactionLogOnDispose,
                paranoidFlushing: paranoidFlushing,
                suggestedReadBuffer: suggestedReadBuffer,
                suggestedWriteBuffer: suggestedWriteBuffer,
                symmetricAlgorithm: symmetricAlgorithm,
                cancellationToken: source.Token);
        }

        /// <summary>
        /// Creates a new instance of a persistent queue.
        /// </summary>
        /// <param name="path">The storage path for queue files.</param>
        /// <param name="loggerFactory">The logger.</param>
        /// <param name="persistent">Persistent queues are not deleted from disk when disposed.</param>
        /// <param name="maxFileSize">The max size for data files.</param>
        /// <param name="throwOnConflict">Throw on file conflicts.</param>
        /// <param name="suggestedMaxTransactionLogSize">The suggested transaction log size.</param>
        /// <param name="trimTransactionLogOnDispose">Trim transaction log on dispose.</param>
        /// <param name="paranoidFlushing">Flush buffers paranoid.</param>
        /// <param name="suggestedReadBuffer">Suggested size of read buffer.</param>
        /// <param name="suggestedWriteBuffer">Suggested size of write buffer.</param>
        /// <param name="symmetricAlgorithm">The symmetric algorithm for queue content encryption.</param>
        /// <param name="cancellationToken">The cancellation token for the async creation.</param>
        /// <returns>An <see cref="IDiskQueue"/> as an async operation.</returns>
        public static async Task<IDiskQueue> Create(
            string path,
            ILoggerFactory loggerFactory,
            bool persistent = true,
            int maxFileSize = Constants._32Megabytes,
            bool throwOnConflict = true,
            int suggestedMaxTransactionLogSize = Constants._32Megabytes,
            bool trimTransactionLogOnDispose = true,
            bool paranoidFlushing = true,
            int suggestedReadBuffer = Constants._1Megabyte,
            int suggestedWriteBuffer = Constants._1Megabyte,
            SymmetricAlgorithm symmetricAlgorithm = null,
            CancellationToken cancellationToken = default)
        {
            var logger = loggerFactory.CreateLogger<IDiskQueue>();
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    await ConfigSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                    try
                    {
                        var instance = new DiskQueue(
                            path,
                            persistent,
                            maxFileSize,
                            throwOnConflict,
                            suggestedMaxTransactionLogSize,
                            trimTransactionLogOnDispose,
                            paranoidFlushing,
                            suggestedReadBuffer,
                            suggestedWriteBuffer,
                            symmetricAlgorithm,
                            loggerFactory);

                        try
                        {
                            instance.ReadMetaState();
                            await instance.ReadTransactionLog().ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            logger.LogError(e, "Error creating instance");
                            instance.UnlockQueue();
                            throw;
                        }

                        instance._disposed = false;

                        logger.LogDebug("Queue instance created");
                        return instance;
                    }
                    catch (DirectoryNotFoundException)
                    {
                        logger.LogError("Error creating instance");
                        throw new Exception("Target storagePath does not exist or is not accessible");
                    }
                    catch (PlatformNotSupportedException ex)
                    {
                        logger.LogError(ex, "Blocked by " + ex.GetType().Name);
                        throw;
                    }
                    catch (UnableToSetupException)
                    {
                        logger.LogError("Unable to set up");
                        throw;
                    }
                    catch (Exception)
                    {
                        if (cancellationToken == default)
                        {
                            throw;
                        }
                        logger.LogDebug("Retrying setup.");
                        await Task.Delay(50, cancellationToken).ConfigureAwait(false);
                    }
                }
                finally
                {
                    ConfigSemaphore.Release();
                }
            }
        }

        private void UnlockQueue()
        {
            _logger.LogDebug("Unlock queue");
            var target = Path.Combine(_path, "lock");
            if (_fileLock != null)
            {
                _fileLock.Dispose();
                File.Delete(target);
            }
            _fileLock = null;
        }

        private void LockQueue()
        {
            _logger.LogDebug("Lock queue");
            var target = Path.Combine(_path, "lock");
            _fileLock = new FileStream(
                target,
                FileMode.Create,
                FileAccess.ReadWrite,
                FileShare.None);
        }

        private static void CreateDirectory(string s)
        {
            Directory.CreateDirectory(s);
            SetPermissions.TryAllowReadWriteForAll(s);
        }

        int IDiskQueueStore.EstimatedCountOfItemsInQueue
        {
            get
            {
                if (_entries == null)
                {
                    return 0;
                }
                try
                {
                    _entriesSemaphore.Wait();
                    return _entries.Count + _checkedOutEntries.Count;
                }
                finally
                {
                    _entriesSemaphore.Release();
                }
            }
        }

        private int CurrentCountOfItemsInQueue
        {
            get
            {
                try
                {
                    _entriesSemaphore.Wait();
                    return _entries.Count + _checkedOutEntries.Count;
                }
                finally
                {
                    _entriesSemaphore.Release();
                }
            }
        }

        private string TransactionLog => Path.Combine(_path, "transaction.log");

        private string Meta => Path.Combine(_path, "meta.state");

        int IDiskQueueStore.CurrentFileNumber => _currentFileNumber;

        /// <inheritdoc />
        public virtual async ValueTask DisposeAsync()
        {
            try
            {
                await ConfigSemaphore.WaitAsync().ConfigureAwait(false);
                if (_disposed)
                {
                    return;
                }

                try
                {
                    _logger.LogDebug("Disposing queue");
                    _disposed = true;
                    try
                    {
                        await _transactionLogSemaphore.WaitAsync().ConfigureAwait(false);
                        if (_trimTransactionLogOnDispose)
                        {
                            await FlushTrimmedTransactionLog().ConfigureAwait(false);
                        }
                    }
                    finally
                    {
                        _transactionLogSemaphore.Release();
                    }
                }
                finally
                {
                    UnlockQueue();
                }

            }
            finally
            {
                ConfigSemaphore.Release();
            }

            _entriesSemaphore.Dispose();
            _transactionLogSemaphore.Dispose();
            _writerSemaphore.Dispose();
            if (!_persistent)
            {
                foreach (var file in Directory.GetFiles(_path))
                {
                    File.Delete(file);
                }
                Directory.Delete(_path, true);
            }
        }

        async Task IDiskQueueStore.AcquireWriter(
            Stream stream,
            Func<Stream, Task<long>> action,
            Action<Stream> onReplaceStream)
        {
            try
            {
                await _writerSemaphore.WaitAsync().ConfigureAwait(false);
                _logger.LogDebug("Writer acquired");
                if (stream.Position != _currentFilePosition)
                {
                    stream.Position = _currentFilePosition;
                }

                _currentFilePosition = await action(stream).ConfigureAwait(false);
                if (_currentFilePosition < _maxFileSize)
                {
                    return;
                }

                Interlocked.Increment(ref _currentFileNumber);
                // If we get to int.MaxValue then start over.
                Interlocked.CompareExchange(ref _currentFileNumber, 0, int.MaxValue);
                _logger.LogDebug("Log file number: " + _currentFileNumber);
                var writer = CreateWriter();
                // we assume same size messages, or near size messages
                // that gives us a good heuristic for creating the size of
                // the new file, so it wouldn't be fragmented
                writer.SetLength(_currentFilePosition);
                _currentFilePosition = 0;
                onReplaceStream(writer);
            }
            finally
            {
                _writerSemaphore.Release();
            }
        }

        async Task IDiskQueueStore.CommitTransaction(ICollection<Operation> operations)
        {
            if (operations.Count == 0)
            {
                return;
            }

            _logger.LogDebug("Committing transaction");
            var transactionBuffer = await GenerateTransactionBuffer(operations).ConfigureAwait(false);

            try
            {
                await _transactionLogSemaphore.WaitAsync().ConfigureAwait(false);
                long txLogSize;
                await using (var stream = await WaitForTransactionLog(transactionBuffer).ConfigureAwait(false))
                {
                    await stream.WriteAsync(transactionBuffer).ConfigureAwait(false);
                    txLogSize = stream.Position;
                    await stream.FlushAsync().ConfigureAwait(false);
                }

                ApplyTransactionOperations(operations);
                await TrimTransactionLogIfNeeded(txLogSize).ConfigureAwait(false);

                Atomic.Write(
                    Meta,
                    stream =>
                    {
                        var bytes = BitConverter.GetBytes(_currentFileNumber);
                        stream.Write(bytes, 0, bytes.Length);
                        bytes = BitConverter.GetBytes(_currentFilePosition);
                        stream.Write(bytes, 0, bytes.Length);
                    });

                if (_paranoidFlushing)
                {
                    await FlushTrimmedTransactionLog().ConfigureAwait(false);
                }
            }
            finally
            {
                _transactionLogSemaphore.Release();
            }
        }

        private async Task<FileStream> WaitForTransactionLog(byte[] transactionBuffer)
        {
            for (var i = 0; i < 10; i++)
            {
                try
                {
                    return new FileStream(TransactionLog,
                                          FileMode.Append,
                                          FileAccess.Write,
                                          FileShare.None,
                                          transactionBuffer.Length,
                                          FileOptions.SequentialScan | FileOptions.WriteThrough);
                }
                catch (Exception)
                {
                    _logger.LogError("Delay reading transaction log");
                    await Task.Delay(250).ConfigureAwait(false);
                }
            }

            const string couldNotAcquireTransactionLogLock = "Could not acquire transaction log lock";
            _logger.LogError(couldNotAcquireTransactionLogLock);
            throw new TimeoutException(couldNotAcquireTransactionLogLock);
        }

        async Task<Entry> IDiskQueueStore.Dequeue(CancellationToken cancellationToken)
        {
            try
            {
                await _entriesSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                _logger.LogDebug("Dequeuing item");
                var first = _entries.First;
                if (first == null)
                {
                    return null;
                }

                var entry = first.Value;

                if (entry.Data == null)
                {
                    await ReadAhead().ConfigureAwait(false);
                }

                _entries.RemoveFirst();
                // we need to create a copy so we will not hold the data
                // in memory as well as the position
                lock (_checkedOutEntries)
                {
                    _checkedOutEntries.Add(new Entry(entry.FileNumber, entry.Start, entry.Length));
                }

                return entry;
            }
            catch (TaskCanceledException)
            {
                return null;
            }
            finally
            {
                _entriesSemaphore.Release();
            }
        }

        /// <summary>
        /// Assumes that entries has at least one entry. Should be called inside a lock.
        /// </summary>
        private async Task ReadAhead()
        {
            long currentBufferSize = 0;
            var firstEntry = _entries.First.Value;
            var lastEntry = firstEntry;
            foreach (var entry in _entries)
            {
                // we can't read ahead to another file or
                // if we have unordered queue, or sparse items
                if (entry != lastEntry
                    && (entry.FileNumber != lastEntry.FileNumber
                        || entry.Start != (lastEntry.Start + lastEntry.Length)))
                {
                    break;
                }

                if (currentBufferSize + entry.Length > _suggestedReadBuffer)
                {
                    break;
                }

                lastEntry = entry;
                currentBufferSize += entry.Length;
            }

            if (lastEntry == firstEntry)
            {
                currentBufferSize = lastEntry.Length;
            }

            var buffer = await ReadEntriesFromFile(firstEntry, currentBufferSize).ConfigureAwait(false);

            var index = 0;
            foreach (var entry in _entries)
            {
                entry.Data = new byte[entry.Length];
                Buffer.BlockCopy(buffer, index, entry.Data, 0, entry.Length);
                index += entry.Length;
                if (entry == lastEntry)
                {
                    break;
                }
            }
        }

        private async Task<byte[]> ReadEntriesFromFile(Entry firstEntry, long currentBufferSize)
        {
            var buffer = new byte[currentBufferSize];
            if (firstEntry.Length < 1)
            {
                return buffer;
            }

            await using var reader = new FileStream(
                GetDataPath(firstEntry.FileNumber),
                FileMode.OpenOrCreate,
                FileAccess.Read,
                FileShare.ReadWrite)
            {
                Position = firstEntry.Start
            };
            var totalRead = 0;
            do
            {
                var bytesRead = await reader.ReadAsync(buffer.AsMemory(totalRead, buffer.Length - totalRead))
                    .ConfigureAwait(false);
                if (bytesRead == 0)
                {
                    throw new InvalidOperationException("End of file reached while trying to read queue item");
                }

                totalRead += bytesRead;
            }
            while (totalRead < buffer.Length);

            return buffer;
        }

        IDiskQueueSession IDiskQueue.OpenSession()
        {
            _logger.LogInformation("Opening session");
            return new DiskQueueSession(this, CreateWriter(), _suggestedWriteBuffer, _symmetricAlgorithm, _loggerFactory.CreateLogger<IDiskQueueSession>());
        }

        void IDiskQueueStore.Reinstate(IEnumerable<Operation> reinstatedOperations)
        {
            var operations = from entry in reinstatedOperations.Reverse()
                             where entry.Type == OperationType.Dequeue
                             select new Operation(
                                 OperationType.Reinstate,
                                 entry.FileNumber,
                                 entry.Start,
                                 entry.Length);
            ApplyTransactionOperations(operations);
        }

        private async Task ReadTransactionLog()
        {
            var requireTxLogTrimming = false;
            Atomic.Read(
                    TransactionLog,
                    stream =>
                    {
                        using var binaryReader = new BinaryReader(stream);
                        var readingTransaction = false;
                        try
                        {
                            var txCount = 0;
                            while (true)
                            {
                                txCount += 1;
                                // this code ensures that we read the full transaction
                                // before we start to apply it. The last truncated transaction will be
                                // ignored automatically.
                                AssertTransactionSeparator(
                                    binaryReader,
                                    txCount,
                                    Marker.StartTransaction,
                                    () => readingTransaction = true);

                                var opsCount = binaryReader.ReadInt32();
                                var txOps = new List<Operation>(opsCount);
                                for (var i = 0; i < opsCount; i++)
                                {
                                    AssertOperationSeparator(binaryReader);
                                    var operation = new Operation(
                                        (OperationType)binaryReader.ReadByte(),
                                        binaryReader.ReadInt32(),
                                        binaryReader.ReadInt32(),
                                        binaryReader.ReadInt32());
                                    txOps.Add(operation);
                                    //if we have non enqueue entries, this means
                                    // that we have not closed properly, so we need
                                    // to trim the log
                                    if (operation.Type != OperationType.Enqueue)
                                    {
                                        requireTxLogTrimming = true;
                                    }
                                }

                                // check that the end marker is in place
                                AssertTransactionSeparator(binaryReader, txCount, Marker.EndTransaction, () => { });
                                readingTransaction = false;
                                ApplyTransactionOperations(txOps);
                            }
                        }
                        catch (EndOfStreamException)
                        {
                            // we have a truncated transaction, need to clear that
                            if (readingTransaction)
                            {
                                requireTxLogTrimming = true;
                            }
                        }
                    });
            if (requireTxLogTrimming)
            {
                await FlushTrimmedTransactionLog().ConfigureAwait(false);
            }
        }

        private async Task FlushTrimmedTransactionLog()
        {
            await using var ms = new MemoryStream();
            await ms.WriteAsync(Constants.StartTransactionSeparator).ConfigureAwait(false);

            var count = BitConverter.GetBytes(((IDiskQueueStore)this).EstimatedCountOfItemsInQueue);
            await ms.WriteAsync(count).ConfigureAwait(false);

            Entry[] checkedOut;
            lock (_checkedOutEntries)
            {
                checkedOut = _checkedOutEntries.ToArray();
            }
            foreach (var entry in checkedOut)
            {
                await WriteEntryToTransactionLog(ms, entry, OperationType.Enqueue).ConfigureAwait(false);
            }

            Entry[] listedEntries;
            try
            {
                await _entriesSemaphore.WaitAsync().ConfigureAwait(false);
                listedEntries = ToArray(_entries);
            }
            finally
            {
                _entriesSemaphore.Release();
            }

            foreach (var entry in listedEntries)
            {
                await WriteEntryToTransactionLog(ms, entry, OperationType.Enqueue).ConfigureAwait(false);
            }
            await ms.WriteAsync(Constants.EndTransactionSeparator).ConfigureAwait(false);
            await ms.FlushAsync().ConfigureAwait(false);
            var transactionBuffer = ms.ToArray();
            Atomic.Write(TransactionLog, stream =>
            {
                stream.SetLength(transactionBuffer.Length);
                stream.Write(transactionBuffer, 0, transactionBuffer.Length);
            });
        }

        /// <summary>
        /// This special purpose function is to work around potential issues with Mono
        /// </summary>
	    private static Entry[] ToArray(LinkedList<Entry> list)
        {
            if (list == null)
            {
                return Array.Empty<Entry>();
            }
            var output = new List<Entry>(25);
            var cur = list.First;
            while (cur != null)
            {
                output.Add(cur.Value);
                cur = cur.Next;
            }
            return output.ToArray();
        }

        private static async Task WriteEntryToTransactionLog(Stream ms, Entry entry, OperationType operationType)
        {
            await ms.WriteAsync(Constants.OperationSeparatorBytes)
                .ConfigureAwait(false);

            ms.WriteByte((byte)operationType);

            var fileNumber = BitConverter.GetBytes(entry.FileNumber);
            await ms.WriteAsync(fileNumber).ConfigureAwait(false);

            var start = BitConverter.GetBytes(entry.Start);
            await ms.WriteAsync(start).ConfigureAwait(false);

            var length = BitConverter.GetBytes(entry.Length);
            await ms.WriteAsync(length).ConfigureAwait(false);
        }

        private void AssertOperationSeparator(BinaryReader reader)
        {
            var separator = reader.ReadInt32();
            if (separator == Constants.OperationSeparator) return; // OK

            ThrowIfStrict("Unexpected data in transaction log. Expected to get transaction separator but got unknown data");
        }

        private int[] ApplyTransactionOperationsInMemory(IEnumerable<Operation> operations)
        {
            if (operations == null) return Array.Empty<int>();

            foreach (var operation in operations)
            {
                switch (operation?.Type)
                {
                    case OperationType.Enqueue:
                        {
                            var entryToAdd = new Entry(operation);
                            _entries.AddLast(entryToAdd);

                            var itemCountAddition = Extensions.GetValueOrDefault(_countOfItemsPerFile, entryToAdd.FileNumber);
                            _countOfItemsPerFile[entryToAdd.FileNumber] = itemCountAddition + 1;
                        }
                        break;

                    case OperationType.Dequeue:
                        var entryToRemove = new Entry(operation);
                        lock (_checkedOutEntries) { _checkedOutEntries.Remove(entryToRemove); }
                        var itemCountRemoval = Extensions.GetValueOrDefault(_countOfItemsPerFile, entryToRemove.FileNumber);
                        _countOfItemsPerFile[entryToRemove.FileNumber] = itemCountRemoval - 1;
                        break;

                    case OperationType.Reinstate:
                        var entryToReinstate = new Entry(operation);
                        _entries.AddFirst(entryToReinstate);
                        lock (_checkedOutEntries)
                        {
                            _checkedOutEntries.Remove(entryToReinstate);
                        }

                        break;
                }
            }

            var filesToRemove = new HashSet<int>(
                from pair in _countOfItemsPerFile
                where pair.Value < 1
                select pair.Key
                );

            foreach (var i in filesToRemove)
            {
                _countOfItemsPerFile.Remove(i);
            }
            return filesToRemove.ToArray();
        }

        /// <summary>
        /// If 'throwOnConflict' was set in the constructor, throw an InvalidOperationException. This will stop program flow.
        /// If not, throw an EndOfStreamException, which should result in silent data truncation.
        /// </summary>
	    private void ThrowIfStrict(string msg)
        {
            if (_throwOnConflict)
            {
                throw new UnableToSetupException(msg);
            }

            throw new EndOfStreamException();   // silently truncate transactions
        }

        private void AssertTransactionSeparator(BinaryReader binaryReader, int txCount, Marker whichSeparator, Action hasData)
        {
            var bytes = binaryReader.ReadBytes(16);
            if (bytes.Length == 0) throw new EndOfStreamException();

            hasData();
            if (bytes.Length != 16)
            {
                // looks like we have a truncated transaction in this case, we will
                // say that we run into end of stream and let the log trimming to deal with this
                if (binaryReader.BaseStream.Length == binaryReader.BaseStream.Position)
                {
                    throw new EndOfStreamException();
                }
                ThrowIfStrict("Unexpected data in transaction log. Expected to get transaction separator but got truncated data. Tx #" + txCount);
            }

            Guid expectedValue, otherValue;
            Marker otherSeparator;
            if (whichSeparator == Marker.StartTransaction)
            {
                expectedValue = Constants.StartTransactionSeparatorGuid;
                otherValue = Constants.EndTransactionSeparatorGuid;
                otherSeparator = Marker.EndTransaction;
            }
            else if (whichSeparator == Marker.EndTransaction)
            {
                expectedValue = Constants.EndTransactionSeparatorGuid;
                otherValue = Constants.StartTransactionSeparatorGuid;
                otherSeparator = Marker.StartTransaction;
            }
            else throw new InvalidProgramException("Wrong kind of separator in inner implementation");

            var separator = new Guid(bytes);
            if (separator != expectedValue)
            {
                if (separator == otherValue) // found a marker, but of the wrong type
                {
                    ThrowIfStrict("Unexpected data in transaction log. Expected " + whichSeparator + " but found " + otherSeparator);
                }
                ThrowIfStrict("Unexpected data in transaction log. Expected to get transaction separator but got unknown data. Tx #" + txCount);
            }
        }

        private void ReadMetaState()
        {
            Atomic.Read(Meta, stream =>
            {
                using var binaryReader = new BinaryReader(stream);
                try
                {
                    _currentFileNumber = binaryReader.ReadInt32();
                    _currentFilePosition = binaryReader.ReadInt64();
                }
                catch (EndOfStreamException)
                {
                }
            });
        }

        private async Task TrimTransactionLogIfNeeded(long txLogSize)
        {
            if (txLogSize < _suggestedMaxTransactionLogSize)
            {
                return; // it is not big enough to care
            }

            var optimalSize = GetOptimalTransactionLogSize();
            if (txLogSize < (optimalSize * 2)) return;  // not enough disparity to bother trimming

            await FlushTrimmedTransactionLog().ConfigureAwait(false);
        }

        private void ApplyTransactionOperations(IEnumerable<Operation> operations)
        {
            int[] filesToRemove;
            try
            {
                _entriesSemaphore.Wait();
                filesToRemove = ApplyTransactionOperationsInMemory(operations);
            }
            finally
            {
                _entriesSemaphore.Release();
            }
            foreach (var fileNumber in filesToRemove)
            {
                if (_currentFileNumber == fileNumber)
                {
                    continue;
                }

                File.Delete(GetDataPath(fileNumber));
            }
        }

        private static async Task<byte[]> GenerateTransactionBuffer(ICollection<Operation> operations)
        {
            await using var ms = new MemoryStream();
            await ms.WriteAsync(Constants.StartTransactionSeparator).ConfigureAwait(false);

            var count = BitConverter.GetBytes(operations.Count);
            await ms.WriteAsync(count).ConfigureAwait(false);

            foreach (var operation in operations)
            {
                await WriteEntryToTransactionLog(ms, new Entry(operation), operation.Type).ConfigureAwait(false);
            }
            await ms.WriteAsync(Constants.EndTransactionSeparator).ConfigureAwait(false);

            await ms.FlushAsync().ConfigureAwait(false);
            var transactionBuffer = ms.ToArray();
            return transactionBuffer;
        }

        private FileStream CreateWriter()
        {
            var dataFilePath = GetDataPath(_currentFileNumber);
            var stream = new FileStream(
                dataFilePath,
                FileMode.OpenOrCreate,
                FileAccess.Write,
                FileShare.ReadWrite,
                0x10000,
                FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.WriteThrough);

            SetPermissions.TryAllowReadWriteForAll(dataFilePath);
            return stream;
        }

        private string GetDataPath(int index)
        {
            return Path.Combine(_path, "data." + index);
        }

        private long GetOptimalTransactionLogSize()
        {
            long size = 0;
            size += 16 /*sizeof(guid)*/; //	initial tx separator
            size += sizeof(int); // 	operation count

            size +=
                ( // entry size == 16
                sizeof(int) + // 		operation separator
                sizeof(int) + // 		file number
                sizeof(int) + //		start
                sizeof(int) //		length
                )
                *
                (CurrentCountOfItemsInQueue);

            return size;
        }
    }
}