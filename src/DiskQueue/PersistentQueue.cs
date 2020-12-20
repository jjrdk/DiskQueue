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

namespace DiskQueue
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Security.Cryptography;
    using System.Threading;
    using System.Threading.Tasks;
    using Implementation;

    internal class PersistentQueue : IPersistentQueue
    {
        private readonly SemaphoreSlim entriesSemaphore = new SemaphoreSlim(1);
        private readonly SemaphoreSlim writerSemaphore = new SemaphoreSlim(1);
        private readonly SemaphoreSlim transactionLogSemaphore = new SemaphoreSlim(1);
        private readonly HashSet<Entry> checkedOutEntries = new HashSet<Entry>();
        private readonly Dictionary<int, int> countOfItemsPerFile = new Dictionary<int, int>();
        private readonly LinkedList<Entry> entries = new LinkedList<Entry>();
        private readonly string path;
        private readonly bool throwOnConflict;
        private static readonly SemaphoreSlim ConfigSemaphore = new SemaphoreSlim(1);
        private static readonly object ConfigLock = new object();
        private volatile bool disposed;
        private FileStream fileLock;
        private readonly SymmetricAlgorithm algo;
        private readonly bool trimTransactionLogOnDispose;
        private readonly int suggestedReadBuffer;
        private readonly int suggestedWriteBuffer;
        private readonly long suggestedMaxTransactionLogSize;
        private readonly int maxFileSize;
        private readonly bool paranoidFlushing;

        private PersistentQueue(
            string path,
            int maxFileSize,
            bool throwOnConflict,
            int maxTransactionLogSize,
            bool trimTransactionLogOnDispose,
            bool paranoidFlushing,
            int suggestedReadBuffer,
            int suggestedWriteBuffer,
            SymmetricAlgorithm algo)
        {
            this.trimTransactionLogOnDispose = trimTransactionLogOnDispose;
            this.algo = algo;
            lock (ConfigLock)
            {
                disposed = true;
                this.trimTransactionLogOnDispose = trimTransactionLogOnDispose;
                this.paranoidFlushing = paranoidFlushing;
                suggestedMaxTransactionLogSize = maxTransactionLogSize;
                this.suggestedReadBuffer = suggestedReadBuffer;
                this.suggestedWriteBuffer = suggestedWriteBuffer;
                this.throwOnConflict = throwOnConflict;

                this.maxFileSize = maxFileSize;
                try
                {
                    this.path = Path.GetFullPath(path);
                    if (!Directory.Exists(this.path))
                    {
                        CreateDirectory(this.path);
                    }

                    LockQueue();
                }
                catch (UnauthorizedAccessException)
                {
                    throw new UnauthorizedAccessException(
                        $"Directory \"{path}\" does not exist or is missing write permissions");
                }
                catch (IOException e)
                {
                    //GC.SuppressFinalize(this); //avoid finalizing invalid instance
                    throw new InvalidOperationException("Another instance of the queue is already in action, or directory does not exists", e);
                }
            }
        }

        public static Task<IPersistentQueue> Create(
            string path,
            TimeSpan maxWait,
            int maxFileSite = Constants._32Megabytes,
            bool throwOnConflict = true,
            int suggestedMaxTransactionLogSize = Constants._32Megabytes,
            bool trimTransactionLogOnDispose = true,
            bool paranoidFlushing = true,
            int suggestedReadBuffer = Constants._1Megabyte,
            int suggestedWriteBuffer = Constants._1Megabyte,
            SymmetricAlgorithm algo = null)
        {
            using var source = new CancellationTokenSource(maxWait);
            return Create(
                path,
                maxFileSite,
                throwOnConflict,
                suggestedMaxTransactionLogSize,
                trimTransactionLogOnDispose,
                paranoidFlushing,
                suggestedReadBuffer,
                suggestedWriteBuffer,
                algo,
                source.Token);
        }

        public static async Task<IPersistentQueue> Create(
            string path,
            int maxFileSite = Constants._32Megabytes,
            bool throwOnConflict = true,
            int suggestedMaxTransactionLogSize = Constants._32Megabytes,
            bool trimTransactionLogOnDispose = true,
            bool paranoidFlushing = true,
            int suggestedReadBuffer = Constants._1Megabyte,
            int suggestedWriteBuffer = Constants._1Megabyte,
            SymmetricAlgorithm algo = null,
            CancellationToken cancellationToken = default)
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    await ConfigSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                    try
                    {
                        var instance = new PersistentQueue(
                            path,
                            maxFileSite,
                            throwOnConflict,
                            suggestedMaxTransactionLogSize,
                            trimTransactionLogOnDispose,
                            paranoidFlushing,
                            suggestedReadBuffer,
                            suggestedWriteBuffer,
                            algo);

                        try
                        {
                            instance.ReadMetaState();
                            await instance.ReadTransactionLog(cancellationToken).ConfigureAwait(false);
                        }
                        catch (Exception)
                        {
                            // GC.SuppressFinalize(instance); //avoid finalizing invalid instance
                            instance.UnlockQueue();
                            throw;
                        }

                        instance.disposed = false;

                        return instance;
                    }
                    catch (DirectoryNotFoundException)
                    {
                        throw new Exception("Target storagePath does not exist or is not accessible");
                    }
                    catch (PlatformNotSupportedException ex)
                    {
                        Console.WriteLine(
                            "Blocked by " + ex.GetType().Name + "; " + ex.Message + "\r\n\r\n" + ex.StackTrace);
                        throw;
                    }
                    catch (UnableToSetupException)
                    {
                        throw;
                    }
                    catch (Exception)
                    {
                        if (cancellationToken == default)
                        {
                            throw;
                        }
                        await Task.Delay(50, cancellationToken).ConfigureAwait(false);
                    }
                }
                finally
                {
                    ConfigSemaphore.Release();
                }
            }
        }

        void UnlockQueue()
        {
            if (path == null) return;
            var target = Path.Combine(path, "lock");
            if (fileLock != null)
            {
                fileLock.Dispose();
                File.Delete(target);
            }
            fileLock = null;
        }

        void LockQueue()
        {
            var target = Path.Combine(path, "lock");
            fileLock = new FileStream(
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

        public int EstimatedCountOfItemsInQueue
        {
            get
            {
                if (entries == null)
                {
                    return 0;
                }
                try
                {
                    entriesSemaphore.Wait();
                    return entries.Count + checkedOutEntries.Count;
                }
                finally
                {
                    entriesSemaphore.Release();
                }
            }
        }

        private int CurrentCountOfItemsInQueue
        {
            get
            {
                try
                {
                    entriesSemaphore.Wait();
                    return entries.Count + checkedOutEntries.Count;
                }
                finally
                {
                    entriesSemaphore.Release();
                }
            }
        }

        public long CurrentFilePosition { get; private set; }

        private string TransactionLog
        {
            get { return Path.Combine(path, "transaction.log"); }
        }

        private string Meta
        {
            get { return Path.Combine(path, "meta.state"); }
        }

        public int CurrentFileNumber { get; private set; }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await ConfigSemaphore.WaitAsync().ConfigureAwait(false);
                if (disposed)
                {
                    return;
                }

                try
                {
                    disposed = true;
                    try
                    {
                        await transactionLogSemaphore.WaitAsync().ConfigureAwait(false);
                        if (trimTransactionLogOnDispose)
                        {
                            await FlushTrimmedTransactionLog(CancellationToken.None).ConfigureAwait(false);
                        }
                    }
                    finally
                    {
                        transactionLogSemaphore.Release();
                    }

                    //GC.SuppressFinalize(this);
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
        }

        public async Task AcquireWriter(
            Stream stream,
            Func<Stream, CancellationToken, Task<long>> action,
            Action<Stream> onReplaceStream,
            CancellationToken cancellationToken = default)
        {
            try
            {
                await writerSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                //lock (writerLock)
                //{
                if (stream.Position != CurrentFilePosition)
                {
                    stream.Position = CurrentFilePosition;
                }

                CurrentFilePosition = await action(stream, cancellationToken).ConfigureAwait(false);
                if (CurrentFilePosition < maxFileSize) return;

                CurrentFileNumber += 1;
                var writer = CreateWriter();
                // we assume same size messages, or near size messages
                // that gives us a good heuristic for creating the size of
                // the new file, so it wouldn't be fragmented
                writer.SetLength(CurrentFilePosition);
                CurrentFilePosition = 0;
                onReplaceStream(writer);
            }
            finally
            {
                writerSemaphore.Release();
            }
        }

        public async Task CommitTransaction(ICollection<Operation> operations, CancellationToken cancellationToken = default)
        {
            if (operations.Count == 0)
            {
                return;
            }

            byte[] transactionBuffer = await GenerateTransactionBuffer(operations, cancellationToken).ConfigureAwait(false);

            try
            {
                await transactionLogSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                long txLogSize;
                await using (var stream = WaitForTransactionLog(transactionBuffer))
                {
                    await stream.WriteAsync(transactionBuffer, cancellationToken).ConfigureAwait(false);
                    txLogSize = stream.Position;
                    await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                }

                ApplyTransactionOperations(operations);
                await TrimTransactionLogIfNeeded(txLogSize, cancellationToken).ConfigureAwait(false);

                Atomic.Write(
                    Meta,
                    stream =>
                    {
                        var bytes = BitConverter.GetBytes(CurrentFileNumber);
                        stream.Write(bytes, 0, bytes.Length);
                        bytes = BitConverter.GetBytes(CurrentFilePosition);
                        stream.Write(bytes, 0, bytes.Length);
                    });

                if (paranoidFlushing)
                {
                    await FlushTrimmedTransactionLog(cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                transactionLogSemaphore.Release();
            }
        }

        FileStream WaitForTransactionLog(byte[] transactionBuffer)
        {
            for (int i = 0; i < 10; i++)
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
                    Thread.Sleep(250);
                }
            }
            throw new TimeoutException("Could not aquire transaction log lock");
        }

        public async Task<Entry> Dequeue(CancellationToken cancellationToken = default)
        {
            try
            {
                await entriesSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                var first = entries.First;
                if (first == null)
                {
                    return null;
                }

                var entry = first.Value;

                if (entry.Data == null)
                {
                    await ReadAhead(cancellationToken).ConfigureAwait(false);
                }
                entries.RemoveFirst();
                // we need to create a copy so we will not hold the data
                // in memory as well as the position
                lock (checkedOutEntries)
                {
                    checkedOutEntries.Add(new Entry(entry.FileNumber, entry.Start, entry.Length));
                }
                return entry;
            }
            finally
            {
                entriesSemaphore.Release();
            }
        }

        /// <summary>
        /// Assumes that entries has at least one entry. Should be called inside a lock.
        /// </summary>
        private async Task ReadAhead(CancellationToken cancellationToken)
        {
            long currentBufferSize = 0;
            var firstEntry = entries.First.Value;
            Entry lastEntry = firstEntry;
            foreach (var entry in entries)
            {
                // we can't read ahead to another file or
                // if we have unordered queue, or sparse items
                if (entry != lastEntry
                    && (entry.FileNumber != lastEntry.FileNumber
                        || entry.Start != (lastEntry.Start + lastEntry.Length)))
                {
                    break;
                }

                if (currentBufferSize + entry.Length > suggestedReadBuffer)
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

            byte[] buffer = await ReadEntriesFromFile(firstEntry, currentBufferSize, cancellationToken).ConfigureAwait(false);

            var index = 0;
            foreach (var entry in entries)
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

        private async Task<byte[]> ReadEntriesFromFile(Entry firstEntry, long currentBufferSize, CancellationToken cancellationToken)
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
                var bytesRead = await reader.ReadAsync(buffer.AsMemory(totalRead, buffer.Length - totalRead), cancellationToken)
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

        public IPersistentQueueSession OpenSession()
        {
            return new PersistentQueueSession(this, CreateWriter(), suggestedWriteBuffer, algo);
        }

        public void Reinstate(IEnumerable<Operation> reinstatedOperations)
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

        private async Task ReadTransactionLog(CancellationToken cancellationToken)
        {
            var requireTxLogTrimming = false;
            Atomic.Read(
                    TransactionLog,
                    stream =>
                    {
                        using var binaryReader = new BinaryReader(stream);
                        bool readingTransaction = false;
                        try
                        {
                            int txCount = 0;
                            while (true)
                            {
                                txCount += 1;
                                // this code ensures that we read the full transaction
                                // before we start to apply it. The last truncated transaction will be
                                // ignored automatically.
                                AssertTransactionSeperator(
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
                                AssertTransactionSeperator(binaryReader, txCount, Marker.EndTransaction, () => { });
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
                await FlushTrimmedTransactionLog(cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task FlushTrimmedTransactionLog(CancellationToken cancellationToken)
        {
            await using var ms = new MemoryStream();
            await ms.WriteAsync(Constants.StartTransactionSeparator, cancellationToken).ConfigureAwait(false);

            var count = BitConverter.GetBytes(EstimatedCountOfItemsInQueue);
            await ms.WriteAsync(count, cancellationToken).ConfigureAwait(false);

            Entry[] checkedOut;
            lock (checkedOutEntries)
            {
                checkedOut = checkedOutEntries.ToArray();
            }
            foreach (var entry in checkedOut)
            {
                await WriteEntryToTransactionLog(ms, entry, OperationType.Enqueue, cancellationToken).ConfigureAwait(false);
            }

            Entry[] listedEntries;
            try
            {
                await entriesSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                listedEntries = ToArray(entries);
            }
            finally
            {
                entriesSemaphore.Release();
            }

            foreach (var entry in listedEntries)
            {
                await WriteEntryToTransactionLog(ms, entry, OperationType.Enqueue, cancellationToken).ConfigureAwait(false);
            }
            await ms.WriteAsync(Constants.EndTransactionSeparator, cancellationToken).ConfigureAwait(false);
            await ms.FlushAsync(cancellationToken).ConfigureAwait(false);
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

        private static async Task WriteEntryToTransactionLog(Stream ms, Entry entry, OperationType operationType, CancellationToken cancellationToken)
        {
            await ms.WriteAsync(Constants.OperationSeparatorBytes, cancellationToken)
                .ConfigureAwait(false);

            ms.WriteByte((byte)operationType);

            var fileNumber = BitConverter.GetBytes(entry.FileNumber);
            await ms.WriteAsync(fileNumber, cancellationToken).ConfigureAwait(false);

            var start = BitConverter.GetBytes(entry.Start);
            await ms.WriteAsync(start, cancellationToken).ConfigureAwait(false);

            var length = BitConverter.GetBytes(entry.Length);
            await ms.WriteAsync(length, cancellationToken).ConfigureAwait(false);
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
                            entries.AddLast(entryToAdd);

                            var itemCountAddition = Extensions.GetValueOrDefault(countOfItemsPerFile, entryToAdd.FileNumber);
                            countOfItemsPerFile[entryToAdd.FileNumber] = itemCountAddition + 1;
                        }
                        break;

                    case OperationType.Dequeue:
                        var entryToRemove = new Entry(operation);
                        lock (checkedOutEntries) { checkedOutEntries.Remove(entryToRemove); }
                        var itemCountRemoval = Extensions.GetValueOrDefault(countOfItemsPerFile, entryToRemove.FileNumber);
                        countOfItemsPerFile[entryToRemove.FileNumber] = itemCountRemoval - 1;
                        break;

                    case OperationType.Reinstate:
                        var entryToReinstate = new Entry(operation);
                        entries.AddFirst(entryToReinstate);
                        lock (checkedOutEntries)
                        {
                            checkedOutEntries.Remove(entryToReinstate);
                        }

                        break;
                }
            }

            var filesToRemove = new HashSet<int>(
                from pair in countOfItemsPerFile
                where pair.Value < 1
                select pair.Key
                );

            foreach (var i in filesToRemove)
            {
                countOfItemsPerFile.Remove(i);
            }
            return filesToRemove.ToArray();
        }

        /// <summary>
        /// If 'throwOnConflict' was set in the constructor, throw an InvalidOperationException. This will stop program flow.
        /// If not, throw an EndOfStreamException, which should result in silent data truncation.
        /// </summary>
	    private void ThrowIfStrict(string msg)
        {
            if (throwOnConflict)
            {
                throw new UnableToSetupException(msg);
            }

            throw new EndOfStreamException();   // silently truncate transactions
        }

        private void AssertTransactionSeperator(BinaryReader binaryReader, int txCount, Marker whichSeparator, Action hasData)
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
                    CurrentFileNumber = binaryReader.ReadInt32();
                    CurrentFilePosition = binaryReader.ReadInt64();
                }
                catch (EndOfStreamException)
                {
                }
            });
        }

        private async Task TrimTransactionLogIfNeeded(long txLogSize, CancellationToken cancellationToken)
        {
            if (txLogSize < suggestedMaxTransactionLogSize)
            {
                return; // it is not big enough to care
            }

            var optimalSize = GetOptimalTransactionLogSize();
            if (txLogSize < (optimalSize * 2)) return;  // not enough disparity to bother trimming

            await FlushTrimmedTransactionLog(cancellationToken).ConfigureAwait(false);
        }

        private void ApplyTransactionOperations(IEnumerable<Operation> operations)
        {
            int[] filesToRemove;
            try
            {
                entriesSemaphore.Wait();
                filesToRemove = ApplyTransactionOperationsInMemory(operations);
            }
            finally
            {
                entriesSemaphore.Release();
            }
            foreach (var fileNumber in filesToRemove)
            {
                if (CurrentFileNumber == fileNumber)
                {
                    continue;
                }

                File.Delete(GetDataPath(fileNumber));
            }
        }

        private static async Task<byte[]> GenerateTransactionBuffer(ICollection<Operation> operations, CancellationToken cancellationToken)
        {
            await using var ms = new MemoryStream();
            await ms.WriteAsync(Constants.StartTransactionSeparator, cancellationToken).ConfigureAwait(false);

            var count = BitConverter.GetBytes(operations.Count);
            await ms.WriteAsync(count, cancellationToken).ConfigureAwait(false);

            foreach (var operation in operations)
            {
                await WriteEntryToTransactionLog(ms, new Entry(operation), operation.Type, cancellationToken).ConfigureAwait(false);
            }
            await ms.WriteAsync(Constants.EndTransactionSeparator, cancellationToken).ConfigureAwait(false);

            await ms.FlushAsync(cancellationToken).ConfigureAwait(false);
            var transactionBuffer = ms.ToArray();
            return transactionBuffer;
        }

        private FileStream CreateWriter()
        {
            var dataFilePath = GetDataPath(CurrentFileNumber);
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
            return Path.Combine(path, "data." + index);
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