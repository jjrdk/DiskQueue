namespace AsyncDiskQueue
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Implementation;

    internal interface IDiskQueueStore
    {
        /// <summary>
        /// <para>UNSAFE. Incorrect use will result in data loss.</para>
        /// Lock and process a data file writer at the current write head.
        /// <para>This will create new files if max size is exceeded</para>
        /// </summary>
        /// <param name="stream">Stream to write</param>
        /// <param name="action">Writing action</param>
        /// <param name="onReplaceStream">Continuation action if a new file is created</param>
        Task AcquireWriter(
            Stream stream,
            Func<Stream, Task<long>> action,
            Action<Stream> onReplaceStream);

        /// <summary>
        /// <para>UNSAFE. Incorrect use will result in data loss.</para>
        /// Commit a sequence of operations to storage
        /// </summary>
        Task CommitTransaction(ICollection<Operation> operations);

        /// <summary>
        /// <para>UNSAFE. Incorrect use will result in data loss.</para>
        /// Dequeue data, returning storage entry
        /// </summary>
        /// <param name="cancellationToken"></param>
        Task<Entry> Dequeue(CancellationToken cancellationToken = default);

        /// <summary>
        /// <para>UNSAFE. Incorrect use will result in data loss.</para>
        /// <para>Undo Enqueue and Dequeue operations.</para>
        /// <para>These MUST have been real operations taken.</para>
        /// </summary>
        void Reinstate(IEnumerable<Operation> reinstatedOperations);

        /// <summary>
        /// Returns the number of items in the queue, but does not include items added or removed
        /// in currently open sessions.
        /// </summary>
        int EstimatedCountOfItemsInQueue { get; }

        /// <summary>
        /// <para>Safe, available for tests and performance.</para>
        /// <para>Current writing file number</para>
        /// </summary>
        int CurrentFileNumber { get; }
    }
}