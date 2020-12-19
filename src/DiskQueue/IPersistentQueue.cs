using System;
using System.Collections.Generic;
using System.IO;
using DiskQueue.Implementation;

namespace DiskQueue
{
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
	/// Wrapper for exposing some inner workings of the persistent queue.
	/// <para>You should be careful using any of these methods</para>
	/// <para>Please read the source code before using these methods in production software</para>
	/// </summary>
	internal interface IPersistentQueue : IAsyncDisposable
	{
        /// <summary>
        /// Open an read/write session
        /// </summary>
        IPersistentQueueSession OpenSession();

        /// <summary>
        /// <para>UNSAFE. Incorrect use will result in data loss.</para>
        /// Lock and process a data file writer at the current write head.
        /// <para>This will create new files if max size is exceeded</para>
        /// </summary>
        /// <param name="stream">Stream to write</param>
        /// <param name="action">Writing action</param>
        /// <param name="onReplaceStream">Continuation action if a new file is created</param>
        /// <param name="cancellationToken"></param>
        Task AcquireWriter(
            Stream stream,
            Func<Stream, Task<long>> action,
            Action<Stream> onReplaceStream,
            CancellationToken cancellationToken = default);

		/// <summary>
		/// <para>UNSAFE. Incorrect use will result in data loss.</para>
		/// Commit a sequence of operations to storage
		/// </summary>
		Task CommitTransaction(ICollection<Operation> operations, CancellationToken cancellationToken = default);

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