namespace AsyncDiskQueue
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
	/// Queue session (exclusive use of the queue to add or remove items)
	/// The queue session should be wrapped in a `using`, as it must be disposed.
	/// If you are sharing access, you should hold the queue session for as little time as possible.
	/// </summary>
	public interface IPersistentQueueSession : IDisposable
	{
		/// <summary>
		/// Queue data for a later decode. Data is written on `Flush()`
		/// </summary>
		Task Enqueue(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default);

        /// <summary>
        /// Try to pull data from the queue. Data is removed from the queue on `Flush()`
        /// </summary>
        /// <param name="cancellationToken"></param>
        Task<byte[]> Dequeue(CancellationToken cancellationToken = default);

        /// <summary>
        /// Commit actions taken in this session since last flush.
        /// If the session is disposed with no flush, actions are not persisted
        /// to the queue (Enqueues are not written, dequeues are left on the queue)
        /// </summary>
        /// <param name="cancellationToken"></param>
        Task Flush(CancellationToken cancellationToken = default);
	}

    /// <summary>
    /// Defines the typed persistent queue interface.
    /// </summary>
    /// <typeparam name="T">The <see cref="Type"/> of items stored in the queue.</typeparam>
    public interface IPersistentQueueSession<T> : IDisposable
    {
        /// <summary>
        /// Queue data for a later decode. Data is written on `Flush()`
        /// </summary>
        Task Enqueue(T data, CancellationToken cancellationToken = default);

        /// <summary>
        /// Try to pull data from the queue. Data is removed from the queue on `Flush()`
        /// </summary>
        /// <param name="cancellationToken"></param>
        Task<T> Dequeue(CancellationToken cancellationToken = default);

        /// <summary>
        /// Commit actions taken in this session since last flush.
        /// If the session is disposed with no flush, actions are not persisted
        /// to the queue (Enqueues are not written, dequeues are left on the queue)
        /// </summary>
        /// <param name="cancellationToken"></param>
        Task Flush(CancellationToken cancellationToken = default);
    }
}
