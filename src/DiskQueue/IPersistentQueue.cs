using System;

namespace DiskQueue
{
	/// <summary>
	/// A queue tied to a specific persistent storage backing.
	/// Enqueue and dequeue operations happen within sessions.
	/// <example>using (var session = q.OpenSession()) {...}</example>
	/// Queue should be disposed after use. This will NOT destroy the backing storage.
	/// </summary>
	public interface IPersistentQueue : IDisposable
	{
		///// <summary>
		///// Internal adjustables. Use with caution. Read the source code.
		///// </summary>
		//IPersistentQueueImpl Internals { get; }

		/// <summary>
		/// Maximum size of files in queue. New files will be rolled-out if this is exceeded.
		/// (i.e. this is NOT the maximum size of the queue)
		/// </summary>
		int MaxFileSize { get; }
	}
}