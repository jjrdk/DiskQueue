namespace AsyncDiskQueue
{
    using System;

    /// <summary>
    /// Wrapper for exposing some inner workings of the persistent queue.
    /// <para>You should be careful using any of these methods</para>
    /// <para>Please read the source code before using these methods in production software</para>
    /// </summary>
    public interface IDiskQueue : IAsyncDisposable
    {
        /// <summary>
        /// Open an read/write session
        /// </summary>
        IDiskQueueSession OpenSession();
    }
}
