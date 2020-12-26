namespace DiskQueue.Reactive.Tests
{
    using System;
    using System.Threading;

    public class CrashSubscriber : IObserver<byte[]>
    {
        private readonly ManualResetEventSlim _waitHandle;
        private int _count = 0;

        public CrashSubscriber(ManualResetEventSlim waitHandle)
        {
            _waitHandle = waitHandle;
        }

        public Exception LastError { get; private set; }

        public bool Completed { get; private set; }

        /// <inheritdoc />
        public void OnCompleted()
        {
            Completed = true;
            _waitHandle.Set();
        }

        /// <inheritdoc />
        public void OnError(Exception error)
        {
            LastError = error;
            _waitHandle.Set();
        }

        /// <inheritdoc />
        public void OnNext(byte[] value)
        {
            if (Interlocked.Increment(ref _count) % 3 == 0)
            {
                _waitHandle.Set();
            }
            else
            {
                throw new Exception("test crash");
            }
        }
    }
}