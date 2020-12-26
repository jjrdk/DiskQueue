using System;

namespace DiskQueue.Reactive.Tests
{
    using System.Threading;

    public class TestSubscriber : IObserver<byte[]>
    {
        private readonly ManualResetEventSlim _waitHandle;

        public TestSubscriber(ManualResetEventSlim waitHandle)
        {
            _waitHandle = waitHandle;
        }

        public byte[] LastMessage { get; private set; }

        public bool Completed { get; private set; }

        public Exception LastException { get; private set; }

        /// <inheritdoc />
        public void OnCompleted()
        {
            Completed = true;
            _waitHandle.Set();
        }

        /// <inheritdoc />
        public void OnError(Exception error)
        {
            LastException = error;
            _waitHandle.Set();
        }

        /// <inheritdoc />
        public void OnNext(byte[] value)
        {
            LastMessage = value;
            _waitHandle.Set();
        }
    }
}
