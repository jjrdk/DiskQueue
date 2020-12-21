using System;

namespace DiskQueue.Reactive.Tests
{
    using System.Threading;

    public class TestSubscriber : IObserver<byte[]>
    {
        private readonly ManualResetEventSlim waitHandle;

        public TestSubscriber(ManualResetEventSlim waitHandle)
        {
            this.waitHandle = waitHandle;
        }

        public byte[] LastMessage { get; private set; }

        public bool Completed { get; private set; }

        public Exception LastException { get; private set; }

        /// <inheritdoc />
        public void OnCompleted()
        {
            Completed = true;
            waitHandle.Set();
        }

        /// <inheritdoc />
        public void OnError(Exception error)
        {
            LastException = error;
            waitHandle.Set();
        }

        /// <inheritdoc />
        public void OnNext(byte[] value)
        {
            LastMessage = value;
            waitHandle.Set();
        }
    }
}
