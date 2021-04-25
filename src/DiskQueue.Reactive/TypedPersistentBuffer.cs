namespace AsyncDiskQueue.Reactive
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class TypedPersistentBuffer<T> : IObserver<T>, IObservable<T>, IAsyncDisposable
    {
        private readonly PersistentBuffer innerBuffer;
        private readonly Func<T, byte[]> serializer;
        private readonly Func<byte[], T> deserializer;
        private readonly List<IObserver<byte[]>> observers = new();

        public TypedPersistentBuffer(PersistentBuffer innerBuffer, Func<T, byte[]> serializer, Func<byte[], T> deserializer)
        {
            this.innerBuffer = innerBuffer;
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        /// <inheritdoc />
        public ValueTask DisposeAsync()
        {
            return innerBuffer.DisposeAsync();
        }

        /// <inheritdoc />
        public void OnCompleted()
        {
            (innerBuffer as IObserver<byte[]>).OnCompleted();
        }

        /// <inheritdoc />
        public void OnError(Exception error)
        {
            (innerBuffer as IObserver<byte[]>).OnError(error);
        }

        /// <inheritdoc />
        public void OnNext(T value)
        {
            var bytes = serializer(value);
            (innerBuffer as IObserver<byte[]>).OnNext(bytes);
        }

        /// <inheritdoc />
        public IDisposable Subscribe(IObserver<T> observer)
        {
            return new Subscription<byte[]>(
                new TranslatingObserver(observer, deserializer),
                observers);
        }

        private class TranslatingObserver : IObserver<byte[]>
        {
            private readonly IObserver<T> innerObserver;
            private readonly Func<byte[], T> deserializer;

            public TranslatingObserver(IObserver<T> innerObserver, Func<byte[], T> deserializer)
            {
                this.innerObserver = innerObserver;
                this.deserializer = deserializer;
            }

            /// <inheritdoc />
            public void OnCompleted()
            {
                innerObserver.OnCompleted();
            }

            /// <inheritdoc />
            public void OnError(Exception error)
            {
                innerObserver.OnError(error);
            }

            /// <inheritdoc />
            public void OnNext(byte[] value)
            {
                var item = deserializer(value);
                innerObserver.OnNext(item);
            }
        }
    }
}