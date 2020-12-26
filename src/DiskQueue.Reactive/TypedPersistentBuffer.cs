namespace AsyncDiskQueue.Reactive
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class TypedPersistentBuffer<T> : IObserver<T>, IObservable<T>, IAsyncDisposable
    {
        private readonly PersistentBuffer _innerBuffer;
        private readonly Func<T, byte[]> _serializer;
        private readonly Func<byte[], T> _deserializer;
        private readonly List<IObserver<byte[]>> _observers = new List<IObserver<byte[]>>();

        public TypedPersistentBuffer(PersistentBuffer innerBuffer, Func<T, byte[]> serializer, Func<byte[], T> deserializer)
        {
            _innerBuffer = innerBuffer;
            _serializer = serializer;
            _deserializer = deserializer;
        }

        /// <inheritdoc />
        public ValueTask DisposeAsync()
        {
            return _innerBuffer.DisposeAsync();
        }

        /// <inheritdoc />
        public void OnCompleted()
        {
            (_innerBuffer as IObserver<byte[]>).OnCompleted();
        }

        /// <inheritdoc />
        public void OnError(Exception error)
        {
            (_innerBuffer as IObserver<byte[]>).OnError(error);
        }

        /// <inheritdoc />
        public void OnNext(T value)
        {
            var bytes = _serializer(value);
            (_innerBuffer as IObserver<byte[]>).OnNext(bytes);
        }

        /// <inheritdoc />
        public IDisposable Subscribe(IObserver<T> observer)
        {
            return new Subscription<byte[]>(
                new TranslatingObserver(observer, _deserializer),
                _observers);
        }

        private class TranslatingObserver : IObserver<byte[]>
        {
            private readonly IObserver<T> _innerObserver;
            private readonly Func<byte[], T> _deserializer;

            public TranslatingObserver(IObserver<T> innerObserver, Func<byte[], T> deserializer)
            {
                _innerObserver = innerObserver;
                _deserializer = deserializer;
            }

            /// <inheritdoc />
            public void OnCompleted()
            {
                _innerObserver.OnCompleted();
            }

            /// <inheritdoc />
            public void OnError(Exception error)
            {
                _innerObserver.OnError(error);
            }

            /// <inheritdoc />
            public void OnNext(byte[] value)
            {
                var item = _deserializer(value);
                _innerObserver.OnNext(item);
            }
        }
    }
}