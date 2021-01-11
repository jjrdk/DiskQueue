namespace AsyncDiskQueue.Broker.Tests
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Abstractions;
    using Serializers.Newtonsoft;

    public class DelegateReceiver<T> : IMessageReceiver
    {
        private readonly Func<T, CancellationToken, Task> _handler;

        public DelegateReceiver(Func<T, CancellationToken, Task> handler)
        {
            Topic = typeof(T).Hash();
            _handler = handler;
        }

        /// <inheritdoc />
        public string Topic { get; }

        /// <inheritdoc />
        public bool Persistent => false;

        /// <inheritdoc />
        public Task OnNext(byte[] msg, CancellationToken cancellationToken)
        {
            var t = Serializer.Deserialize(msg);
            return _handler((T)t.Payload, cancellationToken);
        }
    }

    public static class Message
    {
        private static readonly ConcurrentDictionary<Type, string[]> TypesMap = new();

        public static (byte[] bytes, string[] topics) Create<T>(string source, T payload, string correlationId = null)
        {
            var topics = TypesMap.GetOrAdd(typeof(T), TypeValueFactory);
            var message = new MessagePayload(
                source,
                payload,
                topics.ToArray(),
                new Dictionary<string, object>(),
                TimeSpan.Zero,
                DateTimeOffset.UtcNow,
                correlationId);
            return (Serializer.Serialize(message), topics);
        }

        private static string[] TypeValueFactory(Type t)
        {
            return t.GetInterfaces().Concat(t.GetInheritanceChain()).Select(x => x.FullName).Distinct().ToArray();
        }

    }
}