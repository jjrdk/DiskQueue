namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;

    public record MessagePayload(
        string Source,
        object Payload,
        string[] Topics,
        Dictionary<string, object> Headers,
        TimeSpan TimeToLive,
        DateTimeOffset Timestamp,
        string CorrelationId = null);

    public static class Message
    {
        private static readonly ConcurrentDictionary<Type, string[]> TypesMap = new();

        public static MessagePayload Create<T>(string source, T payload, string correlationId = null)
        {
            var topics = TypesMap.GetOrAdd(typeof(T), TypeValueFactory);
            return new MessagePayload(
                source,
                payload,
                topics.ToArray(),
                new Dictionary<string, object>(),
                TimeSpan.Zero,
                DateTimeOffset.UtcNow,
                correlationId);
        }

        private static string[] TypeValueFactory(Type t)
        {
            return t.GetInterfaces().Concat(t.GetInheritanceChain()).Select(x=>x.FullName).Distinct().ToArray();
        }

    }
}
