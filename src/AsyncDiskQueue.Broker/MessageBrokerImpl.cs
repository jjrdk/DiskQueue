namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    public class MessageBrokerImpl : IMessageBroker
    {
        private readonly JsonSerializerSettings _serializerSettings;
        private readonly DirectoryInfo _directory;
        private readonly ILoggerFactory _loggerFactory;
        private readonly HashSet<Type> _topics = new();
        private readonly Dictionary<Type, (CancellationTokenSource, Task)> _topicWorkers = new();
        private readonly ConcurrentDictionary<string, Task<IPersistentQueue>> _queues = new();
        private readonly List<ISubscription> _subscribers;
        private readonly ConcurrentDictionary<Type, Type[]> _typesMap = new();

        public MessageBrokerImpl(DirectoryInfo directory, ILoggerFactory loggerFactory)
        {
            _directory = directory;
            _loggerFactory = loggerFactory;
            _subscribers = new List<ISubscription>();
            _serializerSettings = new JsonSerializerSettings
            {
                MetadataPropertyHandling = MetadataPropertyHandling.ReadAhead,
                TypeNameHandling = TypeNameHandling.Auto,
                TypeNameAssemblyFormatHandling = TypeNameAssemblyFormatHandling.Simple,
                DefaultValueHandling = DefaultValueHandling.Ignore,
                MissingMemberHandling = MissingMemberHandling.Ignore
            };
        }

        public async Task Publish<T>(string source, T message) where T : class
        {
            if (message == null || source == null)
            {
                return;
            }

            var msg = new MessagePayload(source, message, DateTimeOffset.UtcNow);
            // Put into topic queues
            var tasks = _typesMap
                .GetOrAdd(typeof(T), t => t.GetInterfaces().Concat(t.GetInheritanceChain()).Distinct().ToArray())
                .Where(_topics.Contains)
                .Select(GetTopicAddress)
                .Distinct()
                .Select(GetQueue)
                .Select(
                    async t =>
                    {
                        var queue = await t.ConfigureAwait(false);
                        using var session = queue.OpenSession(Serialize, Deserialize);
                        await session.Enqueue(msg).ConfigureAwait(false);
                        await session.Flush().ConfigureAwait(false);
                    });

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        private byte[] Serialize(MessagePayload item)
        {
            return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(item, _serializerSettings));
        }

        private MessagePayload Deserialize(byte[] bytes)
        {
            return JsonConvert.DeserializeObject<MessagePayload>(Encoding.UTF8.GetString(bytes), _serializerSettings);
        }

        private Task<IPersistentQueue> GetQueue(string topicAddress)
        {
            return _queues.GetOrAdd(topicAddress, (s, l) => PersistentQueue.Create(Path.Combine(_directory.FullName, "topics", s), l, paranoidFlushing: false), _loggerFactory);
        }

        public IDisposable Subscribe<T>(Func<T, Task> subscription) where T : class
        {
            var current = _subscribers.OfType<ISubscription<T>>().FirstOrDefault(s => ReferenceEquals(s.Handler, subscription));
            if (current != null)
            {
                return current as IDisposable;
            }

            _topics.Add(typeof(T));
            EnsureTopicWorker<T>();
            var item = new Subscription<T>(subscription, _subscribers);
            _subscribers.Add(item);

            //var delegates = _subscribers.Where(s => s.GetType().GetInterfaces().Contains(typeof(ISubscription<T>)));

            return item;
        }

        //public void Unsubscribe<T>(Action<MessagePayload<T>> subscription)
        //{
        //    if (!_subscribers.ContainsKey(typeof(T))) return;
        //    var delegates = _subscribers[typeof(T)];
        //    if (delegates.Contains(subscription))
        //        delegates.Remove(subscription);
        //    if (delegates.Count == 0)
        //        _subscribers.Remove(typeof(T));
        //}

        public async ValueTask DisposeAsync()
        {
            var workerTasks = _topicWorkers.Values.Select(
                async tuple =>
                {
                    tuple.Item1.Cancel();
                    await tuple.Item2.ConfigureAwait(false);
                });
            await Task.WhenAll(workerTasks).ConfigureAwait(false);
            _topicWorkers.Clear();

            var tasks = _queues.Select(
             async queue =>
             {
                 var persistentQueue = await queue.Value.ConfigureAwait(false);
                 await persistentQueue.DisposeAsync().ConfigureAwait(false);
             });
            await Task.WhenAll(tasks).ConfigureAwait(false);
            _queues.Clear();
            _subscribers?.Clear();
        }

        private void EnsureTopicWorker<T>() where T : class
        {
            foreach (var subTopic in typeof(T).GetInheritanceChain().Where(subTopic => !_topics.Contains(subTopic)))
            {
                _topics.Add(subTopic);
                var tokenSource = new CancellationTokenSource();
                var topicWorker = TopicWorker<T>(tokenSource.Token);
                _topicWorkers.Add(subTopic, (tokenSource, topicWorker));
            }
        }

        private static string GetTopicAddress(Type topic)
        {
            return $"urn_{topic.Namespace}_{topic.Name.Replace('`', '_')}";
        }

        private async Task TopicWorker<T>(CancellationToken cancellationToken) where T : class
        {
            var queue = await _queues.GetOrAdd(GetTopicAddress(typeof(T)), GetQueue).ConfigureAwait(false);
            await foreach (var item in queue.OpenSession(Serialize, Deserialize).ToAsyncEnumerable(cancellationToken: cancellationToken))
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                var payload = item.Payload as T;
                if (payload == null)
                {
                    continue;
                }

                var tasks = _subscribers.OfType<ISubscription<T>>().Select(s => s.Handler(payload));
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
        }

        private class Subscription<T> : ISubscription<T>, IDisposable
        {
            public Subscription(Func<T, Task> handler, ICollection<ISubscription> subscriptions)
            {
                Handler = handler;
                _subscriptions = subscriptions;
            }

            /// <inheritdoc />
            public bool Persistent { get; } = false;

            public Func<T, Task> Handler { get; }

            private readonly ICollection<ISubscription> _subscriptions;

            /// <inheritdoc />
            public void Dispose()
            {
                _subscriptions.Remove(this);
            }
        }
    }
}