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

        public async Task Publish<T>(string source, T message)
            where T : class
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

        public IAsyncDisposable Subscribe<T>(Func<T, Task> subscription)
            where T : class
        {
            var current = _subscribers.OfType<ISubscription<T>>()
                .FirstOrDefault(s => ReferenceEquals(s.Handler, subscription));
            if (current != null)
            {
                return current as IAsyncDisposable;
            }

            EnsureTopicWorker<T>();
            var item = new Subscription<T>(subscription, _subscribers, OnUnsubscribe<T>);
            _subscribers.Add(item);

            //var delegates = _subscribers.Where(s => s.GetType().GetInterfaces().Contains(typeof(ISubscription<T>)));

            return item;
        }

        public async ValueTask DisposeAsync()
        {
            var workerTasks = _topicWorkers.Values.Select(
                tuple => StopWorker(tuple.Item1, tuple.Item2));
            await Task.WhenAll(workerTasks).ConfigureAwait(false);
            _topicWorkers.Clear();
            _subscribers?.Clear();

            var tasks = _queues.Select(
                async queue =>
                {
                    var persistentQueue = await queue.Value.ConfigureAwait(false);
                    await persistentQueue.DisposeAsync().ConfigureAwait(false);
                });
            await Task.WhenAll(tasks).ConfigureAwait(false);
            _queues.Clear();
        }

        private static async Task StopWorker(CancellationTokenSource tokenSource, Task task)
        {
            tokenSource.Cancel();
            await task.ConfigureAwait(false);
            tokenSource.Dispose();
            task.Dispose();
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
            return _queues.GetOrAdd(
                topicAddress,
                (s, l) => PersistentQueue.Create(
                    Path.Combine(_directory.FullName, "topics", s),
                    l,
                    paranoidFlushing: false),
                _loggerFactory);
        }

        private async Task OnUnsubscribe<T>()
        {
            if (_subscribers.OfType<ISubscription<T>>().Any())
            {
                return;
            }

            var topic = typeof(T);
            _topics.Remove(topic);
            var (cancellationTokenSource, task) = _topicWorkers[topic];
            await StopWorker(cancellationTokenSource, task).ConfigureAwait(false);
            _topicWorkers.Remove(topic);
        }

        private void EnsureTopicWorker<T>()
            where T : class
        {
            var topic = typeof(T);
            _topics.Add(topic);
            var tokenSource = new CancellationTokenSource();
            var topicWorker = TopicWorker<T>(tokenSource.Token);
            _topicWorkers.Add(topic, (tokenSource, topicWorker));
        }

        private static string GetTopicAddress(Type topic)
        {
            return $"urn_{topic.Namespace}_{topic.Name.Replace('`', '_')}";
        }

        private async Task TopicWorker<T>(CancellationToken cancellationToken)
            where T : class
        {
            var queue = await _queues.GetOrAdd(GetTopicAddress(typeof(T)), GetQueue).ConfigureAwait(false);
            await foreach (var item in queue.OpenSession(Serialize, Deserialize)
                .ToAsyncEnumerable(cancellationToken: cancellationToken))
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
    }
}
