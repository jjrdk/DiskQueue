namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using Abstractions;
    using Microsoft.Extensions.Logging;

    public class MessageBroker : IMessageBroker
    {
        private const string BrokerId = "4F545B9DEE8F413A97136C1D4CAEDEF6";
        private readonly DirectoryInfo _directory;
        private readonly ILoggerFactory _loggerFactory;
        private readonly HashSet<string> _topics = new();
        private readonly Dictionary<string, IAsyncDisposable> _topicWorkers = new();
        private readonly ConcurrentDictionary<(string endpoint, string topic), Task<IDiskQueue>> _queues = new();
        private readonly List<ISubscription> _subscribers;

        private MessageBroker(DirectoryInfo directory, ILoggerFactory loggerFactory)
        {
            _directory = directory;
            _loggerFactory = loggerFactory;
            _subscribers = new List<ISubscription>();
        }

        public static async Task<MessageBroker> Create(DirectoryInfo directory, ILoggerFactory loggerFactory)
        {
            var broker = new MessageBroker(directory, loggerFactory);
            if (!Directory.Exists(directory.FullName))
            {
                Directory.CreateDirectory(directory.FullName);
            }
            foreach (var path in Directory.GetDirectories(directory.FullName))
            {
                var endpoint = path.Replace(Path.GetDirectoryName(path)!, string.Empty).Trim(Path.DirectorySeparatorChar);
                foreach (var hashPath in Directory.GetDirectories(path))
                {
                    var topic = hashPath.Replace(Path.GetDirectoryName(hashPath)!, string.Empty).Trim(Path.DirectorySeparatorChar);

                    var queue = await broker._queues.GetOrAdd((endpoint, topic), broker.GetQueue).ConfigureAwait(false);

                    var instance = new PermanentSubscriber(endpoint, topic, queue);
                    broker._subscribers.Add(instance);
                }
            }

            return broker;
        }

        async Task IMessageBroker.Publish(Memory<byte> message, params string[] topics)
        {
            // Put into topic queues
            async Task Enqueue(Task<IDiskQueue> task)
            {
                var queue = await task.ConfigureAwait(false);
                using var session = queue.OpenSession();
                await session.Enqueue(message).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            var tasks = topics.Distinct()
                .Select(t => t.Hash())
                .Where(_topics.Contains)
                .Select(s => _queues.GetOrAdd((BrokerId, s), GetQueue))
                .Select(Enqueue);

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        async Task<IAsyncDisposable> IMessageBroker.Subscribe(ISubscriptionRequest subscriptionRequest)
        {
            var endPoint = subscriptionRequest.SubscriberInfo.EndPoint;
            if (string.IsNullOrWhiteSpace(endPoint) || endPoint == BrokerId)
            {
                return new NullDisposable();
            }
            var tasks = subscriptionRequest.MessageReceivers.Select(
                x => Subscribe(endPoint, x));
            var subscriptions = await Task.WhenAll(tasks).ConfigureAwait(false);
            return new AggregateDisposable(subscriptions);
        }

        private async Task<IAsyncDisposable> Subscribe(string endPoint, IMessageReceiver receiver)
        {
            var current = _subscribers.FirstOrDefault(s => s.EndPoint == endPoint && s.Topic == receiver.Topic);
            if (current != null)
            {
                return current is ISubscriptionSource permanentSubscriber
                    ? permanentSubscriber.Connect(receiver)
                    : current;
            }

            await EnsureTopicWorker(receiver.Topic).ConfigureAwait(false);
            if (receiver.Persistent)
            {
                var queue = await _queues.GetOrAdd((endPoint, receiver.Topic), GetQueue).ConfigureAwait(false);
                var subscription = new PermanentSubscriber(endPoint, receiver.Topic, queue);
                _subscribers.Add(subscription);

                return subscription.Connect(receiver);
            }

            var item = new SubscriptionWorker(endPoint, receiver, OnUnsubscribe);
            _subscribers.Add(item);

            return item;
        }

        public async ValueTask DisposeAsync()
        {
            var subscriberTasks = _subscribers.Select(s => s.DisposeAsync().AsTask());
            await Task.WhenAll(subscriberTasks).ConfigureAwait(false);
            _subscribers.Clear();
            var workerTasks = _topicWorkers.Values.Select(worker => worker.DisposeAsync().AsTask());
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
        }

        private async Task<IDiskQueue> GetQueue((string endpoint, string topic) address)
        {
            return await DiskQueue.Create(Path.Combine(_directory.FullName, address.endpoint.Hash(), address.topic), _loggerFactory)
                .ConfigureAwait(false);
        }

        private async Task OnUnsubscribe(ISubscription subscription)
        {
            _subscribers.Remove(subscription);
            var topic = subscription.Topic;
            if (_subscribers.Any(x => x.Topic == topic))
            {
                return;
            }

            _topics.Remove(topic);
            var worker = _topicWorkers[topic];
            await worker.DisposeAsync().ConfigureAwait(false);
            _topicWorkers.Remove(topic);
        }

        private async Task EnsureTopicWorker(string topic)
        {
            if (_topicWorkers.ContainsKey(topic))
            {
                return;
            }
            _topics.Add(topic);
            var queue = await _queues.GetOrAdd((BrokerId, topic), GetQueue).ConfigureAwait(false);
            var topicWorker = TopicWorker.Create(queue, _subscribers.Where(s => s.Topic == topic));
            _topicWorkers.Add(topic, topicWorker);
        }
    }
}
