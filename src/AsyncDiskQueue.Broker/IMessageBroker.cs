namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Threading.Tasks;

    public interface IMessageBroker : IAsyncDisposable
    {
        Task Publish<T>(string source, T message) where T : class;

        Task<IAsyncDisposable> Subscribe(ISubscriptionRequest subscriptionRequest);
    }
}