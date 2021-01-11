namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Threading.Tasks;
    using Abstractions;

    public interface IMessageBroker : IAsyncDisposable
    {
        Task Publish(Memory<byte> message, params string[] topics);

        Task<IAsyncDisposable> Subscribe(ISubscriptionRequest subscriptionRequest);
    }
}