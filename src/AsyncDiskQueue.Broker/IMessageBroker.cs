namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Threading.Tasks;

    public interface IMessageBroker : IAsyncDisposable
    {
        Task Publish(MessagePayload message);

        Task<IAsyncDisposable> Subscribe(ISubscriptionRequest subscriptionRequest);
    }
}