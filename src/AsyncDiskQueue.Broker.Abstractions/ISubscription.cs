namespace AsyncDiskQueue.Broker.Abstractions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public interface ISubscriptionSource : ISubscription
    {
        IAsyncDisposable Connect(IMessageReceiver receiver);
    }

    public interface IMessageReceiver
    {
        string Topic { get; }

        bool Persistent { get; }

        Task OnNext(byte[] msg, CancellationToken cancellationToken);
    }

    public interface ISubscription : IMessageReceiver, IAsyncDisposable
    {
        string EndPoint { get; }
    }

    public interface ISubscriptionRequest
    {
        SubscriberInfo SubscriberInfo { get; }

        IMessageReceiver[] MessageReceivers { get; }
    }
}
