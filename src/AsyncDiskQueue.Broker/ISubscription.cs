namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Threading.Tasks;

    public interface ISubscription: IAsyncDisposable
    {
        SubscriberInfo SubscriberInfo { get; }
    }

    public interface ISubscription<in T> : ISubscription
    {
        public bool Persistent { get; }

        Func<T, Task> Handler { get; }
    }
}
