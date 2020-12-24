namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Threading.Tasks;

    public interface ISubscription
    {
    }

    public interface ISubscription<in T> : ISubscription
    {
        public bool Persistent { get; }

        Func<T, Task> Handler { get; }
    }
}
