namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Threading.Tasks;

    public interface IMessageBroker : IAsyncDisposable
    {
        Task Publish<T>(string source, T message) where T : class;

        IDisposable Subscribe<T>(Func<T, Task> subscription) where T: class;
    }
}