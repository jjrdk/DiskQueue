namespace AsyncDiskQueue.Reactive
{
    using System;

    internal class SubscriberException : Exception
    {
        public SubscriberException() : base("Subscriber error")
        {
        }
    }
}