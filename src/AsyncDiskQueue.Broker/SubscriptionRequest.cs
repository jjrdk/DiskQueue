namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public class SubscriptionRequest : ISubscriptionRequest
    {
        public SubscriptionRequest(string endPoint, bool persistent, params IMessageReceiver[] handlers)
        {
            SubscriberInfo = new SubscriberInfo(endPoint);
            Persistent = persistent;
            MessageReceivers = handlers;
        }

        /// <inheritdoc />
        public SubscriberInfo SubscriberInfo { get; }

        /// <inheritdoc />
        public IMessageReceiver[] MessageReceivers { get; }

        /// <inheritdoc />
        public bool Persistent { get; }
    }
}