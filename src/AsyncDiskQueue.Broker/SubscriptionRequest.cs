namespace AsyncDiskQueue.Broker
{
    public class SubscriptionRequest : ISubscriptionRequest
    {
        public SubscriptionRequest(string endPoint, params IMessageReceiver[] handlers)
        {
            SubscriberInfo = new SubscriberInfo(endPoint);
            MessageReceivers = handlers;
        }

        /// <inheritdoc />
        public SubscriberInfo SubscriberInfo { get; }

        /// <inheritdoc />
        public IMessageReceiver[] MessageReceivers { get; }
    }
}