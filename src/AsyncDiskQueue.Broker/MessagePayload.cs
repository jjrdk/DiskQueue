namespace AsyncDiskQueue.Broker
{
    using System;

    public record MessagePayload(string Source, object Payload, DateTimeOffset Timestamp, string CorrelationId = null);
}