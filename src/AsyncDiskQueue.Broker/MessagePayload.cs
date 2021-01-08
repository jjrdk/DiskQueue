namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Collections.Generic;

    public record MessagePayload(
        string Source,
        object Payload,
        string[] MessageTypes,
        Dictionary<string, object> Headers,
        TimeSpan TimeToLive,
        DateTimeOffset Timestamp,
        string CorrelationId = null);
}
