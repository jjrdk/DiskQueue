namespace AsyncDiskQueue.Broker.Abstractions
{
    using System;
    using System.Collections.Generic;

    public record MessagePayload(
        string Source,
        object Payload,
        string[] Topics,
        Dictionary<string, object> Headers,
        TimeSpan TimeToLive,
        DateTimeOffset Timestamp,
        string CorrelationId = null);
}
