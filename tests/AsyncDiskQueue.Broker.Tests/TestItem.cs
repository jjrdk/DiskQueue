namespace AsyncDiskQueue.Broker.Tests
{
    public interface ITestItem
    {
        string Value { get; }
    }

    public class TestItem : ITestItem
    {
        public string Value { get; init; }
    }
}