namespace DiskQueue.Tests
{
    using AsyncDiskQueue.Implementation;
    using Xunit;

    public class ConstantChecks
    {
        [Fact]
        public void StartTransactionSeparatorGuid_is_undamaged()
        {
            // This should never be changed! If this test fails existing queues will be unreadable.
            Assert.Equal("b75bfb12-93bb-42b6-acb1-a897239ea3a5", Constants.StartTransactionSeparatorGuid.ToString());
        }

        [Fact]
        public void EndTransactionSeparatorGuid_is_undamaged()
        {
            // This should never be changed! If this test fails existing queues will be unreadable.
            Assert.Equal("866c9705-4456-4e9d-b452-3146b3bfa4ce", Constants.EndTransactionSeparatorGuid.ToString());
        }
    }
}