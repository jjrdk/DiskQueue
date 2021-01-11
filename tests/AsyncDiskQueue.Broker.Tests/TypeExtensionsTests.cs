namespace AsyncDiskQueue.Broker.Tests
{
    using System;
    using System.Linq;
    using Abstractions;
    using Xunit;

    public class TypeExtensionsTests
    {
        [Fact]
        public void CanGetInheritanceHierarchy()
        {
            var baseTypes = typeof(DeepInheritance).GetInheritanceChain().ToArray();

            Assert.Equal(4, baseTypes.Length);
        }

        private class DeepInheritance : AggregateException { }
    }
}
