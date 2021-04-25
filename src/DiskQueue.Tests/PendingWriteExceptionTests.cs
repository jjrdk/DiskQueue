namespace DiskQueue.Tests
{
    using System;
    using AsyncDiskQueue.Implementation;
    using Xunit;

	public class PendingWriteExceptionTests
	{
		[Fact]
		public void Can_get_all_information_from_to_string()
		{
			try
			{
				throw new ArgumentException("foo");
			}
			catch (Exception e)
			{
				var s = new PendingWriteException(new []{e}).ToString();
				Assert.Contains(e.ToString(), s);
			}
		}

		[Fact]
		public void Can_get_exception_detail_information_from_pending_write_exception()
		{
			try
			{
				throw new ArgumentException("foo");
			}
			catch (Exception e)
			{
				var s = new PendingWriteException(new [] { e });
				Assert.Contains(e, s.PendingWritesExceptions);
			}
		}
	}
}