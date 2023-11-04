namespace DiskQueue.Tests;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using AsyncDiskQueue;
using AsyncDiskQueue.Implementation;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

public class TransactionLogTests : PersistentQueueTestsBase
{
    [Fact]
    public async Task Transaction_log_size_shrink_after_queue_disposed()
    {
        long txSizeWhenOpen;
        var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));
        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>(),
            paranoidFlushing: false))
        {
            using (var session = queue.OpenSession())
            {
                for (var j = 0; j < 10; j++)
                {
                    await session.Enqueue(Guid.NewGuid().ToByteArray());
                }

                await session.Flush();
            }

            using (var session = queue.OpenSession())
            {
                for (var j = 0; j < 10; j++)
                {
                    await session.Dequeue();
                }

                await session.Flush();
            }

            txSizeWhenOpen = txLogInfo.Length;
        }

        txLogInfo.Refresh();
        Assert.True(txLogInfo.Length < txSizeWhenOpen);
    }

    [Fact]
    public async Task Count_of_items_will_remain_fixed_after_dequeuing_without_flushing()
    {
        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>(),
            paranoidFlushing: false))
        {
            using (var session = queue.OpenSession())
            {
                for (var j = 0; j < 10; j++)
                {
                    await session.Enqueue(Guid.NewGuid().ToByteArray());
                }

                await session.Flush();
            }

            using (var session = queue.OpenSession())
            {
                for (var j = 0; j < 10; j++)
                {
                    await session.Dequeue();
                }

                Assert.True((await session.Dequeue()).IsEmpty);

                //	session.Flush(); explicitly removed
            }
        }

        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>()))
        {
            Assert.Equal(10, ((IPersistentQueueStore)queue).EstimatedCountOfItemsInQueue);
        }
    }

    [Fact]
    public async Task Dequeue_items_that_were_not_flushed_will_appear_after_queue_restart()
    {
        await using var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>());
        using (var session = queue.OpenSession())
        {
            for (var j = 0; j < 10; j++)
            {
                await session.Enqueue(Guid.NewGuid().ToByteArray());
            }

            await session.Flush();
        }

        using (var session = queue.OpenSession())
        {
            for (var j = 0; j < 10; j++)
            {
                await session.Dequeue();
            }

            Assert.True((await session.Dequeue()).IsEmpty);
        }
    }

    [Fact]
    public async Task If_tx_log_grows_too_large_it_will_be_trimmed_while_queue_is_in_operation()
    {
        var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

        await using var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>(),
                suggestedMaxTransactionLogSize: 32, paranoidFlushing: false)
            ;

        using (var session = queue.OpenSession())
        {
            for (var j = 0; j < 20; j++)
            {
                await session.Enqueue(Guid.NewGuid().ToByteArray());
            }

            await session.Flush();
        }

        // there is no way optimize here, so we should get expected size, even though it is bigger than
        // what we suggested as the max
        txLogInfo.Refresh();
        var txSizeWhenOpen = txLogInfo.Length;

        using (var session = queue.OpenSession())
        {
            for (var j = 0; j < 20; j++)
            {
                await session.Dequeue();
            }

            Assert.True((await session.Dequeue()).IsEmpty);

            await session.Flush();
        }

        txLogInfo.Refresh();
        Assert.True(txLogInfo.Length < txSizeWhenOpen);
    }

    [Fact]
    public async Task Truncated_transaction_is_ignored_with_default_settings()
    {
        var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

        await using (var queue = await PersistentQueue
            .Create(Path, Substitute.For<ILogger<PersistentQueue>>(), trimTransactionLogOnDispose: false,
                paranoidFlushing: false)
        )
        {
            using var session = queue.OpenSession();
            for (var j = 0; j < 20; j++)
            {
                await session.Enqueue(BitConverter.GetBytes(j));
                await session.Flush();
            }
        }

        await using (var txLog = txLogInfo.Open(FileMode.Open))
        {
            txLog.SetLength(txLog.Length - 5); // corrupt last transaction
            txLog.Flush();
        }

        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>()))
        {
            using var session = queue.OpenSession();
            for (var j = 0; j < 19; j++)
            {
                Assert.Equal(j, BitConverter.ToInt32((await session.Dequeue()).Span));
            }

            Assert.True((await session.Dequeue()).IsEmpty); // the last transaction was corrupted
            await session.Flush();
        }
    }

    [Fact]
    public async Task Can_handle_truncated_start_transaction_separator()
    {
        var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>(),
            trimTransactionLogOnDispose: false)
        )
        {
            using var session = queue.OpenSession();
            for (var j = 0; j < 20; j++)
            {
                await session.Enqueue(BitConverter.GetBytes(j));
                await session.Flush();
            }
        }

        await using (var txLog = txLogInfo.Open(FileMode.Open))
        {
            txLog.SetLength(5); // truncate log to halfway through start marker
            txLog.Flush();
        }

        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>()))
        {
            using var session = queue.OpenSession();
            Assert.True((await session.Dequeue()).IsEmpty); // the last transaction was corrupted
            await session.Flush();
        }
    }

    [Fact]
    public async Task Can_handle_truncated_data()
    {
        var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>(),
            trimTransactionLogOnDispose: false)
        )
        {
            using var session = queue.OpenSession();
            for (var j = 0; j < 20; j++)
            {
                await session.Enqueue(BitConverter.GetBytes(j));
                await session.Flush();
            }
        }

        await using (var txLog = txLogInfo.Open(FileMode.Open))
        {
            txLog.SetLength(100); // truncate log to halfway through log entry
            txLog.Flush();
        }

        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>()))
        {
            using var session = queue.OpenSession();
            Assert.True((await session.Dequeue()).IsEmpty); // the last transaction was corrupted
            await session.Flush();
        }
    }

    [Fact]
    public async Task Can_handle_truncated_end_transaction_separator()
    {
        var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>(),
            trimTransactionLogOnDispose: false)
        )
        {
            using var session = queue.OpenSession();
            for (var j = 0; j < 20; j++)
            {
                await session.Enqueue(BitConverter.GetBytes(j));
                await session.Flush();
            }
        }

        await using (var txLog = txLogInfo.Open(FileMode.Open))
        {
            txLog.SetLength(368); // truncate end transaction marker
            txLog.Flush();
        }

        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>()))
        {
            using var session = queue.OpenSession();
            Assert.True((await session.Dequeue()).IsEmpty); // the last transaction was corrupted
            await session.Flush();
        }
    }

    [Fact]
    public async Task Can_handle_transaction_with_only_zero_length_entries()
    {
        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>(),
            trimTransactionLogOnDispose: false)
        )
        {
            using var session = queue.OpenSession();
            for (var j = 0; j < 20; j++)
            {
                await session.Enqueue(Array.Empty<byte>());
                await session.Flush();
            }
        }

        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>()))
        {
            using var session = queue.OpenSession();
            for (var j = 0; j < 20; j++)
            {
                Assert.True((await session.Dequeue()).IsEmpty);
            }

            Assert.True((await session.Dequeue()).IsEmpty);
            await session.Flush();
        }
    }

    [Fact]
    public async Task Can_handle_end_separator_used_as_data()
    {
        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>(),
            trimTransactionLogOnDispose: false)
        )
        {
            using var session = queue.OpenSession();
            for (var j = 0; j < 20; j++)
            {
                await session.Enqueue(Constants.EndTransactionSeparator.ToArray()); // ???
                await session.Flush();
            }

            await session.Flush();
        }

        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>()))
        {
            using var session = queue.OpenSession();
            Assert.Equal(Constants.EndTransactionSeparator.ToArray(), await session.Dequeue());
            await session.Flush();
        }
    }

    [Fact]
    public async Task Can_handle_start_separator_used_as_data()
    {
        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>(),
            trimTransactionLogOnDispose: false)
        )
        {
            using var session = queue.OpenSession();
            for (var j = 0; j < 20; j++)
            {
                await session.Enqueue(Constants.StartTransactionSeparator.ToArray()); // ???
                await session.Flush();
            }

            await session.Flush();
        }

        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>()))
        {
            using var session = queue.OpenSession();
            Assert.Equal(Constants.StartTransactionSeparator.ToArray(), await session.Dequeue());
            await session.Flush();
        }
    }

    [Fact]
    public async Task Can_handle_zero_length_entries_at_start()
    {
        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>(),
            trimTransactionLogOnDispose: false))
        {
            var data = new byte[] { 1 };
            using var session = queue.OpenSession();
            await session.Enqueue(Array.Empty<byte>());
            await session.Flush();
            for (var j = 0; j < 19; j++)
            {
                await session.Enqueue(data);
                await session.Flush();
            }
        }

        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>()))
        {
            using var session = queue.OpenSession();
            for (var j = 0; j < 20; j++)
            {
                var dequeue = await session.Dequeue();
                Assert.Equal(j > 0 ? 1 : 0, dequeue.Length);
                await session.Flush();
            }
        }
    }


    [Fact]
    public async Task Can_handle_zero_length_entries_at_end()
    {
        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>(),
            trimTransactionLogOnDispose: false))
        {
            var data = new byte[] { 1 };
            using var session = queue.OpenSession();
            for (var j = 0; j < 19; j++)
            {
                await session.Enqueue(data);
                await session.Flush();
            }

            await session.Enqueue(Array.Empty<byte>());
            await session.Flush();
        }

        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>()))
        {
            using var session = queue.OpenSession();
            for (var j = 0; j < 20; j++)
            {
                var dequeue = await session.Dequeue();
                Assert.Equal(j < 19 ? 1 : 0, dequeue.Length);
                await session.Flush();
            }
        }
    }

    [Fact]
    public async Task Can_restore_data_when_a_transaction_set_is_partially_truncated()
    {
        var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));
        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>(),
            trimTransactionLogOnDispose: false)
        )
        {
            using var session = queue.OpenSession();
            for (var j = 0; j < 5; j++)
            {
                await session.Enqueue(Array.Empty<byte>());
            }

            await session.Flush();
        }

        await using (var txLog = txLogInfo.Open(FileMode.Open))
        {
            var buf = new byte[(int)txLog.Length];
            _ = txLog.Read(buf, 0, (int)txLog.Length);
            txLog.Write(buf, 0, buf.Length); // a 'good' extra session
            txLog.Write(buf, 0, buf.Length / 2); // a 'bad' extra session
            txLog.Flush();
        }

        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>()))
        {
            using var session = queue.OpenSession();
            for (var j = 0; j < 10; j++)
            {
                Assert.True((await session.Dequeue()).IsEmpty);
            }

            Assert.True((await session.Dequeue()).IsEmpty);
            await session.Flush();
        }
    }

    [Fact]
    public async Task
        Can_restore_data_when_a_transaction_set_is_partially_overwritten_when_throwOnConflict_is_false()
    {
        var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));
        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>(),
            trimTransactionLogOnDispose: false)
        )
        {
            using var session = queue.OpenSession();
            for (var j = 0; j < 5; j++)
            {
                await session.Enqueue(Array.Empty<byte>());
            }

            await session.Flush();
        }

        await using (var txLog = txLogInfo.Open(FileMode.Open))
        {
            var buf = new byte[(int)txLog.Length];
            _ = txLog.Read(buf, 0, (int)txLog.Length);
            txLog.Write(buf, 0, buf.Length - 16); // new session, but with missing end marker
            txLog.Write(Constants.StartTransactionSeparator.Span);
            txLog.Flush();
        }

        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>(),
            maxFileSize: Constants._32Megabytes, throwOnConflict: false)
        )
        {
            using var session = queue.OpenSession();
            for (var j = 0; j < 5; j++) // first 5 should be OK
            {
                Assert.True((await session.Dequeue()).IsEmpty);
            }

            Assert.True((await session.Dequeue()).IsEmpty); // duplicated 5 should be silently lost.
            await session.Flush();
        }
    }

    [Fact]
    public async Task Will_remove_truncated_transaction()
    {
        var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>()))
        {
            using var session = queue.OpenSession();
            for (var j = 0; j < 20; j++)
            {
                await session.Enqueue(BitConverter.GetBytes(j));
                await session.Flush();
            }
        }

        await using (var txLog = txLogInfo.Open(FileMode.Open))
        {
            txLog.SetLength(5); // corrupt all transactions
            txLog.Flush();
        }

        var q = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>());
        await q.DisposeAsync();

        txLogInfo.Refresh();

        Assert.Equal(36, txLogInfo.Length); //empty transaction size
    }

    [Fact]
    public async Task Truncated_transaction_is_ignored_and_can_continue_to_add_items_to_queue()
    {
        var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>(),
            trimTransactionLogOnDispose: false, paranoidFlushing: false)
        )
        {
            using var session = queue.OpenSession();
            for (var j = 0; j < 20; j++)
            {
                await session.Enqueue(BitConverter.GetBytes(j));
                await session.Flush();
            }
        }

        await using (var txLog = txLogInfo.Open(FileMode.Open))
        {
            txLog.SetLength(txLog.Length - 5); // corrupt last transaction
            txLog.Flush();
        }

        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>(),
            trimTransactionLogOnDispose: false)
        )
        {
            using var session = queue.OpenSession();
            for (var j = 20; j < 40; j++)
            {
                await session.Enqueue(BitConverter.GetBytes(j));
            }

            await session.Flush();
        }

        var data = new List<int>();
        await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<PersistentQueue>>()))
        {
            using var session = queue.OpenSession();
            var dequeue = await session.Dequeue();
            while (!dequeue.IsEmpty)
            {
                data.Add(BitConverter.ToInt32(dequeue.Span));
                dequeue = await session.Dequeue();
            }

            await session.Flush();
        }

        var expected = 0;
        foreach (var i in data)
        {
            if (expected == 19)
            {
                continue;
            }

            Assert.Equal(expected, data[i]);
            expected++;
        }
    }
}
