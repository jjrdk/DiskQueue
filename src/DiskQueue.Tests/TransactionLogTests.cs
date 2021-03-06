namespace DiskQueue.Tests
{
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
            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), paranoidFlushing: false).ConfigureAwait(false))
            {
                using (var session = queue.OpenSession())
                {
                    for (var j = 0; j < 10; j++)
                    {
                        await session.Enqueue(Guid.NewGuid().ToByteArray()).ConfigureAwait(false);
                    }

                    await session.Flush().ConfigureAwait(false);
                }

                using (var session = queue.OpenSession())
                {
                    for (var j = 0; j < 10; j++)
                    {
                        await session.Dequeue().ConfigureAwait(false);
                    }

                    await session.Flush().ConfigureAwait(false);
                }

                txSizeWhenOpen = txLogInfo.Length;
            }

            txLogInfo.Refresh();
            Assert.True(txLogInfo.Length < txSizeWhenOpen);
        }

        [Fact]
        public async Task Count_of_items_will_remain_fixed_after_dequeueing_without_flushing()
        {
            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), paranoidFlushing: false).ConfigureAwait(false))
            {
                using (var session = queue.OpenSession())
                {
                    for (var j = 0; j < 10; j++)
                    {
                        await session.Enqueue(Guid.NewGuid().ToByteArray()).ConfigureAwait(false);
                    }

                    await session.Flush().ConfigureAwait(false);
                }

                using (var session = queue.OpenSession())
                {
                    for (var j = 0; j < 10; j++)
                    {
                        await session.Dequeue().ConfigureAwait(false);
                    }

                    Assert.Null(await session.Dequeue().ConfigureAwait(false));

                    //	session.Flush(); explicitly removed
                }
            }

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false))
            {
                Assert.Equal(10, ((IPersistentQueueStore)queue).EstimatedCountOfItemsInQueue);
            }
        }

        [Fact]
        public async Task Dequeue_items_that_were_not_flushed_will_appear_after_queue_restart()
        {
            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false))
            {
                using (var session = queue.OpenSession())
                {
                    for (var j = 0; j < 10; j++)
                    {
                        await session.Enqueue(Guid.NewGuid().ToByteArray()).ConfigureAwait(false);
                    }

                    await session.Flush().ConfigureAwait(false);
                }

                using (var session = queue.OpenSession())
                {
                    for (var j = 0; j < 10; j++)
                    {
                        await session.Dequeue().ConfigureAwait(false);
                    }

                    Assert.Null(await session.Dequeue().ConfigureAwait(false));

                    //	session.Flush(); explicitly removed
                }
            }

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 10; j++)
                {
                    await session.Dequeue().ConfigureAwait(false);
                }

                Assert.Null(await session.Dequeue().ConfigureAwait(false));
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task If_tx_log_grows_too_large_it_will_be_trimmed_while_queue_is_in_operation()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

            await using var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), suggestedMaxTransactionLogSize: 32, paranoidFlushing: false)
                .ConfigureAwait(false);

            using (var session = queue.OpenSession())
            {
                for (var j = 0; j < 20; j++)
                {
                    await session.Enqueue(Guid.NewGuid().ToByteArray()).ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }

            // there is no way optimize here, so we should get expected size, even though it is bigger than
            // what we suggested as the max
            txLogInfo.Refresh();
            var txSizeWhenOpen = txLogInfo.Length;

            using (var session = queue.OpenSession())
            {
                for (var j = 0; j < 20; j++)
                {
                    await session.Dequeue().ConfigureAwait(false);
                }

                Assert.Null(await session.Dequeue().ConfigureAwait(false));

                await session.Flush().ConfigureAwait(false);
            }

            txLogInfo.Refresh();
            Assert.True(txLogInfo.Length < txSizeWhenOpen);
        }

        [Fact]
        public async Task Truncated_transaction_is_ignored_with_default_settings()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

            await using (var queue = await PersistentQueue
                .Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), trimTransactionLogOnDispose: false, paranoidFlushing: false)
                .ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 20; j++)
                {
                    await session.Enqueue(BitConverter.GetBytes(j)).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }
            }

            await using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                txLog.SetLength(txLog.Length - 5); // corrupt last transaction
                txLog.Flush();
            }

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 19; j++)
                {
                    Assert.Equal(j, BitConverter.ToInt32(await session.Dequeue().ConfigureAwait(false), 0));
                }

                Assert.Null(await session.Dequeue().ConfigureAwait(false)); // the last transaction was corrupted
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task Can_handle_truncated_start_transaction_separator()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), trimTransactionLogOnDispose: false)
                .ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 20; j++)
                {
                    await session.Enqueue(BitConverter.GetBytes(j)).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }
            }

            await using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                txLog.SetLength(5); // truncate log to halfway through start marker
                txLog.Flush();
            }

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                Assert.Null(await session.Dequeue().ConfigureAwait(false)); // the last transaction was corrupted
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task Can_handle_truncated_data()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), trimTransactionLogOnDispose: false)
                .ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 20; j++)
                {
                    await session.Enqueue(BitConverter.GetBytes(j)).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }
            }

            await using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                txLog.SetLength(100); // truncate log to halfway through log entry
                txLog.Flush();
            }

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                Assert.Null(await session.Dequeue().ConfigureAwait(false)); // the last transaction was corrupted
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task Can_handle_truncated_end_transaction_separator()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), trimTransactionLogOnDispose: false)
                .ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 20; j++)
                {
                    await session.Enqueue(BitConverter.GetBytes(j)).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }
            }

            await using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                txLog.SetLength(368); // truncate end transaction marker
                txLog.Flush();
            }

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                Assert.Null(await session.Dequeue().ConfigureAwait(false)); // the last transaction was corrupted
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task Can_handle_transaction_with_only_zero_length_entries()
        {
            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), trimTransactionLogOnDispose: false)
                .ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 20; j++)
                {
                    await session.Enqueue(Array.Empty<byte>()).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }
            }

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 20; j++)
                {
                    Assert.Empty(await session.Dequeue().ConfigureAwait(false));
                }

                Assert.Null(await session.Dequeue().ConfigureAwait(false));
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task Can_handle_end_separator_used_as_data()
        {
            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), trimTransactionLogOnDispose: false)
                .ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 20; j++)
                {
                    await session.Enqueue(Constants.EndTransactionSeparator.ToArray()).ConfigureAwait(false); // ???
                    await session.Flush().ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                Assert.Equal(Constants.EndTransactionSeparator.ToArray(), await session.Dequeue().ConfigureAwait(false));
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task Can_handle_start_separator_used_as_data()
        {
            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), trimTransactionLogOnDispose: false)
                .ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 20; j++)
                {
                    await session.Enqueue(Constants.StartTransactionSeparator.ToArray()).ConfigureAwait(false); // ???
                    await session.Flush().ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                Assert.Equal(Constants.StartTransactionSeparator.ToArray(), await session.Dequeue().ConfigureAwait(false));
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task Can_handle_zero_length_entries_at_start()
        {
            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), trimTransactionLogOnDispose: false)
                .ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                await session.Enqueue(Array.Empty<byte>()).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
                for (var j = 0; j < 19; j++)
                {
                    await session.Enqueue(new byte[] { 1 }).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }
            }

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 20; j++)
                {
                    Assert.NotNull(await session.Dequeue().ConfigureAwait(false));
                    await session.Flush().ConfigureAwait(false);
                }
            }
        }


        [Fact]
        public async Task Can_handle_zero_length_entries_at_end()
        {
            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), trimTransactionLogOnDispose: false).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 19; j++)
                {
                    await session.Enqueue(new byte[] { 1 }).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }

                await session.Enqueue(Array.Empty<byte>()).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 20; j++)
                {
                    Assert.NotNull(await session.Dequeue().ConfigureAwait(false));
                    await session.Flush().ConfigureAwait(false);
                }
            }
        }

        [Fact]
        public async Task Can_restore_data_when_a_transaction_set_is_partially_truncated()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));
            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), trimTransactionLogOnDispose: false)
                .ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 5; j++)
                {
                    await session.Enqueue(Array.Empty<byte>()).ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }

            await using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                var buf = new byte[(int)txLog.Length];
                txLog.Read(buf, 0, (int)txLog.Length);
                txLog.Write(buf, 0, buf.Length); // a 'good' extra session
                txLog.Write(buf, 0, buf.Length / 2); // a 'bad' extra session
                txLog.Flush();
            }

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 10; j++)
                {
                    Assert.Empty(await session.Dequeue().ConfigureAwait(false));
                }

                Assert.Null(await session.Dequeue().ConfigureAwait(false));
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task
            Can_restore_data_when_a_transaction_set_is_partially_overwritten_when_throwOnConflict_is_false()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));
            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), trimTransactionLogOnDispose: false)
                .ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 5; j++)
                {
                    await session.Enqueue(Array.Empty<byte>()).ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }

            await using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                var buf = new byte[(int)txLog.Length];
                txLog.Read(buf, 0, (int)txLog.Length);
                txLog.Write(buf, 0, buf.Length - 16); // new session, but with missing end marker
                txLog.Write(Constants.StartTransactionSeparator.Span);
                txLog.Flush();
            }

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), maxFileSize: Constants._32Megabytes, throwOnConflict: false)
                .ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 5; j++) // first 5 should be OK
                {
                    Assert.NotNull(await session.Dequeue().ConfigureAwait(false));
                }

                Assert.Null(await session.Dequeue().ConfigureAwait(false)); // duplicated 5 should be silently lost.
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task Will_remove_truncated_transaction()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 20; j++)
                {
                    await session.Enqueue(BitConverter.GetBytes(j)).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }
            }

            await using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                txLog.SetLength(5); // corrupt all transactions
                txLog.Flush();
            }

            var q = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false);
            await q.DisposeAsync().ConfigureAwait(false);

            txLogInfo.Refresh();

            Assert.Equal(36, txLogInfo.Length); //empty transaction size
        }

        [Fact]
        public async Task Truncated_transaction_is_ignored_and_can_continue_to_add_items_to_queue()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), trimTransactionLogOnDispose: false, paranoidFlushing: false)
                .ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 0; j < 20; j++)
                {
                    await session.Enqueue(BitConverter.GetBytes(j)).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }
            }

            await using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                txLog.SetLength(txLog.Length - 5); // corrupt last transaction
                txLog.Flush();
            }

            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>(), trimTransactionLogOnDispose: false)
                .ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                for (var j = 20; j < 40; j++)
                {
                    await session.Enqueue(BitConverter.GetBytes(j)).ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }

            var data = new List<int>();
            await using (var queue = await PersistentQueue.Create(Path, Substitute.For<ILogger<IPersistentQueue>>()).ConfigureAwait(false))
            {
                using var session = queue.OpenSession();
                var dequeue = await session.Dequeue().ConfigureAwait(false);
                while (dequeue != null)
                {
                    data.Add(BitConverter.ToInt32(dequeue, 0));
                    dequeue = await session.Dequeue().ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
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
}
