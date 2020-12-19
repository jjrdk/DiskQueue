using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using DiskQueue.Implementation;
// ReSharper disable PossibleNullReferenceException
// ReSharper disable AssignNullToNotNullAttribute

namespace DiskQueue.Tests
{
    using System.Threading.Tasks;

    [TestFixture]
    public class TransactionLogTests : PersistentQueueTestsBase
    {
        [Test]
        public async Task Transaction_log_size_shrink_after_queue_disposed()
        {
            long txSizeWhenOpen;
            var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));
            using (var queue = new PersistentQueue(Path))
            {
                queue.Internals.ParanoidFlushing = false;
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        await session.Enqueue(Guid.NewGuid().ToByteArray()).ConfigureAwait(false);
                    }

                    await session.Flush().ConfigureAwait(false);
                }

                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        session.Dequeue();
                    }

                    await session.Flush().ConfigureAwait(false);
                }

                txSizeWhenOpen = txLogInfo.Length;
            }

            txLogInfo.Refresh();
            Assert.Less(txLogInfo.Length, txSizeWhenOpen);
        }

        [Test]
        public async Task Count_of_items_will_remain_fixed_after_dequeueing_without_flushing()
        {
            using (var queue = new PersistentQueue(Path))
            {
                queue.Internals.ParanoidFlushing = false;
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        await session.Enqueue(Guid.NewGuid().ToByteArray()).ConfigureAwait(false);
                    }

                    await session.Flush().ConfigureAwait(false);
                }

                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        session.Dequeue();
                    }

                    Assert.IsNull(session.Dequeue());

                    //	session.Flush(); explicitly removed
                }
            }

            using (var queue = new PersistentQueue(Path))
            {
                Assert.AreEqual(10, queue.EstimatedCountOfItemsInQueue);
            }
        }

        [Test]
        public async Task Dequeue_items_that_were_not_flushed_will_appear_after_queue_restart()
        {
            using (var queue = new PersistentQueue(Path))
            {
                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        await session.Enqueue(Guid.NewGuid().ToByteArray()).ConfigureAwait(false);
                    }

                    await session.Flush().ConfigureAwait(false);
                }

                using (var session = queue.OpenSession())
                {
                    for (int j = 0; j < 10; j++)
                    {
                        session.Dequeue();
                    }

                    Assert.IsNull(session.Dequeue());

                    //	session.Flush(); explicitly removed
                }
            }

            using (var queue = new PersistentQueue(Path))
            {
                using var session = queue.OpenSession();
                for (int j = 0; j < 10; j++)
                {
                    session.Dequeue();
                }

                Assert.IsNull(session.Dequeue());
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task If_tx_log_grows_too_large_it_will_be_trimmed_while_queue_is_in_operation()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

            using var queue = new PersistentQueue(Path)
            {
                SuggestedMaxTransactionLogSize = 32 // single entry
            };
            queue.Internals.ParanoidFlushing = false;

            using (var session = queue.OpenSession())
            {
                for (int j = 0; j < 20; j++)
                {
                    await session.Enqueue(Guid.NewGuid().ToByteArray()).ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }

            // there is no way optimize here, so we should get expected size, even though it is bigger than
            // what we suggested as the max
            txLogInfo.Refresh();
            long txSizeWhenOpen = txLogInfo.Length;

            using (var session = queue.OpenSession())
            {
                for (int j = 0; j < 20; j++)
                {
                    session.Dequeue();
                }

                Assert.IsNull(session.Dequeue());

                await session.Flush().ConfigureAwait(false);
            }

            txLogInfo.Refresh();
            Assert.Less(txLogInfo.Length, txSizeWhenOpen);
        }

        [Test]
        public async Task Truncated_transaction_is_ignored_with_default_settings()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

            using (var queue = new PersistentQueue(Path)
            {
                // aasync Task auto tx log trimming
                TrimTransactionLogOnDispose = false
            })
            {
                queue.Internals.ParanoidFlushing = false;

                using var session = queue.OpenSession();
                for (int j = 0; j < 20; j++)
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

            using (var queue = new PersistentQueue(Path))
            {
                using var session = queue.OpenSession();
                for (int j = 0; j < 19; j++)
                {
                    Assert.AreEqual(j, BitConverter.ToInt32(session.Dequeue(), 0));
                }

                Assert.IsNull(session.Dequeue()); // the last transaction was corrupted
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Can_handle_truncated_start_transaction_separator()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

            using (var queue = new PersistentQueue(Path)
            {
                // aasync Task auto tx log trimming
                TrimTransactionLogOnDispose = false
            })
            {
                using var session = queue.OpenSession();
                for (int j = 0; j < 20; j++)
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

            using (var queue = new PersistentQueue(Path))
            {
                using var session = queue.OpenSession();
                Assert.IsNull(session.Dequeue()); // the last transaction was corrupted
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Can_handle_truncated_data()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

            using (var queue = new PersistentQueue(Path)
            {
                // aasync Task auto tx log trimming
                TrimTransactionLogOnDispose = false
            })
            {
                using var session = queue.OpenSession();
                for (int j = 0; j < 20; j++)
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

            using (var queue = new PersistentQueue(Path))
            {
                using var session = queue.OpenSession();
                Assert.IsNull(session.Dequeue()); // the last transaction was corrupted
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Can_handle_truncated_end_transaction_separator()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

            using (var queue = new PersistentQueue(Path)
            {
                // aasync Task auto tx log trimming
                TrimTransactionLogOnDispose = false
            })
            {
                using var session = queue.OpenSession();
                for (int j = 0; j < 20; j++)
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

            using (var queue = new PersistentQueue(Path))
            {
                using var session = queue.OpenSession();
                Assert.IsNull(session.Dequeue()); // the last transaction was corrupted
                await session.Flush().ConfigureAwait(false);
            }
        }



        [Test]
        public async Task Can_handle_transaction_with_only_zero_length_entries()
        {
            using (var queue = new PersistentQueue(Path)
            {
                // aasync Task auto tx log trimming
                TrimTransactionLogOnDispose = false
            })
            {
                using var session = queue.OpenSession();
                for (int j = 0; j < 20; j++)
                {
                    await session.Enqueue(new byte[0]).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }
            }

            using (var queue = new PersistentQueue(Path))
            {
                using var session = queue.OpenSession();
                for (int j = 0; j < 20; j++)
                {
                    Assert.IsEmpty(session.Dequeue());
                }

                Assert.IsNull(session.Dequeue());
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Can_handle_end_separator_used_as_data()
        {
            using (var queue = new PersistentQueue(Path)
            {
                // aasync Task auto tx log trimming
                TrimTransactionLogOnDispose = false
            })
            {
                using var session = queue.OpenSession();
                for (int j = 0; j < 20; j++)
                {
                    await session.Enqueue(Constants.EndTransactionSeparator).ConfigureAwait(false); // ???
                    await session.Flush().ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }

            using (var queue = new PersistentQueue(Path))
            {
                using var session = queue.OpenSession();
                Assert.AreEqual(Constants.EndTransactionSeparator, session.Dequeue());
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Can_handle_start_separator_used_as_data()
        {
            using (var queue = new PersistentQueue(Path)
            {
                // aasync Task auto tx log trimming
                TrimTransactionLogOnDispose = false
            })
            {
                using var session = queue.OpenSession();
                for (int j = 0; j < 20; j++)
                {
                    await session.Enqueue(Constants.StartTransactionSeparator).ConfigureAwait(false); // ???
                    await session.Flush().ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }

            using (var queue = new PersistentQueue(Path))
            {
                using var session = queue.OpenSession();
                Assert.AreEqual(Constants.StartTransactionSeparator, session.Dequeue());
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Can_handle_zero_length_entries_at_start()
        {
            using (var queue = new PersistentQueue(Path)
            {
                // aasync Task auto tx log trimming
                TrimTransactionLogOnDispose = false
            })
            {
                using var session = queue.OpenSession();
                await session.Enqueue(new byte[0]).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
                for (int j = 0; j < 19; j++)
                {
                    await session.Enqueue(new byte[] {1}).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }
            }

            using (var queue = new PersistentQueue(Path))
            {
                using var session = queue.OpenSession();
                for (int j = 0; j < 20; j++)
                {
                    Assert.IsNotNull(session.Dequeue());
                    await session.Flush().ConfigureAwait(false);
                }
            }
        }


        [Test]
        public async Task Can_handle_zero_length_entries_at_end()
        {
            using (var queue = new PersistentQueue(Path)
            {
                // aasync Task auto tx log trimming
                TrimTransactionLogOnDispose = false
            })
            {
                using var session = queue.OpenSession();
                for (int j = 0; j < 19; j++)
                {
                    await session.Enqueue(new byte[] {1}).ConfigureAwait(false);
                    await session.Flush().ConfigureAwait(false);
                }

                await session.Enqueue(new byte[0]).ConfigureAwait(false);
                await session.Flush().ConfigureAwait(false);
            }

            using (var queue = new PersistentQueue(Path))
            {
                using var session = queue.OpenSession();
                for (int j = 0; j < 20; j++)
                {
                    Assert.IsNotNull(session.Dequeue());
                    await session.Flush().ConfigureAwait(false);
                }
            }
        }

        [Test]
        public async Task Can_restore_data_when_a_transaction_set_is_partially_truncated()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));
            using (var queue = new PersistentQueue(Path)
            {
                // aasync Task auto tx log trimming
                TrimTransactionLogOnDispose = false
            })
            {
                using var session = queue.OpenSession();
                for (int j = 0; j < 5; j++)
                {
                    await session.Enqueue(new byte[0]).ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }

            await using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                var buf = new byte[(int) txLog.Length];
                txLog.Read(buf, 0, (int) txLog.Length);
                txLog.Write(buf, 0, buf.Length); // a 'good' extra session
                txLog.Write(buf, 0, buf.Length / 2); // a 'bad' extra session
                txLog.Flush();
            }

            using (var queue = new PersistentQueue(Path))
            {
                using var session = queue.OpenSession();
                for (int j = 0; j < 10; j++)
                {
                    Assert.IsEmpty(session.Dequeue());
                }

                Assert.IsNull(session.Dequeue());
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task
            Can_restore_data_when_a_transaction_set_is_partially_overwritten_when_throwOnConflict_is_false()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));
            using (var queue = new PersistentQueue(Path)
            {
                // aasync Task auto tx log trimming
                TrimTransactionLogOnDispose = false
            })
            {
                using var session = queue.OpenSession();
                for (int j = 0; j < 5; j++)
                {
                    await session.Enqueue(new byte[0]).ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }

            await using (var txLog = txLogInfo.Open(FileMode.Open))
            {
                var buf = new byte[(int) txLog.Length];
                txLog.Read(buf, 0, (int) txLog.Length);
                txLog.Write(buf, 0, buf.Length - 16); // new session, but with missing end marker
                txLog.Write(Constants.StartTransactionSeparator, 0, 16);
                txLog.Flush();
            }

            using (var queue = new PersistentQueue(Path, Constants._32Megabytes, throwOnConflict: false))
            {
                using var session = queue.OpenSession();
                for (int j = 0; j < 5; j++) // first 5 should be OK
                {
                    Assert.IsNotNull(session.Dequeue());
                }

                Assert.IsNull(session.Dequeue()); // duplicated 5 should be silently lost.
                await session.Flush().ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Will_remove_truncated_transaction()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

            using (var queue = new PersistentQueue(Path))
            {
                using var session = queue.OpenSession();
                for (int j = 0; j < 20; j++)
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

            new PersistentQueue(Path).Dispose();

            txLogInfo.Refresh();

            Assert.AreEqual(36, txLogInfo.Length); //empty transaction size
        }

        [Test]
        public async Task Truncated_transaction_is_ignored_and_can_continue_to_add_items_to_queue()
        {
            var txLogInfo = new FileInfo(System.IO.Path.Combine(Path, "transaction.log"));

            using (var queue = new PersistentQueue(Path)
            {
                // aasync Task auto tx log trimming
                TrimTransactionLogOnDispose = false
            })
            {
                queue.Internals.ParanoidFlushing = false;
                using var session = queue.OpenSession();
                for (int j = 0; j < 20; j++)
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

            using (var queue = new PersistentQueue(Path)
            {
                // aasync Task auto tx log trimming
                TrimTransactionLogOnDispose = false
            })
            {
                using var session = queue.OpenSession();
                for (int j = 20; j < 40; j++)
                {
                    await session.Enqueue(BitConverter.GetBytes(j)).ConfigureAwait(false);
                }

                await session.Flush().ConfigureAwait(false);
            }

            var data = new List<int>();
            using (var queue = new PersistentQueue(Path))
            {
                using var session = queue.OpenSession();
                var dequeue = session.Dequeue();
                while (dequeue != null)
                {
                    data.Add(BitConverter.ToInt32(dequeue, 0));
                    dequeue = session.Dequeue();
                }

                await session.Flush().ConfigureAwait(false);
            }

            var expected = 0;
            foreach (var i in data)
            {
                if (expected == 19)
                    continue;
                Assert.AreEqual(expected, data[i]);
                expected++;
            }
        }
    }
}
