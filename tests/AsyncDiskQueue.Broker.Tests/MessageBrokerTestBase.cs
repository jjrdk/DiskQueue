namespace AsyncDiskQueue.Broker.Tests
{
    using System;
    using System.IO;
    using System.Threading;


    public abstract class MessageBrokerTestBase : IDisposable
    {
        protected const string Path = @"./queue_broker";
        private static readonly object Lock = new();

        protected MessageBrokerTestBase()
        {
            ClearPath();
        }

        public void Dispose()
        {
            ClearPath();

            GC.SuppressFinalize(this);
        }

        private static void ClearPath()
        {
            lock (Lock)
            {
                try
                {
                    if (!Directory.Exists(Path))
                    {
                        return;
                    }

                    var files = Directory.GetFiles(Path, "*", SearchOption.AllDirectories);
                    Array.Sort(files, (s1, s2) => s2.Length.CompareTo(s1.Length)); // sort by length descending
                    foreach (var file in files)
                    {
                        try
                        {
                            File.Delete(file);
                        }
                        catch
                        {
                        }
                    }

                    try
                    {
                        Directory.Delete(Path, true);
                    }
                    catch
                    {
                        Thread.Sleep(100);
                    }
                }
                catch (AggregateException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                    Console.WriteLine("Not allowed to delete queue directory. May fail later");
                }
            }
        }
    }
}