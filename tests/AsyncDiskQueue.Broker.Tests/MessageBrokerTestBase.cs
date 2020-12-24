namespace AsyncDiskQueue.Broker.Tests
{
    using System;
    using System.IO;
    using System.Threading;

    public abstract class MessageBrokerTestBase : IDisposable
    {
        protected const string Path = @"./queue_broker";
        static readonly object _lock = new();

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
            lock (_lock)
            {
                for (int i = 0; i < 10; i++)
                {
                    try
                    {
                        if (Directory.Exists(Path))
                        {
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
                                break;
                            }
                            catch
                            {
                                Thread.Sleep(100);
                            }
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
}