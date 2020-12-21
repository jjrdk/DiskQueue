namespace DiskQueue.Reactive.Tests
{
    using System;
    using System.IO;

    public abstract class QueueObservableTestBase : IDisposable
    {
        protected const string Path = @"./queue_rx";
        static readonly object _lock = new();

        public void Dispose()
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
                                catch { }
                            }

                            Directory.Delete(Path, true);
                            break;
                        }
                    }
                    catch (AggregateException) { }
                    catch (UnauthorizedAccessException)
                    {
                        Console.WriteLine("Not allowed to delete queue directory. May fail later");
                    }
                }
            }

            GC.SuppressFinalize(this);
        }
    }
}