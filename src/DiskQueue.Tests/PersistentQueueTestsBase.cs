namespace DiskQueue.Tests
{
    using System;
    using System.IO;

    public abstract class PersistentQueueTestsBase : IDisposable
    {
        private const string QueuePath = @"./queue";
        private readonly object @lock = new();
        protected readonly string Path;

        protected PersistentQueueTestsBase()
        {
            Path = $"{QueuePath}_{Guid.NewGuid():N}";
            RebuildPath();
        }

        /// <summary>
        /// This ensures that we release all files before we complete a test
        /// </summary>
        public void Dispose()
        {
            GC.SuppressFinalize(this);
            RebuildPath();
        }

        private void RebuildPath()
        {
            lock (@lock)
            {
                try
                {
                    if (Directory.Exists(Path))
                    {
                        var files = Directory.GetFiles(Path, "*", SearchOption.AllDirectories);
                        Array.Sort(files, (s1, s2) => s2.Length.CompareTo(s1.Length)); // sort by length descending
                        foreach (var file in files)
                        {
                            File.Delete(file);
                        }

                        Directory.Delete(Path, true);

                    }
                }
                catch (UnauthorizedAccessException)
                {
                    Console.WriteLine("Not allowed to delete queue directory. May fail later");
                }
            }
        }
    }
}
