namespace DiskQueue.Reactive.Tests;

using System;
using System.Threading;

public class CrashSubscriber : IObserver<ReadOnlyMemory<byte>>
{
    private readonly ManualResetEventSlim waitHandle;
    private int count;

    public CrashSubscriber(ManualResetEventSlim waitHandle)
    {
        this.waitHandle = waitHandle;
    }

    public Exception LastError { get; private set; }

    public bool Completed { get; private set; }

    /// <inheritdoc />
    public void OnCompleted()
    {
        Completed = true;
        waitHandle.Set();
    }

    /// <inheritdoc />
    public void OnError(Exception error)
    {
        LastError = error;
        waitHandle.Set();
    }

    /// <inheritdoc />
    public void OnNext(ReadOnlyMemory<byte> value)
    {
        if (Interlocked.Increment(ref count) % 3 == 0)
        {
            waitHandle.Set();
        }
        else
        {
            throw new Exception("test crash");
        }
    }
}
