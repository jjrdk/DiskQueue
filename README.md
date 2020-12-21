AsyncDiskQueue
=========

A thread-safe, multi-process(ish) persistent queue, based very heavily on  http://ayende.com/blog/3479/rhino-queues-storage-disk .

This version is a fork from https://github.com/i-e-b/DiskQueue. This project brings async interactions to the queue.

Requirements and Environment
----------------------------
Works on .Net 5

Requires access to filesystem storage


Basic Usage
-----------
 - `PersistentQueue.Create(...)` is the main entry point. This will attempt to gain an exclusive lock
   on the given storage location. On first use, a directory will be created with the required files
   inside it.
 - This queue object can be shared among threads. Each thread should call `OpenSession()` to get its 
   own session object.
 - Both `IPersistentQueue`s and `IPersistentQueueSession`s should be wrapped in `using()` clauses, or otherwise
   disposed of properly.
   
Example
-------
Queue on one thread, consume on another; retry some exceptions.

**Note** this is one queue being shared between two sessions. You should not open two queue instances for one storage location at once.

```csharp
IPersistentQueue queue = await new PersistentQueue.Create("queue_a").ConfigureAwait(false);
var t1 = Task.Run(async () =>
{
	while (HaveWork())
	{
		using (var session = queue.OpenSession())
		{
			await session.Enqueue(NextWorkItem()).ConfigureAwait(false);
			await session.Flush().ConfigureAwait(false);
		}
	}
});
var t2 = Task.Run(async ()=> {
	while (true) {
		using (var session = queue.OpenSession()) {
			var data = await session.Dequeue().ConfigureAwait(false);
			if (data == null) { await Task.Delay(100).ConfigureAwait(false); continue;}
			
			try {
				MaybeDoWork(data)
				await session.Flush().ConfigureAwait(false);
			} catch (RetryException) {
				continue;
			} catch {
				await session.Flush().ConfigureAwait(false);
			}
		}
	}
});

await Task.WaitAll(t1, t2).ConfigureAwait(false);
```

Transactions
------------
Each session is a transaction. Any Enqueues or Dequeues will be rolled back when the session is disposed unless
you call `session.Flush()`. Data will only be visible between threads once it has been flushed.
Each flush incurs a performance penalty. By default, each flush is persisted to disk before continuing. You can get more speed at a safety cost by setting `queue.ParanoidFlushing = false;`

Data loss and transaction truncation
------------------------------------
By default, DiskQueue will silently discard transaction blocks that have been truncated; it will throw an `InvalidOperationException`
when transaction block markers are overwritten (this happens if more than one process is using the queue by mistake. It can also happen with some kinds of disk corruption).
If you construct your queue with `throwOnConflict: false`, all recoverable transaction errors will be silently truncated. This should only be used when uptime is more important than data consistency.

```csharp
using (var queue = await new PersistentQueue.Create(path, Constants._32Megabytes, throwOnConflict: false)) {
    . . .
}
```

Multi-Process Usage
-------------------
This scenario is strongly discouraged. However, if you must, follow the recommendations below.

Each `IPersistentQueue` gives exclusive access to the storage until it is disposed.
There is a static helper method `PersistentQueue.Create("path", TimeSpan...)` which will wait to gain access until other processes release the lock or the timeout expires.
If each process uses the lock for a short time and wait long enough, they can share a storage location.

E.g.
```csharp
...
async Task AddToQueue(byte[] data) {
	Thread.Sleep(150);
	await using (var queue = await PersistentQueue.Create(SharedStorage, TimeSpan.FromSeconds(30)).ConfigureAwait(false))
	using (var session = queue.OpenSession()) {
		await session.Enqueue(data).ConfigureAwait(false);
		await session.Flush().ConfigureAwait(false);
	}
}

byte[] ReadQueue() {
	Thread.Sleep(150);
	await using (var queue = await PersistentQueue.Create(SharedStorage, TimeSpan.FromSeconds(30)).ConfigureAwait(false))
	using (var session = queue.OpenSession()) {
		var data = await session.Dequeue().ConfigureAwait(false);
		await session.Flush().ConfigureAwait(false);
		return data;
	}
}
...

```

If you need the transaction semantics of sessions across multiple processes, try a more robust solution like https://github.com/i-e-b/SevenDigital.Messaging

