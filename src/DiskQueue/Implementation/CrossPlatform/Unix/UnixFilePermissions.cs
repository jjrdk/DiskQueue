namespace AsyncDiskQueue.Implementation.CrossPlatform.Unix
{
    using System;

    /// <summary>
	/// Unix file system permission flags
	/// </summary>
	[Flags]
	public enum UnixFilePermissions : uint
	{
		/// <summary> Set-user-ID on execution </summary>
		SIsuid = 2048u,

		/// <summary> Set-group-ID on execution </summary>
		SIsgid = 1024u,

		/// <summary> On directories, restricted deletion flag </summary>
		SIsvtx = 512u,

		/// <summary> Read permission, owner </summary>
		SIrusr = 256u,

		/// <summary> Write permission, owner </summary>
		SIwusr = 128u,

		/// <summary> Execute/search permission, owner </summary>
		SIxusr = 64u,

		/// <summary> Read permission, group </summary>
		SIrgrp = 32u,

		/// <summary> Write permission, group </summary>
		SIwgrp = 16u,

		/// <summary> Execute/search permission, group </summary>
		SIxgrp = 8u,

		/// <summary> Read permission, others </summary>
		SIroth = 4u,

		/// <summary> Write permission, others </summary>
		SIwoth = 2u,

		/// <summary> Execute/search permission, others </summary>
		SIxoth = 1u,

		/// <summary> Read, write, search and execute, group </summary>
		SIrwxg = 56u,

		/// <summary> Read, write, search and execute, owner </summary>
		SIrwxu = 448u,

		/// <summary> Read, write, search and execute, others </summary>
		SIrwxo = 7u,
		
		/// <summary> Read, write, search and execute for owner, group and others </summary>
		Accessperms = 511u,
		
		/// <summary> Restrict delete, set all flags on execute, all permissions to all users </summary>
		Allperms = 4095u,
		
		/// <summary> Read and write, no execute for owner, group and others </summary>
		Deffilemode = 438u,
		
		/// <summary> Type of file flag </summary>
		SIfmt = 61440u,
		
		/// <summary> Directory type </summary>
		SIfdir = 16384u,
		
		/// <summary> Character special file type </summary>
		SIfchr = 8192u,
		
		/// <summary> Block special file type </summary>
		SIfblk = 24576u,
		
		/// <summary> Regular file type </summary>
		SIfreg = 32768u,
		
		/// <summary> FIFO special file type </summary>
		SIfifo = 4096u,
		
		/// <summary> Symbolic link file type </summary>
		SIflnk = 40960u,
		
		/// <summary> Socket file type </summary>
		SIfsock = 49152u
	}
}