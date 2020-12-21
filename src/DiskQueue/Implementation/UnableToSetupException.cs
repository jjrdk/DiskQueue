namespace AsyncDiskQueue.Implementation
{
    using System;

    internal class UnableToSetupException : Exception
    {
        public UnableToSetupException(string message) : base(message)
        {
        }
    }
}