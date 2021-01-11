namespace AsyncDiskQueue.Broker.Abstractions
{
    using System;
    using System.Diagnostics;
    using System.Reflection;

    public class SubscriberInfo
    {
        private readonly int _hashCode;

        public SubscriberInfo(string endPoint)
        {
            EndPoint = endPoint;
            MachineName = Environment.MachineName;
            OperatingSystem = Environment.OSVersion.ToString();
            ClrVersion = Environment.Version.ToString(3);
            var currentProcess = Process.GetCurrentProcess();
            ProcessName = currentProcess.ProcessName;
            var assembly = Assembly.GetExecutingAssembly();
            AssemblyName = assembly.FullName;
            AssemblyVersion = assembly.GetName().Version?.ToString();
            _hashCode = HashCode.Combine(
                MachineName,
                OperatingSystem,
                ClrVersion,
                ProcessName,
                AssemblyName,
                AssemblyVersion);
        }

        public string EndPoint { get; }

        public string ProcessName { get; }

        public string ClrVersion { get; }

        public string MachineName { get; }

        public string OperatingSystem { get; }

        public string AssemblyName { get; }

        public string AssemblyVersion { get; }

        /// <inheritdoc />
        public override int GetHashCode() => _hashCode;
    }
}