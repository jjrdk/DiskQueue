using System.Runtime.CompilerServices;
using Xunit;

[assembly: CollectionBehavior(CollectionBehavior.CollectionPerClass, DisableTestParallelization = true)]
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")]