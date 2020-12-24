namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Collections.Generic;
    using System.Net.Sockets;

    public static class TypeExtensions
    {
        public static IEnumerable<Type> GetInheritanceChain(this Type type)
        {
            while (true)
            {
                yield return type;

                if (type.BaseType == null)
                {
                    yield break;
                }

                type = type.BaseType;
            }
        }
    }
}
