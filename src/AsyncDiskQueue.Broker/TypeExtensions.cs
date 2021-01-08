namespace AsyncDiskQueue.Broker
{
    using System;
    using System.Collections.Generic;
    using System.Security.Cryptography;
    using System.Text;

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

        public static string Hash(this Type type)
        {
            var bytes = Encoding.UTF8.GetBytes(type.FullName);
            return BitConverter.ToString(SHA256.HashData(bytes)).Replace("-", string.Empty);
        }
    }
}
