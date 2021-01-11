namespace AsyncDiskQueue.Broker.Abstractions
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
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

        public static IEnumerable<string> GetInheritanceNames(this Type type)
        {
            return type.GetInheritanceChain().Select(t => t.FullName);
        }

        public static string Hash(this Type type)
        {
            return type.FullName.Hash();
        }

        public static string Hash(this string input)
        {
            var bytes = Encoding.UTF8.GetBytes(input);
            var hashData = SHA256.HashData(bytes);
            return BitConverter.ToString(hashData).Replace("-", string.Empty);
        }
    }
}
