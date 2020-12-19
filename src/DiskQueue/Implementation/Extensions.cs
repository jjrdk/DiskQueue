using System.Collections.Generic;

namespace DiskQueue.Implementation
{
	/// <summary>
	/// Internal extension methods
	/// </summary>
	internal static class Extensions
	{
		/// <summary>
		/// Return value for key if present, otherwise return default.
		/// No new keys or values will be added to the dictionary.
		/// </summary>
		public static T GetValueOrDefault<T, K>(this IDictionary<K, T> self, K key)
		{
			return self.TryGetValue(key, out var value) == false ? default(T) : value;
		}
	}
}