namespace AsyncDiskQueue.Broker
{
    using System.IO;
    using System.IO.Compression;
    using System.Text;
    using Newtonsoft.Json;

    internal static class Serializer
    {
        private static readonly JsonSerializerSettings SerializerSettings;

        static Serializer()
        {
            SerializerSettings = new JsonSerializerSettings
            {
                MetadataPropertyHandling = MetadataPropertyHandling.ReadAhead,
                TypeNameHandling = TypeNameHandling.Objects,
                TypeNameAssemblyFormatHandling = TypeNameAssemblyFormatHandling.Simple,
                DefaultValueHandling = DefaultValueHandling.Ignore,
                MissingMemberHandling = MissingMemberHandling.Ignore
            };
        }

        public static byte[] Serialize(MessagePayload item)
        {
            var bytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(item, SerializerSettings));
            using var output = new MemoryStream();
            using var gzip = new GZipStream(output, CompressionLevel.Optimal, false);
            gzip.Write(bytes);
            gzip.Flush();
            output.Flush();
            return output.ToArray();
        }

        public static MessagePayload Deserialize(byte[] bytes)
        {
            using var gzip = new GZipStream(new MemoryStream(bytes), CompressionMode.Decompress, false);
            using var output = new MemoryStream();
            gzip.CopyTo(output);
            output.Flush();

            var json = Encoding.UTF8.GetString(output.ToArray());
            return JsonConvert.DeserializeObject<MessagePayload>(json, SerializerSettings);
        }
    }
}