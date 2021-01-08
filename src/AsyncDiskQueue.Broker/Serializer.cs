namespace AsyncDiskQueue.Broker
{
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
                TypeNameHandling = TypeNameHandling.All,
                TypeNameAssemblyFormatHandling = TypeNameAssemblyFormatHandling.Simple,
                DefaultValueHandling = DefaultValueHandling.Ignore,
                MissingMemberHandling = MissingMemberHandling.Ignore
            };
        }

        public static byte[] Serialize(MessagePayload item)
        {
            return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(item, SerializerSettings));
        }

        public static MessagePayload Deserialize(byte[] bytes)
        {
            return JsonConvert.DeserializeObject<MessagePayload>(Encoding.UTF8.GetString(bytes), SerializerSettings);
        }

    }
}